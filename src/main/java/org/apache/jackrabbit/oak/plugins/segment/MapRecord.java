/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.lang.Integer.bitCount;
import static java.lang.Integer.highestOneBit;
import static java.lang.Integer.numberOfTrailingZeros;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

class MapRecord extends Record {

    private static final int M = 0xDEECE66D;
    private static final int A = 0xB;
    static final long HASH_MASK = 0xFFFFFFFFL;

    static int getHash(String name) {
        return (name.hashCode() ^ M) * M + A;
    }

    /**
     * Number of bits of the hash code to look at on each level of the trie.
     */
    protected static final int BITS_PER_LEVEL = 5;

    /**
     * Number of buckets at each level of the trie.
     */
    protected static final int BUCKETS_PER_LEVEL = 1 << BITS_PER_LEVEL; // 32

    /**
     * Maximum number of trie levels.
     */
    protected static final int MAX_NUMBER_OF_LEVELS =
            (32 + BITS_PER_LEVEL - 1) / BITS_PER_LEVEL; // 7

    /**
     * Number of bits needed to indicate the current trie level.
     */
    protected static final int LEVEL_BITS = // 4, using nextPowerOfTwo():
            numberOfTrailingZeros(highestOneBit(MAX_NUMBER_OF_LEVELS) << 1);

    /**
     * Number of bits used to indicate the size of a map.
     */
    protected static final int SIZE_BITS = 32 - LEVEL_BITS;

    /**
     * Maximum size of a map.
     */
    protected static final int MAX_SIZE = (1 << SIZE_BITS) - 1; // ~268e6

    protected MapRecord(Segment segment, RecordId id) {
        super(segment, id);
    }

    boolean isLeaf() {
        Segment segment = getSegment();
        int head = segment.readInt(getOffset(0));
        if (isDiff(head)) {
            RecordId base = segment.readRecordId(getOffset(8, 2));
            return new MapRecord(segment, base).isLeaf();
        }
        return !isBranch(head);
    }

    public boolean isDiff() {
        return isDiff(getSegment().readInt(getOffset(0)));
    }

    MapRecord[] getBuckets() {
        Segment segment = getSegment();
        MapRecord[] buckets = new MapRecord[BUCKETS_PER_LEVEL];
        int bitmap = segment.readInt(getOffset(4));
        int ids = 0;
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            if ((bitmap & (1 << i)) != 0) {
                buckets[i] = new MapRecord(
                        segment, segment.readRecordId(getOffset(8, ids++)));
            } else {
                buckets[i] = null;
            }
        }
        return buckets;
    }

    private List<MapRecord> getBucketList(Segment segment) {
        List<MapRecord> buckets = newArrayListWithCapacity(BUCKETS_PER_LEVEL);
        int bitmap = segment.readInt(getOffset(4));
        int ids = 0;
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            if ((bitmap & (1 << i)) != 0) {
                RecordId id = segment.readRecordId(getOffset(8, ids++));
                buckets.add(new MapRecord(segment, id));
            }
        }
        return buckets;
    }

    int size() {
        Segment segment = getSegment();
        int head = segment.readInt(getOffset(0));
        if (isDiff(head)) {
            RecordId base = segment.readRecordId(getOffset(8, 2));
            return new MapRecord(segment, base).size();
        }
        return getSize(head);
    }

    MapEntry getEntry(String name) {
        checkNotNull(name);
        int hash = getHash(name);
        Segment segment = getSegment();

        int head = segment.readInt(getOffset(0));
        if (isDiff(head)) {
            if (hash == segment.readInt(getOffset(4))) {
                RecordId key = segment.readRecordId(getOffset(8));
                if (name.equals(segment.readString(key))) {
                    RecordId value = segment.readRecordId(getOffset(8, 1));
                    return new MapEntry(segment, name, key, value);
                }
            }
            RecordId base = segment.readRecordId(getOffset(8, 2));
            return new MapRecord(segment, base).getEntry(name);
        }

        int size = getSize(head);
        if (size == 0) {
            return null; // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            // this is an intermediate branch record
            // check if a matching bucket exists, and recurse 
            int bitmap = segment.readInt(getOffset(4));
            int mask = (1 << BITS_PER_LEVEL) - 1;
            int shift = 32 - (level + 1) * BITS_PER_LEVEL;
            int index = (hash >> shift) & mask;
            int bit = 1 << index;
            if ((bitmap & bit) != 0) {
                int ids = bitCount(bitmap & (bit - 1));
                RecordId id = segment.readRecordId(getOffset(8, ids));
                return new MapRecord(segment, id).getEntry(name);
            } else {
                return null;
            }
        }

        // use interpolation search to find the matching entry in this map leaf
        int shift = 32 - level * BITS_PER_LEVEL;
        long mask = -1L << shift;
        long h = hash & HASH_MASK;
        int p = 0;
        long pH = h & mask;   // lower bound on hash values in this map leaf
        int q = size - 1;
        long qH = pH | ~mask; // upper bound on hash values in this map leaf
        while (p <= q) {
            assert pH <= qH;

            // interpolate the most likely index of the target entry
            // based on its hash code and the lower and upper bounds
            int i = p + (int) ((q - p) * (h - pH) / (qH - pH));
            assert p <= i && i <= q;

            long iH = segment.readInt(getOffset(4 + i * 4)) & HASH_MASK;
            int diff = Long.valueOf(iH).compareTo(Long.valueOf(h));
            if (diff == 0) {
                RecordId keyId = segment.readRecordId(
                        getOffset(4 + size * 4, i * 2));
                RecordId valueId = segment.readRecordId(
                        getOffset(4 + size * 4, i * 2 + 1));
                diff = segment.readString(keyId).compareTo(name);
                if (diff == 0) {
                    return new MapEntry(segment, name, keyId, valueId);
                }
            }

            if (diff < 0) {
                p = i + 1;
                pH = iH;
            } else {
                q = i - 1;
                qH = iH;
            }
        }
        return null;
    }

    private RecordId getValue(int hash, RecordId key) {
        checkNotNull(key);
        Segment segment = getSegment();

        int head = segment.readInt(getOffset(0));
        if (isDiff(head)) {
            if (hash == segment.readInt(getOffset(4))
                    && key.equals(segment.readRecordId(getOffset(8)))) {
                return segment.readRecordId(getOffset(8, 1));
            }
            RecordId base = segment.readRecordId(getOffset(8, 2));
            return new MapRecord(segment, base).getValue(hash, key);
        }

        int size = getSize(head);
        if (size == 0) {
            return null; // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            // this is an intermediate branch record
            // check if a matching bucket exists, and recurse
            int bitmap = segment.readInt(getOffset(4));
            int mask = (1 << BITS_PER_LEVEL) - 1;
            int shift = 32 - (level + 1) * BITS_PER_LEVEL;
            int index = (hash >> shift) & mask;
            int bit = 1 << index;
            if ((bitmap & bit) != 0) {
                int ids = bitCount(bitmap & (bit - 1));
                RecordId id = segment.readRecordId(getOffset(8, ids));
                return new MapRecord(segment, id).getValue(hash, key);
            } else {
                return null;
            }
        }

        // this is a leaf record; scan the list to find a matching entry
        Long h = hash & HASH_MASK;
        for (int i = 0; i < size; i++) {
            int hashOffset = getOffset(4 + i * 4);
            int diff = h.compareTo(segment.readInt(hashOffset) & HASH_MASK);
            if (diff > 0) {
                return null;
            } else if (diff == 0) {
                int keyOffset = getOffset(4 + size * 4, i * 2);
                if (key.equals(segment.readRecordId(keyOffset))) {
                    int valueOffset = getOffset(4 + size * 4, i * 2 + 1);
                    return segment.readRecordId(valueOffset);
                }
            }
        }
        return null;
    }

    Iterable<String> getKeys() {
        Segment segment = getSegment();

        int head = segment.readInt(getOffset(0));
        if (isDiff(head)) {
            RecordId base = segment.readRecordId(getOffset(8, 2));
            return new MapRecord(segment, base).getKeys();
        }

        int size = getSize(head);
        if (size == 0) {
            return Collections.emptyList(); // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            List<MapRecord> buckets = getBucketList(segment);
            List<Iterable<String>> keys =
                    newArrayListWithCapacity(buckets.size());
            for (MapRecord bucket : buckets) {
                keys.add(bucket.getKeys());
            }
            return concat(keys);
        }

        RecordId[] ids = new RecordId[size];
        for (int i = 0; i < size; i++) {
            ids[i] = segment.readRecordId(getOffset(4 + size * 4, i * 2));
        }

        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = segment.readString(ids[i]);
        }
        return Arrays.asList(keys);
    }

    Iterable<MapEntry> getEntries() {
        return getEntries(null, null);
    }

    private Iterable<MapEntry> getEntries(
            RecordId diffKey, RecordId diffValue) {
        Segment segment = getSegment();

        int head = segment.readInt(getOffset(0));
        if (isDiff(head)) {
            RecordId key = segment.readRecordId(getOffset(8));
            RecordId value = segment.readRecordId(getOffset(8, 1));
            RecordId base = segment.readRecordId(getOffset(8, 2));
            return new MapRecord(segment, base).getEntries(key, value);
        }

        int size = getSize(head);
        if (size == 0) {
            return Collections.emptyList(); // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            List<MapRecord> buckets = getBucketList(segment);
            List<Iterable<MapEntry>> entries =
                    newArrayListWithCapacity(buckets.size());
            for (MapRecord bucket : buckets) {
                entries.add(bucket.getEntries(diffKey, diffValue));
            }
            return concat(entries);
        }

        RecordId[] keys = new RecordId[size];
        RecordId[] values = new RecordId[size];
        for (int i = 0; i < size; i++) {
            keys[i] = segment.readRecordId(getOffset(4 + size * 4, i * 2));
            if (keys[i].equals(diffKey)) {
                values[i] = diffValue;
            } else {
                values[i] = segment.readRecordId(getOffset(4 + size * 4, i * 2 + 1));
            }
        }

        MapEntry[] entries = new MapEntry[size];
        for (int i = 0; i < size; i++) {
            String name = segment.readString(keys[i]);
            entries[i] = new MapEntry(segment, name, keys[i], values[i]);
        }
        return Arrays.asList(entries);
    }

    boolean compare(MapRecord before, NodeStateDiff diff) {
        Segment beforeSegment = before.getSegment();
        int beforeHead = beforeSegment.readInt(before.getOffset(0));

        MapRecord after = this;
        Segment afterSegment = after.getSegment();
        int afterHead = afterSegment.readInt(after.getOffset(0));

        if (isDiff(afterHead)) {
            RecordId base = afterSegment.readRecordId(after.getOffset(8, 2));
            if (base.equals(after.getRecordId())) {
                int hash = afterSegment.readInt(after.getOffset(4));
                RecordId key = afterSegment.readRecordId(after.getOffset(8));
                RecordId afterValue = afterSegment.readRecordId(after.getOffset(8, 1));
                RecordId beforeValue = before.getValue(hash, key);
                String name = beforeSegment.readString(key);
                return diff.childNodeChanged(
                        name,
                        new SegmentNodeState(beforeSegment, beforeValue),
                        new SegmentNodeState(afterSegment, afterValue));
            } else if (isDiff(beforeHead)) {
                RecordId beforeBase =
                        beforeSegment.readRecordId(before.getOffset(8, 2));
                if (base.equals(beforeBase)) {
                    int beforeHash = beforeSegment.readInt(before.getOffset(4));
                    RecordId beforeKey = beforeSegment.readRecordId(before.getOffset(8));
                    RecordId beforeValue = beforeSegment.readRecordId(before.getOffset(8, 1));

                    int afterHash = afterSegment.readInt(after.getOffset(4));
                    RecordId afterKey = afterSegment.readRecordId(after.getOffset(8));
                    RecordId afterValue = afterSegment.readRecordId(after.getOffset(8, 1));

                    if (beforeKey.equals(afterKey)) {
                        String name = beforeSegment.readString(beforeKey);
                        return diff.childNodeChanged(
                                name,
                                new SegmentNodeState(beforeSegment, beforeValue),
                                new SegmentNodeState(afterSegment, afterValue));
                    } else {
                        String beforeName = beforeSegment.readString(beforeKey);
                        String afterName = afterSegment.readString(afterKey);
                        return diff.childNodeChanged(
                                beforeName,
                                new SegmentNodeState(beforeSegment, beforeValue),
                                new SegmentNodeState(afterSegment, after.getValue(beforeHash, beforeKey)))
                               &&
                               diff.childNodeChanged(
                                afterName,
                                new SegmentNodeState(beforeSegment, before.getValue(afterHash, afterKey)),
                                new SegmentNodeState(afterSegment, afterValue));
                    }
                }
            }
        } else if (isBranch(beforeHead) && isBranch(afterHead)) {
            return compareBranch(before, after, diff);
        }

        Iterator<MapEntry> beforeEntries = before.getEntries().iterator();
        Iterator<MapEntry> afterEntries = after.getEntries().iterator();

        MapEntry beforeEntry = nextOrNull(beforeEntries);
        MapEntry afterEntry = nextOrNull(afterEntries);
        while (beforeEntry != null || afterEntry != null) {
            int d = compare(beforeEntry, afterEntry);
            if (d < 0) {
                if (!diff.childNodeDeleted(
                        beforeEntry.getName(), beforeEntry.getNodeState())) {
                    return false;
                }
                beforeEntry = nextOrNull(beforeEntries);
            } else if (d == 0) {
                if (!beforeEntry.getValue().equals(afterEntry.getValue())
                        && !diff.childNodeChanged(
                                beforeEntry.getName(),
                                beforeEntry.getNodeState(),
                                afterEntry.getNodeState())) {
                    return false;
                }
                beforeEntry = nextOrNull(beforeEntries);
                afterEntry = nextOrNull(afterEntries);
            } else {
                if (!diff.childNodeAdded(
                        afterEntry.getName(), afterEntry.getNodeState())) {
                    return false;
                }
                afterEntry = nextOrNull(afterEntries);
            }
        }

        return true;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringBuilder builder = null;
        for (MapEntry entry : getEntries()) {
            if (builder == null) {
                builder = new StringBuilder("{ ");
            } else {
                builder.append(", ");
            }
            builder.append(entry);
        }
        if (builder == null) {
            return "{}";
        } else {
            builder.append(" }");
            return builder.toString();
        }
    }

    //-----------------------------------------------------------< private >--

    /**
     * Compares two map branches. Given the way the comparison algorithm
     * works, the branches are always guaranteed to be at the same level
     * with the same hash prefixes.
     */
    private static boolean compareBranch(
            MapRecord before, MapRecord after, NodeStateDiff diff) {
        MapRecord[] beforeBuckets = before.getBuckets();
        MapRecord[] afterBuckets = after.getBuckets();
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            if (Objects.equal(beforeBuckets[i], afterBuckets[i])) {
                // these buckets are equal (or both empty), so no changes
            } else if (beforeBuckets[i] == null) {
                // before bucket is empty, so all after entries were added
                MapRecord bucket = afterBuckets[i];
                for (MapEntry entry : bucket.getEntries()) {
                    if (!diff.childNodeAdded(
                            entry.getName(), entry.getNodeState())) {
                        return false;
                    }
                }
            } else if (afterBuckets[i] == null) {
                // after bucket is empty, so all before entries were deleted
                MapRecord bucket = beforeBuckets[i];
                for (MapEntry entry : bucket.getEntries()) {
                    if (!diff.childNodeDeleted(
                            entry.getName(), entry.getNodeState())) {
                        return false;
                    }
                }
            } else {
                // both before and after buckets exist; compare recursively
                MapRecord beforeBucket = beforeBuckets[i];
                MapRecord afterBucket = afterBuckets[i];
                if (!afterBucket.compare(beforeBucket, diff)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static int getSize(int head) {
        return head & ((1 << MapRecord.SIZE_BITS) - 1);
    }

    private static int getLevel(int head) {
        return head >>> MapRecord.SIZE_BITS;
    }

    private static boolean isDiff(int head) {
        return head == -1;
    }

    private static boolean isBranch(int head) {
        return isBranch(getSize(head), getLevel(head));
    }

    private static boolean isBranch(int size, int level) {
        return size > MapRecord.BUCKETS_PER_LEVEL
                && level < MapRecord.MAX_NUMBER_OF_LEVELS;
    }

    private static int compare(MapEntry before, MapEntry after) {
        if (before == null) {
            // A null value signifies the end of the list of entries,
            // which is why the return value here is a bit counter-intuitive
            // (null > non-null). The idea is to make a virtual end-of-list
            // sentinel value appear greater than any normal value.
            return 1;
        } else if (after == null) {
            return -1;  // see above
        } else {
            return ComparisonChain.start()
                    .compare(before.getHash() & HASH_MASK, after.getHash() & HASH_MASK)
                    .compare(before.getName(), after.getName())
                    .result();
        }
    }

    private static MapEntry nextOrNull(Iterator<MapEntry> iterator) {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return null;
        }
    }

}

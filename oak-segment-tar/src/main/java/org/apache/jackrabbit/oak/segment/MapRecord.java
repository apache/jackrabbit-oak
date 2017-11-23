/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.lang.Integer.bitCount;
import static java.lang.Integer.highestOneBit;
import static java.lang.Integer.numberOfTrailingZeros;
import static org.apache.jackrabbit.oak.segment.MapEntry.newMapEntry;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * A map. The top level record is either a record of type "BRANCH" or "LEAF"
 * (depending on the data).
 */
public class MapRecord extends Record {

    /**
     * Magic constant from a random number generator, used to generate
     * good hash values.
     */
    private static final int M = 0xDEECE66D;
    private static final int A = 0xB;
    static final long HASH_MASK = 0xFFFFFFFFL;

    @Nonnull
    private final SegmentReader reader;

    /**
     * Generates a hash code for the value, using a random number generator
     * to improve the distribution of the hash values.
     */
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
     * Currently 4.
     */
    protected static final int LEVEL_BITS = // 4, using nextPowerOfTwo():
            numberOfTrailingZeros(highestOneBit(MAX_NUMBER_OF_LEVELS) << 1);

    /**
     * Number of bits used to indicate the size of a map.
     * Currently 28.
     */
    protected static final int SIZE_BITS = 32 - LEVEL_BITS;

    /**
     * Maximum size of a map.
     */
    protected static final int MAX_SIZE = (1 << SIZE_BITS) - 1; // ~268e6

    MapRecord(@Nonnull SegmentReader reader, @Nonnull RecordId id) {
        super(id);
        this.reader = checkNotNull(reader);
    }

    boolean isLeaf() {
        Segment segment = getSegment();
        int head = segment.readInt(getRecordNumber());
        if (isDiff(head)) {
            RecordId base = segment.readRecordId(getRecordNumber(), 8, 2);
            return reader.readMap(base).isLeaf();
        }
        return !isBranch(head);
    }

    public boolean isDiff() {
        return isDiff(getSegment().readInt(getRecordNumber()));
    }

    MapRecord[] getBuckets() {
        Segment segment = getSegment();
        MapRecord[] buckets = new MapRecord[BUCKETS_PER_LEVEL];
        int bitmap = segment.readInt(getRecordNumber(), 4);
        int ids = 0;
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            if ((bitmap & (1 << i)) != 0) {
                buckets[i] = reader.readMap(segment.readRecordId(getRecordNumber(), 8, ids++));
            } else {
                buckets[i] = null;
            }
        }
        return buckets;
    }

    private List<MapRecord> getBucketList(Segment segment) {
        List<MapRecord> buckets = newArrayListWithCapacity(BUCKETS_PER_LEVEL);
        int bitmap = segment.readInt(getRecordNumber(), 4);
        int ids = 0;
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            if ((bitmap & (1 << i)) != 0) {
                RecordId id = segment.readRecordId(getRecordNumber(), 8, ids++);
                buckets.add(reader.readMap(id));
            }
        }
        return buckets;
    }

    int size() {
        Segment segment = getSegment();
        int head = segment.readInt(getRecordNumber());
        if (isDiff(head)) {
            RecordId base = segment.readRecordId(getRecordNumber(), 8, 2);
            return reader.readMap(base).size();
        }
        return getSize(head);
    }

    MapEntry getEntry(String name) {
        checkNotNull(name);
        int hash = getHash(name);
        Segment segment = getSegment();

        int head = segment.readInt(getRecordNumber());
        if (isDiff(head)) {
            if (hash == segment.readInt(getRecordNumber(), 4)) {
                RecordId key = segment.readRecordId(getRecordNumber(), 8);
                if (name.equals(reader.readString(key))) {
                    RecordId value = segment.readRecordId(getRecordNumber(), 8, 1);
                    return newMapEntry(reader, name, key, value);
                }
            }
            RecordId base = segment.readRecordId(getRecordNumber(), 8, 2);
            return reader.readMap(base).getEntry(name);
        }

        int size = getSize(head);
        if (size == 0) {
            return null; // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            // this is an intermediate branch record
            // check if a matching bucket exists, and recurse 
            int bitmap = segment.readInt(getRecordNumber(), 4);
            int mask = (1 << BITS_PER_LEVEL) - 1;
            int shift = 32 - (level + 1) * BITS_PER_LEVEL;
            int index = (hash >> shift) & mask;
            int bit = 1 << index;
            if ((bitmap & bit) != 0) {
                int ids = bitCount(bitmap & (bit - 1));
                RecordId id = segment.readRecordId(getRecordNumber(), 8, ids);
                return reader.readMap(id).getEntry(name);
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

            long iH = segment.readInt(getRecordNumber(), 4 + i * 4) & HASH_MASK;
            int diff = Long.valueOf(iH).compareTo(h);
            if (diff == 0) {
                RecordId keyId = segment.readRecordId(getRecordNumber(), 4 + size * 4, i * 2);
                RecordId valueId = segment.readRecordId(getRecordNumber(), 4 + size * 4, i * 2 + 1);
                diff = reader.readString(keyId).compareTo(name);
                if (diff == 0) {
                    return newMapEntry(reader, name, keyId, valueId);
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

        int head = segment.readInt(getRecordNumber());
        if (isDiff(head)) {
            if (hash == segment.readInt(getRecordNumber(), 4)
                    && key.equals(segment.readRecordId(getRecordNumber(), 8))) {
                return segment.readRecordId(getRecordNumber(), 8, 1);
            }
            RecordId base = segment.readRecordId(getRecordNumber(), 8, 2);
            return reader.readMap(base).getValue(hash, key);
        }

        int size = getSize(head);
        if (size == 0) {
            return null; // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            // this is an intermediate branch record
            // check if a matching bucket exists, and recurse
            int bitmap = segment.readInt(getRecordNumber(), 4);
            int mask = (1 << BITS_PER_LEVEL) - 1;
            int shift = 32 - (level + 1) * BITS_PER_LEVEL;
            int index = (hash >> shift) & mask;
            int bit = 1 << index;
            if ((bitmap & bit) != 0) {
                int ids = bitCount(bitmap & (bit - 1));
                RecordId id = segment.readRecordId(getRecordNumber(), 8, ids);
                return reader.readMap(id).getValue(hash, key);
            } else {
                return null;
            }
        }

        // this is a leaf record; scan the list to find a matching entry
        Long h = hash & HASH_MASK;
        for (int i = 0; i < size; i++) {
            int diff = h.compareTo(segment.readInt(getRecordNumber(), 4 + i * 4) & HASH_MASK);
            if (diff > 0) {
                return null;
            } else if (diff == 0) {
                if (key.equals(segment.readRecordId(getRecordNumber(), 4 + size * 4, i * 2))) {
                    return segment.readRecordId(getRecordNumber(), 4 + size * 4, i * 2 + 1);
                }
            }
        }
        return null;
    }

    Iterable<String> getKeys() {
        Segment segment = getSegment();

        int head = segment.readInt(getRecordNumber());
        if (isDiff(head)) {
            RecordId base = segment.readRecordId(getRecordNumber(), 8, 2);
            return reader.readMap(base).getKeys();
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
            for (final MapRecord bucket : buckets) {
                keys.add(new Iterable<String>() {
                    @Nonnull
                    @Override
                    public Iterator<String> iterator() {
                        return bucket.getKeys().iterator();
                    }
                });
            }
            return concat(keys);
        }

        RecordId[] ids = new RecordId[size];
        for (int i = 0; i < size; i++) {
            ids[i] = segment.readRecordId(getRecordNumber(), 4 + size * 4, i * 2);
        }

        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = reader.readString(ids[i]);
        }
        return Arrays.asList(keys);
    }

    Iterable<MapEntry> getEntries() {
        return getEntries(null, null);
    }

    private Iterable<MapEntry> getEntries(
            final RecordId diffKey, final RecordId diffValue) {
        Segment segment = getSegment();

        int head = segment.readInt(getRecordNumber());
        if (isDiff(head)) {
            RecordId key = segment.readRecordId(getRecordNumber(), 8);
            RecordId value = segment.readRecordId(getRecordNumber(), 8, 1);
            RecordId base = segment.readRecordId(getRecordNumber(), 8, 2);
            return reader.readMap(base).getEntries(key, value);
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
            for (final MapRecord bucket : buckets) {
                entries.add(new Iterable<MapEntry>() {
                    @Nonnull
                    @Override
                    public Iterator<MapEntry> iterator() {
                        return bucket.getEntries(diffKey, diffValue).iterator();
                    }
                });
            }
            return concat(entries);
        }

        MapEntry[] entries = new MapEntry[size];
        for (int i = 0; i < size; i++) {
            RecordId key = segment.readRecordId(getRecordNumber(), 4 + size * 4, i * 2);
            RecordId value;
            if (key.equals(diffKey)) {
                value = diffValue;
            } else {
                value = segment.readRecordId(getRecordNumber(), 4 + size * 4, i * 2 + 1);
            }
            String name = reader.readString(key);
            entries[i] = newMapEntry(reader, name, key, value);
        }
        return Arrays.asList(entries);
    }

    boolean compare(MapRecord before, final NodeStateDiff diff) {
        if (Record.fastEquals(this, before)) {
            return true;
        }

        Segment segment = getSegment();
        int head = segment.readInt(getRecordNumber());
        if (isDiff(head)) {
            int hash = segment.readInt(getRecordNumber(), 4);
            RecordId keyId = segment.readRecordId(getRecordNumber(), 8);
            final String key = reader.readString(keyId);
            final RecordId value = segment.readRecordId(getRecordNumber(), 8, 1);
            MapRecord base = reader.readMap(segment.readRecordId(getRecordNumber(), 8, 2));

            boolean rv = base.compare(before, new DefaultNodeStateDiff() {
                @Override
                public boolean childNodeAdded(String name, NodeState after) {
                    return name.equals(key)
                            || diff.childNodeAdded(name, after);
                }
                @Override
                public boolean childNodeChanged(
                        String name, NodeState before, NodeState after) {
                    return name.equals(key)
                            || diff.childNodeChanged(name, before, after);
                }
                @Override
                public boolean childNodeDeleted(String name, NodeState before) {
                    return diff.childNodeDeleted(name, before);
                }
            });
            if (rv) {
                MapEntry beforeEntry = before.getEntry(key);
                if (beforeEntry == null) {
                    rv = diff.childNodeAdded(
                            key,
                            reader.readNode(value));
                } else if (!value.equals(beforeEntry.getValue())) {
                    rv = diff.childNodeChanged(
                            key,
                            beforeEntry.getNodeState(),
                            reader.readNode(value));
                }
            }
            return rv;
        }

        Segment beforeSegment = before.getSegment();
        int beforeHead = beforeSegment.readInt(before.getRecordNumber());
        if (isDiff(beforeHead)) {
            int hash = beforeSegment.readInt(before.getRecordNumber(), 4);
            RecordId keyId = beforeSegment.readRecordId(before.getRecordNumber(), 8);
            final String key = reader.readString(keyId);
            final RecordId value = beforeSegment.readRecordId(before.getRecordNumber(), 8, 1);
            MapRecord base = reader.readMap(beforeSegment.readRecordId(before.getRecordNumber(), 8, 2));

            boolean rv = this.compare(base, new DefaultNodeStateDiff() {
                @Override
                public boolean childNodeAdded(String name, NodeState after) {
                    return diff.childNodeAdded(name, after);
                }
                @Override
                public boolean childNodeChanged(
                        String name, NodeState before, NodeState after) {
                    return name.equals(key)
                            || diff.childNodeChanged(name, before, after);
                }
                @Override
                public boolean childNodeDeleted(String name, NodeState before) {
                    return name.equals(key)
                            || diff.childNodeDeleted(name, before);
                }
            });
            if (rv) {
                MapEntry afterEntry = this.getEntry(key);
                if (afterEntry == null) {
                    rv = diff.childNodeDeleted(
                            key,
                            reader.readNode(value));
                } else if (!value.equals(afterEntry.getValue())) {
                    rv = diff.childNodeChanged(
                            key,
                            reader.readNode(value),
                            afterEntry.getNodeState());
                }
            }
            return rv;
        }

        if (isBranch(beforeHead) && isBranch(head)) {
            return compareBranch(before, this, diff);
        }

        Iterator<MapEntry> beforeEntries = before.getEntries().iterator();
        Iterator<MapEntry> afterEntries = this.getEntries().iterator();

        MapEntry beforeEntry = nextOrNull(beforeEntries);
        MapEntry afterEntry = nextOrNull(afterEntries);
        while (beforeEntry != null || afterEntry != null) {
            int d = compare(beforeEntry, afterEntry);
            if (d < 0) {
                assert beforeEntry != null;
                if (!diff.childNodeDeleted(
                        beforeEntry.getName(), beforeEntry.getNodeState())) {
                    return false;
                }
                beforeEntry = nextOrNull(beforeEntries);
            } else if (d == 0) {
                assert beforeEntry != null;
                assert afterEntry != null;
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
                assert afterEntry != null;
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

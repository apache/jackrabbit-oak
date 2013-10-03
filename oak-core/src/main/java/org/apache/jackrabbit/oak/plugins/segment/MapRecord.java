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
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.lang.Integer.bitCount;
import static java.lang.Integer.highestOneBit;
import static java.lang.Integer.numberOfTrailingZeros;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

class MapRecord extends Record {

    private static final long M = 0x5DEECE66DL;
    private static final long A = 0xBL;

    static int getHash(String name) {
        return (int) (((name.hashCode() ^ M) * M + A) >> 16);
    }

    private static final Function<MapRecord, Iterable<String>> GET_KEYS =
            new Function<MapRecord, Iterable<String>>() {
                @Override
                public Iterable<String> apply(MapRecord input) {
                    return input.getKeys();
                }
            };

    private static final Function<MapRecord, Iterable<MapEntry>> GET_ENTRIES =
            new Function<MapRecord, Iterable<MapEntry>>() {
                @Override
                public Iterable<MapEntry> apply(MapRecord input) {
                    return input.getEntries();
                }
            };

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
        int head = getSegment().readInt(getOffset(0));
        return !isBranch(getSize(head), getLevel(head));
    }

    RecordId[] getBuckets() {
        return getBuckets(getSegment());
    }

    private RecordId[] getBuckets(Segment segment) {
        RecordId[] buckets = new RecordId[BUCKETS_PER_LEVEL];
        int bitmap = segment.readInt(getOffset(4));
        int ids = 0;
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            if ((bitmap & (1 << i)) != 0) {
                buckets[i] = segment.readRecordId(getOffset(8, ids++));
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
        int head = getSegment().readInt(getOffset(0));
        return getSize(head);
    }

    MapEntry getEntry(String key) {
        checkNotNull(key);
        Segment segment = getSegment();

        int head = segment.readInt(getOffset(0));
        int size = getSize(head);
        if (size == 0) {
            return null; // shortcut
        }

        int hash = getHash(key);
        int level = getLevel(head);
        if (isBranch(size, level)) {
            // this is an intermediate branch record
            // check if a matching bucket exists, and recurse 
            int bitmap = segment.readInt(getOffset(4));
            int mask = BUCKETS_PER_LEVEL - 1;
            int shift = 32 - (level + 1) * LEVEL_BITS;
            int index = (hash >> shift) & mask;
            int bit = 1 << index;
            if ((bitmap & bit) != 0) {
                int ids = bitCount(bitmap & (bit - 1));
                RecordId id = segment.readRecordId(getOffset(8, ids));
                return new MapRecord(segment, id).getEntry(key);
            } else {
                return null;
            }
        }

        // this is a leaf record; scan the list to find a matching entry
        int d = -1;
        for (int i = 0; i < size && d < 0; i++) {
            d = Integer.valueOf(segment.readInt(getOffset(4 + i * 4)))
                    .compareTo(Integer.valueOf(hash));
            if (d == 0) {
                RecordId keyId = segment.readRecordId(
                        getOffset(4 + size * 4, i));
                d = segment.readString(keyId).compareTo(key);
                if (d == 0) {
                    RecordId valueId = segment.readRecordId(
                            getOffset(4 + size * 4, size + i));
                    return new MapEntry(segment, key, keyId, valueId);
                }
            }
        }

        return null;
    }

    Iterable<String> getKeys() {
        Segment segment = getSegment();

        int head = segment.readInt(getOffset(0));
        int size = getSize(head);
        if (size == 0) {
            return Collections.emptyList(); // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            return concat(transform(getBucketList(segment), GET_KEYS));
        }

        RecordId[] ids = new RecordId[size];
        for (int i = 0; i < size; i++) {
            ids[i] = segment.readRecordId(getOffset(4 + size * 4, i));
        }

        String[] keys = new String[size];
        for (int i = 0; i < size; i++) {
            keys[i] = segment.readString(ids[i]);
        }
        return Arrays.asList(keys);
    }

    Iterable<MapEntry> getEntries() {
        Segment segment = getSegment();

        int head = segment.readInt(getOffset(0));
        int size = getSize(head);
        if (size == 0) {
            return Collections.emptyList(); // shortcut
        }

        int level = getLevel(head);
        if (isBranch(size, level)) {
            return concat(transform(getBucketList(segment), GET_ENTRIES));
        }

        RecordId[] keys = new RecordId[size];
        for (int i = 0; i < size; i++) {
            keys[i] = segment.readRecordId(getOffset(4 + size * 4, i));
        }

        RecordId[] values = new RecordId[size];
        for (int i = 0; i < size; i++) {
            values[i] = segment.readRecordId(getOffset(4 + size * 4, size + i));
        }

        MapEntry[] entries = new MapEntry[size];
        for (int i = 0; i < size; i++) {
            String name = segment.readString(keys[i]);
            entries[i] = new MapEntry(segment, name, keys[i], values[i]);
        }
        return Arrays.asList(entries);
    }

    boolean compareAgainstEmptyMap(MapDiff diff) {
        for (MapEntry entry : getEntries()) {
            if (!diff.entryAdded(entry)) {
                return false;
            }
        }
        return true;
    }

    interface MapDiff {
        boolean entryAdded(MapEntry after);
        boolean entryChanged(MapEntry before, MapEntry after);
        boolean entryDeleted(MapEntry before);
    }

    boolean compare(MapRecord base, MapDiff diff) {
        return compare(base, this, diff);
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

    private static boolean compare(
            MapRecord before, MapRecord after, MapDiff diff) {
        Segment beforeSegment = before.getSegment();
        Segment afterSegment = after.getSegment();
        int beforeHead = beforeSegment.readInt(before.getOffset(0));
        int afterHead = afterSegment.readInt(after.getOffset(0));
        if (isBranch(getSize(beforeHead), getLevel(beforeHead))
                && isBranch(getSize(afterHead), getLevel(afterHead))) {
            RecordId[] beforeBuckets = before.getBuckets(beforeSegment);
            RecordId[] afterBuckets = after.getBuckets(afterSegment);
            for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
                if (Objects.equal(beforeBuckets[i], afterBuckets[i])) {
                    // do nothing
                } else if (beforeBuckets[i] == null) {
                    MapRecord bucket =
                            new MapRecord(afterSegment, afterBuckets[i]);
                    for (MapEntry entry : bucket.getEntries()) {
                        if (!diff.entryAdded(entry)) {
                            return false;
                        }
                    }
                } else if (afterBuckets[i] == null) {
                    MapRecord bucket =
                            new MapRecord(beforeSegment, beforeBuckets[i]);
                    for (MapEntry entry : bucket.getEntries()) {
                        if (!diff.entryDeleted(entry)) {
                            return false;
                        }
                    }
                } else {
                    MapRecord beforeBucket =
                            new MapRecord(beforeSegment, beforeBuckets[i]);
                    MapRecord afterBucket =
                            new MapRecord(afterSegment, afterBuckets[i]);
                    if (!compare(beforeBucket, afterBucket, diff)) {
                        return false;
                    }
                }
            }
            return true;
        }

        Iterator<MapEntry> beforeEntries = before.getEntries().iterator();
        Iterator<MapEntry> afterEntries = after.getEntries().iterator();

        MapEntry beforeEntry = nextOrNull(beforeEntries);
        MapEntry afterEntry = nextOrNull(afterEntries);
        while (beforeEntry != null || afterEntry != null) {
            int d = compare(beforeEntry, afterEntry);
            if (d < 0) {
                if (!diff.entryDeleted(beforeEntry)) {
                    return false;
                }
                beforeEntry = nextOrNull(beforeEntries);
            } else if (d == 0) {
                if (!diff.entryChanged(beforeEntry, afterEntry)) {
                    return false;
                }
                beforeEntry = nextOrNull(beforeEntries);
                afterEntry = nextOrNull(afterEntries);
            } else {
                if (!diff.entryAdded(afterEntry)) {
                    return false;
                }
                afterEntry = nextOrNull(afterEntries);
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
                    .compare(before.getHash(), after.getHash())
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

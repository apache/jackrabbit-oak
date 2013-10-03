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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.ComparisonChain;

class MapLeaf extends MapRecord {

    MapLeaf(Segment segment, int offset, int size, int level) {
        super(segment, offset, size, level);
        checkArgument(size != 0 || level == 0);
        checkArgument(size <= BUCKETS_PER_LEVEL || level == MAX_NUMBER_OF_LEVELS);
    }

    MapLeaf(Segment segment, RecordId id, int size, int level) {
        super(segment, id, size, level);
        checkArgument(size != 0 || level == 0);
        checkArgument(size <= BUCKETS_PER_LEVEL || level == MAX_NUMBER_OF_LEVELS);
    }

    @Override
    MapEntry getEntry(String key) {
        if (size == 0) {
            return null;
        }

        Segment segment = getSegment();
        int hash = checkNotNull(key).hashCode();

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

    @Override
    Iterable<String> getKeys() {
        Segment segment = getSegment();

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

    @Override
    Iterable<MapEntry> getEntries() {
        Segment segment = getSegment();

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

    @Override
    boolean compare(MapRecord base, MapDiff diff) {
        if (base instanceof MapLeaf) {
            return compare((MapLeaf) base, this, diff);
        } else {
            return super.compare(base, diff);
        }
    }

    //-----------------------------------------------------------< private >--

    private static boolean compare(
            MapLeaf before, MapLeaf after, MapDiff diff) {
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

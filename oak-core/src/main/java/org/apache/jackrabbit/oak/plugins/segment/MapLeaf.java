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
            d = Integer.compare(segment.readInt(getOffset(4 + i * 4)), hash);
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
            return compare((MapLeaf) base, diff);
        } else {
            return super.compare(base, diff);
        }
    }

    private boolean compare(MapLeaf before, MapDiff diff) {
        Segment bs = before.getSegment();
        int bi = 0;

        MapLeaf after = this;
        Segment as = after.getSegment();
        int ai = 0;

        while (ai < after.size) {
            int afterHash = after.getHash(as, ai);
            String afterKey = after.getKey(as, ai);
            RecordId afterValue = after.getValue(as, ai);

            while (bi < before.size
                    && (before.getHash(bs, bi) < afterHash
                        || (before.getHash(bs, bi) == afterHash
                            && before.getKey(bs, bi).compareTo(afterKey) < 0))) {
                if (!diff.entryDeleted(
                        before.getKey(bs, bi), before.getValue(bs, bi))) {
                    return false;
                }
                bi++;
            }

            if (bi < before.size
                    && before.getHash(bs, bi) == afterHash
                    && before.getKey(bs, bi).equals(afterKey)) {
                RecordId beforeValue = before.getValue(bs, bi);
                if (!afterValue.equals(beforeValue)
                        && !diff.entryChanged(afterKey, beforeValue, afterValue)) {
                    return false;
                }
                bi++;
            } else if (!diff.entryAdded(afterKey, afterValue)) {
                return false;
            }

            ai++;
        }

        while (bi < before.size) {
            if (!diff.entryDeleted(
                    before.getKey(bs, bi), before.getValue(bs, bi))) {
                return false;
            }
            bi++;
        }

        return true;
    }

    //-----------------------------------------------------------< private >--

    private int getHash(Segment segment, int index) {
        return checkNotNull(segment).readInt(getOffset() + 4 + index * 4);
    }

    private String getKey(Segment segment, int index) {
        checkNotNull(segment);
        int offset = getOffset(4 + size * 4, index);
        return segment.readString(segment.readRecordId(offset));
    }

    private RecordId getValue(Segment segment, int index) {
        int offset = getOffset(4 + size * 4, size + index);
        return checkNotNull(segment).readRecordId(offset);
    }

}

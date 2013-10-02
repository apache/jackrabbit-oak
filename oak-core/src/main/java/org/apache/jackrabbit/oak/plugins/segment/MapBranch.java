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
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.lang.Integer.bitCount;
import static java.util.Arrays.asList;

import java.util.Collections;

import javax.annotation.Nullable;

import com.google.common.base.Function;

class MapBranch extends MapRecord {

    private final int bitmap;

    MapBranch(Segment segment, int offset, int size, int level, int bitmap) {
        super(segment, offset, size, level);
        checkArgument(size > BUCKETS_PER_LEVEL);
        checkArgument(level < MAX_NUMBER_OF_LEVELS);
        this.bitmap = bitmap;
    }

    MapBranch(Segment segment, RecordId id, int size, int level, int bitmap) {
        super(segment, id, size, level);
        checkArgument(size > BUCKETS_PER_LEVEL);
        checkArgument(level < MAX_NUMBER_OF_LEVELS);
        this.bitmap = bitmap;
    }

    RecordId[] getBuckets() {
        Segment segment = getSegment();
        int bytes = 8;
        int ids = 0;
        RecordId[] buckets = new RecordId[BUCKETS_PER_LEVEL];
        for (int i = 0; i < buckets.length; i++) {
            if ((bitmap & (1 << i)) != 0) {
                buckets[i] = segment.readRecordId(getOffset(bytes, ids++));
            } else {
                buckets[i] = null;
            }
        }
        return buckets;
    }

    @Override
    MapEntry getEntry(String key) {
        checkNotNull(key);

        int mask = BUCKETS_PER_LEVEL - 1;
        int shift = level * LEVEL_BITS;
        int index = (key.hashCode() >> shift) & mask;

        int bit = 1 << index;
        if ((bitmap & bit) != 0) {
            Segment segment = getSegment();
            int bytes = 8;
            int ids = bitCount(bitmap & (bit - 1));
            RecordId id = segment.readRecordId(getOffset(bytes, ids));
            return segment.readMap(id).getEntry(key);
        } else {
            return null;
        }
    }

    @Override
    Iterable<String> getKeys() {
        final Segment segment = getSegment();
        return concat(transform(
                asList(getBuckets()),
                new Function<RecordId, Iterable<String>>() {
                    @Override @Nullable
                    public Iterable<String> apply(@Nullable RecordId input) {
                        if (input != null) {
                            return segment.readMap(input).getKeys();
                        } else {
                            return Collections.emptyList();
                        }
                    }
                }));
    }

    @Override
    Iterable<MapEntry> getEntries() {
        final Segment segment = getSegment();
        return concat(transform(
                asList(getBuckets()),
                new Function<RecordId, Iterable<MapEntry>>() {
                    @Override @Nullable
                    public Iterable<MapEntry> apply(@Nullable RecordId input) {
                        if (input != null) {
                            return segment.readMap(input).getEntries();
                        } else {
                            return Collections.emptyList();
                        }
                    }
                }));
    }

    @Override
    boolean compare(MapRecord base, MapDiff diff) {
        if (base instanceof MapBranch) {
            return compare((MapBranch) base, diff);
        } else {
            return super.compare(base, diff);
        }
    }

    private boolean compare(MapBranch before, MapDiff diff) {
        MapBranch after = this;
        checkState(after.level == before.level);

        Segment afterSegment = after.getSegment();
        Segment beforeSegment = before.getSegment();
        RecordId[] afterBuckets = after.getBuckets();
        RecordId[] beforeBuckets = before.getBuckets();
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            if (afterBuckets[i] == null) {
                if (beforeBuckets[i] != null) {
                    MapRecord map = beforeSegment.readMap(beforeBuckets[i]);
                    for (MapEntry entry : map.getEntries()) {
                        if (!diff.entryDeleted(entry.getName(), entry.getValue())) {
                            return false;
                        }
                    }
                }
            } else if (beforeBuckets[i] == null) {
                MapRecord map = afterSegment.readMap(afterBuckets[i]);
                for (MapEntry entry : map.getEntries()) {
                    if (!diff.entryAdded(entry.getName(), entry.getValue())) {
                        return false;
                    }
                }
            } else if (!afterBuckets[i].equals(beforeBuckets[i])) {
                MapRecord afterMap = afterSegment.readMap(afterBuckets[i]);
                MapRecord beforeMap = beforeSegment.readMap(beforeBuckets[i]);
                if (!afterMap.compare(beforeMap, diff)) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public boolean compareAgainstEmptyMap(MapDiff diff) {
        Segment segment = getSegment();
        int bytes = 8;
        int ids = 0;
        for (int i = 0; i < BUCKETS_PER_LEVEL; i++) {
            if ((bitmap & (1 << i)) != 0) {
                MapRecord bucket = segment.readMap(
                        segment.readRecordId(getOffset(bytes, ids++)));
                if (!bucket.compareAgainstEmptyMap(diff)) {
                    return false;
                }
            }
        }

        return true;
    }

}

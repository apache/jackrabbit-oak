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
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.lang.Integer.bitCount;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ID_BYTES;

import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

class MapBranch extends MapRecord {

    private final int bitmap;

    MapBranch(SegmentStore store, RecordId id, int size, int level, int bitmap) {
        super(store, id, size, level);
        checkArgument(size > BUCKETS_PER_LEVEL);
        checkArgument(level < MAX_NUMBER_OF_LEVELS);
        this.bitmap = bitmap;
    }

    @Override
    RecordId getEntry(String key) {
        checkNotNull(key);

        int mask = BUCKETS_PER_LEVEL - 1;
        int shift = level * LEVEL_BITS;
        int index = (key.hashCode() >> shift) & mask;

        int bit = 1 << index;
        if ((bitmap & bit) != 0) {
            int offset = getOffset()
                    + 8 + bitCount(bitmap & (bit - 1)) * RECORD_ID_BYTES;
            RecordId id = getSegment().readRecordId(offset);
            return MapRecord.readMap(store, id).getEntry(key);
        } else {
            return null;
        }
    }

    @Override
    Iterable<String> getKeys() {
        return concat(transform(
                getBuckets(),
                new Function<RecordId, Iterable<String>>() {
                    @Override @Nullable
                    public Iterable<String> apply(@Nullable RecordId input) {
                        return MapRecord.readMap(store, input).getKeys();
                    }
                }));
    }

    @Override
    Iterable<Entry> getEntries() {
        return concat(transform(
                getBuckets(),
                new Function<RecordId, Iterable<Entry>>() {
                    @Override @Nullable
                    public Iterable<Entry> apply(@Nullable RecordId input) {
                        return MapRecord.readMap(store, input).getEntries();
                    }
                }));
    }

    private Iterable<RecordId> getBuckets() {
        int n = Integer.bitCount(bitmap);
        int p = getOffset() + 8;
        int q = p + n * RECORD_ID_BYTES;
        Segment segment = getSegment();

        List<RecordId> buckets = Lists.newArrayListWithCapacity(n);
        for (int o = p; o < q; o += RECORD_ID_BYTES) {
            buckets.add(segment.readRecordId(o));
        }
        return buckets;
    }

}

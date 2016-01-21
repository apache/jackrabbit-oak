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

package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MAX_SEGMENT_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ALIGN_BITS;

import java.util.Map;
import java.util.Random;

import javax.annotation.Nonnull;

// FIXME SegmentTestUtils duplicates this
public final class TestUtils {
    private TestUtils() {}

    public static RecordId newRecordId(SegmentTracker factory, Random random) {
        SegmentId id = factory.newDataSegmentId();
        RecordId r = new RecordId(id, newValidOffset(random));
        return r;
    }

    public static int newValidOffset(Random random) {
        return random.nextInt(MAX_SEGMENT_SIZE >> RECORD_ALIGN_BITS) << RECORD_ALIGN_BITS;
    }

    /**
     * Returns a new valid record offset, between {@code a} and {@code b},
     * exclusive.
     */
    public static int newValidOffset(@Nonnull Random random, int a, int b) {
        int p = (a >> RECORD_ALIGN_BITS) + 1;
        int q = (b >> RECORD_ALIGN_BITS);
        return (p + random.nextInt(q - p)) << RECORD_ALIGN_BITS;
    }

    /**
     * Create a random map of record ids.
     *
     * @param rnd
     * @param tracker
     * @param segmentCount  number of segments
     * @param entriesPerSegment  number of records per segment
     * @return  map of record ids
     */
    public static Map<RecordId, RecordId> randomRecordIdMap(Random rnd, SegmentTracker tracker,
            int segmentCount, int entriesPerSegment) {
        Map<RecordId, RecordId> map = newHashMap();
        for (int i = 0; i < segmentCount; i++) {
            SegmentId id = tracker.newDataSegmentId();
            int offset = MAX_SEGMENT_SIZE;
            for (int j = 0; j < entriesPerSegment; j++) {
                offset = newValidOffset(rnd, (entriesPerSegment - j) << RECORD_ALIGN_BITS, offset);
                RecordId before = new RecordId(id, offset);
                RecordId after = new RecordId(
                        tracker.newDataSegmentId(),
                        newValidOffset(rnd, 0, MAX_SEGMENT_SIZE));
                map.put(before, after);
            }
        }
        return map;
    }

}

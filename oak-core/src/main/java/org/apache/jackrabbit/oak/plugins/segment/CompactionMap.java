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

import static com.google.common.collect.Maps.newTreeMap;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ALIGN_BITS;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Immutable, space-optimized mapping of compacted record identifiers.
 * Used to optimize record equality comparisons across a compaction operation
 * without holding references to the {@link SegmentId} instances of the old,
 * compacted segments.
 * <p>
 * The data structure used by this class consists of three parts:
 * <ol>
 *   <li>The {@link #msbs} and {@link #lsbs} arrays store the identifiers
 *       of all old, compacted segments. The identifiers are stored in
 *       increasing order, with the i'th identifier stored in the
 *       {@code msbs[i]} and {@code lsbs[i]} slots. Interpolation search
 *       is used to quickly locate any given identifier.
 *   <li>Each compacted segment identifier is associated with a list of
 *       mapping entries that point from a record offset within that
 *       segment to the new identifier of the compacted record. The
 *       {@link #entryIndex} array is used to to locate these lists within
 *       the larger entry arrays described below. The list of entries for
 *       the i'th identifier consists of entries from {@code entryIndex[i]}
 *       (inclusive) to {@code entryIndex[i+1]} (exclusive). An extra
 *       sentinel slot is added at the end of the array to make the above
 *       rule work also for the last compacted segment identifier.
 *   <li>The mapping entries are stored in the {@link #beforeOffsets},
 *       {@link #afterSegmentIds} and {@link #afterOffsets} arrays. Once the
 *       list of entries for a given compacted segment is found, the
 *       before record offsets are scanned to find a match. If a match is
 *       found, the corresponding compacted record will be identified by the
 *       respective after segment identifier and offset.
 * </ol>
 * <p>
 * Assuming each compacted segment contains {@code n} compacted records on
 * average, the amortized size of each entry in this mapping is about
 * {@code 20/n + 8} bytes, assuming compressed pointers.
 */
class CompactionMap {

    private final long[] msbs;
    private final long[] lsbs;
    private final int[] entryIndex;

    private final short[] beforeOffsets;
    private final SegmentId[] afterSegmentIds;
    private final short[] afterOffsets;

    CompactionMap() {
        this(Collections.<RecordId, RecordId>emptyMap());
    }

    /**
     * Creates a compaction map based from the given record identifiers.
     */
    CompactionMap(Map<RecordId, RecordId> compacted) {
        Map<SegmentId, Map<Integer, RecordId>> mapping = newTreeMap();
        for (Entry<RecordId, RecordId> entry : compacted.entrySet()) {
            RecordId before = entry.getKey();
            SegmentId id = before.getSegmentId();
            Map<Integer, RecordId> map = mapping.get(id);
            if (map == null) {
                map = newTreeMap();
                mapping.put(id, map);
            }
            map.put(before.getOffset(), entry.getValue());
        }

        SegmentId[] ids = mapping.keySet().toArray(new SegmentId[mapping.size()]);
        Arrays.sort(ids);

        this.msbs = new long[ids.length];
        this.lsbs = new long[ids.length];
        this.entryIndex = new int[ids.length + 1];

        this.beforeOffsets = new short[compacted.size()];
        this.afterSegmentIds = new SegmentId[compacted.size()];
        this.afterOffsets = new short[compacted.size()];

        int index = 0;
        for (int i = 0; i < ids.length; i++) {
            msbs[i] = ids[i].getMostSignificantBits();
            lsbs[i] = ids[i].getLeastSignificantBits();
            entryIndex[i] = index;

            Map<Integer, RecordId> map = mapping.get(ids[i]);
            for (Entry<Integer, RecordId> entry : map.entrySet()) {
                int key = entry.getKey();
                RecordId id = entry.getValue();
                beforeOffsets[index] = (short) (key >> RECORD_ALIGN_BITS);
                afterSegmentIds[index] = id.getSegmentId();
                afterOffsets[index] = (short) (id.getOffset() >> RECORD_ALIGN_BITS);
                index++;
            }
        }
        entryIndex[ids.length] = index;
    }

    /**
     * Checks whether the record with the given {@code before} identifier was
     * compacted to a new record with the given {@code after} identifier.
     *
     * @param before before record identifier
     * @param after after record identifier
     * @return whether {@code before} was compacted to {@code after}
     */
    boolean wasCompactedTo(RecordId before, RecordId after) {
        // this a copy of the TarReader#findEntry with tiny changes around the
        // entry sizes

        SegmentId segmentId = before.getSegmentId();
        long msb = segmentId.getMostSignificantBits();
        long lsb = segmentId.getLeastSignificantBits();
        int offset = before.getOffset();

        // The segment identifiers are randomly generated with uniform
        // distribution, so we can use interpolation search to find the
        // matching entry in the index. The average runtime is O(log log n).

        int lowIndex = 0;
        int highIndex = msbs.length - 1;

        // Use floats to prevent integer overflow during interpolation.
        // Lost accuracy is no problem, since we use interpolation only
        // as a guess of where the target value is located and the actual
        // comparisons are still done using the original values.
        float lowValue = Long.MIN_VALUE;
        float highValue = Long.MAX_VALUE;
        float targetValue = msb;

        while (lowIndex <= highIndex) {
            int guessIndex = lowIndex;
            float valueRange = highValue - lowValue;
            if (valueRange >= 1) {
                guessIndex += Math.round(
                        (highIndex - lowIndex) * (targetValue - lowValue)
                        / valueRange);
            }

            long m = msbs[guessIndex];
            if (msb < m) {
                highIndex = guessIndex - 1;
                highValue = m;
            } else if (msb > m) {
                lowIndex = guessIndex + 1;
                lowValue = m;
            } else {
                // getting close...
                long l = lsbs[guessIndex];
                if (lsb < l) {
                    highIndex = guessIndex - 1;
                    highValue = m;
                } else if (lsb > l) {
                    highIndex = guessIndex + 1;
                    highValue = m;
                } else {
                    // getting even closer...
                    int index = entryIndex[guessIndex];
                    int limit = entryIndex[guessIndex + 1];
                    for (int i = index; i < limit; i++) {
                        int o = (beforeOffsets[i] & 0xffff) << RECORD_ALIGN_BITS;
                        if (o < offset) {
                            index++;
                        } else if (o == offset) {
                            // found it! now compare the value
                            return afterSegmentIds[i] == after.getSegmentId()
                                    && (afterOffsets[i] & 0xffff) << RECORD_ALIGN_BITS == after.getOffset();
                        } else {
                            return false;
                        }
                    }
                }
            }
        }

        // not found
        return false;
    }


}

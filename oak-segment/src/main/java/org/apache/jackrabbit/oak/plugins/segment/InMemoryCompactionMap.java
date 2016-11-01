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
import static com.google.common.collect.Maps.newTreeMap;
import static com.google.common.collect.Sets.newTreeSet;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.decode;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.encode;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

/**
 * Immutable, space-optimized mapping of compacted record identifiers.
 * Used to optimize record equality comparisons across a compaction operation
 * without holding references to the {@link SegmentId} instances of the old,
 * compacted segments.
 * <p>
 * The data structure used by this class consists of four parts:
 * <ol>
 *   <li>The {@link #recent} map of recently compacted entries is maintained
 *       while the compaction is in progress and new entries need to be added.
 *       These entries are periodically compressed into the more
 *       memory-efficient structure described below.
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
@Deprecated
public class InMemoryCompactionMap implements PartialCompactionMap {

    /**
     * Number of map entries to keep until compressing this map.
     */
    private static final int COMPRESS_INTERVAL = Integer.getInteger("compress-interval", 100000);

    private final SegmentTracker tracker;

    private Map<RecordId, RecordId> recent = newHashMap();

    private long[] msbs = new long[0];
    private long[] lsbs = new long[0];
    private short[] beforeOffsets = new short[0];

    private int[] entryIndex = new int[0];
    private short[] afterOffsets = new short[0];

    private int[] afterSegmentIds = new int[0];
    private long[] afterMsbs = new long[0];
    private long[] afterLsbs = new long[0];

    InMemoryCompactionMap(@Nonnull SegmentTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    @Deprecated
    public boolean wasCompactedTo(@Nonnull RecordId before, @Nonnull RecordId after) {
        return after.equals(get(before));
    }

    @Override
    @Deprecated
    public boolean wasCompacted(@Nonnull UUID id) {
        return findEntry(id.getMostSignificantBits(), id.getLeastSignificantBits()) != -1;
    }

    @Override
    @Deprecated
    public RecordId get(@Nonnull RecordId before) {
        RecordId after = recent.get(before);
        if (after != null) {
            return after;
        }

        //empty map
        if (msbs.length == 0) {
            return null;
        }

        SegmentId segmentId = before.getSegmentId();
        long msb = segmentId.getMostSignificantBits();
        long lsb = segmentId.getLeastSignificantBits();
        int offset = before.getOffset();

        int entry = findEntry(msb, lsb);
        if (entry != -1) {
            int index = entryIndex[entry];
            int limit = entryIndex[entry + 1];
            for (int i = index; i < limit; i++) {
                int o = decode(beforeOffsets[i]);
                if (o == offset) {
                    // found it!
                    return new RecordId(asSegmentId(i), decode(afterOffsets[i]));
                } else if (o > offset) {
                    return null;
                }
            }
        }

        return null;
    }

    @Nonnull
    private SegmentId asSegmentId(int index) {
        int idx = afterSegmentIds[index];
        return new SegmentId(tracker, afterMsbs[idx], afterLsbs[idx]);
    }

    @Nonnull
    private static UUID asUUID(SegmentId id) {
        return new UUID(id.getMostSignificantBits(),
                id.getLeastSignificantBits());
    }

    @Override
    @Deprecated
    public void put(@Nonnull RecordId before, @Nonnull RecordId after) {
        if (get(before) != null) {
            throw new IllegalArgumentException();
        }
        recent.put(before, after);
        if (recent.size() >= COMPRESS_INTERVAL) {
            compress();
        }
    }

    @Override
    @Deprecated
    public void remove(@Nonnull Set<UUID> uuids) {
        compress(uuids);
    }

    @Override
    @Deprecated
    public void compress() {
        compress(Collections.<UUID>emptySet());
    }

    @Override
    @Deprecated
    public long getSegmentCount() {
        return msbs.length;
    }

    @Override
    @Deprecated
    public long getRecordCount() {
        return afterOffsets.length;
    }

    @Override
    @Deprecated
    public boolean isEmpty() {
        return afterOffsets.length == 0 && recent.isEmpty();
    }

    private void compress(@Nonnull Set<UUID> removed) {
        if (recent.isEmpty() && removed.isEmpty()) {
            // no-op
            return;
        }

        Set<UUID> uuids = newTreeSet();
        int newSize = 0;
        Map<UUID, Map<Integer, RecordId>> mapping = newTreeMap();
        for (Entry<RecordId, RecordId> entry : recent.entrySet()) {
            RecordId before = entry.getKey();

            SegmentId id = before.getSegmentId();
            UUID uuid = new UUID(
                    id.getMostSignificantBits(),
                    id.getLeastSignificantBits());
            if (uuids.add(uuid) && !removed.contains(uuid)) {
                newSize++;
            }

            Map<Integer, RecordId> map = mapping.get(uuid);
            if (map == null) {
                map = newTreeMap();
                mapping.put(uuid, map);
            }
            map.put(before.getOffset(), entry.getValue());
        }

        for (int i = 0; i < msbs.length; i++) {
            UUID uuid = new UUID(msbs[i], lsbs[i]);
            if (uuids.add(uuid) && !removed.contains(uuid)) {
                newSize++;
            }
        }

        long[] newMsbs = new long[newSize];
        long[] newLsbs = new long[newSize];
        int[] newEntryIndex = new int[newSize + 1];

        int newEntries = beforeOffsets.length + recent.size();
        short[] newBeforeOffsets = new short[newEntries];
        short[] newAfterOffsets = new short[newEntries];

        int[] newAfterSegmentIds = new int[newEntries];
        Map<UUID, Integer> newAfterSegments = newHashMap();

        int newIndex = 0;
        int newEntry = 0;
        int oldEntry = 0;
        for (UUID uuid : uuids) {
            long msb = uuid.getMostSignificantBits();
            long lsb = uuid.getLeastSignificantBits();

            if (removed.contains(uuid)) {
                if (oldEntry < msbs.length
                        && msbs[oldEntry] == msb
                        && lsbs[oldEntry] == lsb) {
                    oldEntry++;
                }
                continue;
            }

            // offset -> record
            Map<Integer, RecordId> newSegment = mapping.get(uuid);
            if (newSegment == null) {
                newSegment = newTreeMap();
            }

            if (oldEntry < msbs.length
                    && msbs[oldEntry] == msb
                    && lsbs[oldEntry] == lsb) {
                int index = entryIndex[oldEntry];
                int limit = entryIndex[oldEntry + 1];
                for (int i = index; i < limit; i++) {
                    newSegment.put(decode(beforeOffsets[i]), new RecordId(
                            asSegmentId(i), decode(afterOffsets[i])));
                }
                oldEntry++;
            }

            newMsbs[newEntry] = msb;
            newLsbs[newEntry] = lsb;
            newEntryIndex[newEntry++] = newIndex;
            for (Entry<Integer, RecordId> entry : newSegment.entrySet()) {
                int key = entry.getKey();
                RecordId id = entry.getValue();
                newBeforeOffsets[newIndex] = encode(key);
                newAfterOffsets[newIndex] = encode(id.getOffset());

                UUID aUUID = asUUID(id.getSegmentId());
                int aSIdx;
                if (newAfterSegments.containsKey(aUUID)) {
                    aSIdx = newAfterSegments.get(aUUID);
                } else {
                    aSIdx = newAfterSegments.size();
                    newAfterSegments.put(aUUID, aSIdx);
                }
                newAfterSegmentIds[newIndex] = aSIdx;

                newIndex++;
            }
        }

        newEntryIndex[newEntry] = newIndex;

        this.msbs = newMsbs;
        this.lsbs = newLsbs;
        this.entryIndex = newEntryIndex;

        if (newIndex < newBeforeOffsets.length) {
            this.beforeOffsets = Arrays.copyOf(newBeforeOffsets, newIndex);
            this.afterOffsets = Arrays.copyOf(newAfterOffsets, newIndex);
            this.afterSegmentIds = Arrays.copyOf(newAfterSegmentIds, newIndex);
        } else {
            this.beforeOffsets = newBeforeOffsets;
            this.afterOffsets = newAfterOffsets;
            this.afterSegmentIds = newAfterSegmentIds;
        }

        this.afterMsbs = new long[newAfterSegments.size()];
        this.afterLsbs = new long[newAfterSegments.size()];
        for (Entry<UUID, Integer> entry : newAfterSegments.entrySet()) {
            this.afterMsbs[entry.getValue()] = entry.getKey()
                    .getMostSignificantBits();
            this.afterLsbs[entry.getValue()] = entry.getKey()
                    .getLeastSignificantBits();
        }

        recent = newHashMap();
    }

    /**
     * Finds the given segment identifier (UUID) within the list of
     * identifiers of compacted segments tracked by this instance.
     * Since the UUIDs are randomly generated and we keep the list
     * sorted, we can use interpolation search to achieve
     * {@code O(log log n)} lookup performance.
     *
     * @param msb most significant bits of the UUID
     * @param lsb least significant bits of the UUID
     * @return entry index, or {@code -1} if not found
     */
    private final int findEntry(long msb, long lsb) {
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
            if (valueRange >= 1) { // no point in interpolating further
                // Math.round() also prevents IndexOutOfBoundsExceptions
                // caused by possible inaccuracy in the float computations.
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
                    // found it!
                    return guessIndex;
                }
            }
        }

        // not found
        return -1;
    }

    @Override
    @Deprecated
    public long getEstimatedWeight() {
        // estimation of the object including empty 'recent' map
        long total = 168;

        // msbs
        total += 24 + msbs.length * 8;
        // lsbs
        total += 24 + lsbs.length * 8;
        // beforeOffsets
        total += 24 + beforeOffsets.length * 2;

        // entryIndex
        total += 24 + entryIndex.length * 4;
        // afterOffsets
        total += 24 + afterOffsets.length * 2;

        // afterSegmentIds
        total += 24 + afterSegmentIds.length * 4;
        // afterMsbs
        total += 24 + afterMsbs.length * 8;
        // afterLsbs
        total += 24 + afterLsbs.length * 8;

        return total;
    }

}

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

import static com.google.common.collect.Lists.newArrayList;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * A {@code CompactionMap} is a composite of multiple {@link PartialCompactionMap}
 * instances. Operations performed on this map are delegated back to the individual
 * maps.
 */
@Deprecated
public class CompactionMap {

    /**
     * An empty map.
     */
    @Deprecated
    public static final CompactionMap EMPTY =
            new CompactionMap(Collections.<PartialCompactionMap>emptyList(), 0);

    private final List<PartialCompactionMap> maps;

    /**
     * Generation represents the number of compaction cycles since the system
     * came online. This is not persisted so it will be reset to 0 on each
     * restart
     */
    private final int generation;

    private CompactionMap(@Nonnull List<PartialCompactionMap> maps, int generation) {
        this.maps = maps;
        this.generation = generation;
    }

    /**
     * Checks whether the record with the given {@code before} identifier was
     * compacted to a new record with the given {@code after} identifier.
     *
     * @param before before record identifier
     * @param after  after record identifier
     * @return whether {@code before} was compacted to {@code after}
     */
    @Deprecated
    public boolean wasCompactedTo(@Nonnull RecordId before, @Nonnull RecordId after) {
        for (PartialCompactionMap map : maps) {
            if (map.wasCompactedTo(before, after)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether content in the segment with the given identifier was
     * compacted to new segments.
     *
     * @param id segment identifier
     * @return whether the identified segment was compacted
     */
    @Deprecated
    public boolean wasCompacted(@Nonnull UUID id) {
        for (PartialCompactionMap map : maps) {
            if (map.wasCompacted(id)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Retrieve the record id {@code before} maps to or {@code null}
     * if no such id exists.
     * @param before before record id
     * @return after record id or {@code null}
     */
    @CheckForNull
    @Deprecated
    public RecordId get(@Nonnull RecordId before) {
        for (PartialCompactionMap map : maps) {
            RecordId after = map.get(before);
            if (after != null) {
                return after;
            }
        }
        return null;
    }

    /**
     * Remove all keys from this map where {@code keys.contains(key.asUUID())}.
     * @param uuids  uuids of the keys to remove
     */
    @Deprecated
    public void remove(@Nonnull Set<UUID> uuids) {
        for (PartialCompactionMap map : maps) {
            map.remove(uuids);
        }
    }

    /**
     * Create a new {@code CompactionMap} containing all maps
     * of this instances and additional the passed map {@code head}.
     * @param head
     * @return a new {@code CompactionMap} instance
     */
    @Nonnull
    @Deprecated
    public CompactionMap cons(@Nonnull PartialCompactionMap head) {
        List<PartialCompactionMap> maps = newArrayList(head);
        for (PartialCompactionMap map : this.maps) {
            if (!map.isEmpty()) {
                maps.add(map);
            }
        }
        return new CompactionMap(maps, generation + 1);
    }

    /**
     * Java's lacking libraries...
     * @param longs
     * @return sum of the passed {@code longs}
     */
    @Deprecated
    public static long sum(long[] longs) {
        long sum = 0;
        for (long x : longs) {
            sum += x;
        }
        return sum;
    }

    /**
     * The depth of the compaction map is the number of partial compaction maps
     * this map consists of.
     *
     * @return the depth of this compaction map
     * @see #cons(PartialCompactionMap)
     */
    @Deprecated
    public int getDepth() {
        return maps.size();
    }

    @Deprecated
    public int getGeneration() {
        return generation;
    }

    /**
     * The weight of the compaction map is its  memory consumption bytes
     * @return Estimated weight of the compaction map
     */
    @Deprecated
    public long[] getEstimatedWeights() {
        long[] weights = new long[maps.size()];
        int c = 0;
        for (PartialCompactionMap map : maps) {
            weights[c++] = map.getEstimatedWeight();
        }
        return weights;
    }

    /**
     * Number of segments referenced by the keys in this map. The returned value might only
     * be based on the compressed part of the individual maps.
     * @return  number of segments
     */
    @Deprecated
    public long[] getSegmentCounts() {
        long[] counts = new long[maps.size()];
        int c = 0;
        for (PartialCompactionMap map : maps) {
            counts[c++] = map.getSegmentCount();
        }
        return counts;
    }

    /**
     * Number of records referenced by the keys in this map. The returned value might only
     * be based on the compressed part of the  individual maps.
     * @return  number of records
     */
    @Deprecated
    public long[] getRecordCounts() {
        long[] counts = new long[maps.size()];
        int c = 0;
        for (PartialCompactionMap map : maps) {
            counts[c++] = map.getRecordCount();
        }
        return counts;
    }

}

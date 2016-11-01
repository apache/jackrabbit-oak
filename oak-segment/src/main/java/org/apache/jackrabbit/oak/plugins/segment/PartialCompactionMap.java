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

import java.util.Set;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * A {@code PartialCompactionMap} maps uncompacted to compacted record ids
 * from a single compaction cycle.
 *
 * @see CompactionMap
 */
@Deprecated
public interface PartialCompactionMap {

    /**
     * Checks whether the record with the given {@code before} identifier was
     * compacted to a new record with the given {@code after} identifier.
     *
     * @param before before record identifier
     * @param after after record identifier
     * @return whether {@code before} was compacted to {@code after}
     */
    @Deprecated
    boolean wasCompactedTo(@Nonnull RecordId before, @Nonnull RecordId after);

    /**
     * Checks whether content in the segment with the given identifier was
     * compacted to new segments.
     *
     * @param id segment identifier
     * @return whether the identified segment was compacted
     */
    @Deprecated
    boolean wasCompacted(@Nonnull UUID id);

    /**
     * Retrieve the record id {@code before} maps to or {@code null}
     * if no such id exists.
     * @param before before record id
     * @return after record id or {@code null}
     */
    @CheckForNull
    @Deprecated
    RecordId get(@Nonnull RecordId before);

    /**
     * Adds a new entry to the compaction map. Overwriting a previously
     * added entry is not supported.
     * @param before  before record id
     * @param after  after record id
     * @throws IllegalArgumentException  if {@code before} already exists in the map
     */
    @Deprecated
    void put(@Nonnull RecordId before, @Nonnull RecordId after);

    /**
     * Remove all keys from this map where {@code keys.contains(key.asUUID())}.
     * @param uuids  uuids of the keys to remove
     */
    @Deprecated
    void remove(@Nonnull Set<UUID> uuids);

    /**
     * Compressing this map ensures it takes up as little heap as possible. This
     * operation might be expensive and should only be called in suitable intervals.
     */
    @Deprecated
    void compress();

    /**
     * Number of segments referenced by the keys in this map. The returned value might only
     * be based on the compressed part of the map.
     * @return  number of segments
     */
    @Deprecated
    long getSegmentCount();

    /**
     * Number of records referenced by the keys in this map. The returned value might only
     * be based on the compressed part of the map.
     * @return  number of records
     */
    @Deprecated
    long getRecordCount();

    /**
     * Determine whether this map contains keys at all.
     * @return  {@code true} iff this map is empty
     */
    @Deprecated
    boolean isEmpty();

    /**
     * The weight of the compaction map is its heap memory consumption in bytes.
     * @return  Estimated weight of the compaction map
     */
    @Deprecated
    long getEstimatedWeight();
}

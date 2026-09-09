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
package org.apache.jackrabbit.oak.segment;

import java.io.Closeable;
import java.io.IOException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Content-addressed cache mapping a record's content-hash key to the {@link RecordId} of an
 * already-emitted, byte-identical record, so the {@link RecordRepacker} can reuse it instead of
 * emitting a duplicate. Two implementations:
 * <ul>
 *   <li>{@link ShardedRecordIdMap} - exact and unbounded (memory-mapped/off-heap): every duplicate is
 *       collapsed, at the cost of a whole-store filesystem spill and a canonical copy that may live far
 *       from its referrers;</li>
 *   <li>{@link MmapWindowDedupCache} - a bounded recency window in a fixed-size memory-mapped file
 *       (the default): only records whose content recurs while still in the window are deduplicated,
 *       older entries are forgotten. Off-heap and its spill is bounded to the window size;</li>
 *   <li>{@link WindowDedupCache} - the same bounded recency window kept in the Java heap: no spill at
 *       all, but the window's entries count against the heap.</li>
 * </ul>
 * In both cases a miss means the caller emits a fresh record; the repacker's exact alias still
 * preserves the input's physical record sharing regardless of this cache.
 */
interface DedupCache extends Closeable {

    /** @return the mapped id for {@code key}, or {@code null} if none is (currently) held. */
    @Nullable
    RecordId get(@NotNull byte[] key);

    /**
     * Map {@code key} to {@code value} unless already mapped. Returns the existing mapping if one is
     * present (leaving it unchanged), otherwise {@code null} (the caller's {@code value} was stored).
     */
    @Nullable
    RecordId putIfAbsent(@NotNull byte[] key, @NotNull RecordId value) throws IOException;

    /**
     * Return the id mapped to {@code key}, computing and storing it via {@code writer} if absent. The
     * computation runs while the (shard) lock is held, so two callers with the same key never both
     * emit.
     */
    @NotNull
    RecordId computeIfAbsent(@NotNull byte[] key, @NotNull ShardedRecordIdMap.RecordIdSupplier writer)
            throws IOException;

    /** Number of deduplication hits (content found), or {@code -1} if this cache does not track it. */
    default long getHits() {
        return -1;
    }

    /** Number of deduplication misses (fresh emits), or {@code -1} if this cache does not track it. */
    default long getMisses() {
        return -1;
    }

    @Override
    void close();
}

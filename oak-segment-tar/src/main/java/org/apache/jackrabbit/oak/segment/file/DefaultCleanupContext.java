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

package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.file.tar.CleanupContext;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;

class DefaultCleanupContext implements CleanupContext {

    /**
     * When {@code true}, the positional "dangling future segment" reclamation is disabled entirely and
     * cleanup relies only on generational and reference-based reclamation. The positional heuristic
     * (see {@link #isDanglingFutureSegment}) assumes the physical TAR order matches the gc.log compacted
     * root, which does not hold for a store assembled out of compaction order (e.g. a promoted cold
     * standby), where it can reclaim live, head-referenced compacted segments (OAK-12400). Skipping it
     * never reclaims a live segment; genuine aborted-compaction leftovers are instead collected by
     * ordinary generational GC once the head advances past their generation. Default {@code false}.
     */
    private static final String SKIP_DANGLING_FUTURE_RECLAIM = "oak.segment.cleanup.skipDanglingFutureReclaim";

    private final @NotNull SegmentTracker segmentTracker;
    private final @NotNull Predicate<GCGeneration> old;
    private final @Nullable UUID rootSegmentUUID;
    private final boolean skipDanglingFutureReclaim;
    private boolean aheadOfRoot;

    DefaultCleanupContext(@NotNull SegmentTracker tracker, @NotNull Predicate<GCGeneration> old, @NotNull String compactedRoot) {
        this.segmentTracker = tracker;
        this.old = old;
        this.skipDanglingFutureReclaim = SystemPropertySupplier
                .create(SKIP_DANGLING_FUTURE_RECLAIM, Boolean.FALSE)
                .get();

        RecordId rootId =  RecordId.fromString(tracker, compactedRoot);
        if (rootId.equals(RecordId.NULL)) {
            rootSegmentUUID = null;
            aheadOfRoot = false;
        } else {
            rootSegmentUUID = rootId.getSegmentId().asUUID();
            aheadOfRoot = true;
        }
    }

    /**
     * Reference-based reclamation for bulk segments.
     */
    private boolean isUnreferencedBulkSegment(UUID id, boolean referenced) {
        return !isDataSegmentId(id.getLeastSignificantBits()) && !referenced;
    }

    /**
     * Generational reclamation for data segments.
     */
    private boolean isOldDataSegment(UUID id, GCGeneration generation) {
        return isDataSegmentId(id.getLeastSignificantBits()) && old.test(generation);
    }

    /**
     * Special reclamation for unused future segments. Aborting compaction will lead to persisted, but unused
     * TAR entries with higher generation than the root and set compacted flag. Due to incremental compaction,
     * a purely generational approach for this cleanup is no longer feasible as segments of higher generation
     * than the root may be part of a valid repository tree. Observation: compacted segments are unused iff
     * they are persisted after the last compacted root. This context relies on the cleanup algorithm to mark
     * TAR entries in reverse order and will consider each compacted segment to be reclaimable until the root
     * has been encountered, i.e. as long as {@code aheadOfRoot} is true.
     * <p>
     * When {@link #SKIP_DANGLING_FUTURE_RECLAIM} is enabled this reclamation is skipped entirely, so live
     * head-referenced compacted segments can never be wrongly reclaimed on a store assembled out of
     * compaction order (OAK-12400); genuine leftovers are then reclaimed by generational GC instead.
     */
    private boolean isDanglingFutureSegment(UUID id, GCGeneration generation) {
        if (skipDanglingFutureReclaim) {
            return false;
        }
        return (aheadOfRoot &= !id.equals(rootSegmentUUID)) && generation.isCompacted();
    }

    /**
     * Returns IDs of directly referenced segments. Since reference-based reclamation
     * is only used for bulk segments, data segment IDs are filtered out.
     */
    @Override
    public Set<UUID> initialReferences() {
        return segmentTracker.getReferencedSegmentIds().stream()
                .filter(SegmentId::isBulkSegmentId)
                .map(SegmentId::asUUID)
                .collect(Collectors.toSet());
    }

    @Override
    public boolean shouldReclaim(UUID id, GCGeneration generation, boolean referenced) {
        return isDanglingFutureSegment(id, generation) || isUnreferencedBulkSegment(id, referenced) ||
                isOldDataSegment(id, generation);
    }

    @Override
    public boolean shouldFollow(UUID from, UUID to) {
        return !isDataSegmentId(to.getLeastSignificantBits());
    }
}

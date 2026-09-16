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
     * When {@code true}, "dangling future segment" detection is order-independent: a compacted segment
     * is treated as an unused future segment iff its generation is strictly newer than the head
     * generation, instead of relying on the physical scan position of the last compacted root. The
     * positional heuristic (below) assumes segments are laid out in compaction write order (compacted
     * root written last), which does not hold for a segment store assembled out of compaction order
     * (e.g. a promoted cold standby, whose segments are written via {@code FileStore.writeSegment} in
     * sync-arrival order), where it can reclaim live, head-referenced compacted segments (OAK-12400).
     * <p>
     * Default {@code false} preserves the existing behaviour; set to {@code true} to opt in.
     */
    private static final String DANGLING_BY_GENERATION = "oak.segment.cleanup.danglingByGeneration";

    private final @NotNull SegmentTracker segmentTracker;
    private final @NotNull Predicate<GCGeneration> old;
    private final @Nullable UUID rootSegmentUUID;
    private final @NotNull GCGeneration headGeneration;
    private final boolean danglingByGeneration;
    private boolean aheadOfRoot;

    DefaultCleanupContext(@NotNull SegmentTracker tracker, @NotNull Predicate<GCGeneration> old, @NotNull String compactedRoot,
            @NotNull GCGeneration headGeneration) {
        this.segmentTracker = tracker;
        this.old = old;
        this.headGeneration = headGeneration;
        this.danglingByGeneration = SystemPropertySupplier
                .create(DANGLING_BY_GENERATION, Boolean.FALSE)
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
     * The reverse-order/position assumption only holds for a store written by compaction (the compacted root
     * is written last). It is violated by a store assembled out of compaction order (e.g. a promoted cold
     * standby), where live head-referenced compacted segments can be encountered before the root and get
     * wrongly reclaimed (OAK-12400). When {@link #DANGLING_BY_GENERATION} is enabled, detection is instead
     * generation-based: a compacted segment is an unused future segment iff its generation is strictly newer
     * than the head generation, which is independent of physical segment order.
     */
    private boolean isDanglingFutureSegment(UUID id, GCGeneration generation) {
        if (danglingByGeneration) {
            return generation.isCompacted() && generation.compareWith(headGeneration) > 0;
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

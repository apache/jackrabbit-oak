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
package org.apache.jackrabbit.oak.segment.file;

import java.io.IOException;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link org.apache.jackrabbit.oak.segment.file.DefaultCleanupContext}, covering the
 * OAK-12400 scenario: the positional "dangling future segment" heuristic can reclaim live,
 * head-referenced compacted segments on a store assembled out of compaction order (e.g. a promoted
 * cold standby). Skipping that reclamation is opt-in via the system property
 * {@code oak.segment.cleanup.skipDanglingFutureReclaim} (default {@code false} - existing behaviour).
 */
public class DefaultCleanupContextTest {

    private static final String SKIP_DANGLING_FUTURE_RECLAIM = "oak.segment.cleanup.skipDanglingFutureReclaim";

    /** A compacted generation referenced by the live head; must be retained. */
    private static final GCGeneration COMPACTED_HEAD_GENERATION = GCGeneration.newGCGeneration(26, 11, true);

    /** A genuinely unused future generation (aborted/uncommitted full compaction target). */
    private static final GCGeneration FUTURE_COMPACTED_GENERATION = GCGeneration.newGCGeneration(27, 12, true);

    /** An older compacted generation (below the head). */
    private static final GCGeneration OLD_COMPACTED_GENERATION = GCGeneration.newGCGeneration(25, 10, true);

    private static final String NULL_ROOT = RecordId.NULL.toString10();

    private SegmentTracker tracker;
    private String compactedRoot;
    private UUID rootSegment;
    private UUID liveCompactedDataSegment;

    @Before
    public void setUp() throws IOException {
        System.clearProperty(SKIP_DANGLING_FUTURE_RECLAIM);
        MemoryStore store = new MemoryStore();
        tracker = new SegmentTracker((msb, lsb) -> new SegmentId(store, msb, lsb));

        // A non-null gc.log root arms the positional heuristic (aheadOfRoot = true); its segment is
        // the sentinel that flips aheadOfRoot to false once encountered in the reverse scan.
        SegmentId root = tracker.newDataSegmentId();
        rootSegment = root.asUUID();
        compactedRoot = new RecordId(root, 0).toString10();

        // A distinct compacted data segment. On a promoted standby this stands for a live,
        // head-referenced compacted segment that the reverse scan reaches BEFORE the root sentinel.
        liveCompactedDataSegment = tracker.newDataSegmentId().asUUID();
    }

    @After
    public void tearDown() {
        System.clearProperty(SKIP_DANGLING_FUTURE_RECLAIM);
    }

    private DefaultCleanupContext newCleanupContext(Predicate<GCGeneration> old) {
        return newCleanupContext(old, compactedRoot);
    }

    private DefaultCleanupContext newCleanupContext(Predicate<GCGeneration> old, String root) {
        return new DefaultCleanupContext(tracker, old, root);
    }

    // ---- Default (legacy positional heuristic): oak.segment.cleanup.skipDanglingFutureReclaim=false ----

    /**
     * Reproduces OAK-12400: with the default positional heuristic, a live compacted segment
     * encountered before the gc.log root sentinel is (wrongly) reclaimable - the mechanism that
     * deletes referenced segments on a store not written in compaction order.
     */
    @Test
    public void reclaimsLiveCompactedSegmentBeforeRootByDefault() {
        DefaultCleanupContext context = newCleanupContext(generation -> false);
        Assert.assertTrue(context.shouldReclaim(liveCompactedDataSegment, COMPACTED_HEAD_GENERATION, false));
    }

    /**
     * Positional heuristic is correct on a compaction-ordered store: once the gc.log root sentinel is
     * encountered (reverse scan), subsequent compacted segments behind it are retained.
     */
    @Test
    public void retainsSegmentAfterRootEncounteredByDefault() {
        DefaultCleanupContext context = newCleanupContext(generation -> false);
        Assert.assertFalse(context.shouldReclaim(rootSegment, COMPACTED_HEAD_GENERATION, false));
        Assert.assertFalse(context.shouldReclaim(liveCompactedDataSegment, COMPACTED_HEAD_GENERATION, false));
    }

    /** With no compacted root in gc.log the positional heuristic is disarmed. */
    @Test
    public void disarmedWhenNoCompactedRootByDefault() {
        DefaultCleanupContext context = newCleanupContext(generation -> false, NULL_ROOT);
        Assert.assertFalse(context.shouldReclaim(liveCompactedDataSegment, COMPACTED_HEAD_GENERATION, false));
    }

    // ---- Opt-in skip: oak.segment.cleanup.skipDanglingFutureReclaim=true ----

    /** The fix: a live compacted segment reached before the root is retained (no positional reclamation). */
    @Test
    public void retainsLiveCompactedSegmentBeforeRootWhenEnabled() {
        System.setProperty(SKIP_DANGLING_FUTURE_RECLAIM, "true");
        DefaultCleanupContext context = newCleanupContext(generation -> false);
        Assert.assertFalse(context.shouldReclaim(liveCompactedDataSegment, COMPACTED_HEAD_GENERATION, false));
    }

    /** A genuine future compacted leftover is also retained (no positional reclamation); generational GC handles it later. */
    @Test
    public void retainsFutureCompactedSegmentWhenEnabled() {
        System.setProperty(SKIP_DANGLING_FUTURE_RECLAIM, "true");
        DefaultCleanupContext context = newCleanupContext(generation -> false);
        Assert.assertFalse(context.shouldReclaim(liveCompactedDataSegment, FUTURE_COMPACTED_GENERATION, false));
    }

    /** Skipping future reclamation does not disable ordinary generational reclamation of old data segments. */
    @Test
    public void stillReclaimsOldDataSegmentsWhenEnabled() {
        System.setProperty(SKIP_DANGLING_FUTURE_RECLAIM, "true");
        DefaultCleanupContext context = newCleanupContext(generation -> true);
        Assert.assertTrue(context.shouldReclaim(liveCompactedDataSegment, OLD_COMPACTED_GENERATION, false));
    }

    /** Skipping future reclamation does not disable reference-based reclamation of unreferenced bulk segments. */
    @Test
    public void stillReclaimsUnreferencedBulkSegmentsWhenEnabled() {
        System.setProperty(SKIP_DANGLING_FUTURE_RECLAIM, "true");
        DefaultCleanupContext context = newCleanupContext(generation -> false);
        UUID unreferencedBulk = tracker.newBulkSegmentId().asUUID();
        Assert.assertTrue(context.shouldReclaim(unreferencedBulk, OLD_COMPACTED_GENERATION, false));
    }

    // ---- shouldFollow: reference propagation is only for bulk segments ----

    @Test
    public void followsOnlyBulkSegmentReferences() {
        DefaultCleanupContext context = newCleanupContext(generation -> false);
        UUID from = tracker.newDataSegmentId().asUUID();
        UUID dataTarget = tracker.newDataSegmentId().asUUID();
        UUID bulkTarget = tracker.newBulkSegmentId().asUUID();
        Assert.assertFalse(context.shouldFollow(from, dataTarget));
        Assert.assertTrue(context.shouldFollow(from, bulkTarget));
    }
}

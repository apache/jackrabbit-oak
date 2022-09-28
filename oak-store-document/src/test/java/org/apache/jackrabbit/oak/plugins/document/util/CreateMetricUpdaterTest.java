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
package org.apache.jackrabbit.oak.plugins.document.util;

import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.oak.plugins.document.Collection.BLOBS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.getCreateStatsConsumer;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.getJournalStatsConsumer;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.isNodesCollectionUpdated;
import static org.apache.jackrabbit.oak.stats.StatsOptions.DEFAULT;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Junit for {@link CreateMetricUpdater}
 */
public class CreateMetricUpdaterTest extends BaseUpdaterTest {

    private final CreateMetricUpdater cMUWithoutThrottling = new CreateMetricUpdater(provider.getMeter(NODES_CREATE, DEFAULT),
            provider.getMeter(NODES_CREATE_SPLIT, DEFAULT),
            provider.getTimer(NODES_CREATE_TIMER, METRICS_ONLY),
            provider.getMeter(JOURNAL_CREATE, DEFAULT),
            provider.getTimer(JOURNAL_CREATE_TIMER, METRICS_ONLY));
    private final CreateMetricUpdater cMUWithThrottling = new CreateMetricUpdater(provider.getMeter(NODES_CREATE_THROTTLING, DEFAULT),
            provider.getMeter(NODES_CREATE_SPLIT_THROTTLING, DEFAULT),
            provider.getTimer(NODES_CREATE_THROTTLING_TIMER, METRICS_ONLY),
            provider.getMeter(JOURNAL_CREATE_THROTTLING, DEFAULT),
            provider.getTimer(JOURNAL_CREATE_THROTTLING_TIMER, METRICS_ONLY));

    @Test(expected = NullPointerException.class)
    public void updateWithNullNodesPredicate() {
        cMUWithThrottling.update(NODES, 100, ids, true, null, getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        fail("Shouldn't reach here");
    }

    @Test(expected = NullPointerException.class)
    public void updateWithNullCreateStatsConsumer() {
        cMUWithThrottling.update(NODES, 100, ids, true, isNodesCollectionUpdated(), null, c -> c == JOURNAL, getJournalStatsConsumer());
        fail("Shouldn't reach here");
    }

    @Test(expected = NullPointerException.class)
    public void updateWithNullJournalPredicate() {
        cMUWithoutThrottling.update(NODES, 100, ids, true, isNodesCollectionUpdated(), getCreateStatsConsumer(), null, getJournalStatsConsumer());
        fail("Shouldn't reach here");
    }

    @Test(expected = NullPointerException.class)
    public void updateWithNullJournalStatsConsumer() {
        cMUWithoutThrottling.update(NODES, 100, ids, true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, null);
        fail("Shouldn't reach here");
    }

    @Test
    public void updateNodes() {
        cMUWithoutThrottling.update(NODES, 100, ids, true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertNodesWithoutThrottling(2, 0, 50);

        cMUWithThrottling.update(NODES, 100, ids, true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertNodesWithThrottling(2, 0, 50);
    }

    @Test
    public void updateNodesWithPreviousDocId() {
        cMUWithoutThrottling.update(NODES, 100, of("15:p/a/b/c/d/e/f/g/h/i/j/k/l/m/r182f83543dd-0-0/3"), true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertNodesWithoutThrottling(1, 1, 100);

        cMUWithThrottling.update(NODES, 100, of("15:p/a/b/c/d/e/f/g/h/i/j/k/l/m/r182f83543dd-0-0/3"), true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertNodesWithThrottling(1, 1, 100);
    }

    @Test
    public void updateNodesNotSuccessfully() {
        cMUWithThrottling.update(NODES, 100, ids, false, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertNodesWithThrottling(0, 0, 0);
    }

    @Test
    public void updateNodesEmptyList() {
        cMUWithoutThrottling.update(NODES, 100, of(), true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertNodesWithoutThrottling(0, 0, 0);
    }

    @Test
    public void updateNonNodesJournalCollection() {
        cMUWithoutThrottling.update(BLOBS, 100, of(), true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertNodesWithoutThrottling(0, 0, 0);
        assertJournalWithoutThrottling(0, 0);

        cMUWithThrottling.update(CLUSTER_NODES, 100, of(), true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertNodesWithThrottling(0, 0, 0);
        assertJournalWithThrottling(0, 0);
    }

    @Test
    public void updateWithJournalSuccessfully() {
        cMUWithThrottling.update(JOURNAL, 100, ids, true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertJournalWithThrottling(2, 100);

        cMUWithoutThrottling.update(JOURNAL, 100, ids, true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertJournalWithoutThrottling(2, 100);
    }

    @Test
    public void updateWithJournalNotSuccessfully() {
        cMUWithThrottling.update(JOURNAL, 100, ids, false, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertJournalWithThrottling(2, 100);

        cMUWithoutThrottling.update(JOURNAL, 100, ids, false, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertJournalWithoutThrottling(2, 100);
    }

    @Test
    public void updateWithJournalEmptyList() {
        cMUWithThrottling.update(JOURNAL, 100, of(), true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertJournalWithThrottling(0, 100);

        cMUWithoutThrottling.update(JOURNAL, 100, of(), true, isNodesCollectionUpdated(), getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());
        assertJournalWithoutThrottling(0, 100);
    }

    // helper methods
    private void assertNodesWithThrottling(final long nodesCreate, final long nodesCreateSplit, final long nodesCreateTimer) {
        assertEquals(nodesCreate, getMeter(NODES_CREATE_THROTTLING).getCount());
        assertEquals(nodesCreateSplit, getMeter(NODES_CREATE_SPLIT_THROTTLING).getCount());
        assertEquals(nodesCreateTimer, getTimer(NODES_CREATE_THROTTLING_TIMER).getSnapshot().getMax());
    }

    private void assertNodesWithoutThrottling(final long nodesCreate, final long nodesCreateSplit, final long nodesCreateTimer) {
        assertEquals(nodesCreate, getMeter(NODES_CREATE).getCount());
        assertEquals(nodesCreateSplit, getMeter(NODES_CREATE_SPLIT).getCount());
        assertEquals(nodesCreateTimer, getTimer(NODES_CREATE_TIMER).getSnapshot().getMax());
    }

    private void assertJournalWithoutThrottling(final long journalCreate, final long journalCreateTimer) {
        assertEquals(journalCreate, getMeter(JOURNAL_CREATE).getCount());
        assertEquals(journalCreateTimer, getTimer(JOURNAL_CREATE_TIMER).getSnapshot().getMax());
    }

    private void assertJournalWithThrottling(final long journalCreate, final long journalCreateTimer) {
        assertEquals(journalCreate, getMeter(JOURNAL_CREATE_THROTTLING).getCount());
        assertEquals(journalCreateTimer, getTimer(JOURNAL_CREATE_THROTTLING_TIMER).getSnapshot().getMax());
    }
}
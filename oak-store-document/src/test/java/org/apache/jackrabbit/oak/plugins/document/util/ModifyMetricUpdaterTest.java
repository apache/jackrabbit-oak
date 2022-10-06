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

import org.apache.jackrabbit.oak.stats.MeterStats;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.getStatsConsumer;
import static org.apache.jackrabbit.oak.stats.StatsOptions.DEFAULT;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Junit for {@link ModifyMetricUpdater}
 */
public class ModifyMetricUpdaterTest extends BaseUpdaterTest {

    private final ModifyMetricUpdater mMUWithoutThrottling = new ModifyMetricUpdater(provider.getMeter(NODES_CREATE_UPSERT, DEFAULT),
            provider.getTimer(NODES_CREATE_UPSERT_TIMER, METRICS_ONLY),
            provider.getMeter(NODES_UPDATE, DEFAULT),
            provider.getTimer(NODES_UPDATE_TIMER, METRICS_ONLY),
            provider.getMeter(NODES_UPDATE_RETRY_COUNT, DEFAULT),
            provider.getMeter(NODES_UPDATE_FAILURE, DEFAULT));
    private final ModifyMetricUpdater mMUWithThrottling = new ModifyMetricUpdater(provider.getMeter(NODES_CREATE_UPSERT_THROTTLING, DEFAULT),
            provider.getTimer(NODES_CREATE_UPSERT_THROTTLING_TIMER, METRICS_ONLY),
            provider.getMeter(NODES_UPDATE_THROTTLING, DEFAULT),
            provider.getTimer(NODES_UPDATE_THROTTLING_TIMER, METRICS_ONLY),
            provider.getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING, DEFAULT),
            provider.getMeter(NODES_UPDATE_FAILURE_THROTTLING, DEFAULT));


    @Test(expected = NullPointerException.class)
    public void updateWithNullNodesPredicate() {
        mMUWithThrottling.update(NODES, 0, 100, true, true, null, getStatsConsumer(), getStatsConsumer(), MeterStats::mark, MeterStats::mark);
        fail("Shouldn't reach here");
    }

    @Test(expected = NullPointerException.class)
    public void updateWithNullCreateStatsConsumer() {
        mMUWithoutThrottling.update(NODES, 0, 100, true, true, c -> c == NODES, null, getStatsConsumer(), MeterStats::mark, MeterStats::mark);
        fail("Shouldn't reach here");
    }

    @Test(expected = NullPointerException.class)
    public void updateWithNullUpdateStatsConsumer() {
        mMUWithThrottling.update(NODES, 0, 100, true, true, c -> c == NODES, getStatsConsumer(), null, MeterStats::mark, MeterStats::mark);
        fail("Shouldn't reach here");
    }

    @Test(expected = NullPointerException.class)
    public void updateWithNullRetryStatsConsumer() {
        mMUWithoutThrottling.update(NODES, 0, 100, true, true, c -> c == NODES, getStatsConsumer(), getStatsConsumer(), null, MeterStats::mark);
        fail("Shouldn't reach here");
    }

    @Test(expected = NullPointerException.class)
    public void updateWithNullFailureStatsConsumer() {
        mMUWithThrottling.update(NODES, 0, 100, true, true, c -> c == NODES, getStatsConsumer(), getStatsConsumer(), MeterStats::mark, null);
        fail("Shouldn't reach here");
    }

    @Test
    public void updateNonNodesCollection() {
        mMUWithoutThrottling.update(JOURNAL, 0, 100, true, true, c -> c == NODES, getStatsConsumer(), getStatsConsumer(), MeterStats::mark, MeterStats::mark);
        assertCreateWithoutThrottling(0, 0);
        assertUpdateWithoutThrottling(0, 0);
        assertEquals(0, getMeter(NODES_UPDATE_RETRY_COUNT).getCount());
        assertEquals(0, getMeter(NODES_UPDATE_FAILURE).getCount());

        mMUWithThrottling.update(CLUSTER_NODES, 0, 100, true, true, c -> c == NODES, getStatsConsumer(), getStatsConsumer(), MeterStats::mark, MeterStats::mark);
        assertCreateWithThrottling(0, 0);
        assertUpdateWithThrottling(0, 0);
        assertEquals(0, getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING).getCount());
        assertEquals(0, getMeter(NODES_UPDATE_FAILURE_THROTTLING).getCount());
    }

    @Test
    public void updateNodesFailure() {
        mMUWithoutThrottling.update(NODES, 3, 100, false, true, c -> c == NODES, getStatsConsumer(), getStatsConsumer(), MeterStats::mark, MeterStats::mark);
        assertEquals(3, getMeter(NODES_UPDATE_RETRY_COUNT).getCount());
        assertEquals(1, getMeter(NODES_UPDATE_FAILURE).getCount());

        mMUWithThrottling.update(NODES, 3, 100, false, true, c -> c == NODES, getStatsConsumer(), getStatsConsumer(), MeterStats::mark, MeterStats::mark);
        assertEquals(3, getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING).getCount());
        assertEquals(1, getMeter(NODES_UPDATE_FAILURE_THROTTLING).getCount());
    }

    @Test
    public void updateNodesNewEntry() {
        mMUWithoutThrottling.update(NODES, 3, 100, true, true, c -> c == NODES, getStatsConsumer(), getStatsConsumer(), MeterStats::mark, MeterStats::mark);
        assertCreateWithoutThrottling(1, 100);
        assertUpdateWithoutThrottling(0, 0);
        assertEquals(3, getMeter(NODES_UPDATE_RETRY_COUNT).getCount());
        assertEquals(0, getMeter(NODES_UPDATE_FAILURE).getCount());

        mMUWithThrottling.update(NODES, 0, 100, true, true, c -> c == NODES, getStatsConsumer(), getStatsConsumer(), MeterStats::mark, MeterStats::mark);
        assertCreateWithThrottling(1, 100);
        assertUpdateWithThrottling(0, 0);
        assertEquals(0, getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING).getCount());
        assertEquals(0, getMeter(NODES_UPDATE_FAILURE_THROTTLING).getCount());
    }

    @Test
    public void updateNodesExistingEntry() {
        mMUWithoutThrottling.update(NODES, 3, 100, true, false, c -> c == NODES, getStatsConsumer(), getStatsConsumer(), MeterStats::mark, MeterStats::mark);
        assertCreateWithoutThrottling(0, 0);
        assertUpdateWithoutThrottling(1, 100);
        assertEquals(3, getMeter(NODES_UPDATE_RETRY_COUNT).getCount());
        assertEquals(0, getMeter(NODES_UPDATE_FAILURE).getCount());

        mMUWithThrottling.update(NODES, 0, 100, true, false, c -> c == NODES, getStatsConsumer(), getStatsConsumer(), MeterStats::mark, MeterStats::mark);
        assertCreateWithThrottling(0, 0);
        assertUpdateWithThrottling(1, 100);
        assertEquals(0, getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING).getCount());
        assertEquals(0, getMeter(NODES_UPDATE_FAILURE_THROTTLING).getCount());
    }

    // helper methods
    private void assertUpdateWithoutThrottling(final long nodesUpdate, final long nodesUpdateTimer) {
        assertEquals(nodesUpdate, getMeter(NODES_UPDATE).getCount());
        assertEquals(nodesUpdateTimer, getTimer(NODES_UPDATE_TIMER).getSnapshot().getMax());
    }

    private void assertUpdateWithThrottling(final long nodesUpdate, final long nodesUpdateTimer) {
        assertEquals(nodesUpdate, getMeter(NODES_UPDATE_THROTTLING).getCount());
        assertEquals(nodesUpdateTimer, getTimer(NODES_UPDATE_THROTTLING_TIMER).getSnapshot().getMax());
    }

    private void assertCreateWithoutThrottling(final long nodesCreate, final long nodesCreateTimer) {
        assertEquals(nodesCreate, getMeter(NODES_CREATE_UPSERT).getCount());
        assertEquals(nodesCreateTimer, getTimer(NODES_CREATE_UPSERT_TIMER).getSnapshot().getMax());
    }

    private void assertCreateWithThrottling(final long nodesCreate, final long nodesCreateTimer) {
        assertEquals(nodesCreate, getMeter(NODES_CREATE_UPSERT_THROTTLING).getCount());
        assertEquals(nodesCreateTimer, getTimer(NODES_CREATE_UPSERT_THROTTLING_TIMER).getSnapshot().getMax());
    }
}
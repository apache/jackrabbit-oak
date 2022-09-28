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
package org.apache.jackrabbit.oak.plugins.document.util;

import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.getCreateStatsConsumer;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.isNodesCollectionUpdated;
import static org.apache.jackrabbit.oak.stats.StatsOptions.DEFAULT;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Junit for {@link UpsertMetricUpdater}
 */
public class UpsertMetricUpdaterTest extends BaseUpdaterTest {

    private final UpsertMetricUpdater uMUWithoutThrottling = new UpsertMetricUpdater(provider.getMeter(NODES_CREATE_UPSERT, DEFAULT),
            provider.getMeter(NODES_CREATE_SPLIT, DEFAULT),
            provider.getTimer(NODES_CREATE_UPSERT_TIMER, METRICS_ONLY));
    private final UpsertMetricUpdater uMUWithThrottling = new UpsertMetricUpdater(provider.getMeter(NODES_CREATE_UPSERT_THROTTLING, DEFAULT),
            provider.getMeter(NODES_CREATE_SPLIT_THROTTLING, DEFAULT),
            provider.getTimer(NODES_CREATE_UPSERT_THROTTLING_TIMER, METRICS_ONLY));

    @Test(expected = NullPointerException.class)
    public void updateWithNullPredicate() {
        uMUWithoutThrottling.update(NODES, 100, ids, null, getCreateStatsConsumer());
        fail("Shouldn't reach here");
    }

    @Test(expected = NullPointerException.class)
    public void updateWithNullStatsConsumer() {
        uMUWithThrottling.update(NODES, 100, ids, isNodesCollectionUpdated(), null);
        fail("Shouldn't reach here");
    }

    @Test
    public void updateNodesEmptyList() {
        uMUWithoutThrottling.update(NODES, 100, of(), isNodesCollectionUpdated(), getCreateStatsConsumer());
        assertWithoutThrottling(0, 0, 0);
    }

    @Test
    public void updateNonNodesCollection() {
        uMUWithThrottling.update(JOURNAL, 100, of(), isNodesCollectionUpdated(), getCreateStatsConsumer());
        assertWithThrottling(0, 0, 0);
    }

    @Test
    public void updateNodes() {
        uMUWithThrottling.update(NODES, 100, of("a", "b"), isNodesCollectionUpdated(), getCreateStatsConsumer());
        assertWithThrottling(2, 0, 50);

        uMUWithoutThrottling.update(NODES, 100, of("a", "b"), isNodesCollectionUpdated(), getCreateStatsConsumer());
        assertWithoutThrottling(2, 0, 50);
    }

    @Test
    public void updateNodesWithPreviousDocId() {
        uMUWithThrottling.update(NODES, 100, of("15:p/a/b/c/d/e/f/g/h/i/j/k/l/m/r182f83543dd-0-0/3"), isNodesCollectionUpdated(), getCreateStatsConsumer());
        assertWithThrottling(1, 1, 100);

        uMUWithoutThrottling.update(NODES, 100, of("15:p/a/b/c/d/e/f/g/h/i/j/k/l/m/r182f83543dd-0-0/3"), isNodesCollectionUpdated(), getCreateStatsConsumer());
        assertWithoutThrottling(1, 1, 100);
    }

    // helper methods
    private void assertWithThrottling(final long nodesCreate, final long nodesCreateSplit, final long nodesCreateTimer) {
        assertEquals(nodesCreate, getMeter(NODES_CREATE_UPSERT_THROTTLING).getCount());
        assertEquals(nodesCreateSplit, getMeter(NODES_CREATE_SPLIT_THROTTLING).getCount());
        assertEquals(nodesCreateTimer, getTimer(NODES_CREATE_UPSERT_THROTTLING_TIMER).getSnapshot().getMax());
    }

    private void assertWithoutThrottling(final long nodesCreate, final long nodesCreateSplit, final long nodesCreateTimer) {
        assertEquals(nodesCreate, getMeter(NODES_CREATE_UPSERT).getCount());
        assertEquals(nodesCreateSplit, getMeter(NODES_CREATE_SPLIT).getCount());
        assertEquals(nodesCreateTimer, getTimer(NODES_CREATE_UPSERT_TIMER).getSnapshot().getMax());
    }
}
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

import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.getStatsConsumer;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.isNodesCollectionUpdated;
import static org.apache.jackrabbit.oak.stats.StatsOptions.DEFAULT;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Junit for {@link RemoveMetricUpdater}
 */
public class RemoveMetricUpdaterTest extends BaseUpdaterTest {

    private final RemoveMetricUpdater rMUWithoutThrottling = new RemoveMetricUpdater(provider.getMeter(NODES_REMOVE, DEFAULT),
            provider.getTimer(NODES_REMOVE_TIMER, METRICS_ONLY));
    private final RemoveMetricUpdater rMUWithThrottling = new RemoveMetricUpdater(provider.getMeter(NODES_REMOVE_THROTTLING, DEFAULT),
            provider.getTimer(NODES_REMOVE_THROTTLING_TIMER, METRICS_ONLY));

    @Test(expected = NullPointerException.class)
    public void updateNodesNullConsumer() {
        rMUWithThrottling.update(NODES, 0, 100, isNodesCollectionUpdated(), null);
        fail("Shouldn't reach here");
    }

    @Test(expected = NullPointerException.class)
    public void updateNodesNullPredicate() {
        rMUWithoutThrottling.update(NODES, 0, 100, null, getStatsConsumer());
        fail("Shouldn't reach here");
    }

    @Test
    public void updateNodesNoRemoveCount() {
        rMUWithoutThrottling.update(NODES, 0, 100, isNodesCollectionUpdated(), getStatsConsumer());
        assertWithThrottling(0, 0);
        assertWithoutThrottling(0, 0);
    }

    @Test
    public void updateNonNodesCollection() {
        rMUWithThrottling.update(JOURNAL, 10, 100, isNodesCollectionUpdated(), getStatsConsumer());
        assertWithThrottling(0, 0);
        assertWithoutThrottling(0, 0);
    }

    @Test
    public void updateNodes() {
        rMUWithThrottling.update(NODES, 1, 100, isNodesCollectionUpdated(), getStatsConsumer());
        assertWithThrottling(1, 100);

        rMUWithoutThrottling.update(NODES, 1, 100, isNodesCollectionUpdated(), getStatsConsumer());
        assertWithoutThrottling(1, 100);
    }

    // helper methods
    private void assertWithoutThrottling(final long nodesRemoved, final long nodesRemovedTimer) {
        assertEquals(nodesRemoved, getMeter(NODES_REMOVE).getCount());
        assertEquals(nodesRemovedTimer, getTimer(NODES_REMOVE_TIMER).getSnapshot().getMax());
    }

    private void assertWithThrottling(final long nodesRemoved, final long nodesRemovedTimer) {
        assertEquals(nodesRemoved, getMeter(NODES_REMOVE_THROTTLING).getCount());
        assertEquals(nodesRemovedTimer, getTimer(NODES_REMOVE_THROTTLING_TIMER).getSnapshot().getMax());
    }
}
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite;

import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CompositeNodeStoreStatsTest {

    @Test
    public void testNodeCountsMaxSizeLimit() {
        CompositeNodeStoreStats stats = new CompositeNodeStoreStats(StatisticsProvider.NOOP, "NODE", true, 6, 20);
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < i + 5; j++) {
                stats.onCreateNodeObject("/xyz" + i);
            }
        }

        assertEquals(9, stats.getMaxNodePathCount());
        assertEquals(5, stats.getNodePathCountsMap().size());

        stats.onCreateNodeObject("/xyz" + 6);

        assertEquals(8, stats.getMaxNodePathCount());
        assertEquals(5, stats.getNodePathCountsMap().size());
        for (int i = 0; i < 5; i++) {
            assertEquals("Invalid count for /xyz" + i, (long) i + 4, (long) stats.getNodePathCountsMap().get("/xyz" + i));
        }

        for (int i = 0; i < 3; i++) {
            stats.onCreateNodeObject("/xyz" + 6);
        }
        assertEquals(5, stats.getMaxNodePathCount());
        assertEquals(5, stats.getNodePathCountsMap().size());

        stats.onCreateNodeObject("/xyz" + 6); // this should remove the smallest value
        assertEquals(4, stats.getNodePathCountsMap().size());

        stats.onCreateNodeObject("/xyz" + 6); // this should add the new value
        assertEquals(5, stats.getNodePathCountsMap().size());
    }

    @Test
    public void testNodeCountsMaxValueLimit() {
        CompositeNodeStoreStats stats = new CompositeNodeStoreStats(StatisticsProvider.NOOP, "NODE", true, 5, 20);
        for (int i = 0; i < 19; i++) {
            stats.onCreateNodeObject("/xyz");
        }
        assertEquals(19, stats.getMaxNodePathCount());
        assertEquals(19l, (long) stats.getNodePathCountsMap().get("/xyz"));

        stats.onCreateNodeObject("/xyz");
        assertEquals(10, stats.getMaxNodePathCount());
        assertEquals(10l, (long) stats.getNodePathCountsMap().get("/xyz"));

    }
}

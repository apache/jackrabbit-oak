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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilder.newMongoDocumentNodeStoreBuilder;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongoDocumentStoreMetricsTest extends AbstractMongoConnectionTest {

    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private StatisticsProvider statsProvider = new DefaultStatisticsProvider(executorService);

    @After
    public void after() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void updateCounters() throws Exception {
        MongoDocumentStore store = new MongoDocumentStore(
                mongoConnection.getDB(), newMongoDocumentNodeStoreBuilder());
        MongoDocumentStoreMetrics metrics = new MongoDocumentStoreMetrics(store, statsProvider);
        metrics.run();
        // document for root node
        assertEquals(1, getCount("MongoDB.nodes.count"));
        // one cluster node
        assertEquals(1, getCount("MongoDB.clusterNodes.count"));

        List<UpdateOp> updates = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            updates.add(new UpdateOp("id-" + i, true));
        }
        assertTrue(store.create(Collection.NODES, updates));

        metrics.run();
        assertEquals(11, getCount("MongoDB.nodes.count"));
    }

    private long getCount(String name) {
        return statsProvider.getCounterStats(name, METRICS_ONLY).getCount();
    }
}

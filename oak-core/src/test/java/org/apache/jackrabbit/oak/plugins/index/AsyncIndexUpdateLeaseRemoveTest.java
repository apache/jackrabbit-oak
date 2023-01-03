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
package org.apache.jackrabbit.oak.plugins.index;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate.ASYNC;
import static org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate.leasify;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AsyncIndexUpdateLeaseRemoveTest {
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private MetricStatisticsProvider statsProvider =
            new MetricStatisticsProvider(ManagementFactory.getPlatformMBeanServer(), executor);

    @After
    public void shutDown() {
        statsProvider.close();
        new ExecutorCloser(executor).close();
    }

    @Test
    public void releaseLeaseOnlyAfterIndexingLanePause() throws Exception {
        NodeStore store = new MemoryNodeStore();
        IndexEditorProvider provider = new PropertyIndexEditorProvider();
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider) {
            @Override
            protected AsyncUpdateCallback newAsyncUpdateCallback(NodeStore store, String name, long leaseTimeOut,
                                                                 String beforeCheckpoint,
                                                                 AsyncIndexStats indexStats, AtomicBoolean stopFlag) {
                return new AsyncUpdateCallback(store, name, leaseTimeOut, beforeCheckpoint,
                        indexStats, stopFlag) {
                    @Override
                    void close() throws CommitFailedException {
                        // overridden close method so that lease is not deleted on async cycle completion.
                        //super.close();
                    }
                };
            }
        };
        async.setLeaseTimeOut(250);
        async.run();
        Long leaseValue = getLeaseValue(store);
        assertNotNull(leaseValue);
        async.getIndexStats().abortAndPause();
        async.getIndexStats().releaseLeaseForPausedLane();
        async.run();
        leaseValue = getLeaseValue(store);
        assertEquals(IndexStatsMBean.STATUS_DONE, async.getIndexStats().getStatus());
        assertNull(leaseValue);

        async.getIndexStats().resume();
        async.run();
        leaseValue = getLeaseValue(store);
        assertNotNull(leaseValue);
        // Not pausing indexing lane now.
        // async.getIndexStats().abortAndPause();

        // now as lane is not paused lease will not be released.
        async.getIndexStats().releaseLeaseForPausedLane();
        async.run();
        leaseValue = getLeaseValue(store);
        System.out.println("new lease value:" + leaseValue);
        assertEquals(IndexStatsMBean.STATUS_DONE, async.getIndexStats().getStatus());
        assertNotNull(leaseValue);
    }

    private Long getLeaseValue(NodeStore store) {
        NodeBuilder builder1 = store.getRoot().builder();
        NodeBuilder async1 = builder1.getChildNode(ASYNC);
        return async1.getProperty(leasify("async")) == null ? null : async1.getProperty(leasify("async")).getValue(Type.LONG);
    }
}

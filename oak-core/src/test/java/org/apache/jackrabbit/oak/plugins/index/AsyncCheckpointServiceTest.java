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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.observation.ChangeCollectorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class AsyncCheckpointServiceTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final MemoryNodeStore nodeStore = new AsyncCheckpointServiceTest.FakeClusterableMemoryNodeStore();
    private final AsyncCheckpointService service = new AsyncCheckpointService();

    @Test
    public void asyncReg() {
        injectDefaultServices();
        String name1 = "checkpoint-async-test-1";
        String name2 = "checkpoint-async-test-2";
        String name3 = "checkpoint-async-test-3";
        Map<String,Object> config1 = ImmutableMap.<String, Object>of(
                "name", "checkpoint-async-test-1",
                "enable", true,
                "minConcurrentCheckpoints", 3L,
                "checkpointLifetime", 60 * 60 * 24L,
                "timeIntervalBetweenCheckpoints", 60 * 15L
        );

        Map<String,Object> config2 = ImmutableMap.<String, Object>of(
                "name", "checkpoint-async-test-2",
                "enable", false,
                "minConcurrentCheckpoints", 3L,
                "checkpointLifetime", 60 * 60 * 24L,
                "timeIntervalBetweenCheckpoints", 60 * 15L
        );

        Map<String,Object> config3 = ImmutableMap.<String, Object>of(
                "name", "checkpoint-async-test-3",
                "enable", true,
                "minConcurrentCheckpoints", 4L,
                "checkpointLifetime", 60 * 24L,
                "timeIntervalBetweenCheckpoints", 60 * 15L
        );
        MockOsgi.activate(service, context.bundleContext(), config1);
        MockOsgi.activate(service, context.bundleContext(), config2);
        MockOsgi.activate(service, context.bundleContext(), config3);
        assertEquals(1, context.getServices(Runnable.class, "(oak.checkpoint.async="+name1+")").length);
        assertEquals(0, context.getServices(Runnable.class, "(oak.checkpoint.async="+name2+")").length);
        assertEquals(1, context.getServices(Runnable.class, "(oak.checkpoint.async="+name3+")").length);
        AsyncCheckpointCreator checkpointCreator1 = getCheckpointCreator("checkpoint-async-test-1");
        assertEquals(3, checkpointCreator1.getMinConcurrentCheckpoints());
        assertEquals(60 * 60 * 24, checkpointCreator1.getCheckpointLifetimeInSeconds());
        AsyncCheckpointCreator checkpointCreator3 = getCheckpointCreator("checkpoint-async-test-3");
        assertEquals(4, checkpointCreator3.getMinConcurrentCheckpoints());
        assertEquals(60 * 24, checkpointCreator3.getCheckpointLifetimeInSeconds());
        MockOsgi.deactivate(service, context.bundleContext());
        assertNull(context.getService(Runnable.class));
    }


    private AsyncCheckpointCreator getCheckpointCreator(String name) {
        return (AsyncCheckpointCreator) context.getServices(Runnable.class, "(oak.checkpoint.async="+name+")")[0];
    }

    private void injectDefaultServices() {
        context.registerService(NodeStore.class, nodeStore);
        MockOsgi.injectServices(service, context.bundleContext());
    }

    private static class FakeClusterableMemoryNodeStore extends MemoryNodeStore implements Clusterable {
        @NotNull
        @Override
        public String getInstanceId() {
            return "foo";
        }

        @Override
        public String getVisibilityToken() {
            return "";
        }

        @Override
        public boolean isVisible(String visibilityToken, long maxWaitMillis) throws InterruptedException {
            return true;
        }
    }
}

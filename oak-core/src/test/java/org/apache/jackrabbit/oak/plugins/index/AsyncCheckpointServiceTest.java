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

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AsyncCheckpointServiceTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final MemoryNodeStore nodeStore = new AsyncCheckpointServiceTest.FakeClusterableMemoryNodeStore();
    private final AsyncCheckpointService service = new AsyncCheckpointService();

    /**
     * This test is used to verify the registration of the AsyncCheckpointService with the OSGI context.
     * The test verifies this with 3 different configurations of the AsyncCheckpointService, 2 of them enabled and 1 disabled.
     */
    @Test
    public void asyncReg() {
        injectDefaultServices();
        // Create 3 configurations of the AsyncCheckpointService, 2 of them enabled and 1 disabled.
        String name1 = "checkpoint-async-test-1";
        String name2 = "checkpoint-async-test-2";
        String name3 = "checkpoint-async-test-3";
        Map<String,Object> config1 = Map.of(
                "name", "checkpoint-async-test-1",
                "enable", true,
                "minConcurrentCheckpoints", 3L,
                "maxConcurrentCheckpoints", 10L,
                "checkpointLifetime", 60 * 60 * 24L,
                "timeIntervalBetweenCheckpoints", 60 * 15L
        );

        Map<String,Object> config2 = Map.of(
                "name", "checkpoint-async-test-2",
                "enable", false,
                "minConcurrentCheckpoints", 3L,
                "maxConcurrentCheckpoints", 10L,
                "checkpointLifetime", 60 * 60 * 24L,
                "timeIntervalBetweenCheckpoints", 60 * 15L
        );

        Map<String,Object> config3 = Map.of(
                "name", "checkpoint-async-test-3",
                "enable", true,
                "minConcurrentCheckpoints", 4L,
                "maxConcurrentCheckpoints", 2L,
                "checkpointLifetime", 60 * 24L,
                "timeIntervalBetweenCheckpoints", 60 * 15L
        );
        // Activate the service with the above 3 configurations.
        MockOsgi.activate(service, context.bundleContext(), config1);
        MockOsgi.activate(service, context.bundleContext(), config2);
        MockOsgi.activate(service, context.bundleContext(), config3);
        // Verify that the configs that are enabled are registered with the OSGI context and the one that is disabled is not.
        assertEquals(1, context.getServices(Runnable.class, "(oak.checkpoint.async="+name1+")").length);
        assertEquals(0, context.getServices(Runnable.class, "(oak.checkpoint.async="+name2+")").length);
        assertEquals(1, context.getServices(Runnable.class, "(oak.checkpoint.async="+name3+")").length);

        // Get the instances fo the async tasks that are registered as per the enabled configs and verify the values of the
        // configured minConcurrentCheckpoints and checkpointLifetimeInSeconds.
        AsyncCheckpointCreator checkpointCreator1 = getCheckpointCreator("checkpoint-async-test-1");
        assertEquals(3, checkpointCreator1.getMinConcurrentCheckpoints());
        assertEquals(10, checkpointCreator1.getMaxConcurrentCheckpoints());
        assertEquals(60 * 60 * 24, checkpointCreator1.getCheckpointLifetimeInSeconds());
        AsyncCheckpointCreator checkpointCreator3 = getCheckpointCreator("checkpoint-async-test-3");
        assertEquals(4, checkpointCreator3.getMinConcurrentCheckpoints());
        assertEquals(5, checkpointCreator3.getMaxConcurrentCheckpoints());
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

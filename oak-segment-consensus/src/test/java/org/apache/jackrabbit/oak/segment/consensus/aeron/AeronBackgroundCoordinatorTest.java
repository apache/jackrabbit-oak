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
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import io.aeron.cluster.service.Cluster;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AeronBackgroundCoordinatorTest {

    @Test
    public void scheduleGenesisCreationIfMissingQueuesTask() {
        RecordingTaskScheduler scheduler = new RecordingTaskScheduler();
        AeronBackgroundCoordinator coordinator = new AeronBackgroundCoordinator(scheduler, 7L, 11L, 13L);
        AtomicInteger calls = new AtomicInteger();

        boolean scheduled = coordinator.scheduleGenesisCreationIfMissing(new MemoryNodeStore(), calls::incrementAndGet);

        assertTrue(scheduled);
        assertEquals(1, scheduler.tasks.size());
        assertEquals("genesis-creator", scheduler.tasks.get(0).name);
        assertEquals(7L, scheduler.tasks.get(0).delayMs);
        scheduler.runNext();
        assertEquals(1, calls.get());
    }

    @Test
    public void scheduleGenesisCreationIfMissingSkipsExistingGenesis() throws Exception {
        RecordingTaskScheduler scheduler = new RecordingTaskScheduler();
        AeronBackgroundCoordinator coordinator = new AeronBackgroundCoordinator(scheduler, 7L, 11L, 13L);
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        seedGenesis(nodeStore);

        boolean scheduled = coordinator.scheduleGenesisCreationIfMissing(nodeStore, () -> {
        });

        assertFalse(scheduled);
        assertTrue(scheduler.tasks.isEmpty());
    }

    @Test
    public void scheduleLeaderDiscoveryRetriesUntilLeaderAppears() {
        RecordingTaskScheduler scheduler = new RecordingTaskScheduler();
        AeronBackgroundCoordinator coordinator = new AeronBackgroundCoordinator(scheduler, 7L, 11L, 13L);
        LeaderDiscoveryService leaderDiscoveryService = mock(LeaderDiscoveryService.class);
        when(leaderDiscoveryService.discoverLeader(Cluster.Role.FOLLOWER)).thenReturn(null, "http://leader:8080");
        AtomicReference<String> leader = new AtomicReference<>();

        coordinator.scheduleLeaderDiscovery(() -> Cluster.Role.FOLLOWER, leaderDiscoveryService, leader::set);

        assertEquals(1, scheduler.tasks.size());
        assertEquals("aeron-leader-discovery", scheduler.tasks.get(0).name);
        assertEquals(11L, scheduler.tasks.get(0).delayMs);
        scheduler.runNext();
        assertNull(leader.get());
        assertEquals(1, scheduler.tasks.size());
        assertEquals("aeron-leader-discovery-retry", scheduler.tasks.get(0).name);
        assertEquals(13L, scheduler.tasks.get(0).delayMs);
        scheduler.runNext();
        assertEquals("http://leader:8080", leader.get());
    }

    private static void seedGenesis(MemoryNodeStore nodeStore) throws Exception {
        NodeBuilder root = nodeStore.getRoot().builder();
        root.child("oak-chain")
            .child("00")
            .child("00")
            .child("00")
            .child("0x0000000000000000000000000000000000000000")
            .child("content")
            .child("genesis");
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static final class RecordingTaskScheduler implements AeronBackgroundCoordinator.TaskScheduler {
        private final List<ScheduledTask> tasks = new ArrayList<>();

        @Override
        public void schedule(String name, long delayMs, Runnable task) {
            tasks.add(new ScheduledTask(name, delayMs, task));
        }

        @Override
        public void close() {
            tasks.clear();
        }

        void runNext() {
            ScheduledTask task = tasks.remove(0);
            task.runnable.run();
        }
    }

    private static final class ScheduledTask {
        private final String name;
        private final long delayMs;
        private final Runnable runnable;

        private ScheduledTask(String name, long delayMs, Runnable runnable) {
            this.name = name;
            this.delayMs = delayMs;
            this.runnable = runnable;
        }
    }
}

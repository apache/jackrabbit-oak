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
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Owns delayed Aeron background tasks that were previously spawned ad hoc.
 */
class AeronBackgroundCoordinator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AeronBackgroundCoordinator.class);
    private static final String GENESIS_ADDRESS = "0x0000000000000000000000000000000000000000";

    interface TaskScheduler {
        void schedule(String name, long delayMs, Runnable task);

        void close();
    }

    private static final class ExecutorTaskScheduler implements TaskScheduler {
        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "aeron-background");
            thread.setDaemon(true);
            return thread;
        });
        private final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public void schedule(String name, long delayMs, Runnable task) {
            if (closed.get()) {
                return;
            }
            try {
                executor.schedule(task, delayMs, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                log.debug("Skipping scheduled task {} during shutdown", name);
            }
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            executor.shutdownNow();
        }
    }

    private final TaskScheduler scheduler;
    private final long genesisDelayMs;
    private final long leaderDiscoveryInitialDelayMs;
    private final long leaderDiscoveryRetryDelayMs;

    AeronBackgroundCoordinator() {
        this(new ExecutorTaskScheduler(), 2000L, 3000L, 5000L);
    }

    AeronBackgroundCoordinator(TaskScheduler scheduler,
                               long genesisDelayMs,
                               long leaderDiscoveryInitialDelayMs,
                               long leaderDiscoveryRetryDelayMs) {
        this.scheduler = scheduler;
        this.genesisDelayMs = genesisDelayMs;
        this.leaderDiscoveryInitialDelayMs = leaderDiscoveryInitialDelayMs;
        this.leaderDiscoveryRetryDelayMs = leaderDiscoveryRetryDelayMs;
    }

    boolean scheduleGenesisCreationIfMissing(NodeStore nodeStore, Runnable createGenesisTask) {
        if (nodeStore == null || createGenesisTask == null) {
            return false;
        }
        try {
            if (hasGenesisContent(nodeStore)) {
                return false;
            }
        } catch (RuntimeException e) {
            log.warn("Failed to inspect genesis state: {}", e.getMessage());
            return false;
        }

        scheduler.schedule("genesis-creator", genesisDelayMs, () -> {
            try {
                createGenesisTask.run();
            } catch (RuntimeException e) {
                log.error("❌ Failed to create genesis", e);
            }
        });
        return true;
    }

    boolean hasGenesisContent(NodeStore nodeStore) {
        if (nodeStore == null) {
            return false;
        }
        return nodeStore.getRoot()
            .getChildNode("oak-chain")
            .getChildNode("00")
            .getChildNode("00")
            .getChildNode("00")
            .getChildNode(GENESIS_ADDRESS)
            .getChildNode("content")
            .getChildNode("genesis")
            .exists();
    }

    void scheduleLeaderDiscovery(Supplier<Cluster.Role> localRole,
                                 LeaderDiscoveryService leaderDiscoveryService,
                                 Consumer<String> leaderConsumer) {
        if (localRole == null || leaderDiscoveryService == null || leaderConsumer == null) {
            return;
        }
        scheduleLeaderDiscoveryAttempt(localRole, leaderDiscoveryService, leaderConsumer, false);
    }

    private void scheduleLeaderDiscoveryAttempt(Supplier<Cluster.Role> localRole,
                                                LeaderDiscoveryService leaderDiscoveryService,
                                                Consumer<String> leaderConsumer,
                                                boolean retry) {
        long delayMs = retry ? leaderDiscoveryRetryDelayMs : leaderDiscoveryInitialDelayMs;
        String taskName = retry ? "aeron-leader-discovery-retry" : "aeron-leader-discovery";
        scheduler.schedule(taskName, delayMs, () -> {
            try {
                String leaderUrl = leaderDiscoveryService.discoverLeader(localRole.get());
                if (leaderUrl != null) {
                    leaderConsumer.accept(leaderUrl);
                } else if (!retry) {
                    scheduleLeaderDiscoveryAttempt(localRole, leaderDiscoveryService, leaderConsumer, true);
                }
            } catch (RuntimeException e) {
                log.warn("Aeron Cluster leader discovery failed: {}", e.getMessage());
            }
        });
    }

    void schedule(String name, long delayMs, Runnable task) {
        if (task == null) {
            return;
        }
        scheduler.schedule(name, delayMs, task);
    }

    @Override
    public void close() {
        scheduler.close();
    }
}

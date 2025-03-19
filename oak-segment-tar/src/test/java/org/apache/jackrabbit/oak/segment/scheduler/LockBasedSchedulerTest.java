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
package org.apache.jackrabbit.oak.segment.scheduler;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreStats;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Test;

public class LockBasedSchedulerTest {

    private NodeState getRoot(Scheduler scheduler) {
        return scheduler.getHeadNodeState().getChildNode("root");
    }

    /**
     * OAK-7162
     *
     * This test guards against race conditions which may happen when the head
     * state in {@link Revisions} is changed from outside the scheduler. If a
     * race condition happens at that point, data from a single commit will be
     * lost.
     */
    @Test
    public void testSimulatedRaceOnRevisions() throws Exception {
        final MemoryStore ms = new MemoryStore();
        StatisticsProvider statsProvider = StatisticsProvider.NOOP;
        SegmentNodeStoreStats stats = new SegmentNodeStoreStats(statsProvider);
        final LockBasedScheduler scheduler = LockBasedScheduler.builder(ms.getRevisions(), ms.getReader(), stats)
                .build();

        final RecordId initialHead = ms.getRevisions().getHead();
        ExecutorService executorService = newFixedThreadPool(10);
        final AtomicInteger count = new AtomicInteger();
        final Random rand = new Random();

        try {
            Callable<PropertyState> commitTask = new Callable<PropertyState>() {
                @Override
                public PropertyState call() throws Exception {
                    String property = "prop" + count.incrementAndGet();
                    Commit commit = createCommit(scheduler, property, "value");
                    SegmentNodeState result = (SegmentNodeState) scheduler.schedule(commit);

                    return result.getProperty(property);
                }
            };

            Callable<Void> parallelTask = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Thread.sleep(rand.nextInt(10));
                    ms.getRevisions().setHead(ms.getRevisions().getHead(), initialHead);
                    return null;
                }
            };

            List<Future<?>> results = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                results.add(executorService.submit(commitTask));
                executorService.submit(parallelTask);
            }

            for (Future<?> result : results) {
                assertNotNull(
                        "PropertyState must not be null! The corresponding commit got lost because of a race condition.",
                        result.get());
            }
        } finally {
            new ExecutorCloser(executorService).close();
        }
    }

    private Commit createCommit(final Scheduler scheduler, final String property, String value) {
        NodeBuilder a = getRoot(scheduler).builder();
        a.setProperty(property, value);
        Commit commit = new Commit(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        return commit;
    }
}

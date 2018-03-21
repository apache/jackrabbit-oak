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

package org.apache.jackrabbit.oak.segment;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.junit.Test;

public class CommitsTrackerTest {

    static class DequedCommitTask implements Runnable {
        private final CommitsTracker commitsTracker;
        private final String threadName;
        private final CountDownLatch latch;

        public DequedCommitTask(CommitsTracker commitsTracker, String threadName, CountDownLatch latch) {
            this.commitsTracker = commitsTracker;
            this.threadName = threadName;
            this.latch = latch;
        }

        @Override
        public void run() {
            Thread.currentThread().setName(threadName);
            commitsTracker.trackDequedCommitOf(Thread.currentThread());
            latch.countDown();
        }
    }

    @Test
    public void testSizeConstraints() throws InterruptedException {
        CommitsTracker commitsTracker = new CommitsTracker(10, false);
        ExecutorService executorService = newFixedThreadPool(30);
        final CountDownLatch addLatch = new CountDownLatch(25);

        Runnable executedCommitTask = () -> {
            commitsTracker.trackExecutedCommitOf(Thread.currentThread());
            addLatch.countDown();
        };

        Runnable queuedCommitTask = () -> {
            Thread t = Thread.currentThread();
            commitsTracker.trackQueuedCommitOf(t);
            addLatch.countDown();
        };

        try {
            for (int i = 0; i < 20; i++) {
                executorService.submit(executedCommitTask);
            }

            for (int i = 0; i < 5; i++) {
                executorService.submit(queuedCommitTask);
            }

            addLatch.await();
            Map<String, Long> commitsCountMap = commitsTracker.getCommitsCountMap();
            Map<String, String> queuedWritersMap = commitsTracker.getQueuedWritersMap();

            assertTrue(commitsCountMap.size() >= 10);
            assertTrue(commitsCountMap.size() < 20);
            assertEquals(5, queuedWritersMap.size());

            CountDownLatch removeLatch = new CountDownLatch(5);
            for (String threadName : queuedWritersMap.keySet()) {
                executorService.submit(new DequedCommitTask(commitsTracker, threadName, removeLatch));
            }

            removeLatch.await();
            queuedWritersMap = commitsTracker.getQueuedWritersMap();
            assertEquals(0, queuedWritersMap.size());
        } finally {
            new ExecutorCloser(executorService).close();
        }
    }
}

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

package org.apache.jackrabbit.oak.plugins.observation;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Before;
import org.junit.Test;

public class CommitRateLimiterTest {
    private static final NodeState AFTER = createAfter();

    private static NodeState createAfter() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("a");
        builder.setProperty("x", 42);
        return builder.getNodeState();
    }

    private CommitRateLimiter limiter;

    @Before
    public void setup() {
        limiter = new CommitRateLimiter();
    }

    @Test
    public void commit() throws CommitFailedException {
        assertSame(AFTER, limiter.processCommit(EMPTY_NODE, AFTER, null));
    }

    @Test(expected = CommitFailedException.class)
    public void blockCommits() throws CommitFailedException, InterruptedException {
        // using a latch to avoid having to rely on timing
        final CountDownLatch latch = new CountDownLatch(1);
        CommitRateLimiter limiter = new CommitRateLimiter() {
            @Override
            public boolean getBlockCommits() {
                // this method is called in the 'try' loop, so it
                // that InterruptedException will be converted
                // to CommitFailedException as expected
                // (sure, this is an implementation detail, 
                // but I don't see a good alternative here)
                latch.countDown();
                return super.getBlockCommits();
            }
        };
        limiter.blockCommits();
        final Thread mainThread = Thread.currentThread();
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    // wait forever to avoid timing problems
                    // (if the CommitRateLimiter is changed to not call
                    // getBlockCommits(), then this wouldn't work - but
                    // how could it not call getBlockCommits()?)
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                mainThread.interrupt();
            }
        };
        t.start();
        try {
            limiter.processCommit(EMPTY_NODE, AFTER, null);
        } finally {
            try {
                t.join();
            } catch (InterruptedException e) {
                // once in a while, we get a spurious 
                // wakeup in the CommitRateLimiter,
                // so that the Thresd.interrupt() above
                // will reach t.join().
                Thread.currentThread().interrupt();
            }
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void delayCommits() throws CommitFailedException {
        limiter.setDelay(1000);
        long t0 = Clock.ACCURATE.getTime();
        assertSame(AFTER, limiter.processCommit(EMPTY_NODE, AFTER, null));
        assertTrue(Clock.ACCURATE.getTime() - t0 >= 1000);
    }

    private final FutureTask<Long> commit = new FutureTask<Long>(new Callable<Long>() {
        @Override
        public Long call() throws Exception {
            long t0 = Clock.ACCURATE.getTime();
            Clock.ACCURATE.waitUntil(Clock.ACCURATE.getTime() + 100);
            assertSame(AFTER, limiter.processCommit(EMPTY_NODE, AFTER, null));
            return Clock.ACCURATE.getTime() - t0;
        }
    });

    @Test
    public void delayCommitsWithReset() throws InterruptedException, ExecutionException, TimeoutException {
        limiter.setDelay(10000);
        new Thread(commit).start();
        limiter.setDelay(0);
        assertTrue(commit.get(1, TimeUnit.SECONDS) >= 100);
    }

    @Test(expected = ExecutionException.class)
    public void delayCommitsWithInterrupt() throws InterruptedException, ExecutionException, TimeoutException {
        limiter.setDelay(10000);
        Thread t = new Thread(commit);
        t.start();
        t.interrupt();
        commit.get(1, TimeUnit.SECONDS);
    }
}

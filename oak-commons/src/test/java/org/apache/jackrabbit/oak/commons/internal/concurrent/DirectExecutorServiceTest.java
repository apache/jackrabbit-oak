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
package org.apache.jackrabbit.oak.commons.internal.concurrent;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Unit cases for DirectExecutorService
 */
public class DirectExecutorServiceTest {

    private DirectExecutorService executor;

    @Before
    public void setUp() {
        executor = new DirectExecutorService();
    }

    @Test
    public void testRunnableRunsInCallingThread() {
        final Thread[] runThread = new Thread[1];
        executor.execute(() -> runThread[0] = Thread.currentThread());
        // Should be main test thread, not a pool thread
        Assert.assertEquals(Thread.currentThread(), runThread[0]);
    }

    @Test
    public void testCallableReturnsValue() throws Exception {
        Future<String> future = executor.submit(() -> "hello");
        Assert.assertEquals("hello", future.get());
    }

    @Test
    public void testShutdownBehavior() {
        Assert.assertFalse(executor.isShutdown());
        executor.shutdown();
        Assert.assertTrue(executor.isShutdown());
        Assert.assertTrue(executor.isTerminated());
    }

    @Test
    public void testShutdownNowBehavior() {
        Assert.assertFalse(executor.isShutdown());
        executor.shutdownNow();
        Assert.assertTrue(executor.isShutdown());
        Assert.assertTrue(executor.isTerminated());
        Assert.assertEquals(0, executor.shutdownNow().size()); // It doesn't queue tasks
    }

    @Test(expected = RejectedExecutionException.class)
    public void testRejectsTaskAfterShutdown() {
        executor.shutdown();
        executor.execute(() -> {});
    }

    @Test
    public void testAwaitTerminationReturnsTrue() {
        executor.shutdown();
        Assert.assertTrue(executor.awaitTermination(100, TimeUnit.MILLISECONDS));
    }

}
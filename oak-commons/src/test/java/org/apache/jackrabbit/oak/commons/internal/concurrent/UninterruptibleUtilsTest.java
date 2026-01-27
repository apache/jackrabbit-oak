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
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Unit cases for {@link UninterruptibleUtils}
 */
public class UninterruptibleUtilsTest {

    @Test
    public void testNullLatch() {
        Assert.assertThrows(NullPointerException.class,
                () -> UninterruptibleUtils.awaitUninterruptibly(null));
    }

    @Test
    public void testWaitsUntilLatchReachesZero() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Thread t = new Thread(() -> UninterruptibleUtils.awaitUninterruptibly(latch));
        t.start();

        // Ensure the thread is actually waiting
        Thread.sleep(5);
        Assert.assertTrue(t.isAlive());

        latch.countDown();
        t.join(10);

        Assert.assertFalse(t.isAlive());
    }

    @Test
    public void testSwallowInterruptsButRestoreFlag() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            UninterruptibleUtils.awaitUninterruptibly(latch);
            // After returning, interrupted flag should be set if we interrupted during wait
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        });

        t.start();
        Thread.sleep(5);

        // Interrupt while it's waiting
        t.interrupt();

        Thread.sleep(5);
        latch.countDown();
        t.join(10);

        Assert.assertFalse(t.isAlive());
    }

    @Test
    public void testWithNullTimeoutAndLatch() {
        CountDownLatch latch = new CountDownLatch(1);

        Assert.assertThrows(NullPointerException.class,
                () -> UninterruptibleUtils.awaitUninterruptibly(null, 1, TimeUnit.MILLISECONDS));

        Assert.assertThrows(NullPointerException.class,
                () -> UninterruptibleUtils.awaitUninterruptibly(latch, 1, null));
    }

    @Test
    public void testWithNegativeTimeout() {
        CountDownLatch latch = new CountDownLatch(1);

        Assert.assertThrows(IllegalArgumentException.class,
                () -> UninterruptibleUtils.awaitUninterruptibly(latch, -1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testWithTimeoutReturnsTrueWhenLatchCountsDownInTime() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            boolean result = UninterruptibleUtils.awaitUninterruptibly(latch, 1, TimeUnit.MILLISECONDS);
            Assert.assertTrue(result);
        });

        t.start();
        Thread.sleep(10);
        latch.countDown();
        t.join(20);

        Assert.assertFalse(t.isAlive());
    }

    @Test
    public void testWithTimeoutReturnsFalseWhenTimeoutExpires() {
        CountDownLatch latch = new CountDownLatch(1);

        long start = System.nanoTime();
        boolean result = UninterruptibleUtils.awaitUninterruptibly(latch, 100, TimeUnit.MILLISECONDS);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        Assert.assertFalse(result);
        Assert.assertTrue("Elapsed time should be close to timeout", elapsedMillis >= 90L);
    }

    @Test
    public void testWithTimeoutSwallowInterruptsButRestoreFlag() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            boolean result = UninterruptibleUtils.awaitUninterruptibly(latch, 500, TimeUnit.MILLISECONDS);
            // Might be true or false depending on when we count down, but interrupt flag must be set.
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        });

        t.start();
        Thread.sleep(5);
        t.interrupt();
        Thread.sleep(5);
        latch.countDown();
        t.join(20);

        Assert.assertFalse(t.isAlive());
    }

}
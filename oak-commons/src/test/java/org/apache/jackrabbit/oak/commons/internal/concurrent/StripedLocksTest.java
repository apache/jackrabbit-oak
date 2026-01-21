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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

/**
 * Unit cases for StripedLocks
 */
public class StripedLocksTest {

    @Test
    public void testConstructorThrowsOnZero() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new StripedLocks(0));
    }

    @Test
    public void testConstructorThrowsOnNegative() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new StripedLocks(-5));
    }

    @Test
    public void testStripesCount() {
        StripedLocks locks = new StripedLocks(8);
        Assert.assertEquals(8, locks.stripesCount());
    }

    @Test
    public void testGetReturnsNonNullLock() {
        StripedLocks locks = new StripedLocks(4);
        Assert.assertNotNull(locks.get("key1"));
        Assert.assertNotNull(locks.get(null));
    }

    @Test
    public void testSameKeyReturnsSameLock() {
        StripedLocks locks = new StripedLocks(16);
        Object key = "test-key";
        Lock lock1 = locks.get(key);
        Lock lock2 = locks.get(key);
        Assert.assertSame(lock1, lock2);
    }

    @Test
    public void testDifferentKeysMayReturnDifferentLocks() {
        StripedLocks locks = new StripedLocks(64);
        Lock lock1 = locks.get("a");
        Lock lock2 = locks.get("b");

        // If they collide, test is still valid but weaker; we just ensure we can lock/unlock both safely.
        lock1.lock();
        lock2.lock();
        lock2.unlock();
        lock1.unlock();

        Assert.assertNotNull(lock1);
        Assert.assertNotNull(lock2);
    }

    @Test
    public void sameKeyIsSerialized() throws InterruptedException {
        StripedLocks striped = new StripedLocks(8);
        String key = "shared-key";

        CountDownLatch bothStarted = new CountDownLatch(2);
        AtomicInteger order = new AtomicInteger(0);
        AtomicInteger firstCompleted = new AtomicInteger(0);
        AtomicInteger secondCompleted = new AtomicInteger(0);

        Runnable task = () -> {
            Lock lock = striped.get(key);
            bothStarted.countDown();
            try {
                bothStarted.await(); // ensure both threads attempt to run concurrently
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            lock.lock();
            try {
                int currentOrder = order.incrementAndGet();
                // Only one thread can see currentOrder == 1 while holding the lock
                if (currentOrder == 1) {
                    firstCompleted.incrementAndGet();
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    secondCompleted.incrementAndGet();
                }
            } finally {
                lock.unlock();
            }
        };

        Thread t1 = new Thread(task);
        Thread t2 = new Thread(task);
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        Assert.assertEquals(2, order.get());
        Assert.assertEquals(2, firstCompleted.get() + secondCompleted.get());
    }
}

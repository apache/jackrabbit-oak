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
package org.apache.jackrabbit.oak.plugins.document.locks;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.junit.Test;

public class TreeNodeDocumentsLocksTest {

    /**
     * Test for the OAK-3949. It uses multiple threads to acquire a shared lock
     * collection. There's also another set of threads that acquires parent
     * locks exclusively. Such combination can't lead to a deadlock.
     */
    @Test
    public void testBulkAcquire() throws InterruptedException {
        int threadCount = 10;
        int locksCount = 100;

        final TreeNodeDocumentLocks locks = new TreeNodeDocumentLocks();
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < threadCount; i++) {
            final List<String> keys = new ArrayList<String>(locksCount);
            for (int j = 0; j < locksCount; j++) {
                keys.add(String.format("2:/parent_%d/lock_%d", j, j));
            }
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    Lock lock = locks.acquire(keys);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    } finally {
                        lock.unlock();
                    }
                }
            }));
        }

        for (int j = 0; j < threadCount; j++) {
            final int from = j * 10;
            final int to = (j + 1) * 10;
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = from; i < to; i++) {
                            Lock parentLock = locks.acquireExclusive(String.format("1:/parent_%d", i));
                            Thread.sleep(100);
                            Lock childLock = locks.acquire(String.format("2:/parent_%d/lock_%d", i, i));
                            Thread.sleep(100);
                            childLock.unlock();
                            Thread.sleep(100);
                            parentLock.unlock();
                        }
                    } catch (InterruptedException e) {
                    }
                }
            }));
        }

        Collections.shuffle(threads);
        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join(10000);
            if (t.isAlive()) {
                fail("Thread hasn't stopped in 10s");
            }
        }
    }
}
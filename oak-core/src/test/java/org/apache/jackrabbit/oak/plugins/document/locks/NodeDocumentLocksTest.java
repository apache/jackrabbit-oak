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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class NodeDocumentLocksTest {

    @Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { new StripedNodeDocumentLocks(), "StripedNodeDocumentLocks" },
                { new TreeNodeDocumentLocks(), "TreeNodeDocumentLocks" } });
    }

    private final NodeDocumentLocks locks;

    public NodeDocumentLocksTest(NodeDocumentLocks locks, String name) {
        this.locks = locks;
    }

    @Test
    public void testBulkAcquireNonConflicting() throws InterruptedException {
        testBulkAcquire(false);
    }

    @Test
    public void testBulkAcquireConflicting() throws InterruptedException {
        testBulkAcquire(true);
    }

    private void testBulkAcquire(boolean conflicting) throws InterruptedException {
        int threadCount = 10;
        int locksPerThread = 100;

        List<Thread> threads = new ArrayList<Thread>();
        String keyName = conflicting ? "lock_%d" : "lock_%d_%d";
        for (int i = 0; i < threadCount; i++) {
            final List<String> keys = new ArrayList<String>(locksPerThread);
            for (int j = 0; j < locksPerThread; j++) {
                keys.add(String.format(keyName, j, i));
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

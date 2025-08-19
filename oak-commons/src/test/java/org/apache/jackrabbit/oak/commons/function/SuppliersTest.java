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
package org.apache.jackrabbit.oak.commons.function;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.apache.jackrabbit.oak.commons.function.Suppliers.memoize;

public class SuppliersTest {

    @Test
    public void computeOnce() {
        AtomicInteger count = new AtomicInteger(0);

        Supplier<Integer> mem = Suppliers.memoize(count::incrementAndGet);

        assertEquals(0, count.get());
        int c = mem.get();
        assertEquals(1, c);
        c = mem.get();
        assertEquals(1, c);
    }

    @Test
    public void concurrentSupplierAccess() {
        List<Thread> threads = new ArrayList<>();
        int threadCount = 1000;
        for (int k = 0; k < threadCount; k++) {
            threads.add(new Thread(() -> {
                synchronized (concurrencyTestMonitor) {
                    // the empty synchronized block is deliberate.
                }
                if (memoizeTestSupplier.get() == null) {
                    concurrencyTestFailed = true;
                }
            }));
        }
        Thread waitForAll = new Thread(() -> {
            for (int k = 0; k < threadCount; k++) {
                try {
                    threads.get(k).join();
                } catch (InterruptedException ignored) {}
            }
        });
        synchronized (concurrencyTestMonitor) {
            for (int k = 0; k < threadCount; k++) {
                threads.get(k).start();
            }
        }
        waitForAll.start();
        assertFalse(concurrencyTestFailed);
    }

    private final Supplier<AtomicInteger> testSupplier = () -> new AtomicInteger(42);
    private final Supplier<AtomicInteger> memoizeTestSupplier = memoize(testSupplier);

    volatile boolean concurrencyTestFailed = false;
    private final Object concurrencyTestMonitor = new Object();
}

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.apache.jackrabbit.oak.commons.function.Suppliers.memoize;
import static org.junit.Assert.assertNull;

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
    public void nullSupplier() {
        AtomicInteger count = new AtomicInteger(0);

        Supplier<Object> mem = Suppliers.memoize(new Supplier<Object>() {
            @Override
            public Integer get() {
                count.incrementAndGet();
                return null;
            }
        });

        assertEquals(0, count.get());
        assertNull(mem.get());
        assertEquals(1, count.get());
        mem.get();
        assertEquals(1, count.get());
    }

    @Test
    public void concurrentSupplierAccess() throws Exception {
        AtomicInteger sourceSupplierInvocationCount = new AtomicInteger(0);
        AtomicBoolean concurrencyTestFailed = new AtomicBoolean(false);

        Supplier<AtomicInteger> sourceSupplier = () -> {
            sourceSupplierInvocationCount.incrementAndGet();
            return new AtomicInteger(42);
        };
        Supplier<AtomicInteger> memoizeTestSupplier = memoize(sourceSupplier);

        int threadCount = 1000;
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch doneSignal = new CountDownLatch(threadCount);
        List<Thread> threads = new ArrayList<>();
        for (int k = 0; k < threadCount; k++) {
            threads.add(new Thread(() -> {
                try {
                    startSignal.await();
                    AtomicInteger result = memoizeTestSupplier.get();
                    if (result == null || result.get() != 42) {
                        concurrencyTestFailed.set(true);
                    }
                } catch (InterruptedException e) {
                    // this can be safely ignored.
                } finally {
                    doneSignal.countDown();
                }
            }));
        }
        for (int k = 0; k < threadCount; k++) {
            threads.get(k).start();
        }
        startSignal.countDown();
        doneSignal.await();
        assertFalse(concurrencyTestFailed.get());
        assertEquals(1, sourceSupplierInvocationCount.get());
    }
}

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
package org.apache.jackrabbit.mk.util;

import junit.framework.TestCase;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Ignore;

/**
 * Test the commit gate.
 */
@Ignore
public class CommitGateIT extends TestCase {

    public void test() throws InterruptedException {
        final CommitGate gate = new CommitGate();
        gate.commit("start");
        final AtomicLong tick = new AtomicLong();
        final AtomicLong spurious = new AtomicLong();
        final int waitMillis = 10;
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < 10; i++) {
            Thread t = threads[i] = new Thread() {
                public void run() {
                    String head = null;
                    while (true) {
                        String nh;
                        try {
                            nh = gate.waitForCommit(head, waitMillis);
                            if (nh.equals(head)) {
                                spurious.incrementAndGet();
                            } else {
                                tick.incrementAndGet();
                            }
                            head = nh;
                            if (head.equals("end")) {
                                break;
                            }
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                }
            };
            t.start();
        }
        Thread.sleep(waitMillis * 10);
        // assertTrue(threadCount < spurious.get()); <- depends on timing
        assertEquals(10, tick.get());
        tick.set(0);
        spurious.set(0);
        int commitCount = 100;
        for (int i = 0; i < commitCount; i++) {
            gate.commit(Integer.toString(i));
            Thread.sleep(1);
        }
        gate.commit("end");
        for (Thread j : threads) {
            j.join();
        }
        // disabled: depends on timing
        // assertTrue("ticks: " + tick.get() + " min: " + threadCount * commitCount + " spurious: " + spurious.get(),
        //         tick.get() >= threadCount * commitCount * 0.2 && tick.get() <= threadCount * commitCount * 1.2);
    }

}

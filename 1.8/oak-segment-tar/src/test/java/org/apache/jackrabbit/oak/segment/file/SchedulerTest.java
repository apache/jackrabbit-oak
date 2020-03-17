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

package org.apache.jackrabbit.oak.segment.file;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Test;

public class SchedulerTest {
    private final Scheduler scheduler = new Scheduler("test-scheduler");

    @After
    public void tearDown() {
        scheduler.close();
    }

    @Test
    public void execute() throws Exception {
        TestTask task = new TestTask(1);
        scheduler.execute("execute", task);

        assertTrue(task.await());
        assertEquals("execute", task.getThreadName());
    }

    @Test
    public void scheduleOnce() throws Exception {
        TestTask task = new TestTask(1);
        scheduler.scheduleOnce("scheduleOnce", 1, SECONDS, task);

        assertNull(task.getThreadName());
        assertTrue(task.await());
        assertEquals("scheduleOnce", task.getThreadName());
    }

    @Test
    public void scheduleAtFixedRate() throws Exception {
        TestTask task = new TestTask(5);
        scheduler.scheduleAtFixedRate("scheduleAtFixedRate", 200, MILLISECONDS, task);

        assertNull(task.getThreadName());
        assertTrue(task.await());
        assertEquals("scheduleAtFixedRate", task.getThreadName());
    }

    private static class TestTask implements Runnable {
        private final AtomicReference<String> threadName = new AtomicReference<>();
        private final CountDownLatch done;

        public TestTask(int count) {
            done = new CountDownLatch(count);
        }

        @Override
        public void run() {
            if (done.getCount() == 1) {
                threadName.set(currentThread().getName());
            }
            done.countDown();
        }

        public boolean await() throws InterruptedException {
            return done.await(5, SECONDS);
        }

        public String getThreadName() {
            return threadName.get();
        }
    }

}
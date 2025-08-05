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
package org.apache.jackrabbit.oak.index;

import org.apache.jackrabbit.guava.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ThreadMonitorTest {

    private ThreadMonitor monitor;

    @Before
    public void setUp() {
        monitor = ThreadMonitor.newInstance();
    }

    @After
    public void tearDown() {
        monitor = null;
    }

    private Thread createThread(String name) {
        return new Thread(name) {
            @Override
            public void run() {
                // Simulate some work
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
    }

    private Thread[] createThreads(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> createThread("test-thread-" + i))
                .toArray(Thread[]::new);
    }

    @Test
    public void monitorMultipleThreads() {
        Thread[] threads = createThreads(3);
        for (Thread t : threads) {
            monitor.registerThread(t);
            t.start();
        }

        monitor.start();
        String statsString = monitor.printStatistics("Test print statistics");
        assertTrue(statsString.contains("Test print statistics"));
        for (Thread t : threads) {
            assertTrue("Did not find expected string in output. Expected: \"" + t.getName() + "\". Output: " + statsString,
                    statsString.contains("Thread " + t.getName()));
        }
        for (Thread t : threads) {
            monitor.unregisterThread(t);
        }
        // Thread should not be found in the statistics after unregistering
        statsString = monitor.printStatistics();
        for (Thread t : threads) {
            assertFalse("Did not find expected string in output. Expected: \"" + t.getName() + "\". Output: " + statsString,
                    statsString.contains("Thread " + t.getName()));
        }
    }

    @Test
    public void registerBeforeStartingMonitor() {
        monitor.start();
        Thread t = createThread("test-thread");
        monitor.registerThread(t);
        System.out.println(monitor.printStatistics());
    }

    @Test
    public void threadTerminatesWhileMonitored() throws InterruptedException {
        Thread t = createThread("test-thread");
        monitor.registerThread(t);
        monitor.start();
        t.start();
        t.join(); // Wait for the thread to finish
        String statsString = monitor.printStatistics();
        assertTrue(statsString.contains("Thread " + t.getName()));
    }

    @Test
    public void testAutoRegisteringThreadFactory() {
        monitor.start();
        ThreadMonitor.AutoRegisteringThreadFactory factory = new ThreadMonitor.AutoRegisteringThreadFactory(
                monitor,
                new ThreadFactoryBuilder().setNameFormat("test-thread").build()
        );
        ExecutorService executor = Executors.newSingleThreadExecutor(factory);
        try {
            Thread t = createThread("test-thread");
            executor.submit(t);
            String statsString = monitor.printStatistics();
            assertTrue("Did not find expected string in output. Expected: \"" + t.getName() + "\". Output: " + statsString,
                    statsString.contains("Thread " + t.getName()));
            new ExecutorCloser(executor).close();
        } finally {
            new ExecutorCloser(executor).close();
        }
    }
}
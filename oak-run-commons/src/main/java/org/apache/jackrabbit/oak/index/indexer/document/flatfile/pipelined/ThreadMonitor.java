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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Keeps track of a list of threads and prints statistics of CPU usage of the threads. It also prints statistics
 * of memory usage and garbage collections
 */
public class ThreadMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadMonitor.class);
    private final CopyOnWriteArraySet<Thread> monitoredThreads = new CopyOnWriteArraySet<>();

    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private final List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    private final Stopwatch stopwatch = Stopwatch.createUnstarted();

    public void start() {
        stopwatch.start();
    }

    public void registerThread(Thread thread) {
        monitoredThreads.add(thread);
    }

    public void unregisterThread(Thread thread) {
        monitoredThreads.remove(thread);
    }

    public void printStatistics() {
        printStatistics("Thread/Memory report");
    }

    public void printStatistics(String heading) {
        long timeSinceStartMillis = stopwatch.elapsed().toMillis();

        StringBuilder sb = new StringBuilder(heading + ". Time since start of monitoring: " + stopwatch + "\n");

        // Memory usage
        sb.append(String.format("  Heap memory usage: %s, Non-heap memory usage: %s\n",
                memoryMXBean.getHeapMemoryUsage(), memoryMXBean.getNonHeapMemoryUsage()));

        // Garbage collection
        for (GarbageCollectorMXBean gcBean : gcMXBeans) {
            sb.append(String.format("  Collector: %s, collectionCount: %d, collectionTime: %d ms (%.2f%%)\n",
                    gcBean.getName(), gcBean.getCollectionCount(), gcBean.getCollectionTime(),
                    FormattingUtils.safeComputePercentage(gcBean.getCollectionTime(), timeSinceStartMillis))
            );
        }

        // Thread CPU usage
        monitoredThreads.stream().sorted(Comparator.comparing(Thread::getId)).forEach(thread -> {
            long threadCpuTimeMillis = threadMXBean.getThreadCpuTime(thread.getId()) / 1_000_000;
            long threadUserTimeMillis = threadMXBean.getThreadUserTime(thread.getId()) / 1_000_000;
            double threadCpuTimePercentage = FormattingUtils.safeComputePercentage(threadCpuTimeMillis, timeSinceStartMillis);
            double threadUserTimePercentage = FormattingUtils.safeComputePercentage(threadUserTimeMillis, timeSinceStartMillis);
            sb.append(String.format("  Thread %-26s - cpuTime: %7d (%.2f%%), userTime: %7d (%.2f%%)\n",
                    thread.getName() + "/" + thread.getId(),
                    threadCpuTimeMillis, threadCpuTimePercentage,
                    threadUserTimeMillis, threadUserTimePercentage)
            );
        });
        // Remove the last newline
        LOG.info(sb.substring(0, sb.length() - 1));
    }

    /**
     * Thread factory that registers all new threads with a given thread monitor. This can be passed to an Executor, so
     * that all of its threads will be monitored, which may be simpler than manually registering each individual thread.
     */
    public static class AutoRegisteringThreadFactory implements ThreadFactory {
        private final ThreadMonitor threadMonitor;
        private final ThreadFactory delegate;

        /**
         * @param threadMonitor The thread monitor where to register new threads.
         * @param delegate      A thread factory to create new threads.
         */
        public AutoRegisteringThreadFactory(ThreadMonitor threadMonitor, ThreadFactory delegate) {
            this.threadMonitor = threadMonitor;
            this.delegate = delegate;
        }

        /**
         * Uses Executors.defaultThreadFactory() to create new threads.
         *
         * @param threadMonitor The thread monitor where to register new threads.
         */
        public AutoRegisteringThreadFactory(ThreadMonitor threadMonitor) {
            this.threadMonitor = threadMonitor;
            this.delegate = Executors.defaultThreadFactory();
        }

        @Override
        public Thread newThread(@NotNull Runnable r) {
            Thread t = delegate.newThread(r);
            threadMonitor.registerThread(t);
            return t;
        }
    }
}

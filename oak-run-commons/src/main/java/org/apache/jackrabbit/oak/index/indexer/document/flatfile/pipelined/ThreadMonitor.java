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
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


public class ThreadMonitor {
    static class AutoRegisteringThreadFactory implements ThreadFactory {
        private final ThreadMonitor threadMonitor;
        private final ThreadFactory delegate;

        public AutoRegisteringThreadFactory(ThreadMonitor threadMonitor, ThreadFactory delegate) {
            this.threadMonitor = threadMonitor;
            this.delegate = delegate;
        }

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

    private static final Logger LOG = LoggerFactory.getLogger(ThreadMonitor.class);
    private final CopyOnWriteArraySet<Thread> threadIds = new CopyOnWriteArraySet<>();

    private final Stopwatch stopwatch = Stopwatch.createUnstarted();

    public void start() {
        stopwatch.start();
    }

    public void registerThread(Thread thread) {
        threadIds.add(thread);
    }

    public void unregisterThread(Thread thread) {
        threadIds.remove(thread);
    }

    public void printStatistics() {
        printStatistics("Thread/Memory report");
    }

    public void printStatistics(String heading) {
        long timeSinceStartMillis = stopwatch.elapsed().toMillis();

        StringBuilder sb = new StringBuilder(heading + ". Time elapsed: " + stopwatch + "\n");
        MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        sb.append(String.format("  Heap memory usage: %s, Non-heap memory usage: %s\n",
                memoryMxBean.getHeapMemoryUsage(), memoryMxBean.getNonHeapMemoryUsage()));
        for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            sb.append(String.format("  Collector: %s, collectionCount: %d, collectionTime: %d ms (%.2f%%)\n",
                    gcBean.getName(), gcBean.getCollectionCount(), gcBean.getCollectionTime(),
                    FormattingUtils.safeComputePercentage(gcBean.getCollectionTime(), timeSinceStartMillis))
            );
        }

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        threadIds.stream().sorted(Comparator.comparing(Thread::getId)).forEach(thread -> {
            long threadCpuTimeMillis = threadMXBean.getThreadCpuTime(thread.getId()) / 1_000_000;
            long threadUserTimeMillis = threadMXBean.getThreadUserTime(thread.getId()) / 1_000_000;
            double threadCpuTimePercentage = FormattingUtils.safeComputePercentage(threadCpuTimeMillis, timeSinceStartMillis);
            double threadUserTimePercentage = FormattingUtils.safeComputePercentage(threadUserTimeMillis, timeSinceStartMillis);
            sb.append(String.format("  Thread %d/%s - cpuTime: %d (%.2f%%), userTime: %d (%.2f%%)\n",
                    thread.getId(), thread.getName(),
                    threadCpuTimeMillis, threadCpuTimePercentage,
                    threadUserTimeMillis, threadUserTimePercentage)
            );
        });
        // Remove the last newline
        LOG.info(sb.substring(0, sb.length() - 1));
    }
}

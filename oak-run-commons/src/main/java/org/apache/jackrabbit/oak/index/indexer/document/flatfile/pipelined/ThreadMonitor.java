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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Comparator;
import java.util.concurrent.CopyOnWriteArraySet;

public class ThreadMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadMonitor.class);
    private final CopyOnWriteArraySet<Thread> threadIds = new CopyOnWriteArraySet<>();

    private final Stopwatch stopwatch = Stopwatch.createUnstarted();

    public void start() {
        stopwatch.start();
    }

    public void registerThread(Thread thread) {
        threadIds.add(thread);
    }

    public void printStatistics() {
        long timeSinceStartMillis = stopwatch.elapsed().toMillis();

        StringBuilder sb = new StringBuilder("MemoryMonitor report:\n");
        for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            sb.append(String.format("  Collector: %s, collectionCount: %d, collectionTime: %d ms (%.2f%%)\n",
                    gcBean.getName(), gcBean.getCollectionCount(), gcBean.getCollectionTime(), percentage(gcBean.getCollectionTime(), timeSinceStartMillis))
            );
        }
        MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        sb.append(String.format("  Heap memory usage: %s, Non-heap memory usage: %s\n", memoryMxBean.getHeapMemoryUsage(), memoryMxBean.getNonHeapMemoryUsage()));
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        threadIds.stream().sorted(Comparator.comparing(Thread::getName)).forEach(thread -> {
            long threadCpuTimeMillis = threadMXBean.getThreadCpuTime(thread.getId()) / 1_000_000;
            long threadUserTimeMillis = threadMXBean.getThreadUserTime(thread.getId()) / 1_000_000;
            double threadCpuTimePercentage = percentage(threadCpuTimeMillis, timeSinceStartMillis);
            double threadUserTimePercentage = percentage(threadUserTimeMillis, timeSinceStartMillis);
            sb.append(String.format("  Thread: %s, CPU time: %d (%.2f%%), User time: %d (%.2f%%)\n",
                    thread.getName(), threadCpuTimeMillis, threadCpuTimePercentage, threadUserTimeMillis, threadUserTimePercentage)
            );
        });
        // Remove the last newline
        LOG.info(sb.substring(0, sb.length() - 1));
    }

    private static double percentage(long value, long total) {
        return (double) value / total * 100;
    }
}

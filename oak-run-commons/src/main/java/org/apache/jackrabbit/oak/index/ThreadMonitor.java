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

import org.apache.jackrabbit.oak.commons.time.Stopwatch;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Keeps track of a list of threads and prints statistics of CPU usage of the threads. It also prints statistics
 * of memory usage and garbage collections
 */
public class ThreadMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadMonitor.class);

    private static final ThreadMonitor NOOP_INSTANCE = new ThreadMonitor() {
        @Override
        public void start() {
            // NOOP
        }

        @Override
        public void registerThread(@NotNull Thread thread) {
            // NOOP
        }

        @Override
        public void unregisterThread(@NotNull Thread thread) {
            // NOOP
        }

        @Override
        public String printStatistics() {
            return "ThreadMonitor is not available.";
        }
    };

    public static ThreadMonitor newInstance() {
        if (ManagementFactory.getThreadMXBean() == null || !ManagementFactory.getThreadMXBean().isThreadCpuTimeSupported()) {
            LOG.warn("ThreadMXBean is not available or thread CPU time is not supported. ThreadMonitor will not work.");
            return NOOP_INSTANCE;
        }
        if (ManagementFactory.getMemoryMXBean() == null) {
            LOG.warn("MemoryMXBean is not available. ThreadMonitor will not work.");
            return NOOP_INSTANCE;
        }
        if (ManagementFactory.getGarbageCollectorMXBeans() == null) {
            LOG.warn("No GarbageCollectorMXBeans are available. ThreadMonitor will not work.");
            return NOOP_INSTANCE;
        }
        return new ThreadMonitor();
    }

    private static class ThreadInitialValues {
        static final ThreadInitialValues EMPTY = new ThreadInitialValues(0, 0);
        final long cpuTimeMillis;
        final long userTimeMillis;

        private ThreadInitialValues(long cpuTimeMillis, long userTimeMillis) {
            this.cpuTimeMillis = cpuTimeMillis;
            this.userTimeMillis = userTimeMillis;
        }
    }

    private static class GCInitialValues {
        final long collectionCount;
        final long collectionTimeMillis;

        private GCInitialValues(long collectionCount, long collectionTimeMillis) {
            this.collectionCount = collectionCount;
            this.collectionTimeMillis = collectionTimeMillis;
        }
    }

    private final Map<Thread, ThreadInitialValues> monitoredThreads = Collections.synchronizedMap(new IdentityHashMap<>());
    private final HashMap<String, GCInitialValues> monitoredGCs = new HashMap<>();
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private final List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();

    private ThreadMonitor() {
    }

    public void start() {
        stopwatch.start();
        monitoredThreads.replaceAll((thread, initialValues) -> {
            // Update initial values to current CPU and user time
            return new ThreadInitialValues(
                    threadMXBean.getThreadCpuTime(thread.getId()) / 1_000_000,
                    threadMXBean.getThreadUserTime(thread.getId()) / 1_000_000
            );
        });
        // Garbage collection initial values
        for (GarbageCollectorMXBean gcBean : gcMXBeans) {
            monitoredGCs.put(gcBean.getName(),
                    new GCInitialValues(gcBean.getCollectionCount(), gcBean.getCollectionTime()));
        }
    }

    public void registerThread(@NotNull Thread thread) {
        if (stopwatch.isRunning()) {
            registerThreadInternal(thread, new ThreadInitialValues(
                    threadMXBean.getThreadCpuTime(thread.getId()) / 1_000_000,
                    threadMXBean.getThreadUserTime(thread.getId()) / 1_000_000
            ));
        } else {
            registerThreadInternal(thread, ThreadInitialValues.EMPTY);
        }
    }

    private void registerThreadInternal(@NotNull Thread thread, ThreadInitialValues initialValues) {
        ThreadInitialValues prev = monitoredThreads.putIfAbsent(thread, initialValues);
        if (prev != null) {
            LOG.warn("Thread {} is already registered in ThreadMonitor.", thread.getName());
        }
    }

    public void unregisterThread(@NotNull Thread thread) {
        monitoredThreads.remove(thread);
    }

    public String printStatistics() {
        return printStatistics("Thread/Memory report");
    }

    public String printStatistics(@NotNull String heading) {
        if (!stopwatch.isRunning()) {
            LOG.warn("ThreadMonitor has not been started. Call start() before printing statistics.");
            return "";
        }
        try {
            long timeSinceStartMillis = stopwatch.elapsed().toMillis();

            StringBuilder sb = new StringBuilder(heading + ". Time since start of monitoring: " + FormattingUtils.formatToSeconds(stopwatch) + "\n");
            // Memory usage
            sb.append(String.format("  Heap memory usage: %s, Non-heap memory usage: %s\n",
                    memoryMXBean.getHeapMemoryUsage(), memoryMXBean.getNonHeapMemoryUsage()));

            // Garbage collection
            for (GarbageCollectorMXBean gcBean : gcMXBeans) {
                GCInitialValues initialValues = monitoredGCs.get(gcBean.getName());
                long collectionCount = gcBean.getCollectionCount() - initialValues.collectionCount;
                long collectionTimeMillis = gcBean.getCollectionTime() - initialValues.collectionTimeMillis;
                sb.append(String.format("  Collector: %s, collectionCount: %d, collectionTime: %d ms (%.2f%%)\n",
                        gcBean.getName(), collectionCount, collectionTimeMillis,
                        FormattingUtils.safeComputePercentage(collectionTimeMillis, timeSinceStartMillis))
                );
            }

            // Thread CPU usage
            monitoredThreads.entrySet().stream()
                    .sorted(Comparator.comparing(e -> e.getKey().getId()))
                    .forEach(entry -> {
                        Thread thread = entry.getKey();
                        ThreadInitialValues initialValues = entry.getValue();
                        // Compute CPU and user time since the start of monitoring
                        long threadCpuTimeMillis = threadMXBean.getThreadCpuTime(thread.getId()) / 1_000_000 - initialValues.cpuTimeMillis;
                        long threadUserTimeMillis = threadMXBean.getThreadUserTime(thread.getId()) / 1_000_000 - initialValues.userTimeMillis;
                        double threadCpuTimePercentage = FormattingUtils.safeComputePercentage(threadCpuTimeMillis, timeSinceStartMillis);
                        double threadUserTimePercentage = FormattingUtils.safeComputePercentage(threadUserTimeMillis, timeSinceStartMillis);
                        sb.append(String.format("  Thread %-26s - cpuTime: %7d (%.2f%%), userTime: %7d (%.2f%%)\n",
                                thread.getName() + "/" + thread.getId(),
                                threadCpuTimeMillis, threadCpuTimePercentage,
                                threadUserTimeMillis, threadUserTimePercentage)
                        );
                    });
            // Remove the last newline
            return sb.substring(0, sb.length() - 1);
        } catch (Exception e) {
            // This is just for monitoring, so suppress any errors in order not to crash the application
            LOG.error("Error while printing thread statistics", e);
            return "Error while printing thread statistics: " + e.getMessage();
        }
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
        public AutoRegisteringThreadFactory(@NotNull ThreadMonitor threadMonitor, @NotNull ThreadFactory delegate) {
            this.threadMonitor = threadMonitor;
            this.delegate = delegate;
        }

        /**
         * Uses Executors.defaultThreadFactory() to create new threads.
         *
         * @param threadMonitor The thread monitor where to register new threads.
         */
        public AutoRegisteringThreadFactory(@NotNull ThreadMonitor threadMonitor) {
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

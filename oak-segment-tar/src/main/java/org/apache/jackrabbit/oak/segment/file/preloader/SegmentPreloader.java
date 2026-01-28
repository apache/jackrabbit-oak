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
package org.apache.jackrabbit.oak.segment.file.preloader;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.internal.function.Suppliers;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.DelegatingPersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCachePreloadingConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

/**
 * A {@link PersistentCache} decorator that preloads segments into the cache by
 * asynchronously preloading segments referenced by a segment that is being read
 * from the cache.
 *
 * @see PersistentCachePreloadingConfiguration
 */
public class SegmentPreloader extends DelegatingPersistentCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentPreloader.class);

    private static final Thread.UncaughtExceptionHandler UNCAUGHT_EXCEPTION_HANDLER = (t, e) -> {
        if (!(e instanceof InterruptedException)) {
            LOG.warn("Uncaught exception in thread {} ({}, {})", t.getName(), e.getClass(), e.getMessage(), e);
        }
    };

    private static final int DISPATCH_QUEUE_MAX_SIZE = 10_000;

    private final Map<Integer, String> inProgressPrefetch;

    private final PersistentCache delegate;

    private final ExecutorService dispatchPool;

    private final ExecutorService preloadPool;

    private final Supplier<TarFiles> tarFiles;

    private final int maxPreloadDepth;

    private volatile int preloadDepth;

    /**
     * Factory method that decorates the given {@link PersistentCache} with a
     * {@link SegmentPreloader} if the given configuration requires preloading.
     * Otherwise, the given {@code delegate} is returned as-is.
     *
     * @param delegate the cache to decorate
     * @param config   the preloading configuration
     * @param tarFiles a supplier of the {@link TarFiles} instance used to read segments and segment graphs
     * @return the decorated cache or the given {@code delegate} if no preloading is configured
     */
    public static @NotNull PersistentCache decorate(@NotNull PersistentCache delegate, @NotNull PersistentCachePreloadingConfiguration config, @NotNull Supplier<TarFiles> tarFiles) {
        if (config.getConcurrency() > 0 && config.getMaxPreloadDepth() > 0) {
            return new SegmentPreloader(delegate, config, tarFiles);
        }
        return delegate;
    }

    private SegmentPreloader(@NotNull PersistentCache delegate, @NotNull PersistentCachePreloadingConfiguration config, @NotNull Supplier<TarFiles> tarFiles) {
        this.delegate = delegate;
        this.tarFiles = Suppliers.memoize(tarFiles);
        this.inProgressPrefetch = new ConcurrentHashMap<>();
        this.maxPreloadDepth = config.getMaxPreloadDepth();
        this.preloadDepth = adaptPreloadDepth(this.maxPreloadDepth, 0);
        this.dispatchPool = new ThreadPoolExecutor(1,1,
                1, TimeUnit.SECONDS,
                new PriorityBlockingQueue<>(),
                r -> {
                    Thread thread = new Thread(r, "segment-preload-dispatcher");
                    thread.setUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
                    return thread;
                }) {

            private volatile long lastLoggedTime = System.currentTimeMillis();

            @Override
            public void execute(@NotNull Runnable command) {
                if (getQueue().size() < DISPATCH_QUEUE_MAX_SIZE) {
                    super.execute(command);
                }
            }

            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                clearInProgressTask(r);
                int size = getQueue().size();
                int adaptedPreloadDepth = adaptPreloadDepth(maxPreloadDepth, size / (double) DISPATCH_QUEUE_MAX_SIZE);
                if (adaptedPreloadDepth != preloadDepth) {
                    preloadDepth = adaptedPreloadDepth;
                    LOG.debug("Adjusted preload depth to {} (queue size: {})", preloadDepth, size);
                }

                long now = System.currentTimeMillis();
                if (lastLoggedTime + 15_000 < now) {
                    lastLoggedTime = now;
                    LOG.info("Dispatch pool queue size: {}, current preload depth: {}", size, preloadDepth);
                }
            }
        };

        int preloadThreads = config.getConcurrency();
        ThreadPoolExecutor preloadPool = new ThreadPoolExecutor(Math.max(1, preloadThreads / 4), preloadThreads,
                5, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(preloadThreads * 4),
                r -> {
                    String threadName = String.format("segment-preload-%s", Long.toHexString(System.nanoTime() & 0xFFFFF));
                    Thread thread = new Thread(r, threadName);
                    thread.setPriority(Thread.MIN_PRIORITY);
                    thread.setUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
                    return thread;
                },
                (r, executor) -> {
                    try {
                        // force the caller thread to wait for space in the queue (this is always a thread in the dispatchPool)
                        // this creates back-pressure to the dispatchPool, slowing down the dispatching of new preload tasks
                        executor.getQueue().put(r);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }) {

            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                clearInProgressTask(r);
            }
        };
        preloadPool.allowCoreThreadTimeOut(true);
        this.preloadPool = preloadPool;
    }

    @VisibleForTesting
    static int adaptPreloadDepth(int maxPreloadDepth, double queueFillPercentage) {
        double remainingCapacity = 1.0 - queueFillPercentage;
        double capacitySlice = 1.0 / maxPreloadDepth;
        for (int i = 1; i < maxPreloadDepth; i++) {
            if (remainingCapacity <= i * capacitySlice) {
                return i;
            }
        }
        return maxPreloadDepth;
    }

    @Override
    protected PersistentCache delegate() {
        return delegate;
    }

    @Override
    public @Nullable Buffer readSegment(long msb, long lsb, @NotNull Callable<Buffer> loader) {
        Buffer buffer = super.readSegment(msb, lsb, loader);
        dispatch(msb, lsb, getReferencedSegments(lsb, buffer), 1);
        return buffer;
    }

    private void dispatch(long msb, long lsb, SegmentIds referencedSegments, int depth) {
        if (depth <= preloadDepth && !referencedSegments.isEmpty() && SegmentId.isDataSegmentId(lsb)) {
            execute(dispatchPool, createDispatchTask(msb, lsb, referencedSegments, depth));
        }
    }

    @NotNull DispatchTask createDispatchTask(long msb, long lsb, SegmentIds referencedSegments, int depth) {
        return new DispatchTask(msb, lsb, referencedSegments, depth);
    }

    private void preload(long msb, long lsb, int depth) {
        execute(preloadPool, createPreloadTask(msb, lsb, depth));
    }

    @NotNull SegmentPreloader.PreloadTask createPreloadTask(long msb, long lsb, int depth) {
        return new PreloadTask(tarFiles.get(), msb, lsb, depth);
    }

    private void execute(ExecutorService pool, Runnable r) {
        if (!pool.isShutdown() && registerInProgressTask(r)) {
            pool.execute(r);
        }
    }

    private boolean registerInProgressTask(Runnable r) {
        return inProgressPrefetch.putIfAbsent(r.hashCode(), Thread.currentThread().getName()) == null;
    }

    private void clearInProgressTask(Runnable r) {
        inProgressPrefetch.remove(r.hashCode());
    }

    @Override
    public void close() {
        try {
            preloadPool.shutdown();
            dispatchPool.shutdown();
            if (!preloadPool.awaitTermination(4, TimeUnit.SECONDS)) {
                preloadPool.shutdownNow();
            }
            if (!dispatchPool.awaitTermination(1, TimeUnit.SECONDS)) {
                dispatchPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            preloadPool.shutdownNow();
            dispatchPool.shutdownNow();
        }
    }

    private boolean wrapsInterruptedException(RuntimeException e) {
        Throwable candidate = e;
        while (candidate.getCause() != null) {
            if (candidate.getCause() instanceof InterruptedException) {
                return true;
            }
            candidate = candidate.getCause();
        }
        return false;
    }

    class DispatchTask implements Runnable, Comparable<DispatchTask> {

        private final long msb;

        private final long lsb;

        private final SegmentIds references;

        private final int depth;

        private final long creationTime = System.nanoTime();

        private DispatchTask(long msb, long lsb, SegmentIds references, int depth) {
            checkArgument(depth <= maxPreloadDepth, "depth must be <= %d, is %d", maxPreloadDepth, depth);
            this.msb = msb;
            this.lsb = lsb;
            this.references = references;
            this.depth = depth;
            LOG.debug("Created: {}", this);
        }

        @Override
        public void run() {
            LOG.debug("Running: {}", this);

            try {
                for (int i = 0; i < references.size(); i++) {
                    long refMsb = references.getMsb(i);
                    long refLsb = references.getLsb(i);
                    preload(refMsb, refLsb, depth);
                }
            } catch (RuntimeException e) {
                if (wrapsInterruptedException(e)) {
                    Thread.currentThread().interrupt();
                    return;
                }
                throw e;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o.getClass() != DispatchTask.class) {
                return false;
            }
            DispatchTask that = (DispatchTask) o;
            return msb == that.msb && lsb == that.lsb && depth == that.depth;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), msb, lsb, depth);
        }

        @Override
        public String toString() {
            return "DispatchTask{segmentId=" + new UUID(msb, lsb) + ", depth=" + depth + ", references=" + references.size() + '}';
        }

        private int getPreloadDepth() {
            return depth;
        }

        private long getCreationTime() {
            return creationTime;
        }

        @Override
        public int compareTo(@NotNull SegmentPreloader.DispatchTask o) {
            return Comparator
                    .comparing(DispatchTask::getPreloadDepth)
                    .thenComparing(DispatchTask::getCreationTime)
                    .compare(this, o);
        }
    }

    private static SegmentIds getReferencedSegments(long lsb, @Nullable Buffer buffer) {
        if (buffer == null || !SegmentId.isDataSegmentId(lsb)) {
            return SegmentIds.EMPTY;
        }
        SegmentData segmentData = SegmentData.newSegmentData(buffer);
        int referencedSegmentsCount = segmentData.getSegmentReferencesCount();
        if (referencedSegmentsCount == 0) {
            return SegmentIds.EMPTY;
        }
        SegmentIds segmentIds = new SegmentIds(referencedSegmentsCount);
        for (int i = 0; i < referencedSegmentsCount; i++) {
            segmentIds.add(i, segmentData.getSegmentReferenceMsb(i), segmentData.getSegmentReferenceLsb(i));
        }
        return segmentIds;
    }

    class PreloadTask implements Runnable {

        private final TarFiles tarFiles;

        private final long msb;

        private final long lsb;

        private final int depth;

        private PreloadTask(TarFiles tarFiles, long msb, long lsb, int depth) {
            checkArgument(depth <= maxPreloadDepth, "depth must be <= %d, is %d", maxPreloadDepth, depth);
            this.tarFiles = tarFiles;
            this.msb = msb;
            this.lsb = lsb;
            this.depth = depth;
            LOG.debug("Created: {}", this);
        }

        @Override
        public void run() {
            LOG.debug("Running: {}", this);
            try {
                if (depth < preloadDepth || !containsSegment(msb, lsb)) {
                    Buffer buffer = delegate().readSegment(msb, lsb, () -> tarFiles.readSegment(msb, lsb));
                    dispatch(msb, lsb, getReferencedSegments(lsb, buffer), depth + 1);
                }
            } catch (RuntimeException e) {
                if (wrapsInterruptedException(e)) {
                    Thread.currentThread().interrupt();
                    return;
                }
                throw e;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o.getClass() != PreloadTask.class) {
                return false;
            }
            PreloadTask that = (PreloadTask) o;
            return msb == that.msb && lsb == that.lsb;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), msb, lsb);
        }

        @Override
        public String toString() {
            return "PreloadTask{segmentId=" + new UUID(msb, lsb) + ", depth=" + depth + '}';
        }
    }
}

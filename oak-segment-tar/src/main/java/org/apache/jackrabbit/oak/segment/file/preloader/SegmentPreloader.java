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
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.DelegatingPersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCachePreloadingConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

    private final Map<Integer, String> inProgressPrefetch;

    private final ConcurrentHashMap<String, Map<UUID, Set<UUID>>> graphCache;

    private final PersistentCache delegate;

    private final ExecutorService dispatchPool;

    private final ExecutorService preloadPool;

    private final int preloadDepth;

    private final Supplier<TarFiles> tarFiles;

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
        this.graphCache = new ConcurrentHashMap<>();
        this.preloadDepth = config.getMaxPreloadDepth();
        this.dispatchPool = new ThreadPoolExecutor(1,1,
                1, TimeUnit.SECONDS,
                new PriorityBlockingQueue<>(),
                r -> new Thread(r, "segment-preload-dispatcher")) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                clearInProgressTask(r);
            }
        };
        int preloadThreads = config.getConcurrency();
        ThreadPoolExecutor preloadPool = new ThreadPoolExecutor(Math.max(1, preloadThreads / 4), preloadThreads,
                5, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(preloadThreads * 4),
                r -> {
                    String threadName = String.format("segment-preload-%s", Long.toHexString(System.nanoTime() & 0xFFFFF));
                    Thread thread = new Thread(r, threadName);
                    thread.setUncaughtExceptionHandler((t, e) -> {
                        if (!(e instanceof InterruptedException)) {
                            LOG.warn("Uncaught exception in thread {}", t.getName(), e);
                        }
                    });
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

    @Override
    protected PersistentCache delegate() {
        return delegate;
    }

    @Override
    public @Nullable Buffer readSegment(long msb, long lsb, @NotNull Callable<Buffer> loader) {
        dispatch(msb, lsb);
        return delegate().readSegment(msb, lsb, loader);
    }

    private void dispatch(long msb, long lsb) {
        dispatch(msb, lsb, 1);
    }

    private void dispatch(long msb, long lsb, int depth) {
        execute(dispatchPool, createDispatchTask(msb, lsb, depth));
    }

    @NotNull SegmentPreloader.DispatchTask createDispatchTask(long msb, long lsb, int depth) {
        TarFiles tars = tarFiles.get();
        return new DispatchTask(tars, tars::getIndices, msb, lsb, depth);
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

    class DispatchTask implements Runnable, Comparable<DispatchTask> {

        private final TarFiles tarFiles;

        private final Supplier<Map<String, Set<UUID>>> indicesSupplier;

        private final long msb;

        private final long lsb;

        private final int depth;

        private final long creationTime = System.nanoTime();

        private DispatchTask(@NotNull TarFiles tarFiles, Supplier<Map<String, Set<UUID>>> indicesSupplier, long msb, long lsb, int depth) {
            checkArgument(depth <= preloadDepth, "depth must be <= %d, is %d", preloadDepth, depth);
            this.tarFiles = tarFiles;
            this.indicesSupplier = indicesSupplier;
            this.msb = msb;
            this.lsb = lsb;
            this.depth = depth;
            LOG.debug("Created: {}", this);
        }

        @Override
        public void run() {
            LOG.debug("Running: {}", this);
            UUID uuid = new UUID(msb, lsb);
            Map<String, Set<UUID>> indices = indicesSupplier.get();
            String archiveName = indices.entrySet().stream()
                    .filter(entry -> entry.getValue().contains(uuid))
                    .findFirst()
                    .map(Map.Entry::getKey)
                    .orElse(null);

            Map<UUID, Set<UUID>> graph = graphCache.computeIfAbsent(archiveName, name -> {
                try {
                    return tarFiles.getGraph(name);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            for (UUID reference : graph.get(uuid)) {
                long refMsb = reference.getMostSignificantBits();
                long refLsb = reference.getLeastSignificantBits();
                if (!delegate.containsSegment(refMsb, refLsb)) {
                    preload(refMsb, refLsb, depth);
                } else if (depth < preloadDepth && SegmentId.isDataSegmentId(refLsb)) {
                    dispatch(refMsb, refLsb, depth + 1);
                }
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
            return "DispatchTask{segmentId=" + new UUID(msb, lsb) + ", depth=" + depth + '}';
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

    class PreloadTask implements Runnable {

        private final TarFiles tarFiles;

        private final long msb;

        private final long lsb;

        private final int depth;

        private PreloadTask(TarFiles tarFiles, long msb, long lsb, int depth) {
            checkArgument(depth <= preloadDepth, "depth must be <= %d, is %d", preloadDepth, depth);
            this.tarFiles = tarFiles;
            this.msb = msb;
            this.lsb = lsb;
            this.depth = depth;
            LOG.debug("Created: {}", this);
        }

        @Override
        public void run() {
            LOG.debug("Running: {}", this);
            if (depth < preloadDepth && SegmentId.isDataSegmentId(lsb)) {
                dispatch(msb, lsb, depth + 1);
            }
            if (!delegate.containsSegment(msb, lsb)) {
                Buffer segmentBuffer = tarFiles.readSegment(msb, lsb);
                if (segmentBuffer != null) {
                    delegate.writeSegment(msb, lsb, segmentBuffer);
                }
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

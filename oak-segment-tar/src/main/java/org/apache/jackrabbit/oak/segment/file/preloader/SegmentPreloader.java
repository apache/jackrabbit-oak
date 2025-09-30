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
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
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
 * asynchronously prefetching segments referenced by a segment that is being read
 * from the cache.
 *
 * @see PersistentCachePreloadingConfiguration
 */
public class SegmentPreloader extends DelegatingPersistentCache implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentPreloader.class);

    private final Map<Integer, String> inProgressPrefetch;

    private final ConcurrentHashMap<String, Map<UUID, Set<UUID>>> graphCache;

    private final Set<UUID> cachedSegments;

    private final PersistentCache delegate;

    private final ExecutorService dispatchPool;

    private final ExecutorService prefetchPool;

    private final int prefetchDepth;

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
        if (config.getConcurrency() > 0 && config.getPrefetchDepth() > 0) {
            return new SegmentPreloader(delegate, config, tarFiles);
        }
        return delegate;
    }

    private SegmentPreloader(@NotNull PersistentCache delegate, @NotNull PersistentCachePreloadingConfiguration config, @NotNull Supplier<TarFiles> tarFiles) {
        this.delegate = delegate;
        this.tarFiles = Suppliers.memoize(tarFiles);
        this.inProgressPrefetch = new ConcurrentHashMap<>();
        this.graphCache = new ConcurrentHashMap<>();
        this.cachedSegments = ConcurrentHashMap.newKeySet();
        this.prefetchDepth = config.getPrefetchDepth();
        this.dispatchPool = new ThreadPoolExecutor(1,1,
                1, TimeUnit.SECONDS,
                new PriorityBlockingQueue<>(),
                r -> new Thread(r, "segment-prefetch-dispatcher")) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                clearInProgressTask(r);
            }
        };
        int prefetchThreads = config.getConcurrency();
        this.prefetchPool = new ThreadPoolExecutor(Math.max(1, prefetchThreads / 4), prefetchThreads,
                10, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(prefetchThreads * 4),
                r -> {
                    String threadName = String.format("segment-prefetch-%s", Long.toHexString(System.nanoTime() & 0xFFFFF));
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
                        // this creates back-pressure to the dispatchPool, slowing down the dispatching of new prefetch tasks
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
    }

    @Override
    protected PersistentCache delegate() {
        return delegate;
    }

    @Override
    public @Nullable Buffer readSegment(long msb, long lsb, @NotNull Callable<Buffer> loader) {
        dispatch(tarFiles.get(), msb, lsb);
        return delegate().readSegment(msb, lsb, loader);
    }

    private void dispatch(@NotNull TarFiles tarFiles, long msb, long lsb) {
        dispatch(tarFiles, tarFiles::getIndices, msb, lsb, 0);
    }

    private void dispatch(@NotNull TarFiles tarFiles, Supplier<Map<String, Set<UUID>>> indicesSupplier, long msb, long lsb, int depth) {
        execute(dispatchPool, new PrefetchDispatchTask(tarFiles, indicesSupplier, msb, lsb, depth));
    }

    private void prefetch(TarFiles tarFiles, Supplier<Map<String, Set<UUID>>> indicesSupplier, long msb, long lsb, int depth) {
        execute(prefetchPool, new PrefetchTask(tarFiles, indicesSupplier, msb, lsb, depth));
    }

    private void execute(ExecutorService pool, Runnable r) {
        if (registerInProgressTask(r)) {
            pool.execute(r);
        }
    }

    private boolean registerInProgressTask(Runnable r) {
        return inProgressPrefetch.putIfAbsent(r.hashCode(), Thread.currentThread().getName()) == null;
    }

    private void clearInProgressTask(Runnable r) {
        inProgressPrefetch.remove(r.hashCode());
    }

    @VisibleForTesting
    boolean hasInProgressTasks() {
        return !inProgressPrefetch.isEmpty();
    }

    @Override
    public void close() {
        try {
            prefetchPool.shutdown();
            dispatchPool.shutdown();
            if (!prefetchPool.awaitTermination(4, TimeUnit.SECONDS)) {
                prefetchPool.shutdownNow();
            }
            if (!dispatchPool.awaitTermination(1, TimeUnit.SECONDS)) {
                dispatchPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            prefetchPool.shutdownNow();
            dispatchPool.shutdownNow();
        }
    }

    private class PrefetchDispatchTask implements Runnable, Comparable<PrefetchDispatchTask> {

        private final TarFiles tarFiles;

        private final Supplier<Map<String, Set<UUID>>> indicesSupplier;

        private final long msb;

        private final long lsb;

        private final int depth;

        private final long creationTime = System.nanoTime();

        PrefetchDispatchTask(@NotNull TarFiles tarFiles, Supplier<Map<String, Set<UUID>>> indicesSupplier, long msb, long lsb, int depth) {
            checkArgument(depth < prefetchDepth, "depth must be < %d, is %d", prefetchDepth, depth);
            this.tarFiles = tarFiles;
            this.indicesSupplier = indicesSupplier;
            this.msb = msb;
            this.lsb = lsb;
            this.depth = depth + 1;
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
                if (!cachedSegments.contains(reference) && !delegate.containsSegment(refMsb, refLsb)) {
                    prefetch(tarFiles, () -> indices, refMsb, refLsb, depth);
                } else if (depth < prefetchDepth - 1 && SegmentId.isDataSegmentId(refLsb)) {
                    dispatch(tarFiles, () -> indices, refMsb, refLsb, depth);
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o.getClass() == PrefetchDispatchTask.class)) {
                return false;
            }
            PrefetchDispatchTask that = (PrefetchDispatchTask) o;
            return msb == that.msb && lsb == that.lsb && depth == that.depth;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), msb, lsb, depth);
        }

        @Override
        public String toString() {
            return "PrefetchDispatchTask{segmentId=" + new UUID(msb, lsb) + ", depth=" + depth + '}';
        }

        private int getPrefetchDepth() {
            return depth;
        }

        private long getCreationTime() {
            return creationTime;
        }

        @Override
        public int compareTo(@NotNull SegmentPreloader.PrefetchDispatchTask o) {
            return Comparator
                    .comparing(PrefetchDispatchTask::getPrefetchDepth)
                    .thenComparing(PrefetchDispatchTask::getCreationTime)
                    .compare(this, o);
        }
    }

    private class PrefetchTask implements Runnable {

        private final TarFiles tarFiles;

        private final Supplier<Map<String, Set<UUID>>> indicesSupplier;

        private final long msb;

        private final long lsb;

        private final int depth;

        PrefetchTask(TarFiles tarFiles, Supplier<Map<String, Set<UUID>>> indicesSupplier, long msb, long lsb, int depth) {
            checkArgument(depth <= prefetchDepth, "depth must be <= %d, is %d", prefetchDepth, depth);
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
            try {
                if (depth < prefetchDepth && SegmentId.isDataSegmentId(lsb)) {
                    dispatch(tarFiles, indicesSupplier, msb, lsb, depth);
                }
                UUID uuid = new UUID(msb, lsb);
                if (!cachedSegments.contains(uuid) && !delegate.containsSegment(msb, lsb)) {
                    Buffer segmentBuffer = tarFiles.readSegment(msb, lsb);
                    if (segmentBuffer != null) {
                        cachedSegments.add(uuid);
                        delegate.writeSegment(msb, lsb, segmentBuffer);
                    }
                }
            } catch (SegmentNotFoundException e) {
                LOG.warn("SegmentNotFoundException during prefetch of segment {}", new UUID(msb, lsb), e);
                throw e;
            } catch (Exception e) {
                LOG.warn("Exception during prefetch of segment {}", new UUID(msb, lsb), e);
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o.getClass() == PrefetchTask.class)) {
                return false;
            }
            PrefetchTask that = (PrefetchTask) o;
            return msb == that.msb && lsb == that.lsb;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), msb, lsb);
        }

        @Override
        public String toString() {
            return "PrefetchTask{segmentId=" + new UUID(msb, lsb) + ", depth=" + depth + '}';
        }
    }
}

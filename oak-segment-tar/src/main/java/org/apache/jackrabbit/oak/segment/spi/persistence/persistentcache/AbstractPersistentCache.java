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
 *
 */
package org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.commons.time.Stopwatch;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractPersistentCache implements PersistentCache, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractPersistentCache.class);

    public static final int THREADS = Integer.getInteger("oak.segment.cache.threads", 10);
    public static final int WRITE_QUEUE_SIZE = Integer.getInteger("oak.segment.cache.writeQueueSize", THREADS * 100);

    protected ExecutorService executor;
    protected AtomicLong cacheSize = new AtomicLong(0);
    protected PersistentCache nextCache;
    protected final Set<String> writesPending;
    protected AtomicLong discardCount = new AtomicLong();

    protected SegmentCacheStats segmentCacheStats;

    /**
     * Name of the feature toggle for the OAK-12282 bounded write-queue fix.
     * See {@link #FT_OAK_12282_BOUNDED_WRITE_QUEUE_ENABLED}.
     */
    public static final String FT_OAK_12282 = "FT_OAK-12282";

    /**
     * Whether the bounded write queue introduced in OAK-12282 is active.
     * <p>
     * When {@code true} (default), the executor uses a queue bounded to
     * {@link #WRITE_QUEUE_SIZE} and silently discards write tasks when full,
     * preventing OOM under high write load. This is safe because the disk
     * cache is an optimisation only — a dropped write means the segment is
     * fetched from remote storage on the next read.
     * <p>
     * Set to {@code false} via the {@link org.apache.jackrabbit.oak.spi.toggle.FeatureToggle}
     * registered with the Whiteboard to revert to the pre-fix unbounded queue.
     * <strong>Note:</strong> changing this flag requires a process restart, as
     * the executor is created at startup.
     */
    public static final AtomicBoolean FT_OAK_12282_BOUNDED_WRITE_QUEUE_ENABLED = new AtomicBoolean(true);

    public AbstractPersistentCache() {
        // Formerly Executors.newFixedThreadPool() was used here, which creates an unbounded
        // LinkedBlockingQueue — allowing unlimited segment buffers to pile up in memory under
        // high write load. The bounded queue (gated by FT_OAK_12282) prevents OOM by dropping
        // write tasks when full; this is safe because the disk cache is an optimisation only.
        BlockingQueue<Runnable> writeQueue = FT_OAK_12282_BOUNDED_WRITE_QUEUE_ENABLED.get()
                ? new LinkedBlockingQueue<>(WRITE_QUEUE_SIZE)
                : new LinkedBlockingQueue<>();
        executor = new ThreadPoolExecutor(
                THREADS, THREADS,
                0L, TimeUnit.MILLISECONDS,
                writeQueue,
                (r, e) -> {
                    discardCount.incrementAndGet();
                    logger.debug("Segment write task discarded: write queue full (capacity={})", WRITE_QUEUE_SIZE);
                });
        writesPending = ConcurrentHashMap.newKeySet();
    }

    public PersistentCache linkWith(AbstractPersistentCache nextCache) {
        this.nextCache = nextCache;
        return nextCache;
    }

    @Override
    public Buffer readSegment(long msb, long lsb, @NotNull Callable<Buffer> loader) {
        Buffer segment = readSegmentInternal(msb, lsb);
        if (segment != null) {
            segmentCacheStats.hitCount.incrementAndGet();
            return segment;
        }
        segmentCacheStats.missCount.incrementAndGet();

        // Either use the next cache or the 'loader'
        Callable<Buffer> nextLoader = nextCache != null
                ? () -> nextCache.readSegment(msb, lsb, loader)
                : loader;

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            segment = nextLoader.call();

            if (segment != null) {
                recordCacheLoadTimeInternal(stopwatch.elapsed(TimeUnit.NANOSECONDS), true);
                writeSegment(msb, lsb, segment);
            }

            return segment;
        } catch (RepositoryNotReachableException e) {
            recordCacheLoadTimeInternal(stopwatch.elapsed(TimeUnit.NANOSECONDS), false);

            // rethrow exception so that this condition can be distinguished from other types of errors (see OAK-9303)
            throw e;
        } catch (Exception t) {
            logger.error("Exception while loading segment {} from remote store or linked cache", new UUID(msb, lsb), t);
            recordCacheLoadTimeInternal(stopwatch.elapsed(TimeUnit.NANOSECONDS), false);
        }
        return segment;
    }

    /**
     * Reads the segment from the cache.
     * If segment is not found, this method does not query next cache that was set with {@link #linkWith(AbstractPersistentCache)}
     *
     * @param msb the most significant bits of the identifier of the segment
     * @param lsb the least significant bits of the identifier of the segment
     * @return byte buffer containing the segment data or null if the segment doesn't exist
     */
    protected abstract Buffer readSegmentInternal(long msb, long lsb);

    /**
     * Records time spent to load data from external source, after cache miss.
     *
     * @param loadTime   load time in nanoseconds
     * @param successful indicates whether loading of the segment into cache was successful
     */
    protected final void recordCacheLoadTimeInternal(long loadTime, boolean successful) {
        if (successful) {
            segmentCacheStats.loadSuccessCount.incrementAndGet();
        } else {
            segmentCacheStats.loadExceptionCount.incrementAndGet();
        }
        segmentCacheStats.loadTime.addAndGet(loadTime);
    }

    /**
     * @return Statistics for this cache.
     */
    @NotNull
    @Override
    public AbstractCacheStats getCacheStats() {
        return segmentCacheStats;
    }

    @Override
    public void close() {
        try {
            executor.shutdown();
            if (executor.awaitTermination(60, SECONDS)) {
                logger.debug("The persistent cache scheduler was successfully shut down");
            } else {
                logger.warn("The persistent cache scheduler takes too long to shut down");
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupt while shutting down the persistent cache scheduler", e);
            currentThread().interrupt();
        }
    }

    public int getWritesPending() {
        return writesPending.size();
    }

    public long getWriteDiscardCount() {
        return discardCount.get();
    }
}

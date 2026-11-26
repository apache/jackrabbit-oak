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
package org.apache.jackrabbit.oak.segment.remote.persistentcache;

import java.nio.ByteBuffer;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.log.LogSilencer;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.segment.spi.SegmentReadInterruptedException;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous-writer variant of {@link AbstractPersistentDiskCacheV2}. Inside the cache-miss loader, the loaded
 * buffer is enqueued on a pool of background writer threads and the caller is unblocked immediately; cache hits
 * that arrive before the disk write completes are served from the in-memory pending-writes buffer to avoid false
 * misses.
 * <p>
 * Writers check pending entries before and after writing to handle eviction and supersession:
 * <ul>
 *     <li>Pre-write: before writing, the worker verifies the entry it took off the queue is still the active
 *         pending entry for that segment id (identity comparison). A newer pending entry for the same id
 *         supersedes the older one, and the older one is skipped.</li>
 *     <li>Post-write: the worker uses {@code pendingSegments.remove(key, value)} to clean up its entry
 *         atomically. If that returns {@code false} (a concurrent discard or eviction won the race) and no
 *         superseding pending entry exists, the worker invokes {@link #cleanupOrphan} to delete the just-written
 *         file.</li>
 * </ul>
 * <p>
 * Delayed writes let the cache evict newly loaded entries
 * before disk I/O starts. Writers release the oldest entry when its delay expires or the queue reaches
 * the preemptive flush threshold, below its hard capacity. Pending buffers remain readable throughout.
 */
public class PersistentDiskCacheV2Async extends AbstractPersistentDiskCacheV2 {
    public static final String NAME = "Segment Disk Cache";

    private static final Logger LOG = LoggerFactory.getLogger(PersistentDiskCacheV2Async.class);

    static final class PendingSegment {
        final UUID segmentId;
        final ByteBuffer buffer;

        PendingSegment(UUID segmentId, ByteBuffer buffer) {
            this.segmentId = segmentId;
            this.buffer = buffer;
        }
    }

    private static final PendingSegment SENTINEL = new PendingSegment(new UUID(0L, 0L), null);

    static final int DEFAULT_WRITER_THREADS =
            SystemPropertySupplier.create("oak.segment.disk.cache.async.writers", 1)
                    .loggingTo(LOG)
                    .validateWith(v -> v > 0)
                    .get();

    static final int DEFAULT_PENDING_QUEUE_SIZE =
            SystemPropertySupplier.create("oak.segment.disk.cache.async.queue.size", 128)
                    .loggingTo(LOG)
                    .validateWith(v -> v > 0)
                    .get();

    private static final long QUEUE_PUT_WARN_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final long CLOSE_SENTINEL_OFFER_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(1);

    private static final String QUEUE_SATURATION_LOG_KEY = "writer-queue-saturated";

    static final String WRITE_DELAY_MILLIS_PROP = "oak.segment.disk.cache.async.write.delay.millis";
    static final String PREEMPTIVE_FLUSH_SIZE_PROP = "oak.segment.disk.cache.async.preemptive.flush.size";

    private final AsyncDiskCacheStats asyncStats;
    private final ExecutorService writerPool;
    private final int writerThreadCount;
    private final PendingWriteQueue queue;
    private final ConcurrentMap<UUID, PendingSegment> pendingSegments = new ConcurrentHashMap<>();
    private final long startNanos = System.nanoTime();
    private final AtomicLong totalEnqueueWaitNanos = new AtomicLong();

    public PersistentDiskCacheV2Async(Path directory, int cacheMaxSizeMB, DiskCacheIOMonitor diskCacheIOMonitor) throws IOException {
        this(directory, cacheMaxSizeMB, diskCacheIOMonitor, DEFAULT_WRITER_THREADS, DEFAULT_PENDING_QUEUE_SIZE);
    }

    public PersistentDiskCacheV2Async(Path directory, int cacheMaxSizeMB, DiskCacheIOMonitor diskCacheIOMonitor,
                                      int writerThreads, int pendingSegmentsQueueSize) throws IOException {
        this(directory, cacheMaxSizeMB, diskCacheIOMonitor, writerThreads, pendingSegmentsQueueSize,
                SystemPropertySupplier.create(WRITE_DELAY_MILLIS_PROP, 100L)
                        .loggingTo(LOG).validateWith(v -> v >= 0).get(),
                SystemPropertySupplier.create(PREEMPTIVE_FLUSH_SIZE_PROP,
                                Math.max(1, pendingSegmentsQueueSize / 2))
                        .loggingTo(LOG).get());
    }

    PersistentDiskCacheV2Async(Path directory, int cacheMaxSizeMB, DiskCacheIOMonitor diskCacheIOMonitor,
                               int writerThreads, int pendingSegmentsQueueSize, long writeDelayMillis,
                               int preemptiveFlushSize) throws IOException {
        super(directory, cacheMaxSizeMB, diskCacheIOMonitor, NAME);
        this.writerThreadCount = writerThreads;
        this.asyncStats = diskCacheIOMonitor.asyncStats();
        this.queue = new PendingWriteQueue(pendingSegmentsQueueSize, preemptiveFlushSize, writeDelayMillis,
                asyncStats);
        LOG.info("Persistent disk cache pending writes: delay={} ms, preemptiveFlushSize={}, maxSize={}",
                writeDelayMillis, preemptiveFlushSize, pendingSegmentsQueueSize);
        AtomicInteger writerThreadIndex = new AtomicInteger();
        this.writerPool = Executors.newFixedThreadPool(writerThreads, runnable -> {
            Thread thread = new Thread(runnable, "disk-cache-writer-" + writerThreadIndex.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
        rehydrateFromDisk();
        for (int i = 0; i < writerThreads; i++) {
            writerPool.submit(this::processQueue);
        }
    }

    @Override
    protected void handleLoadedSegment(UUID segmentId, ByteBuffer buffer) {
        ensureOpen();
        // Snapshot the buffer's framing (position/limit) now, on the loader thread, before the caller gets
        // its instance back. duplicate() shares the backing bytes but gives the pending entry an independent,
        // stable cursor. The writer and getPendingSegment still take their own duplicate of this
        // snapshot, since a single pending entry is read concurrently by the writer and by any cache hits.
        ByteBuffer snapshot = buffer.duplicate();
        snapshot.rewind();
        PendingSegment pending = new PendingSegment(segmentId, snapshot);
        pendingSegments.put(segmentId, pending);

        long start = System.nanoTime();
        try {
            queue.put(pending);
        } catch (InterruptedException e) {
            // Handle as an interrupted read instead of letting it surface as a generic failure that
            // the segment store would misclassify as a SegmentNotFoundException.
            // The queue's interruptible wait already cleared the interrupt flag when it threw.
            pendingSegments.remove(segmentId, pending);
            throw new SegmentReadInterruptedException(
                    "Interrupted while enqueuing segment " + segmentId + " for caching", e);
        } catch (IllegalStateException e) {
            pendingSegments.remove(segmentId, pending);
            throw e;
        }
        long waited = System.nanoTime() - start;
        long totalWait = totalEnqueueWaitNanos.addAndGet(waited);

        if (waited > QUEUE_PUT_WARN_NANOS) {
            maybeWarn(waited, totalWait);
        }
    }

    private void maybeWarn(long waitedNanos, long totalWaitNanos) {
        if (logSilencer.silence(QUEUE_SATURATION_LOG_KEY)) {
            return;
        }
        long sinceStartNanos = System.nanoTime() - startNanos;
        // Sum of wait times across all loader threads divided by wall-clock; can exceed 100% under
        // concurrent enqueues — a value of N% roughly means N/100 threads were blocked on average.
        double pct = sinceStartNanos > 0
                ? (100.0 * totalWaitNanos) / sinceStartNanos
                : 0.0;
        LOG.warn("Persistent disk cache writer queue saturated: enqueue blocked for {} ms. " +
                        "totalEnqueuingTime: {} ms / percentageTimeEnqueuing: {}%{}",
                TimeUnit.NANOSECONDS.toMillis(waitedNanos),
                TimeUnit.NANOSECONDS.toMillis(totalWaitNanos),
                String.format("%.2f", pct),
                LogSilencer.SILENCING_POSTFIX);
    }

    @Override
    protected ByteBuffer getPendingSegment(UUID segmentId) {
        PendingSegment pending = pendingSegments.get(segmentId);
        if (pending == null) {
            return null;
        }
        // Both the caller and the internal write threads may read from the buffer concurrently, which mutates
        // its position. Return a duplicate (shallow copy with independent position) reset to the start.
        ByteBuffer copy = pending.buffer.duplicate();
        copy.rewind();
        return copy;
    }

    @Override
    protected boolean onEvictionHook(UUID segmentId) {
        // Removing the segment from the queue itself would require a linear scan.
        // Removing it from pendingSegments is enough: the writer thread re-checks membership both before and
        // after the disk write, and falls back to cleanupOrphan() if it detects a discard mid-write. With cache
        // maintenance forced synchronous (see CacheBuilder#asyncMaintenance), this hook runs on the same
        // caller thread that triggered the eviction, so there is no stale, delayed notification that could race
        // an unrelated, newer pending write for the same segment id.
        if (pendingSegments.remove(segmentId) != null) {
            asyncStats.recordPendingEviction();
            return true;
        }
        return false;
    }

    @Override
    protected void doClose() {
        queue.startDraining();
        try {
            stopWriters();
        } finally {
            // Forced shutdown can leave entries that no worker will dequeue. Discard them to release
            // their buffers and subtract this queue's backlog from gauges reused when the cache reopens.
            for (PendingSegment pending : queue.discardPending()) {
                pendingSegments.remove(pending.segmentId, pending);
            }
        }
    }

    private void stopWriters() {
        // We use offer() with a timeout so a saturated queue cannot deadlock close(); if any sentinel
        // cannot be placed, we fall back to shutdownNow() which interrupts the workers — they break out of
        // queue.take() or their in-flight doWrite via the existing InterruptedException handlers.
        boolean saturated = false;
        for (int i = 0; i < writerThreadCount; i++) {
            try {
                if (!queue.offer(SENTINEL, CLOSE_SENTINEL_OFFER_TIMEOUT_NANOS, TimeUnit.NANOSECONDS)) {
                    LOG.warn("Writer queue saturated during close; interrupting workers");
                    saturated = true;
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                saturated = true;
                break;
            }
        }
        if (saturated) {
            writerPool.shutdownNow();
        }
        new ExecutorCloser(writerPool, 10, TimeUnit.SECONDS).close();
    }

    /**
     * Write the buffer to disk for {@code segmentId}. Tests override this to inject latch-based pauses or
     * simulated failures.
     */
    protected void doWrite(UUID segmentId, ByteBuffer buffer) throws IOException {
        writeSegmentInternal(segmentId, buffer);
    }

    /**
     * Delete an orphan segment file produced when a write completed after the cache entry was already evicted
     * (and no superseding pending entry exists). Tests override this to record invocations instead of touching
     * the filesystem.
     */
    protected void cleanupOrphan(UUID segmentId) {
        Path path = getSegmentPath(segmentId.toString());
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            LOG.info("Cleanup of orphan segment file {} failed", path, e);
        }
    }

    @TestOnly
    int queueSize() {
        return queue.size();
    }

    @TestOnly
    int pendingSegmentsSize() {
        return pendingSegments.size();
    }

    @Override
    protected String writeStatsContext() {
        return ", queueSize: " + queue.size() + ", pendingSegments: " + pendingSegments.size()
                + ", evictedPending: " + asyncStats.getEvictedPendingCount()
                + ", writesSkippedEvicted: " + asyncStats.getWritesSkippedEvictedCount()
                + ", writesSkippedSuperseded: " + asyncStats.getWritesSkippedSupersededCount()
                + ", orphanCleanups: " + asyncStats.getOrphanCleanupsCount()
                + ", orphanBytes: " + asyncStats.getOrphanBytesCount();
    }

    private void processQueue() {
        while (true) {
            try {
                PendingSegment pending = queue.take();
                if (pending == SENTINEL) {
                    LOG.info("Writer thread received sentinel, terminating");
                    break;
                }
                // Identity comparison is intentional: a newer PendingSegment for the same id must not satisfy
                // this check, so the stale one is skipped.
                PendingSegment current = pendingSegments.get(pending.segmentId);
                if (current != pending) {
                    asyncStats.recordWriteSkipped(current == null);
                    LOG.debug("Segment {} no longer pending (discarded or superseded), skipping write",
                            pending.segmentId);
                    continue;
                }
                boolean writeSucceeded = false;
                int segmentBytes = pending.buffer.remaining();
                try {
                    ByteBuffer bufferCopy = pending.buffer.asReadOnlyBuffer();
                    bufferCopy.rewind();
                    doWrite(pending.segmentId, bufferCopy);
                    writeSucceeded = true;
                } catch (Exception e) {
                    logWriteFailure(pending.segmentId, e);
                } finally {
                    // remove(key, value) is atomic. If it returns false, a concurrent discard or supersession
                    // won the race. The file we just wrote is an orphan from the cache's perspective unless a
                    // newer pending entry replaced us (which will overwrite the file when it runs).
                    boolean removedOurs = pendingSegments.remove(pending.segmentId, pending);
                    if (writeSucceeded && !removedOurs && !pendingSegments.containsKey(pending.segmentId)) {
                        asyncStats.recordOrphanCleanup(segmentBytes);
                        cleanupOrphan(pending.segmentId);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}

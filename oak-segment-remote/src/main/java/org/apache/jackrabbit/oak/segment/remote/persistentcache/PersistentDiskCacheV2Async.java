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

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.segment.spi.SegmentReadInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Asynchronous-writer variant of {@link AbstractPersistentDiskCacheV2}. Inside the cache-miss loader, the loaded
 * buffer is enqueued on a bounded queue for background writers. Cache hits before the write completes
 * are served from memory. Writers skip entries that have been evicted or superseded and remove orphaned
 * files when eviction races with an in-progress write.
 */
public class PersistentDiskCacheV2Async extends AbstractPersistentDiskCacheV2 {
    public static final String NAME = "Segment Disk Cache";

    private static final Logger LOG = LoggerFactory.getLogger(PersistentDiskCacheV2Async.class);

    private static final class PendingSegment {
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

    private static final long CLOSE_SENTINEL_OFFER_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(1);

    private static final int WRITE_LOCK_COUNT = 64;

    private final ExecutorService writerPool;
    private final int writerThreadCount;
    private final ArrayBlockingQueue<PendingSegment> queue;
    private final ConcurrentMap<UUID, PendingSegment> pendingSegments = new ConcurrentHashMap<>();
    private final Object[] writeLocks = new Object[WRITE_LOCK_COUNT];

    public PersistentDiskCacheV2Async(Path directory, int cacheMaxSizeMB, DiskCacheIOMonitor diskCacheIOMonitor) throws IOException {
        this(directory, cacheMaxSizeMB, diskCacheIOMonitor, DEFAULT_WRITER_THREADS, DEFAULT_PENDING_QUEUE_SIZE);
    }

    public PersistentDiskCacheV2Async(Path directory, int cacheMaxSizeMB, DiskCacheIOMonitor diskCacheIOMonitor,
                                      int writerThreads, int pendingSegmentsQueueSize) throws IOException {
        super(directory, cacheMaxSizeMB, diskCacheIOMonitor, NAME);
        this.writerThreadCount = writerThreads;
        this.queue = new ArrayBlockingQueue<>(pendingSegmentsQueueSize);
        for (int i = 0; i < writeLocks.length; i++) {
            writeLocks[i] = new Object();
        }
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

        try {
            queue.put(pending);
            ensureOpen();
        } catch (InterruptedException e) {
            pendingSegments.remove(segmentId, pending);
            throw new SegmentReadInterruptedException(
                    "Interrupted while enqueuing segment " + segmentId + " for caching", e);
        } catch (IllegalStateException e) {
            queue.remove(pending);
            pendingSegments.remove(segmentId, pending);
            throw e;
        }
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
        // Removing the segment from the queue itself would be too expensive (linear scan of an ArrayBlockingQueue).
        // Removing it from pendingSegments is enough: the writer thread re-checks membership both before and
        // after the disk write, and falls back to cleanupOrphan() if it detects a discard mid-write. With cache
        // maintenance forced synchronous (see CacheBuilder#asyncMaintenance), this hook runs on the same
        // caller thread that triggered the eviction, so there is no stale, delayed notification that could race
        // an unrelated, newer pending write for the same segment id.
        return pendingSegments.remove(segmentId) != null;
    }

    @Override
    protected void doClose() {
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
        queue.clear();
        pendingSegments.clear();
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

    private void processQueue() {
        while (true) {
            try {
                PendingSegment pending = queue.take();
                if (pending == SENTINEL) {
                    break;
                }
                // An evicted segment can be reloaded while its old write is in progress. Serialize
                // writes of the same id so the old writer cannot overwrite or delete the newer file.
                synchronized (writeLocks[Math.floorMod(pending.segmentId.hashCode(), writeLocks.length)]) {
                    writePending(pending);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void writePending(PendingSegment pending) {
        UUID segmentId = pending.segmentId;
        if (pendingSegments.get(segmentId) != pending) {
            return;
        }

        boolean written = false;
        try {
            doWrite(segmentId, pending.buffer.asReadOnlyBuffer());
            written = true;
        } catch (Exception e) {
            logWriteFailure(segmentId, e);
        } finally {
            if (!pendingSegments.remove(segmentId, pending) && written && !pendingSegments.containsKey(segmentId)) {
                cleanupOrphan(segmentId);
            }
        }
    }
}

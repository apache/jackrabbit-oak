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
import org.apache.jackrabbit.oak.segment.spi.SegmentReadInterruptedException;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class PersistentDiskCacheV2AsyncTest extends AbstractPersistentDiskCacheV2Test {

    private static final int WRITER_THREADS = 4;
    private static final int PENDING_QUEUE_SIZE = 100;

    @Override
    protected AbstractPersistentDiskCacheV2 createCache(Path dir, int cacheMaxSizeMB,
                                                       DiskCacheIOMonitor monitor) throws IOException {
        return new PersistentDiskCacheV2Async(dir, cacheMaxSizeMB, monitor, WRITER_THREADS, PENDING_QUEUE_SIZE);
    }

    @Override
    protected String expectedCacheName() {
        return PersistentDiskCacheV2Async.NAME;
    }

    @Test
    public void readsPendingSegmentBeforeWriterPublishesFile() throws Exception {
        UUID id = UUID.randomUUID();
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();
        Path dir = tempFolder.newFolder().toPath();
        ByteBuffer expected = createRandomBuffer(1024);
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        try (PersistentDiskCacheV2Async asyncCache = new PersistentDiskCacheV2Async(dir, CACHE_SIZE_MB, noopMonitor(), 1, 2) {
            @Override
            protected void doWrite(UUID segmentId, ByteBuffer buffer) throws IOException {
                writing.countDown();
                awaitWriteRelease(releaseWrite);
                super.doWrite(segmentId, buffer);
            }
        }) {
            try {
                ByteBuffer loaded = asyncCache.readSegment(msb, lsb, expected::duplicate);
                assertTrue(writing.await(5, TimeUnit.SECONDS));
                assertFalse(Files.exists(segmentPath(dir, id)));
                loaded.get();
                ByteBuffer firstHit = asyncCache.readSegment(msb, lsb, () -> {
                    throw new AssertionError("pending cache hit must not call the loader");
                });
                assertBufferEquals(expected, firstHit);
                firstHit.get();
                assertBufferEquals(expected, asyncCache.readSegment(msb, lsb, () -> null));
            } finally {
                releaseWrite.countDown();
            }
        }
        assertTrue(Files.exists(segmentPath(dir, id)));
    }

    @Test
    public void evictionDuringWriteDoesNotRemoveReplacementFile() throws Exception {
        UUID id = UUID.randomUUID();
        Path dir = tempFolder.newFolder().toPath();
        ByteBuffer first = ByteBuffer.wrap(new byte[] {1});
        ByteBuffer replacement = ByteBuffer.wrap(new byte[] {2});
        CountDownLatch firstWriting = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger writes = new AtomicInteger();
        try (PersistentDiskCacheV2Async asyncCache = new PersistentDiskCacheV2Async(dir, CACHE_SIZE_MB, noopMonitor(), 2, 2) {
            @Override
            protected void doWrite(UUID segmentId, ByteBuffer buffer) throws IOException {
                if (writes.incrementAndGet() == 1) {
                    firstWriting.countDown();
                    awaitWriteRelease(releaseFirst);
                }
                super.doWrite(segmentId, buffer);
            }
        }) {
            try {
                asyncCache.handleLoadedSegment(id, first);
                assertTrue(firstWriting.await(5, TimeUnit.SECONDS));
                assertTrue(asyncCache.onEvictionHook(id));
                asyncCache.handleLoadedSegment(id, replacement);
                assertBufferEquals(replacement, asyncCache.getPendingSegment(id));
            } finally {
                releaseFirst.countDown();
            }
        }
        assertEquals(2, writes.get());
        assertBufferEquals(replacement, ByteBuffer.wrap(Files.readAllBytes(segmentPath(dir, id))));
    }

    private static void awaitWriteRelease(CountDownLatch release) throws IOException {
        try {
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for test to release cache writer");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    /**
     * {@code close()} must flush all pending async writes to disk before returning — no enqueued segment
     * may be dropped on shutdown. This is the behaviour the AzureCheck tools rely on when they close the
     * cache at the end of a run.
     * <p>
     * A single writer thread with a queue sized to the whole batch lets the rapid enqueues build a real
     * backlog that one writer cannot drain instantly, so the segments are genuinely still pending when
     * {@code close()} is called. The cache is sized large enough that no eviction interferes. Crucially,
     * file existence is asserted immediately after {@code close()} returns with no intervening sleep:
     * {@code close()} joins the writer pool, so if every file is present at that point, the drain happened
     * during {@code close()} rather than via writers that merely kept running in the background.
     */
    @Test
    public void testCloseFlushesPendingAsyncWrites() throws Exception {
        Path dir = tempFolder.newFolder("close-flush").toPath();
        final int count = 300;
        final int segmentSize = 4096;
        PersistentDiskCacheV2Async asyncCache =
                new PersistentDiskCacheV2Async(dir, 64, noopMonitor(), 1, count);

        long[][] ids = new long[count][];
        for (int i = 0; i < count; i++) {
            ids[i] = randomSegmentId();
            ByteBuffer buffer = createRandomBuffer(segmentSize);
            asyncCache.readSegment(ids[i][0], ids[i][1], () -> buffer);
        }

        // Graceful drain: the close sentinel sits behind the queued segments, so the writer processes the
        // entire backlog before terminating; close() then joins the writer pool before returning.
        asyncCache.close();

        for (long[] id : ids) {
            Path file = segmentPath(dir, id);
            assertTrue("pending async write must be flushed to disk by close(): " + file, Files.exists(file));
            assertEquals("flushed file must contain the full segment", segmentSize, Files.size(file));
        }
    }

    /**
     * If the loader thread is interrupted while enqueuing a loaded segment for the background writer
     * ({@code ArrayBlockingQueue.put} acquires its lock via {@code lockInterruptibly}, which throws
     * {@code InterruptedException} when the interrupt flag is set — the aborted/timed-out transaction stack
     * trace seen in the field), {@code handleLoadedSegment} must honor the interrupt as a cancelled read by
     * throwing {@link SegmentReadInterruptedException} — so the segment store does not misclassify it as a
     * missing segment — and must not leak the pending entry it had registered.
     */
    @Test
    public void testEnqueueInterruptIsHonouredAndDoesNotLeakPendingEntry() throws Exception {
        Path dir = tempFolder.newFolder("enqueue-interrupt").toPath();
        try (PersistentDiskCacheV2Async asyncCache =
                 new PersistentDiskCacheV2Async(dir, CACHE_SIZE_MB, noopMonitor(), 1, 1)) {
            long[] id = randomSegmentId();
            ByteBuffer buffer = createRandomBuffer(1024);

            // Pre-interrupt the calling thread: ArrayBlockingQueue.put acquires its lock via
            // lockInterruptibly, which throws InterruptedException immediately when the interrupt
            // flag is set.
            Thread.currentThread().interrupt();

            SegmentReadInterruptedException failure = assertThrows(SegmentReadInterruptedException.class,
                    () -> asyncCache.readSegment(id[0], id[1], buffer::duplicate));
            assertTrue(failure.getCause() instanceof InterruptedException);
            assertFalse(asyncCache.containsSegment(id[0], id[1]));
            assertNull(asyncCache.getPendingSegment(new UUID(id[0], id[1])));
        } finally {
            Thread.interrupted(); // clear flag so it does not bleed into subsequent tests
        }
    }
}

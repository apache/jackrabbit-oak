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
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Race-condition tests for {@link PersistentDiskCacheV2Async}'s writer protocol: discard-before-write,
 * discard-mid-write (orphan cleanup), supersession during write, and write-failure handling. Determinism
 * comes from a {@link ControllablePersistentDiskCacheV2Async} that overrides {@code doWrite} so it pauses
 * on per-segment latches.
 */
public class PersistentDiskCacheV2AsyncRaceTest {

    private static final ByteBuffer DUMMY_BUFFER = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5});
    private static final long QUIESCENCE_TIMEOUT_MS = 5000;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Path cacheDir;
    private ControllablePersistentDiskCacheV2Async cache;
    private ScheduledExecutorService executor;
    private DiskCacheIOMonitor monitor;

    @Before
    public void setUp() throws IOException {
        cacheDir = tempFolder.newFolder("cache").toPath();
        executor = Executors.newSingleThreadScheduledExecutor();
        monitor = new DiskCacheIOMonitor(new DefaultStatisticsProvider(executor));
        cache = new ControllablePersistentDiskCacheV2Async(cacheDir, monitor);
    }

    @After
    public void tearDown() {
        cache.close();
        executor.shutdownNow();
    }

    /**
     * Happy path — write succeeds, no cleanup.
     */
    @Test
    public void testHappyPath() throws Exception {
        cache.enqueue("seg-1", DUMMY_BUFFER);
        awaitCondition(() -> cache.doWriteCalls.size() == 1 && isQuiesced(),
                "doWrite invoked and writer quiesced");

        assertEquals(List.of(uuid("seg-1")), cache.doWriteCalls);
        assertTrue(cache.cleanupOrphanCalls.isEmpty());
        assertCounters(0, 0, 0, 0, 0);
    }

    /**
     * Discard before the writer's pre-check — doWrite must be skipped.
     */
    @Test
    public void testDiscardBeforePreCheck() throws Exception {
        cache.blockNext("blocker");
        cache.enqueue("blocker", DUMMY_BUFFER);
        cache.awaitEntered("blocker");

        cache.enqueue("seg-B", DUMMY_BUFFER);    // queued behind the blocked writer
        cache.discard("seg-B");                  // discard while still in queue
        cache.release("blocker");

        awaitCondition(() -> cache.doWriteCalls.size() == 1 && isQuiesced(),
                "blocker written, seg-B skipped, writer quiesced");

        assertEquals(List.of(uuid("blocker")), cache.doWriteCalls);   // seg-B was never written
        assertTrue(cache.cleanupOrphanCalls.isEmpty());
        assertCounters(1, 1, 0, 0, 0);
    }

    /**
     * Discard mid-write — orphan cleanup must fire.
     */
    @Test
    public void testDiscardMidWriteTriggersOrphanCleanup() throws Exception {
        cache.blockNext("seg-B");
        cache.enqueue("seg-B", DUMMY_BUFFER);
        cache.awaitEntered("seg-B");

        cache.discard("seg-B");                  // discard while writer is inside doWrite
        cache.release("seg-B");

        awaitCondition(() -> cache.cleanupOrphanCalls.size() == 1, "cleanupOrphan invoked");

        assertEquals(List.of(uuid("seg-B")), cache.doWriteCalls);
        assertEquals(List.of(uuid("seg-B")), cache.cleanupOrphanCalls);
        assertTrue(isQuiesced());
        assertCounters(1, 0, 0, 1, DUMMY_BUFFER.remaining());
    }

    /**
     * Supersession during write — new pending entry for same id; no cleanup, second write runs.
     */
    @Test
    public void testSupersessionDuringWriteSkipsCleanup() throws Exception {
        cache.blockNext("foo");
        cache.enqueue("foo", DUMMY_BUFFER);
        cache.awaitEntered("foo");

        cache.discard("foo");                    // removes A from pendingSegments
        cache.enqueue("foo", DUMMY_BUFFER);      // new PendingSegment B for the same id
        cache.release("foo");                    // A's doWrite returns; B will be processed next

        awaitCondition(() -> cache.doWriteCalls.size() == 2 && isQuiesced(),
                "both A and B written, writer quiesced");

        // Two doWrites: A (blocked then released) and B (processed after, no block remaining).
        assertEquals(List.of(uuid("foo"), uuid("foo")), cache.doWriteCalls);
        // No cleanup: A's post-check saw a superseding B → skipped; B's write succeeded → no cleanup.
        assertTrue(cache.cleanupOrphanCalls.isEmpty());
        assertCounters(1, 0, 0, 0, 0);
    }

    /**
     * Pre-check identity comparison — when both an old and a new PendingSegment for the same id sit in
     * the queue, only the newer one must be written. {@code containsKey} alone would not catch this;
     * the pre-check relies on identity comparison.
     */
    @Test
    public void testPreCheckSkipsStaleSupersededEntryInQueue() throws Exception {
        cache.blockNext("blocker");
        cache.enqueue("blocker", DUMMY_BUFFER);
        cache.awaitEntered("blocker");

        cache.enqueue("foo", DUMMY_BUFFER);      // A1 — sits in the queue while blocker is in flight
        cache.discard("foo");                    // A1 removed from pendingSegments, but still in queue
        cache.enqueue("foo", DUMMY_BUFFER);      // A2 — new PendingSegment, also enqueued
        cache.release("blocker");

        awaitCondition(() -> cache.doWriteCalls.size() == 2 && isQuiesced(),
                "blocker + A2 written (A1 skipped), writer quiesced");

        // blocker was written; A1 was skipped at pre-check (get("foo") == A2, not A1); A2 was written.
        assertEquals(List.of(uuid("blocker"), uuid("foo")), cache.doWriteCalls);
        assertTrue(cache.cleanupOrphanCalls.isEmpty());
        assertCounters(1, 0, 1, 0, 0);
    }

    /**
     * Regression test for the supersession-after-eviction race. With real disk writes, A's file must remain
     * on disk after B's write completes — the underlying file open mode must be idempotent so B can overwrite
     * the file A left behind. Before the fix to use CREATE+TRUNCATE_EXISTING, B's CREATE_NEW would throw
     * FileAlreadyExistsException and the error-path delete would leave the cache with a phantom entry pointing
     * at a non-existent file.
     */
    @Test
    public void testSupersessionLeavesFileOnDiskAfterRealWrites() throws Exception {
        cache.realWrite = true;
        cache.blockNext("foo");
        cache.enqueue("foo", DUMMY_BUFFER);
        cache.awaitEntered("foo");

        cache.discard("foo");                    // removes A from pendingSegments
        cache.enqueue("foo", DUMMY_BUFFER);      // new PendingSegment B for the same id
        cache.release("foo");                    // A's doWrite returns; B will be processed next

        awaitCondition(() -> cache.doWriteCalls.size() == 2 && isQuiesced(),
                "both A and B written, writer quiesced");

        assertTrue(cache.cleanupOrphanCalls.isEmpty());

        // Segments are stored one level deep, in a bucket named by the first two hex chars of the UUID.
        String name = uuid("foo").toString();
        Path file = cacheDir.resolve(name.substring(0, 2)).resolve(name);
        assertTrue("file should exist on disk after supersession", Files.exists(file));
        assertEquals("file should have full segment bytes", DUMMY_BUFFER.limit(), Files.size(file));
        assertCounters(1, 0, 0, 0, 0);
    }

    /**
     * close() must return promptly even when the writer queue is full and a worker is blocked
     * mid-write. With a single sentinel-via-put protocol, this would deadlock until the worker
     * unblocked. The fix relies on offer-with-timeout + shutdownNow fallback.
     */
    @Test
    public void testCloseUnblocksOnSaturatedQueue() throws Exception {
        // Replace the @Before-created cache with a tiny one (queue size 1, 1 writer) so we can
        // saturate the queue easily.
        cache.close();
        cache = new ControllablePersistentDiskCacheV2Async(
                tempFolder.newFolder("tiny").toPath(),
                new DiskCacheIOMonitor(StatisticsProvider.NOOP),
                1, 1, 0, 1);

        cache.blockNext("blocker");
        cache.enqueue("blocker", DUMMY_BUFFER);
        cache.awaitEntered("blocker");

        // Worker is parked in doWrite for "blocker" → queue is empty but worker is busy.
        // One more enqueue fills the queue (size 1).
        cache.enqueue("filler", DUMMY_BUFFER);

        CountDownLatch closed = new CountDownLatch(1);
        Thread closer = new Thread(() -> {
            cache.close();
            closed.countDown();
        }, "close-test");
        closer.setDaemon(true);
        closer.start();

        boolean completed = closed.await(3, TimeUnit.SECONDS);
        if (!completed) {
            cache.release("blocker");
            fail("close() did not complete within 3 seconds with a saturated queue");
        }
    }

    /**
     * doWrite throws — pendingSegments still drained, no orphan cleanup attempted.
     */
    @Test
    public void testWriteFailureDrainsPendingWithoutCleanup() throws Exception {
        cache.throwOnWrite = true;
        cache.enqueue("seg-1", DUMMY_BUFFER);
        awaitCondition(() -> cache.doWriteCalls.size() == 1 && isQuiesced(),
                "doWrite invoked (and threw), writer quiesced");

        assertEquals(List.of(uuid("seg-1")), cache.doWriteCalls);
        // writeSucceeded was false → cleanupOrphan must not be invoked
        assertTrue(cache.cleanupOrphanCalls.isEmpty());
        assertCounters(0, 0, 0, 0, 0);
    }

    @Test
    public void testRepeatedDiscardCountsPendingEvictionOnce() throws Exception {
        cache.blockNext("seg-B");
        cache.enqueue("seg-B", DUMMY_BUFFER);
        cache.awaitEntered("seg-B");

        assertTrue(cache.discard("seg-B"));
        assertFalse(cache.discard("seg-B"));
        assertFalse(cache.discard("absent"));
        cache.release("seg-B");

        assertCounters(1, 0, 0, 1, DUMMY_BUFFER.remaining());
    }

    @Test
    public void testDiscardDuringFailedWriteDoesNotCountOrphan() throws Exception {
        cache.throwOnWrite = true;
        cache.blockNext("seg-B");
        cache.enqueue("seg-B", DUMMY_BUFFER);
        cache.awaitEntered("seg-B");
        cache.discard("seg-B");
        cache.release("seg-B");

        assertCounters(1, 0, 0, 0, 0);
        assertTrue(cache.cleanupOrphanCalls.isEmpty());
    }

    @Test
    public void testDiscardMidWriteDeletesRealOrphanAndCountsBytes() throws Exception {
        cache.realWrite = true;
        cache.blockNext("seg-B");
        cache.enqueue("seg-B", DUMMY_BUFFER);
        cache.awaitEntered("seg-B");
        cache.discard("seg-B");
        cache.release("seg-B");

        assertCounters(1, 0, 0, 1, DUMMY_BUFFER.remaining());
        assertEquals(List.of(uuid("seg-B")), cache.cleanupOrphanCalls);
        assertFalse(Files.exists(cache.getSegmentPath(uuid("seg-B").toString())));
        assertEquals(", queueSize: 0, pendingSegments: 0, evictedPending: 1, writesSkippedEvicted: 0"
                + ", writesSkippedSuperseded: 0, orphanCleanups: 1, orphanBytes: 5", cache.writeStatsContext());
    }

    @Test
    public void testSupersessionWithoutEvictionCountsOnlySkippedWrite() throws Exception {
        cache.blockNext("blocker");
        cache.enqueue("blocker", DUMMY_BUFFER);
        cache.awaitEntered("blocker");
        cache.enqueue("foo", DUMMY_BUFFER);
        cache.enqueue("foo", DUMMY_BUFFER);
        cache.release("blocker");

        assertCounters(0, 0, 1, 0, 0);
        assertEquals(List.of(uuid("blocker"), uuid("foo")), cache.doWriteCalls);
        assertTrue(cache.cleanupOrphanCalls.isEmpty());
    }

    @Test
    public void testAlreadyDeletedOrphanStillCountsWastedWrite() throws Exception {
        cache.deleteBeforeCleanup = true;
        completeOrphanWrite();

        assertFalse(Files.exists(cache.getSegmentPath(uuid("seg-B").toString())));
    }

    @Test
    public void testOrphanDeletionFailureStillCountsWastedWrite() throws Exception {
        cache.deleteBeforeCleanup = true;
        cache.failCleanup = true;
        completeOrphanWrite();

        assertTrue(Files.isDirectory(cache.getSegmentPath(uuid("seg-B").toString())));
    }

    // ----- helpers -----

    private void completeOrphanWrite() throws Exception {
        cache.realWrite = true;
        cache.blockNext("seg-B");
        cache.enqueue("seg-B", DUMMY_BUFFER);
        cache.awaitEntered("seg-B");
        cache.discard("seg-B");
        cache.release("seg-B");
        assertCounters(1, 0, 0, 1, DUMMY_BUFFER.remaining());
        assertEquals(List.of(uuid("seg-B")), cache.cleanupOrphanCalls);
    }

    private void assertCounters(long evictedPending, long skippedEvicted, long skippedSuperseded,
                                long orphanCleanups, long orphanBytes) {
        // An empty queue/map is not a completion barrier: the worker may still be recording a skip or cleanup.
        cache.close();
        assertEquals("pending evictions", evictedPending, monitor.asyncStats().getEvictedPendingCount());
        assertEquals("writes skipped (evicted)", skippedEvicted, monitor.asyncStats().getWritesSkippedEvictedCount());
        assertEquals("writes skipped (superseded)", skippedSuperseded, monitor.asyncStats().getWritesSkippedSupersededCount());
        assertEquals("orphan cleanups", orphanCleanups, monitor.asyncStats().getOrphanCleanupsCount());
        assertEquals("orphan bytes", orphanBytes, monitor.asyncStats().getOrphanBytesCount());
    }

    private static UUID uuid(String name) {
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
    }

    private boolean isQuiesced() {
        return cache.queueSize() == 0 && cache.pendingSegmentsSize() == 0;
    }

    private void awaitCondition(BooleanSupplier condition, String description) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(QUIESCENCE_TIMEOUT_MS);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        fail("Timeout waiting for: " + description
                + " [queueSize=" + cache.queueSize()
                + ", pendingSegmentsSize=" + cache.pendingSegmentsSize()
                + ", doWriteCalls=" + cache.doWriteCalls
                + ", cleanupOrphanCalls=" + cache.cleanupOrphanCalls + "]");
    }

    /**
     * Test double for {@link PersistentDiskCacheV2Async} that records every {@code doWrite} and
     * {@code cleanupOrphan} invocation and can pause {@code doWrite} on a per-segment latch so tests can
     * deterministically observe the "mid-write" state. Also exposes the package-private hooks
     * {@code handleLoadedSegment} and {@code onEvictionHook} so tests can drive enqueue and discard
     * directly, without going through the cache's loader path.
     */
    static class ControllablePersistentDiskCacheV2Async extends PersistentDiskCacheV2Async {
        final List<UUID> doWriteCalls = new CopyOnWriteArrayList<>();
        final List<UUID> cleanupOrphanCalls = new CopyOnWriteArrayList<>();
        final ConcurrentMap<UUID, CountDownLatch> gates = new ConcurrentHashMap<>();
        final ConcurrentMap<UUID, CountDownLatch> entered = new ConcurrentHashMap<>();
        volatile boolean throwOnWrite = false;
        volatile boolean realWrite = false;
        volatile boolean deleteBeforeCleanup = false;
        volatile boolean failCleanup = false;

        ControllablePersistentDiskCacheV2Async(Path directory, DiskCacheIOMonitor monitor) throws IOException {
            this(directory, monitor, 1, 32);
        }

        ControllablePersistentDiskCacheV2Async(Path directory, DiskCacheIOMonitor monitor,
                                               int writerThreads, int queueSize) throws IOException {
            super(directory, 1, monitor, writerThreads, queueSize);
        }

        ControllablePersistentDiskCacheV2Async(Path directory, DiskCacheIOMonitor monitor,
                                               int writerThreads, int queueSize, long delayMillis,
                                               int preemptiveFlushSize) throws IOException {
            super(directory, 1, monitor, writerThreads, queueSize, delayMillis, preemptiveFlushSize);
        }

        void blockNext(String segmentId) {
            UUID id = uuid(segmentId);
            gates.put(id, new CountDownLatch(1));
            entered.put(id, new CountDownLatch(1));
        }

        void release(String segmentId) {
            CountDownLatch gate = gates.get(uuid(segmentId));
            if (gate != null) {
                gate.countDown();
            }
        }

        void awaitEntered(String segmentId) throws InterruptedException {
            CountDownLatch enter = entered.get(uuid(segmentId));
            assertTrue("doWrite did not enter for " + segmentId,
                    enter != null && enter.await(5, TimeUnit.SECONDS));
        }

        void enqueue(String segmentId, ByteBuffer buffer) {
            handleLoadedSegment(uuid(segmentId), buffer);
        }

        boolean discard(String segmentId) {
            return onEvictionHook(uuid(segmentId));
        }

        @Override
        protected void doWrite(UUID segmentId, ByteBuffer buffer) throws IOException {
            doWriteCalls.add(segmentId);
            CountDownLatch enter = entered.get(segmentId);
            if (enter != null) {
                enter.countDown();
            }
            CountDownLatch gate = gates.get(segmentId);
            if (gate != null) {
                try {
                    if (!gate.await(5, TimeUnit.SECONDS)) {
                        throw new IOException("gate not released for " + segmentId);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
            if (throwOnWrite) {
                throw new IOException("simulated write failure");
            }
            if (realWrite) {
                super.doWrite(segmentId, buffer);
            }
        }

        @Override
        protected void cleanupOrphan(UUID segmentId) {
            cleanupOrphanCalls.add(segmentId);
            if (realWrite) {
                if (deleteBeforeCleanup) {
                    Path path = getSegmentPath(segmentId.toString());
                    try {
                        Files.delete(path);
                        if (failCleanup) {
                            // A non-empty directory at the segment path forces deleteIfExists to fail.
                            Files.createDirectory(path);
                            Files.createFile(path.resolve("blocker"));
                        }
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
                super.cleanupOrphan(segmentId);
            }
        }
    }
}

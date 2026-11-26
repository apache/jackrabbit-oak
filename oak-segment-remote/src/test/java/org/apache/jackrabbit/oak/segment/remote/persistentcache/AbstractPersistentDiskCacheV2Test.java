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
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.segment.spi.SegmentReadInterruptedException;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCache;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Shared behavioral tests for the V2 persistent disk cache. Each concrete subclass supplies the variant under
 * test via {@link #createCache(Path, int)} and the variant's expected {@link AbstractCacheStats#getName()}
 * via {@link #expectedCacheName()}. Variant-specific tests (e.g. the async writer/eviction race protocol)
 * belong in their own test files.
 */
public abstract class AbstractPersistentDiskCacheV2Test {
    private static final Logger logger = LoggerFactory.getLogger(AbstractPersistentDiskCacheV2Test.class);

    protected static final int CACHE_SIZE_MB = 1;
    private static final Random random = new Random();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    protected AbstractPersistentDiskCacheV2 cache;
    protected File cacheDir;

    /**
     * Build a fresh cache rooted at {@code dir} with the given size in MB. Used both by {@link #setUp()} and
     * by tests that construct additional caches (rehydration, accounting).
     */
    protected final AbstractPersistentDiskCacheV2 createCache(Path dir, int cacheMaxSizeMB) throws IOException {
        return createCache(dir, cacheMaxSizeMB, noopMonitor());
    }

    protected abstract AbstractPersistentDiskCacheV2 createCache(Path dir, int cacheMaxSizeMB,
                                                                DiskCacheIOMonitor monitor) throws IOException;

    /**
     * The name the variant under test reports via {@code getCacheStats().getName()}.
     */
    protected abstract String expectedCacheName();

    @Before
    public void setUp() throws IOException {
        cacheDir = tempFolder.newFolder("cache");
        cache = createCache(cacheDir.toPath(), CACHE_SIZE_MB);
    }

    @After
    public void tearDown() {
        if (cache != null) {
            cache.close();
            cache = null;
        }
    }

    @Test
    public void testReadSegmentWithLoader() {
        long[] segmentId = randomSegmentId();
        ByteBuffer expectedBuffer = createRandomBuffer(1024);

        ByteBuffer result = cache.readSegment(segmentId[0], segmentId[1], expectedBuffer::duplicate);

        assertNotNull(result);
        assertBufferEquals(expectedBuffer, result);
    }

    @Test
    public void testReadSegmentCacheHit() {
        long[] segmentId = randomSegmentId();
        ByteBuffer originalBuffer = createRandomBuffer(1024);

        // First read - cache miss
        ByteBuffer firstRead = cache.readSegment(segmentId[0], segmentId[1], () -> originalBuffer);
        assertNotNull(firstRead);

        // Second read - cache hit
        ByteBuffer secondRead = cache.readSegment(segmentId[0], segmentId[1], () -> {
            fail("Loader should not be called on cache hit");
            return null;
        });

        assertNotNull(secondRead);
        assertBufferEquals(originalBuffer, secondRead);
    }

    @Test
    public void testContainsSegmentAfterWrite() {
        long[] segmentId = randomSegmentId();
        ByteBuffer buffer = createRandomBuffer(1024);

        assertFalse(cache.containsSegment(segmentId[0], segmentId[1]));

        cache.readSegment(segmentId[0], segmentId[1], () -> buffer);

        assertTrue(cache.containsSegment(segmentId[0], segmentId[1]));
    }

    @Test
    public void testCacheEviction() {
        int segmentSize = 256 * 1024; // 256KB per segment
        long[][] segmentIds = new long[10][2];

        // Fill cache beyond capacity
        for (int i = 0; i < 10; i++) {
            segmentIds[i] = randomSegmentId();
            ByteBuffer buffer = createRandomBuffer(segmentSize);
            cache.readSegment(segmentIds[i][0], segmentIds[i][1], () -> buffer);
        }

        // Force cache maintenance so any pending eviction listeners run before we inspect membership.
        cache.cleanUp();

        // Some segments should have been evicted
        int cachedCount = 0;
        for (long[] segmentId : segmentIds) {
            if (cache.containsSegment(segmentId[0], segmentId[1])) {
                cachedCount++;
            }
        }

        assertTrue("Some segments should have been evicted", cachedCount < 10);
    }

    @Test
    public void testConcurrentAccess() throws Exception {
        int numThreads = 10;
        int segmentsPerThread = 20;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numThreads);
        ConcurrentMap<String, ByteBuffer> expectedBuffers = new ConcurrentHashMap<>();
        AtomicInteger errorCount = new AtomicInteger(0);

        // Create tasks for concurrent cache access
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startLatch.await();

                    for (int j = 0; j < segmentsPerThread; j++) {
                        long[] segmentId = randomSegmentId();
                        String segmentKey = new UUID(segmentId[0], segmentId[1]).toString();
                        ByteBuffer buffer = createRandomBuffer(1024);

                        // Store expected buffer
                        expectedBuffers.putIfAbsent(segmentKey, buffer.duplicate());

                        // Write to cache
                        ByteBuffer result = cache.readSegment(segmentId[0], segmentId[1], buffer::duplicate);
                        assertNotNull(result);

                        // Verify immediate read
                        ByteBuffer verifyRead = cache.readSegment(segmentId[0], segmentId[1], () -> {
                            fail("Loader should not be called for existing segment");
                            return null;
                        });
                        assertNotNull(verifyRead);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    logger.error("Thread {} encountered error", threadId, e);
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for all threads to complete
        assertTrue("Threads did not complete in time",
                completionLatch.await(30, TimeUnit.SECONDS));

        executor.shutdown();
        assertTrue("Executor did not terminate",
                executor.awaitTermination(10, TimeUnit.SECONDS));

        // Verify no errors occurred
        assertEquals("Concurrent access should not produce errors", 0, errorCount.get());

        // Wait for async writes to complete (no-op for the sync variant)
        Thread.sleep(500);

        // Verify all segments are accessible
        for (Map.Entry<String, ByteBuffer> entry : expectedBuffers.entrySet()) {
            UUID uuid = UUID.fromString(entry.getKey());
            long msb = uuid.getMostSignificantBits();
            long lsb = uuid.getLeastSignificantBits();

            if (cache.containsSegment(msb, lsb)) {
                ByteBuffer cached = cache.readSegment(msb, lsb, () -> null);
                assertNotNull("Cached segment should be readable", cached);
            }
        }
    }

    /**
     * Rehydration: UUID-named files present in the cache directory at construction time are loaded into the
     * in-memory cache and their bytes counted toward cacheSize.
     */
    @Test
    public void testRehydrateValidSegmentFiles() throws IOException {
        Path dir = tempFolder.newFolder("rehydrate").toPath();
        int segmentSize = 1024;
        UUID[] ids = {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        for (UUID id : ids) {
            writeSegmentFile(dir, id, createRandomBytes(segmentSize));
        }

        try (AbstractPersistentDiskCacheV2 rehydrated = createCache(dir, CACHE_SIZE_MB)) {
            for (UUID id : ids) {
                assertTrue("segment " + id + " should be rehydrated",
                        rehydrated.containsSegment(id.getMostSignificantBits(), id.getLeastSignificantBits()));
            }
            assertEquals((long) ids.length * segmentSize, rehydrated.cacheSize());
        }
    }

    /**
     * Files whose names are not valid segment IDs are deleted during rehydration, wherever they sit in the
     * tree: stray files at the root, stray files nested inside a bucket directory, and leftover {@code .tmp}
     * files from a crashed write. Valid UUID-named files (stored in their bucket) are rehydrated. Zero-byte
     * UUID-named files (likely failed writes from a crashed run) have valid names so they are preserved on
     * disk; they are simply not rehydrated into the in-memory cache.
     */
    @Test
    public void testRehydrateDeletesInvalidNamedFilesRecursively() throws IOException {
        Path dir = tempFolder.newFolder("rehydrate-mixed").toPath();

        UUID validId = UUID.randomUUID();
        Path validPath = writeSegmentFile(dir, validId, createRandomBytes(1024));
        Path bucket = validPath.getParent();

        // Stray non-UUID files, at the root and nested inside a bucket directory: both deleted.
        Path strayRoot = dir.resolve("oldSegment.txt");
        Files.write(strayRoot, "stray data".getBytes());
        Path strayNested = bucket.resolve("nested.txt");
        Files.write(strayNested, "nested data".getBytes());

        // A leftover temp file from a crashed write (UUID prefix + .tmp suffix is not a valid UUID): deleted.
        Path leftoverTemp = bucket.resolve(UUID.randomUUID() + ".tmp");
        Files.write(leftoverTemp, "partial".getBytes());

        // Zero-byte UUID-named file: valid name, preserved on disk but not rehydrated.
        UUID zeroByteId = UUID.randomUUID();
        Path zeroBytePath = segmentPath(dir, zeroByteId);
        Files.createDirectories(zeroBytePath.getParent());
        Files.createFile(zeroBytePath);

        try (AbstractPersistentDiskCacheV2 rehydrated = createCache(dir, CACHE_SIZE_MB)) {
            assertTrue("valid UUID file should be rehydrated",
                    rehydrated.containsSegment(validId.getMostSignificantBits(), validId.getLeastSignificantBits()));
            assertFalse("zero-byte UUID file should not be rehydrated",
                    rehydrated.containsSegment(zeroByteId.getMostSignificantBits(), zeroByteId.getLeastSignificantBits()));
            assertEquals(1024L, rehydrated.cacheSize());

            assertTrue("valid segment file should be preserved", Files.exists(validPath));
            assertFalse("stray non-UUID file at root should be deleted", Files.exists(strayRoot));
            assertFalse("stray non-UUID file nested in a bucket should be deleted", Files.exists(strayNested));
            assertFalse("leftover .tmp file should be deleted", Files.exists(leftoverTemp));
            assertTrue("zero-byte UUID file should be preserved (valid name)", Files.exists(zeroBytePath));
        }
    }

    /**
     * When the on-disk total exceeds the cache limit, the cache's weight-based eviction discards the excess during
     * the put loop in rehydration. The exact set of survivors is implementation-specific (the cache's eviction is
     * approximate), so we only assert convergence to within the limit and that no orphan files remain.
     */
    @Test
    public void testRehydrateConvergesToCapacityWhenOverfull() throws IOException {
        Path dir = tempFolder.newFolder("rehydrate-overflow").toPath();
        int segmentSize = 256 * 1024;
        int numFiles = 10;
        UUID[] ids = new UUID[numFiles];
        for (int i = 0; i < numFiles; i++) {
            ids[i] = UUID.randomUUID();
            writeSegmentFile(dir, ids[i], createRandomBytes(segmentSize));
        }

        // 1 MB cache fits 4 × 256 KB segments; the remaining 6 must be evicted during rehydration.
        long maxBytes = 1024L * 1024L;
        try (AbstractPersistentDiskCacheV2 rehydrated = createCache(dir, 1)) {
            int contained = 0;
            int filesOnDisk = 0;
            for (UUID id : ids) {
                if (rehydrated.containsSegment(id.getMostSignificantBits(), id.getLeastSignificantBits())) {
                    contained++;
                }
                if (Files.exists(segmentPath(dir, id))) {
                    filesOnDisk++;
                }
            }

            assertTrue("Some segments must have been evicted (contained=" + contained + ")",
                    contained < numFiles);
            assertEquals("cacheSize should equal the sum of sizes of the contained segments",
                    (long) contained * segmentSize, rehydrated.cacheSize());
            assertTrue("cacheSize should fit within the limit", rehydrated.cacheSize() <= maxBytes);
            assertEquals("files-on-disk count should match contained-segment count", contained, filesOnDisk);

            // No orphans: every UUID-named file on disk must correspond to a contained segment.
            for (UUID id : ids) {
                if (Files.exists(segmentPath(dir, id))) {
                    assertTrue("file on disk must be tracked by the cache: " + id,
                            rehydrated.containsSegment(id.getMostSignificantBits(), id.getLeastSignificantBits()));
                }
            }
        }
    }

    @Test
    public void testClose() {
        long[] segmentId = randomSegmentId();
        ByteBuffer buffer = createRandomBuffer(1024);

        // Add a segment to the cache
        cache.readSegment(segmentId[0], segmentId[1], () -> buffer);
        assertTrue(cache.containsSegment(segmentId[0], segmentId[1]));

        // Close the cache
        cache.close();

        try {
            cache.readSegment(segmentId[0], segmentId[1], () -> buffer);
            fail("Expected IllegalStateException after cache is closed");
        } catch (IllegalStateException e) {
            // Expected exception
        }
        try {
            cache.containsSegment(segmentId[0], segmentId[1]);
            fail("Expected IllegalStateException after cache is closed");
        } catch (IllegalStateException e) {
            // Expected exception
        }
        assertNotNull(cache.getCacheStats());
    }

    @Test
    public void testGetCacheStats() {
        assertNotNull(cache.getCacheStats());
        assertEquals(expectedCacheName(), cache.getCacheStats().getName());
    }

    @Test
    public void testCacheDirectoryCreation() throws IOException {
        File newDir = new File(tempFolder.getRoot(), "newCache");
        assertFalse(newDir.exists());

        try (AbstractPersistentDiskCacheV2 ignored = createCache(newDir.toPath(), CACHE_SIZE_MB)) {
            assertTrue(newDir.exists());
            assertTrue(newDir.isDirectory());
        }
    }

    @Test
    public void testMultipleReadsOfSameSegment() {
        long[] segmentId = randomSegmentId();
        ByteBuffer originalBuffer = createRandomBuffer(1024);

        for (int i = 0; i < 100; i++) {
            ByteBuffer result = cache.readSegment(segmentId[0], segmentId[1], originalBuffer::duplicate);
            assertNotNull(result);
            assertBufferEquals(originalBuffer, result);
        }
    }

    @Test
    public void testDifferentSegments() {
        long[][] segmentIds = new long[3][2];
        ByteBuffer[] buffers = new ByteBuffer[3];

        for (int i = 0; i < 3; i++) {
            segmentIds[i] = randomSegmentId();
            buffers[i] = createRandomBuffer(1024);
            final int index = i;
            cache.readSegment(segmentIds[i][0], segmentIds[i][1], () -> buffers[index]);
        }

        for (int i = 0; i < 3; i++) {
            assertTrue(cache.containsSegment(segmentIds[i][0], segmentIds[i][1]));
            ByteBuffer result = cache.readSegment(segmentIds[i][0], segmentIds[i][1], () -> null);
            assertBufferEquals(buffers[i], result);
        }
    }

    @Test
    public void testSegmentFilesCreatedOnDisk() {
        long[] segmentId = randomSegmentId();
        ByteBuffer buffer = createRandomBuffer(1024);
        cache.readSegment(segmentId[0], segmentId[1], () -> buffer);
        assertTrue(cache.containsSegment(segmentId[0], segmentId[1]));
        // Close to flush any async writers; the sync variant writes inside the loader so close() is unnecessary
        // there but harmless.
        cache.close();

        Path expectedPath = segmentPath(cacheDir.toPath(), segmentId);
        assertTrue("Segment file should exist on disk", Files.exists(expectedPath));
    }

    /**
     * All 256 bucket directories ({@code 00}–{@code ff}) are pre-created at startup, so the write path never
     * has to create one. {@code UUID.toString()} is lowercase hex, so the first two characters — and thus the
     * bucket name — are always one of these 256 values.
     */
    @Test
    public void testAllBucketDirectoriesPreCreatedAtStartup() {
        Path dir = cacheDir.toPath();
        for (int i = 0; i < 256; i++) {
            String bucket = String.format("%02x", i);
            assertTrue("bucket " + bucket + " should be pre-created at startup",
                    Files.isDirectory(dir.resolve(bucket)));
        }
    }

    /**
     * Segments must be stored one directory level deep, in a bucket named by the first two hex characters of
     * the UUID (e.g. {@code <dir>/55/558e8400-...}), never flat at the cache root. A single flat directory
     * with hundreds of thousands of entries makes filesystem metadata operations — and thus segment writes —
     * progressively slower (GRANITE-61960).
     */
    @Test
    public void testSegmentFilesStoredHierarchically() {
        long[] segmentId = randomSegmentId();
        ByteBuffer buffer = createRandomBuffer(1024);
        cache.readSegment(segmentId[0], segmentId[1], () -> buffer);
        assertTrue(cache.containsSegment(segmentId[0], segmentId[1]));
        cache.close();

        UUID id = new UUID(segmentId[0], segmentId[1]);
        Path bucketPath = segmentPath(cacheDir.toPath(), id);
        assertTrue("Segment file should be stored in a hierarchical bucket dir: " + bucketPath,
                Files.exists(bucketPath));
        assertEquals("Bucket directory must be named by the first two hex chars of the UUID",
                id.toString().substring(0, 2), bucketPath.getParent().getFileName().toString());
        assertFalse("Segment file must not be stored flat in the cache root",
                Files.exists(cacheDir.toPath().resolve(id.toString())));
    }

    /**
     * Self-healing: if the on-disk file is missing when {@code readSegment} reports a cache hit (external
     * tampering, manual cleanup, or a subtle race we haven't caught), the cache must invalidate the phantom
     * entry and reload via the supplied loader instead of throwing.
     */
    @Test
    public void testReadSegmentRecoversFromMissingFile() throws Exception {
        long[] id = randomSegmentId();
        ByteBuffer original = createRandomBuffer(1024);

        // First read: cache miss + write to disk.
        cache.readSegment(id[0], id[1], () -> original);

        // Wait for the file to appear (sync is immediate; async is a few ms).
        Path filePath = segmentPath(cacheDir.toPath(), id);
        awaitFile(filePath, TimeUnit.SECONDS.toMillis(5));

        // Simulate external tampering: delete the file while the cache still has the entry.
        Files.delete(filePath);
        assertTrue("entry should still be tracked by the cache after external file delete",
                cache.containsSegment(id[0], id[1]));

        // Re-read: the cache must invalidate the phantom entry and reload via the loader.
        AtomicInteger loaderCalls = new AtomicInteger();
        ByteBuffer recovered = cache.readSegment(id[0], id[1], () -> {
            loaderCalls.incrementAndGet();
            return original.duplicate();
        });
        assertEquals("loader must be re-invoked after invalidate", 1, loaderCalls.get());
        assertNotNull(recovered);
        assertBufferEquals(original, recovered);

        // The file should be re-created (sync writes inline; async writes asynchronously).
        awaitFile(filePath, TimeUnit.SECONDS.toMillis(5));
        assertTrue("file should be re-created on recovery", Files.exists(filePath));
    }

    /**
     * If the calling thread is interrupted during the cache's NIO disk read (e.g. a
     * scheduled load canceled via {@code FutureTask.cancel(true)}), the read surfaces as a
     * {@code ClosedByInterruptException}. The cache must:
     * <ol>
     *     <li>honor the interrupt by propagating a {@link SegmentReadInterruptedException} instead of
     *         swallowing it as a cache miss and silently falling back to the loader; and</li>
     *     <li>clear the interrupt flag that NIO leaves set, so it does not close the next channel and
     *         poison every subsequent read on the same thread. A later read of a segment that exists on disk must still succeed.</li>
     * </ol>
     */
    @Test
    public void testInterruptDuringReadIsHonouredAndDoesNotPoisonSubsequentReads() throws Exception {
        Path dir = tempFolder.newFolder("interrupt-read").toPath();
        UUID seg1 = UUID.randomUUID();
        UUID seg2 = UUID.randomUUID();
        writeSegmentFile(dir, seg1, createRandomBytes(1024));
        writeSegmentFile(dir, seg2, createRandomBytes(1024));

        try (AbstractPersistentDiskCacheV2 interruptCache = createCache(dir, CACHE_SIZE_MB)) {
            Thread.currentThread().interrupt();
            SegmentReadInterruptedException failure = assertThrows(SegmentReadInterruptedException.class,
                    () -> interruptCache.readSegment(seg1.getMostSignificantBits(), seg1.getLeastSignificantBits(),
                            () -> { throw new AssertionError("interrupted hit must not invoke the loader"); }));
            assertTrue(failure.getCause() instanceof ClosedByInterruptException);
            assertFalse(Thread.currentThread().isInterrupted());
            assertNotNull(interruptCache.readSegment(seg2.getMostSignificantBits(), seg2.getLeastSignificantBits(),
                    () -> { throw new AssertionError("subsequent read must hit the cache"); }));
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * Honors {@link PersistentCache#readSegment}'s contract: a {@code null} return from the loader means
     * "segment doesn't exist", and {@code readSegment} must surface that as a {@code null} return — not
     * as an exception.
     */
    @Test
    public void testReadSegmentReturnsNullWhenLoaderReturnsNull() {
        long[] segmentId = randomSegmentId();
        ByteBuffer result = cache.readSegment(segmentId[0], segmentId[1], () -> null);
        assertNull("readSegment must return null when the loader returns null", result);
        assertFalse("cache must not record an entry for a non-existent segment",
                cache.containsSegment(segmentId[0], segmentId[1]));
    }

    /**
     * A {@code null} loader result must not be cached as a negative entry. A subsequent {@code readSegment}
     * for the same id, with a loader that now returns a real buffer, must call the loader and return its
     * value.
     */
    @Test
    public void testReadSegmentDoesNotCacheNullLoaderResult() {
        long[] segmentId = randomSegmentId();
        assertNull(cache.readSegment(segmentId[0], segmentId[1], () -> null));

        ByteBuffer real = createRandomBuffer(1024);
        AtomicInteger loaderCalls = new AtomicInteger();
        ByteBuffer result = cache.readSegment(segmentId[0], segmentId[1], () -> {
            loaderCalls.incrementAndGet();
            // Return a duplicate so the sync variant's in-place buffer.flip() in writeSegmentInternal
            // doesn't mutate the buffer the test compares against below.
            return real.duplicate();
        });

        assertEquals("loader must be re-invoked after a null result was not cached", 1, loaderCalls.get());
        assertNotNull(result);
        assertBufferEquals(real, result);
    }

    /**
     * {@link RepositoryNotReachableException} is the contract-specified way for a loader to signal "remote
     * store is down" (distinct from "segment doesn't exist"). It must reach the caller as itself, not
     * double-wrapped in {@code RuntimeException}, so callers can distinguish unavailability from missing
     * segments.
     */
    @Test
    public void testReadSegmentRethrowsRepositoryNotReachable() {
        long[] segmentId = randomSegmentId();
        RepositoryNotReachableException expected = new RepositoryNotReachableException(new IOException("azure down"));
        try {
            cache.readSegment(segmentId[0], segmentId[1], () -> { throw expected; });
            fail("expected RepositoryNotReachableException");
        } catch (RepositoryNotReachableException e) {
            assertSame("the original RepositoryNotReachableException must be rethrown as-is", expected, e);
        }
    }

    /**
     * Any unchecked exception thrown by the loader must reach the caller as itself, not opaque-wrapped
     * by the cache layer. The Oak cache's {@code get} propagates runtime exceptions from the mapping
     * function directly. This keeps the call site debuggable and frees us from special-casing each known
     * runtime exception type at the cache layer.
     */
    @Test
    public void testReadSegmentRethrowsRuntimeExceptionFromLoaderUnwrapped() {
        long[] segmentId = randomSegmentId();
        IllegalStateException expected = new IllegalStateException("boom");
        try {
            cache.readSegment(segmentId[0], segmentId[1], () -> { throw expected; });
            fail("expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertSame("the original RuntimeException must be rethrown as-is", expected, e);
        }
    }

    /**
     * Cache-miss counterpart to {@link #testInterruptDuringReadIsHonouredAndDoesNotPoisonSubsequentReads()}:
     * on a miss the loader runs inside the cache's {@code get} mapping function, which propagates a thrown
     * {@link SegmentReadInterruptedException} as-is. The cache must rethrow it unchanged, so an interrupt
     * surfacing through the loader (e.g. a linked cache aborting its NIO read) is honored as a cancellation
     * rather than swallowed and reported as a cache miss.
     */
    @Test
    public void testReadSegmentRethrowsInterruptedFromLoaderUnwrapped() {
        long[] segmentId = randomSegmentId();
        SegmentReadInterruptedException expected = new SegmentReadInterruptedException(
                "Interrupted while reading segment from cache", new ClosedByInterruptException());
        try {
            cache.readSegment(segmentId[0], segmentId[1], () -> { throw expected; });
            fail("expected SegmentReadInterruptedException");
        } catch (SegmentReadInterruptedException e) {
            assertSame("the original SegmentReadInterruptedException must be rethrown as-is, "
                    + "not swallowed as a cache miss", expected, e);
        }
    }

    /**
     * A checked {@link InterruptedException} surfacing from the loader (e.g. the async writer's enqueue
     * interrupted by an aborted transaction) must be honored as a cancelled read: {@code readSegment} must
     * convert it to a {@link SegmentReadInterruptedException} — not a generic {@code RuntimeException} that
     * the segment store would misclassify as a missing segment and report via a benign
     * {@code SegmentNotFoundException} — while preserving the original {@code InterruptedException} as the
     * cause.
     */
    @Test
    public void testReadSegmentConvertsInterruptedExceptionFromLoader() {
        long[] segmentId = randomSegmentId();
        InterruptedException interrupted = new InterruptedException("enqueue interrupted");
        SegmentReadInterruptedException failure = assertThrows(SegmentReadInterruptedException.class,
                () -> cache.readSegment(segmentId[0], segmentId[1], () -> { throw interrupted; }));
        assertSame(interrupted, failure.getCause());
        assertFalse("an interrupted read must not be cached as an entry",
                cache.containsSegment(segmentId[0], segmentId[1]));
    }

    /**
     * Checked exceptions from the loader must reach the caller as the direct cause of the outer
     * {@code RuntimeException}, not doubly nested. The {@code PersistentCache.readSegment} signature
     * doesn't declare checked exceptions, so the V2 cache wraps once in {@code RuntimeException} — but no
     * more than once.
     */
    @Test
    public void testReadSegmentChecksedExceptionFromLoaderIsDirectCause() {
        long[] segmentId = randomSegmentId();
        IOException expected = new IOException("backend boom");
        try {
            cache.readSegment(segmentId[0], segmentId[1], () -> { throw expected; });
            fail("expected RuntimeException");
        } catch (RuntimeException e) {
            assertSame("checked exception must be the direct cause, not nested in ExecutionException",
                    expected, e.getCause());
        }
    }

    /**
     * Writes must atomically replace any pre-existing file at the final segment path. The cache uses a
     * write-to-temp + atomic rename so that a JVM crash mid-write never leaves a partial file at the
     * final path; this test detects the swap by comparing the fileKey (inode on POSIX) before and after.
     * If a future regression switches back to in-place truncate-and-write, the fileKey stays the same
     * and the assertion fires.
     */
    @Test
    public void testWriteAtomicallyReplacesExistingFile() throws Exception {
        long[] segmentId = randomSegmentId();
        Path filePath = segmentPath(cacheDir.toPath(), segmentId);

        // Plant a file at the segment path after construction so the in-memory cache doesn't know
        // about it — readSegment will then go through the loader/write path on the next call.
        Files.createDirectories(filePath.getParent());
        Files.write(filePath, "stale-data".getBytes());
        Object originalKey = Files.readAttributes(filePath, BasicFileAttributes.class).fileKey();
        assumeNotNull("fileKey not supported on this filesystem", originalKey);

        ByteBuffer buffer = createRandomBuffer(1024);
        cache.readSegment(segmentId[0], segmentId[1], () -> buffer);
        // For the async variant, flush pending writes.
        cache.close();

        Object newKey = Files.readAttributes(filePath, BasicFileAttributes.class).fileKey();
        assertNotEquals("write must atomically replace the file (different inode/fileKey)",
                originalKey, newKey);
        assertEquals("final file should have the new buffer size", 1024L, Files.size(filePath));
    }

    /**
     * The cacheSize counter is incremented by buffer.limit() at insertion (in the loader) and decremented
     * by removalEvent.getValue() in onRemoval. This test exercises both paths and verifies that, after
     * eviction overflow, the counter matches the sum of segment sizes that are still tracked by the cache.
     */
    @Test
    public void testCacheSizeAccountingAcrossInsertAndEvict() throws IOException {
        cache.close();
        // Tight cache: 1 MB max, 256 KB segments → roughly 4 fit before eviction kicks in.
        cache = createCache(cacheDir.toPath(), 1);

        final int segmentSize = 256 * 1024;
        final int numSegments = 10;
        long[][] ids = new long[numSegments][];
        for (int i = 0; i < numSegments; i++) {
            ids[i] = randomSegmentId();
            ByteBuffer buf = createRandomBuffer(segmentSize);
            cache.readSegment(ids[i][0], ids[i][1], () -> buf);
        }

        // cleanUp() applies pending evictions to the cache's internal state and, with maintenance forced
        // synchronous (see CacheBuilder#asyncMaintenance), the eviction listener - and so the
        // cacheSize decrement - has already run by the time it returns.
        cache.cleanUp();

        long trackedBytes = 0;
        for (long[] id : ids) {
            if (cache.containsSegment(id[0], id[1])) {
                trackedBytes += segmentSize;
            }
        }

        assertEquals("cacheSize should match the sum of sizes of segments still tracked by the cache",
                trackedBytes, cache.cacheSize());

        // Sanity: at least one segment was evicted (otherwise this test isn't exercising the decrement path).
        assertTrue("Test setup should have forced at least one eviction; trackedBytes=" + trackedBytes,
                trackedBytes < (long) numSegments * segmentSize);
    }

    /**
     * {@code writeSegment} populates the cache so a subsequent {@code readSegment} for the same id is
     * served from the cache without re-invoking the loader.
     */
    @Test
    public void testWriteSegmentPopulatesCacheForSubsequentRead() throws Exception {
        long[] id = randomSegmentId();
        ByteBuffer original = createRandomBuffer(1024);

        cache.writeSegment(id[0], id[1], original.duplicate());

        // Wait for async writes; sync is immediate.
        Path filePath = segmentPath(cacheDir.toPath(), id);
        awaitFile(filePath, TimeUnit.SECONDS.toMillis(5));

        assertTrue("containsSegment must report the entry after writeSegment",
                cache.containsSegment(id[0], id[1]));

        AtomicInteger loaderCalls = new AtomicInteger();
        ByteBuffer read = cache.readSegment(id[0], id[1], () -> {
            loaderCalls.incrementAndGet();
            return null;
        });
        assertEquals("loader must not run when writeSegment already cached the segment",
                0, loaderCalls.get());
        assertNotNull(read);
        assertBufferEquals(original, read);
    }

    /**
     * Repeated {@code writeSegment} calls for the same id are no-ops: {@code cacheSize} does not double-count
     * and the on-disk file (in both variants) ends up at the size of a single segment.
     */
    @Test
    public void testWriteSegmentIsIdempotent() throws Exception {
        long[] id = randomSegmentId();
        ByteBuffer original = createRandomBuffer(1024);

        cache.writeSegment(id[0], id[1], original.duplicate());
        Path filePath = segmentPath(cacheDir.toPath(), id);
        awaitFile(filePath, TimeUnit.SECONDS.toMillis(5));
        long sizeAfterFirst = cache.cacheSize();

        for (int i = 0; i < 5; i++) {
            cache.writeSegment(id[0], id[1], original.duplicate());
        }
        // Settle any (would-be) async work.
        Thread.sleep(200);

        assertEquals("cacheSize must not double-count repeated writeSegment for the same id",
                sizeAfterFirst, cache.cacheSize());
        assertEquals("on-disk file must be a single segment's worth of bytes",
                1024L, Files.size(filePath));
    }

    /**
     * {@code writeSegment} must not mutate the caller's buffer position. The caller may continue to use the
     * buffer (e.g. to also pass it to a downstream archive writer) after handing it to the cache.
     */
    @Test
    public void testWriteSegmentDoesNotMutateCallerBufferPosition() {
        long[] id = randomSegmentId();
        ByteBuffer buffer = createRandomBuffer(1024);
        int positionBefore = buffer.position();
        int limitBefore = buffer.limit();

        cache.writeSegment(id[0], id[1], buffer);

        assertEquals("caller's buffer position must be unchanged", positionBefore, buffer.position());
        assertEquals("caller's buffer limit must be unchanged", limitBefore, buffer.limit());
    }

    @Test
    public void testWriteSegmentCachesOnlyRemainingBytes() throws Exception {
        long[] id = randomSegmentId();
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}, 1, 3);
        ByteBuffer expected = ByteBuffer.wrap(new byte[] {2, 3, 4});

        cache.writeSegment(id[0], id[1], buffer);

        assertEquals("caller's buffer position must be unchanged", 1, buffer.position());
        assertEquals("caller's buffer limit must be unchanged", 4, buffer.limit());
        Path file = segmentPath(cacheDir.toPath(), id);
        awaitFile(file, TimeUnit.SECONDS.toMillis(5));
        assertBufferEquals(expected, ByteBuffer.wrap(Files.readAllBytes(file)));
        assertEquals(expected.remaining(), cache.cacheSize());
        assertBufferEquals(expected, cache.readSegment(id[0], id[1], () -> {
            throw new AssertionError("a cached segment must not invoke the loader");
        }));
    }

    /**
     * {@code writeSegment} after {@code close()} must throw {@code IllegalStateException}, mirroring the
     * behaviour of the other public-API methods.
     */
    @Test
    public void testWriteSegmentAfterCloseThrows() {
        cache.close();
        long[] id = randomSegmentId();
        ByteBuffer buffer = createRandomBuffer(1024);
        try {
            cache.writeSegment(id[0], id[1], buffer);
            fail("expected IllegalStateException after close");
        } catch (IllegalStateException expected) {
            // ok
        }
    }

    @Test
    public void testWriteSegmentUpdatesWriteMetrics() throws Exception {
        WriteMetrics metrics = new WriteMetrics();
        UUID id = UUID.randomUUID();
        ByteBuffer buffer = createRandomBuffer(1024);
        Path dir = tempFolder.newFolder("write-metrics").toPath();
        try (AbstractPersistentDiskCacheV2 monitored = createCache(dir, CACHE_SIZE_MB, metrics.monitor)) {
            monitored.writeSegment(id.getMostSignificantBits(), id.getLeastSignificantBits(), buffer);
        }
        assertEquals(1024L, Files.size(segmentPath(dir, id)));
        metrics.assertCompletedWrite(segmentPath(dir, id), id, 1024);
    }

    @Test
    public void testReadMissUpdatesWriteMetrics() throws Exception {
        WriteMetrics metrics = new WriteMetrics();
        UUID id = UUID.randomUUID();
        Path dir = tempFolder.newFolder("read-miss-metrics").toPath();
        try (AbstractPersistentDiskCacheV2 monitored = createCache(dir, CACHE_SIZE_MB, metrics.monitor)) {
            assertNotNull(monitored.readSegment(id.getMostSignificantBits(), id.getLeastSignificantBits(),
                    () -> createRandomBuffer(2048)));
        }
        assertEquals(2048L, Files.size(segmentPath(dir, id)));
        metrics.assertCompletedWrite(segmentPath(dir, id), id, 2048);
    }

    @Test
    public void testFailedOpenDoesNotUpdateWriteMetrics() throws Exception {
        WriteMetrics metrics = new WriteMetrics();
        UUID id = UUID.randomUUID();
        Path dir = tempFolder.newFolder("failed-open-metrics").toPath();
        Path path = segmentPath(dir, id);
        Path tempPath = path.resolveSibling(path.getFileName() + ".tmp");
        try (AbstractPersistentDiskCacheV2 monitored = createCache(dir, CACHE_SIZE_MB, metrics.monitor)) {
            Files.createDirectory(tempPath);
            monitored.writeSegment(id.getMostSignificantBits(), id.getLeastSignificantBits(),
                    createRandomBuffer(1024));
        }
        assertFalse(Files.exists(path));
        assertFalse(Files.exists(tempPath));
        metrics.assertNoCompletedWrite();
        verify(metrics.monitor, never()).beforeSegmentWrite(any(File.class), anyLong(), anyLong(), anyInt());
    }

    @Test
    public void testFailedRenameDoesNotUpdateWriteMetrics() throws Exception {
        WriteMetrics metrics = new WriteMetrics();
        UUID id = UUID.randomUUID();
        Path dir = tempFolder.newFolder("failed-rename-metrics").toPath();
        Path path = segmentPath(dir, id);
        try (AbstractPersistentDiskCacheV2 monitored = createCache(dir, CACHE_SIZE_MB, metrics.monitor)) {
            Files.createDirectory(path);
            Files.createFile(path.resolve("blocker"));
            monitored.writeSegment(id.getMostSignificantBits(), id.getLeastSignificantBits(),
                    createRandomBuffer(1024));
        }
        assertTrue(Files.isDirectory(path));
        assertFalse(Files.exists(path.resolveSibling(path.getFileName() + ".tmp")));
        verify(metrics.monitor).beforeSegmentWrite(path.toFile(), id.getMostSignificantBits(),
                id.getLeastSignificantBits(), 1024);
        metrics.assertNoCompletedWrite();
    }

    @Test
    public void testDuplicateWriteDoesNotUpdateWriteMetricsAgain() throws Exception {
        WriteMetrics metrics = new WriteMetrics();
        UUID id = UUID.randomUUID();
        ByteBuffer buffer = createRandomBuffer(1024);
        Path dir = tempFolder.newFolder("duplicate-write-metrics").toPath();
        try (AbstractPersistentDiskCacheV2 monitored = createCache(dir, CACHE_SIZE_MB, metrics.monitor)) {
            for (int i = 0; i < 5; i++) {
                monitored.writeSegment(id.getMostSignificantBits(), id.getLeastSignificantBits(), buffer);
            }
        }
        metrics.assertCompletedWrite(segmentPath(dir, id), id, 1024);
    }

    // ----- helpers -----

    private static final class WriteMetrics {
        private final MeterStats bytes = mock(MeterStats.class);
        private final TimerStats time = mock(TimerStats.class);
        private final DiskCacheIOMonitor monitor;

        private WriteMetrics() {
            StatisticsProvider provider = mock(StatisticsProvider.class, delegatesTo(StatisticsProvider.NOOP));
            when(provider.getMeter(DiskCacheIOMonitor.OAK_SEGMENT_CACHE_DISK_SEGMENT_WRITE_BYTES,
                    StatsOptions.METRICS_ONLY)).thenReturn(bytes);
            when(provider.getTimer(DiskCacheIOMonitor.OAK_SEGMENT_CACHE_DISk_SEGMENT_WRITE_TIME,
                    StatsOptions.METRICS_ONLY)).thenReturn(time);
            monitor = spy(new DiskCacheIOMonitor(provider));
        }

        private void assertCompletedWrite(Path path, UUID id, int length) {
            ArgumentCaptor<Long> elapsed = ArgumentCaptor.forClass(Long.class);
            InOrder hooks = inOrder(monitor);
            hooks.verify(monitor).beforeSegmentWrite(path.toFile(), id.getMostSignificantBits(),
                    id.getLeastSignificantBits(), length);
            hooks.verify(monitor).afterSegmentWrite(eq(path.toFile()), eq(id.getMostSignificantBits()),
                    eq(id.getLeastSignificantBits()), eq(length), elapsed.capture());
            assertTrue("write duration should be positive", elapsed.getValue() > 0);
            verify(bytes).mark(length);
            verify(time).update(elapsed.getValue(), TimeUnit.NANOSECONDS);
            verifyNoMoreInteractions(bytes, time);
        }

        private void assertNoCompletedWrite() {
            verify(monitor, never()).afterSegmentWrite(any(File.class), anyLong(), anyLong(), anyInt(), anyLong());
            verifyNoInteractions(bytes, time);
        }
    }

    protected static long[] randomSegmentId() {
        return new long[]{random.nextLong(), random.nextLong()};
    }

    /**
     * Mirrors {@link AbstractPersistentDiskCacheV2#getSegmentPath} so tests can locate a segment's on-disk
     * file. Segments are stored one directory level deep, in a bucket named by the first two hex characters
     * of the UUID (e.g. {@code <dir>/55/558e8400-...}).
     */
    protected static Path segmentPath(Path dir, UUID id) {
        String name = id.toString();
        return dir.resolve(name.substring(0, 2)).resolve(name);
    }

    protected static Path segmentPath(Path dir, long[] id) {
        return segmentPath(dir, new UUID(id[0], id[1]));
    }

    /**
     * Seed a segment file at its hierarchical on-disk path (creating the bucket directory), as the cache
     * itself would have written it. Used by rehydration and self-healing tests.
     */
    protected static Path writeSegmentFile(Path dir, UUID id, byte[] bytes) throws IOException {
        Path path = segmentPath(dir, id);
        Files.createDirectories(path.getParent());
        Files.write(path, bytes);
        return path;
    }

    protected static ByteBuffer createRandomBuffer(int size) {
        return ByteBuffer.wrap(createRandomBytes(size));
    }

    protected static byte[] createRandomBytes(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    protected static void assertBufferEquals(ByteBuffer expected, ByteBuffer actual) {
        expected.rewind();
        actual.rewind();
        assertEquals("ByteBuffer sizes differ", expected.remaining(), actual.remaining());
        while (expected.hasRemaining()) {
            assertEquals("ByteBuffer content differs", expected.get(), actual.get());
        }
        expected.rewind();
        actual.rewind();
    }

    protected static DiskCacheIOMonitor noopMonitor() {
        return new DiskCacheIOMonitor(StatisticsProvider.NOOP);
    }

    private static void awaitFile(Path path, long timeoutMs) throws InterruptedException {
        await(() -> Files.exists(path), timeoutMs, "Timed out waiting for file: " + path);
    }

    /**
     * Polls {@code condition} until it's true or {@code timeoutMs} elapses. Used to wait for the asynchronous
     * variant's background writer to flush a pending segment to disk; the sync variant writes inline, so the
     * condition is already true on the first check.
     */
    protected static void await(BooleanSupplier condition, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(5);
        }
        fail(message);
    }
}

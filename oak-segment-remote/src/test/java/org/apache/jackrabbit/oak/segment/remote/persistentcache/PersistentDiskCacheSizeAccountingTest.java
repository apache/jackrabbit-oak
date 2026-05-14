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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.AbstractPersistentCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Regression tests for OAK-12212.
 *
 * <p>Before the OAK-12212 fix, the in-memory {@code cacheSize} counter
 * maintained by {@link PersistentDiskCache} could drift well above the actual
 * size of the cache directory: the {@code writesPending} guard in
 * {@link PersistentDiskCache#writeSegment(long, long, org.apache.jackrabbit.oak.commons.Buffer)}
 * only prevents <em>simultaneously running</em> write tasks for the same
 * segment id, not <em>sequentially running</em> ones. Every {@code writeSegment}
 * invocation that reached the body still added {@code fileSize} to
 * {@code cacheSize}, yet on POSIX file systems {@code Files.move} with
 * {@code ATOMIC_MOVE} maps to {@code rename(2)} and silently replaces an
 * existing destination — so repeated writes of the same segment id produced
 * a single file on disk but multiple increments of the in-memory counter.
 * The cleanup path could only subtract the actual length of the (one) file
 * it deleted, so the over-counted bytes were never repaid.
 *
 * <p>The fix is gated by {@link PersistentDiskCache#FT_OAK_12212_DISABLE}.
 * With the toggle in its default state (fix enabled), {@code writeSegment}
 * short-circuits when the segment is already on disk; flipping the toggle
 * restores the original behaviour for emergency rollback.
 *
 * <p>To make the tests deterministic, the cache's internal worker executor
 * is replaced with a single-threaded one and explicitly drained after every
 * {@code writeSegment} call using a marker task.
 */
public class PersistentDiskCacheSizeAccountingTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private static final int SEGMENT_LEN = 256 * 1024;

    private File cacheFolder;
    private DiskCacheIOMonitor ioMonitor;
    private PersistentDiskCache persistentCache;

    /**
     * Captures the running counter value reported via
     * {@link DiskCacheIOMonitor#updateCacheSize(long, long)} every time the
     * cache mutates its in-memory {@code cacheSize}. The latest value mirrors
     * the current state of the (otherwise package-private) {@code AtomicLong}.
     */
    private final AtomicLong lastReportedCacheSize = new AtomicLong(0);

    @Before
    public void setUp() throws Exception {
        cacheFolder = temporaryFolder.newFolder();
        ioMonitor = mock(DiskCacheIOMonitor.class);
        doAnswer(inv -> {
            lastReportedCacheSize.set(inv.getArgument(0, Long.class));
            return null;
        }).when(ioMonitor).updateCacheSize(anyLong(), anyLong());
        // The OAK-12212 kill switch is a process-wide AtomicBoolean. Reset
        // it before each test so previous tests cannot leak their toggle
        // state into the next one.
        PersistentDiskCache.FT_OAK_12212_DISABLE.set(false);
    }

    @After
    public void tearDown() {
        if (persistentCache != null) {
            persistentCache.close();
            persistentCache = null;
        }
        PersistentDiskCache.FT_OAK_12212_DISABLE.set(false);
    }

    /**
     * Writes the same segment id repeatedly with a maximum cache size big
     * enough that the cleanup path never runs.
     *
     * <p>With the OAK-12212 fix enabled (default), only the first write
     * actually touches disk and the counter stays at one segment size.
     * Without the fix the cache directory still holds exactly one
     * segment-sized file (POSIX rename silently replaces), but the
     * in-memory counter is incremented {@code writes} times.
     */
    @Test
    public void cacheSizeCounterMustMatchDirectorySizeAfterRepeatedWritesOfSameSegment()
            throws Exception {
        // Cache big enough that the cleanup path never runs during the test.
        persistentCache = new PersistentDiskCache(cacheFolder, /* maxCacheSizeMB */ 1024, ioMonitor);
        replaceExecutorWithSingleThreaded(persistentCache);

        final byte[] segmentBytes = randomBytes(SEGMENT_LEN);
        final UUID segmentId = UUID.randomUUID();
        final long msb = segmentId.getMostSignificantBits();
        final long lsb = segmentId.getLeastSignificantBits();
        final int writes = 4; // mirrors the ~4x drift seen in the heap dump

        for (int i = 0; i < writes; i++) {
            persistentCache.writeSegment(msb, lsb, org.apache.jackrabbit.oak.commons.Buffer.wrap(segmentBytes));
            drainExecutor(persistentCache);
        }

        // Sanity check: updateCacheSize must have been invoked at least once
        // per write, so the captured value is meaningful.
        verify(ioMonitor, atLeastOnce()).updateCacheSize(anyLong(), anyLong());

        File segmentFile = new File(cacheFolder, segmentId.toString());
        assertTrue("Segment file must exist on disk after the writes", segmentFile.isFile());

        long actualDirectorySize = FileUtils.sizeOfDirectory(cacheFolder);
        assertEquals(
                "Directory must hold exactly one segment-sized file. If this fails"
                        + " the test setup is broken (e.g. the platform's Files.move"
                        + " did not replace the existing destination); rerun on a POSIX"
                        + " file system.",
                SEGMENT_LEN, actualDirectorySize);

        long reportedCacheSize = lastReportedCacheSize.get();

        // Core invariant established by the OAK-12212 fix: the in-memory
        // counter must reflect the actual size on disk.
        assertEquals(
                "In-memory cacheSize counter has drifted above the actual cache"
                        + " directory size. counter=" + reportedCacheSize
                        + ", directorySize=" + actualDirectorySize
                        + ", writes=" + writes
                        + ", segmentSize=" + SEGMENT_LEN + ".",
                (long) actualDirectorySize, reportedCacheSize);
    }

    /**
     * Same workload but with a tight {@code maxCacheSizeBytes} so the
     * cleanup path would run between writes. Without the OAK-12212 fix, the
     * cleanup decrements the counter by the actual length on disk but the
     * extra increments contributed by previous redundant writes are never
     * repaid; the end state has a counter that no longer matches the
     * directory size. With the fix enabled, redundant writes never happen
     * in the first place, so the counter and the directory stay in sync.
     */
    @Test
    public void cacheSizeCounterMustMatchDirectorySizeAcrossWriteAndCleanupCycles()
            throws Exception {
        // Small max so cleanUp() actually triggers between writes.
        final int maxCacheSizeMB = 1;
        persistentCache = new PersistentDiskCache(cacheFolder, maxCacheSizeMB, ioMonitor);
        replaceExecutorWithSingleThreaded(persistentCache);

        final byte[] segmentBytes = randomBytes(SEGMENT_LEN);
        final UUID segmentId = UUID.randomUUID();
        final long msb = segmentId.getMostSignificantBits();
        final long lsb = segmentId.getLeastSignificantBits();
        // Enough writes for the cleanup path to fire several times.
        final int writes = 8;

        for (int i = 0; i < writes; i++) {
            persistentCache.writeSegment(msb, lsb, org.apache.jackrabbit.oak.commons.Buffer.wrap(segmentBytes));
            drainExecutor(persistentCache);
        }

        long actualDirectorySize = FileUtils.sizeOfDirectory(cacheFolder);
        long reportedCacheSize = lastReportedCacheSize.get();

        assertEquals(
                "In-memory cacheSize counter has drifted out of sync with the"
                        + " cache directory size after repeated write+cleanup cycles."
                        + " counter=" + reportedCacheSize
                        + ", directorySize=" + actualDirectorySize
                        + ", writes=" + writes
                        + ", segmentSize=" + SEGMENT_LEN + ".",
                actualDirectorySize, reportedCacheSize);
    }

    /**
     * Activates the kill switch ({@code FT_OAK_12212_DISABLE = true}) and
     * verifies that the legacy buggy behaviour is restored: after writing
     * the same segment id N times the in-memory counter is
     * {@code N * segmentSize} while the directory holds a single
     * segment-sized file.
     *
     * <p>This pins down what the toggle actually controls and ensures the
     * rollback path is wired correctly, so we can flip the toggle in
     * production with confidence if the fix ever needs to be disabled.
     */
    @Test
    public void killSwitchRestoresLegacyDoubleCountingBehaviour() throws Exception {
        PersistentDiskCache.FT_OAK_12212_DISABLE.set(true);

        persistentCache = new PersistentDiskCache(cacheFolder, /* maxCacheSizeMB */ 1024, ioMonitor);
        replaceExecutorWithSingleThreaded(persistentCache);

        final byte[] segmentBytes = randomBytes(SEGMENT_LEN);
        final UUID segmentId = UUID.randomUUID();
        final long msb = segmentId.getMostSignificantBits();
        final long lsb = segmentId.getLeastSignificantBits();
        final int writes = 4;

        for (int i = 0; i < writes; i++) {
            persistentCache.writeSegment(msb, lsb, org.apache.jackrabbit.oak.commons.Buffer.wrap(segmentBytes));
            drainExecutor(persistentCache);
        }

        long actualDirectorySize = FileUtils.sizeOfDirectory(cacheFolder);
        long reportedCacheSize = lastReportedCacheSize.get();

        assertEquals(
                "With the kill switch active the directory still holds exactly"
                        + " one segment file (POSIX rename replaces silently).",
                SEGMENT_LEN, actualDirectorySize);
        assertEquals(
                "Kill switch must restore the legacy behaviour: cacheSize is"
                        + " incremented by segmentSize on every write call.",
                (long) writes * SEGMENT_LEN, reportedCacheSize);
        assertNotEquals(
                "Sanity: with the kill switch active the in-memory counter"
                        + " must diverge from the directory size.",
                (long) actualDirectorySize, reportedCacheSize);
    }

    /**
     * Time-bombed removal reminder. If this test fails, the feature toggle
     * {@code FT_OAK-12212} and its guard in
     * {@link PersistentDiskCache#writeSegment} should be removed — the fix
     * has been in production long enough.
     */
    @Test
    public void ft_oak_12212_toggleShouldBeRemoved() {
        assertTrue("Feature toggle " + PersistentDiskCache.FT_OAK_12212 + " is overdue for removal",
                LocalDate.now().isBefore(LocalDate.of(2027, 5, 14)));
    }

    // --- Helpers ----------------------------------------------------------

    private static byte[] randomBytes(int length) {
        byte[] ret = new byte[length];
        new java.util.Random(42).nextBytes(ret);
        return ret;
    }

    /**
     * Replaces the cache's internal worker executor (created in
     * {@link AbstractPersistentCache}'s constructor) with a
     * single-threaded one. Together with {@link #drainExecutor} this makes
     * write task ordering and completion deterministic in tests, removing
     * the {@code writesPending} race that otherwise makes the magnitude of
     * the {@code cacheSize} drift timing-dependent.
     */
    private static void replaceExecutorWithSingleThreaded(AbstractPersistentCache cache)
            throws Exception {
        Field executorField = AbstractPersistentCache.class.getDeclaredField("executor");
        executorField.setAccessible(true);
        ExecutorService old = (ExecutorService) executorField.get(cache);
        old.shutdownNow();
        executorField.set(cache, Executors.newSingleThreadExecutor());
    }

    /**
     * Submits a no-op marker task to the cache's worker executor and waits
     * for it to complete. With a single-threaded executor this guarantees
     * that all previously submitted write tasks (and their trailing
     * {@code cleanUp()} calls) have finished.
     */
    private static void drainExecutor(AbstractPersistentCache cache) throws Exception {
        Field executorField = AbstractPersistentCache.class.getDeclaredField("executor");
        executorField.setAccessible(true);
        ExecutorService executor = (ExecutorService) executorField.get(cache);
        Future<?> marker = executor.submit(() -> { });
        marker.get(30, TimeUnit.SECONDS);
    }
}

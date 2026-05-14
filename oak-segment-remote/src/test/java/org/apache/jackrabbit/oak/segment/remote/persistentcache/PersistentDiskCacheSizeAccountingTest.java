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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Regression tests demonstrating that the in-memory {@code cacheSize} counter
 * maintained by {@link PersistentDiskCache} can drift well above the actual
 * size of the cache directory. See OAK-12212 for details.
 *
 * <p>Inspection of
 * {@link PersistentDiskCache#writeSegment(long, long, org.apache.jackrabbit.oak.commons.Buffer)}
 * shows that the {@code writesPending} guard only prevents <em>simultaneously
 * running</em> write tasks for the same segment id, not <em>sequentially
 * running</em> ones. Every {@code writeSegment} invocation that reaches the
 * body still adds {@code fileSize} to {@code cacheSize}, yet on POSIX file
 * systems {@code Files.move} with {@code ATOMIC_MOVE} maps to {@code rename(2)}
 * and silently replaces an existing destination — so repeated writes of the
 * same segment id produce a single file on disk but multiple increments of
 * the in-memory counter. The cleanup path can only subtract the
 * <em>actual</em> length of the (one) file it deletes, so the over-counted
 * bytes are never repaid.
 *
 * <p>To make the test deterministic, the cache's internal worker executor is
 * replaced with a single-threaded one and explicitly drained after every
 * {@code writeSegment} call using a marker task. With the default 10-thread
 * executor the bug still manifests, but the magnitude of the drift depends on
 * timing (when several tasks race on {@code writesPending} only one wins).
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
    }

    @After
    public void tearDown() {
        if (persistentCache != null) {
            persistentCache.close();
            persistentCache = null;
        }
    }

    /**
     * Writes the same segment id repeatedly with a maximum cache size big
     * enough that the cleanup path never runs. On a POSIX file system every
     * write but the first silently replaces the previously written file, so
     * the cache directory always holds exactly one segment-sized file. The
     * in-memory counter, however, is incremented once per call.
     *
     * <p>Expected (correct) behaviour: counter equals on-disk size.<br>
     * Actual (current, buggy) behaviour: counter equals
     * {@code writes * segmentSize}, i.e. drifts upward by
     * {@code (writes - 1) * segmentSize}.
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

        // Core invariant being violated by the current implementation:
        // the in-memory counter must reflect the actual size on disk.
        assertEquals(
                "In-memory cacheSize counter has drifted above the actual cache"
                        + " directory size. counter=" + reportedCacheSize
                        + ", directorySize=" + actualDirectorySize
                        + ", writes=" + writes
                        + ", segmentSize=" + SEGMENT_LEN
                        + ". Each writeSegment call unconditionally increments"
                        + " cacheSize by fileSize even though Files.move silently"
                        + " replaces the previously written file on POSIX systems,"
                        + " so the counter equals writes * segmentSize rather than"
                        + " a single segmentSize.",
                (long) actualDirectorySize, reportedCacheSize);
    }

    /**
     * Same workload but with a tight {@code maxCacheSizeBytes} so the cleanup
     * path does run between writes. After cleanup deletes the file, the
     * counter is decremented by the actual length on disk, but the
     * <em>extra</em> increments contributed by previous redundant writes are
     * never repaid. The end state therefore has a counter that no longer
     * matches the directory size — this is the long-running version of the
     * heap dump observation, where {@code cacheSize} grows monotonically
     * above what the disk holds.
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

        // The in-memory counter must reflect the actual directory size, no
        // matter how many redundant writes/cleanups have happened.
        assertEquals(
                "In-memory cacheSize counter has drifted out of sync with the"
                        + " cache directory size after repeated write+cleanup cycles."
                        + " counter=" + reportedCacheSize
                        + ", directorySize=" + actualDirectorySize
                        + ", writes=" + writes
                        + ", segmentSize=" + SEGMENT_LEN
                        + ". The over-counted bytes from every redundant write are"
                        + " never repaid because cleanUp only subtracts the actual"
                        + " size of the file it deletes.",
                actualDirectorySize, reportedCacheSize);
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

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

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PersistentDiskCacheTest extends AbstractPersistentCacheTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Before
    public void setUp() throws Exception {
        persistentCache = new PersistentDiskCache(temporaryFolder.newFolder(), 10 * 1024, new DiskCacheIOMonitor(StatisticsProvider.NOOP));
    }

    @After
    public void tearDown() {
        persistentCache.close();
        persistentCache = null;
    }

    @Test
    public void cleanupTest() throws Exception {
        persistentCache.close();
        persistentCache = new PersistentDiskCache(temporaryFolder.newFolder(), 0, new DiskCacheIOMonitor(StatisticsProvider.NOOP), 500);
        final List<TestSegment> testSegments = new ArrayList<>(SEGMENTS);
        final List<Map<String, Buffer>> segmentsRead = new ArrayList<>(THREADS);

        for (int i = 0; i < SEGMENTS; ++i) {
            testSegments.add(TestSegment.createSegment());
        }

        for (int i = 0; i < THREADS; ++i) {
            final Map<String, Buffer> segmentsReadThisThread = new HashMap<>(SEGMENTS_PER_THREAD);
            segmentsRead.add(segmentsReadThisThread);
        }

        runConcurrently((nThread, nSegment) -> {
            TestSegment segment = testSegments.get(nSegment);
            long[] id = segment.getSegmentId();
            try {
                persistentCache.writeSegment(id[0], id[1], segment.getSegmentBuffer());
            } catch (Throwable t) {
                errors.incrementAndGet();
            } finally {
                done.incrementAndGet();
            }
        });

        waitWhile(() -> done.get() < SEGMENTS);
        waitWhile(() -> persistentCache.getWritesPending() > 0);

        assertEquals("Errors have occurred while writing", 0, errors.get());
        assertNoTimeout();

        done.set(0);
        waitWhile(() -> ((PersistentDiskCache) persistentCache).cleanupInProgress.get());

        persistentCache.cleanUp();

        runConcurrently((nThread, nSegment) -> {
            final TestSegment segment = testSegments.get(nSegment);
            final long[] id = segment.getSegmentId();
            try {
                final Map<String, Buffer> segmentsReadThisThread = segmentsRead.get(nThread);
                final Buffer segmentRead = persistentCache.readSegment(id[0], id[1], () -> null);
                segmentsReadThisThread.put(new UUID(id[0], id[1]).toString(), segmentRead);
            } catch (Throwable t) {
                errors.incrementAndGet();
            } finally {
                done.incrementAndGet();
            }
        });

        waitWhile(() -> done.get() < SEGMENTS);

        assertNoTimeout();
        assertEquals("Errors have occurred while reading", 0, errors.get());
        errors.set(0);

        for (int i = 0; i < THREADS; ++i) {
            for (int j = i * SEGMENTS_PER_THREAD; j < (i + 1) * SEGMENTS_PER_THREAD; ++j) {
                TestSegment testSegment = testSegments.get(j);
                Map<String, Buffer> segmentsReadThisThread = segmentsRead.get(i);
                long[] segmentReadId = testSegment.getSegmentId();
                Buffer segmentRead = segmentsReadThisThread.get(new UUID(segmentReadId[0], segmentReadId[1]).toString());
                if (segmentRead == null) {
                    errors.incrementAndGet();
                }
            }
        }
        assertEquals("Segment(s) not cleaned up in cache", 0, SEGMENTS - errors.get());
    }

    /**
     * Reproduces the Fix-A bug: writeSegment() always called cacheSize.addAndGet(fileSize) even
     * when the segment file already existed on disk.  Every Caffeine L2-eviction followed by a
     * re-request caused a re-write of the same file, adding to the counter without adding new
     * bytes on disk.  Over time this drove cacheSize to ~80 GB while the actual disk held only
     * 19.6 GB, making isCacheFull() permanently true and collapsing the disk-cache hit rate.
     */
    @Test
    public void testCacheSizeNotInflatedOnReWrite() throws Exception {
        persistentCache.close();
        File cacheDir = temporaryFolder.newFolder();
        persistentCache = new PersistentDiskCache(cacheDir, 10 * 1024, new DiskCacheIOMonitor(StatisticsProvider.NOOP));

        TestSegment segment = TestSegment.createSegment();
        long[] id = segment.getSegmentId();

        // Write the same segment 5 times to simulate repeated L2 eviction + re-read
        for (int i = 0; i < 5; i++) {
            persistentCache.writeSegment(id[0], id[1], segment.getSegmentBuffer());
        }
        waitWhile(() -> persistentCache.getWritesPending() > 0);
        Thread.sleep(100);

        // cacheSize counter must equal actual disk usage — not 5× the segment size
        long cacheSizeCounter = ((PersistentDiskCache) persistentCache).getCacheSizeForTesting();
        File segmentFile = new File(cacheDir, new UUID(id[0], id[1]).toString());
        assertEquals("cacheSize inflated by repeated writes of the same segment",
                segmentFile.length(), cacheSizeCounter);
    }

    /**
     * Reproduces the Fix-C bug: cacheSize was initialized to 0 on startup regardless of segments
     * already present on disk from a previous session.  The counter therefore under-reported disk
     * usage, isCacheFull() stayed false longer than it should, and cleanup did not run to evict
     * old files — allowing disk usage to silently grow past the configured maximum.
     */
    @Test
    public void testCacheSizeInitializedFromExistingFiles() throws Exception {
        persistentCache.close();
        File cacheDir = temporaryFolder.newFolder();

        // Pre-populate the directory to simulate a restarted instance with leftover segments
        byte[] data = new byte[4096];
        new Random().nextBytes(data);
        Files.write(new File(cacheDir, UUID.randomUUID().toString()).toPath(), data);
        long expectedSize = data.length;

        persistentCache = new PersistentDiskCache(cacheDir, 10 * 1024, new DiskCacheIOMonitor(StatisticsProvider.NOOP));

        assertEquals("cacheSize should reflect existing files so isCacheFull() is accurate after restart",
                expectedSize, ((PersistentDiskCache) persistentCache).getCacheSizeForTesting());
    }

    /**
     * Reproduces the Fix-B bug: cleanUpInternal() decremented cacheSize <em>before</em> deleting
     * the file.  In the window between the decrement and the actual delete a concurrent
     * writeSegment task could replace the file and increment cacheSize back, then the cleanup
     * delete removed the newly-written file.  The net effect was one phantom increment per race
     * occurrence — under high concurrent write load this drove cacheSize far above the real
     * on-disk bytes.
     *
     * <p>The test runs {@value AbstractPersistentCacheTest#THREADS} writer threads against a
     * 1 MB cache, forcing cleanup to fire continuously and maximise the probability of the race.
     * After all work drains, the in-memory counter must equal the actual directory size.</p>
     */
    @Test
    public void testCacheSizeConsistentUnderConcurrentWriteAndCleanup() throws Exception {
        persistentCache.close();
        File cacheDir = temporaryFolder.newFolder();
        // 1 MB max with 0 ms temp-file grace so cleanup fires after every few writes
        persistentCache = new PersistentDiskCache(cacheDir, 1, new DiskCacheIOMonitor(StatisticsProvider.NOOP), 0);

        runConcurrently((nThread, nSegment) -> {
            TestSegment segment = TestSegment.createSegment();
            long[] id = segment.getSegmentId();
            try {
                persistentCache.writeSegment(id[0], id[1], segment.getSegmentBuffer());
            } catch (Throwable t) {
                errors.incrementAndGet();
            } finally {
                done.incrementAndGet();
            }
        });

        waitWhile(() -> done.get() < SEGMENTS);
        waitWhile(() -> persistentCache.getWritesPending() > 0);
        waitWhile(() -> ((PersistentDiskCache) persistentCache).cleanupInProgress.get());

        assertEquals("Errors during concurrent writes", 0, errors.get());
        assertNoTimeout();

        // One final explicit cleanup pass to drain any in-flight work
        persistentCache.cleanUp();
        waitWhile(() -> ((PersistentDiskCache) persistentCache).cleanupInProgress.get());

        long counter = ((PersistentDiskCache) persistentCache).getCacheSizeForTesting();
        long onDisk = FileUtils.sizeOfDirectory(cacheDir);
        assertEquals(
                "cacheSize counter must equal actual on-disk bytes after concurrent write+cleanup",
                onDisk, counter);
    }

    @Test
    public void testIOMonitor() throws IOException {
        DiskCacheIOMonitor ioMonitorAdapter = Mockito.mock(DiskCacheIOMonitor.class);

        persistentCache.close();
        File cacheFolder = temporaryFolder.newFolder();
        persistentCache = new PersistentDiskCache(cacheFolder, 0, ioMonitorAdapter);

        UUID segmentUUID = UUID.randomUUID();

        persistentCache.readSegment(segmentUUID.getMostSignificantBits(), segmentUUID.getLeastSignificantBits(), () -> null);

        //Segment not in cache, monitor methods not invoked
        verify(ioMonitorAdapter, never()).beforeSegmentRead(any(), anyLong(), anyLong(), anyInt());
        verify(ioMonitorAdapter, never()).afterSegmentRead(any(), anyLong(), anyLong(), anyInt(), anyLong());

        //place segment in disk cache
        File segmentFile = new File(cacheFolder, segmentUUID.toString());
        segmentFile.createNewFile();

        persistentCache.readSegment(segmentUUID.getMostSignificantBits(), segmentUUID.getLeastSignificantBits(), () -> null);

        verify(ioMonitorAdapter, times(1)).beforeSegmentRead(eq(segmentFile), eq(segmentUUID.getMostSignificantBits()), eq(segmentUUID.getLeastSignificantBits()), anyInt());
        verify(ioMonitorAdapter, times(1)).afterSegmentRead(eq(segmentFile), eq(segmentUUID.getMostSignificantBits()), eq(segmentUUID.getLeastSignificantBits()), anyInt(), anyLong());
    }
}
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

import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PersistentDiskCacheCounterTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private PersistentDiskCache cache;

    @After
    public void tearDown() {
        if (cache != null) {
            cache.close();
            cache = null;
        }
    }

    @Test
    public void testStartupInitializesCountersFromExistingFiles() throws Exception {
        File cacheFolder = temporaryFolder.newFolder();

        for (int size : new int[]{100, 200, 300}) {
            File f = new File(cacheFolder, UUID.randomUUID().toString());
            try (FileOutputStream fos = new FileOutputStream(f)) {
                fos.write(new byte[size]);
            }
        }

        cache = new PersistentDiskCache(cacheFolder, 10 * 1024, new DiskCacheIOMonitor(StatisticsProvider.NOOP));

        assertEquals(3, cache.getCacheStats().getElementCount());
        assertEquals(600, cache.getCacheStats().estimateCurrentWeight());
    }

    @Test
    public void testStartupExcludesTempFiles() throws Exception {
        File cacheFolder = temporaryFolder.newFolder();

        File segment = new File(cacheFolder, UUID.randomUUID().toString());
        try (FileOutputStream fos = new FileOutputStream(segment)) {
            fos.write(new byte[100]);
        }

        // One temp file (.part) that must NOT be counted
        File temp = new File(cacheFolder, UUID.randomUUID().toString() + System.nanoTime() + PersistentDiskCache.TEMP_FILE_SUFFIX);
        try (FileOutputStream fos = new FileOutputStream(temp)) {
            fos.write(new byte[999]);
        }

        cache = new PersistentDiskCache(cacheFolder, 10 * 1024, new DiskCacheIOMonitor(StatisticsProvider.NOOP));

        assertEquals(1, cache.getCacheStats().getElementCount());
        assertEquals(100, cache.getCacheStats().estimateCurrentWeight());
    }

    @Test
    public void testStartupEmptyDirectoryLeavesCountersAtZero() throws Exception {
        cache = new PersistentDiskCache(temporaryFolder.newFolder(), 10 * 1024,
                new DiskCacheIOMonitor(StatisticsProvider.NOOP));

        assertEquals(0, cache.getCacheStats().getElementCount());
        assertEquals(0, cache.getCacheStats().estimateCurrentWeight());
    }

    @Test
    public void testWriteIncrementsCounters() throws Exception {
        File cacheFolder = temporaryFolder.newFolder();
        cache = new PersistentDiskCache(cacheFolder, 10 * 1024, new DiskCacheIOMonitor(StatisticsProvider.NOOP));

        assertEquals("elementCount should start at 0", 0, cache.getCacheStats().getElementCount());
        assertEquals("currentWeight should start at 0", 0, cache.getCacheStats().estimateCurrentWeight());

        AbstractPersistentCacheTest.TestSegment segment = AbstractPersistentCacheTest.TestSegment.createSegment();
        long[] id = segment.getSegmentId();
        cache.writeSegment(id[0], id[1], segment.getSegmentBuffer());
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> cache.getWritesPending() == 0);

        File segmentFile = new File(cacheFolder, new UUID(id[0], id[1]).toString());
        assertTrue("Segment file must exist on disk — write failed", segmentFile.exists());

        assertEquals("elementCount should be 1 after write", 1, cache.getCacheStats().getElementCount());
        assertEquals("currentWeight should equal segment size after write",
                AbstractPersistentCacheTest.TestSegment.SEGMENT_LEN,
                cache.getCacheStats().estimateCurrentWeight());
    }

    @Test
    public void testEvictionDecrementsCounters() throws Exception {
        // maxCacheSizeMB=0 means every segment triggers cleanup; tempFilesWaitMs=0 disables temp-file protection
        cache = new PersistentDiskCache(temporaryFolder.newFolder(), 0,
                new DiskCacheIOMonitor(StatisticsProvider.NOOP), 0);

        AbstractPersistentCacheTest.TestSegment segment = AbstractPersistentCacheTest.TestSegment.createSegment();
        long[] id = segment.getSegmentId();
        cache.writeSegment(id[0], id[1], segment.getSegmentBuffer());
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> cache.getWritesPending() == 0);

        // Ensure any in-progress cleanup finishes, then force a cleanup run
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !cache.cleanupInProgress.get());
        cache.cleanUp();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !cache.cleanupInProgress.get());

        assertEquals("elementCount should be 0 after eviction", 0, cache.getCacheStats().getElementCount());
        assertEquals("currentWeight should be 0 after eviction", 0, cache.getCacheStats().estimateCurrentWeight());
    }

    @Test
    public void testPartialEvictionKeepsCountersAccurate() throws Exception {
        File cacheFolder = temporaryFolder.newFolder();
        // 1 MB limit; segments are 256 KB each — 6 segments (1.5 MB) trigger partial eviction to the 66% watermark
        cache = new PersistentDiskCache(cacheFolder, 1, new DiskCacheIOMonitor(StatisticsProvider.NOOP), 0);

        int segmentCount = 6;
        for (int i = 0; i < segmentCount; i++) {
            AbstractPersistentCacheTest.TestSegment segment = AbstractPersistentCacheTest.TestSegment.createSegment();
            long[] id = segment.getSegmentId();
            cache.writeSegment(id[0], id[1], segment.getSegmentBuffer());
        }
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> cache.getWritesPending() == 0);

        // Drain any write-triggered cleanup, then force a final sweep
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !cache.cleanupInProgress.get());
        cache.cleanUp();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> !cache.cleanupInProgress.get());

        // Count regular, non-temp files actually on disk
        long actualCount = 0;
        long actualSize = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(cacheFolder.toPath())) {
            for (Path path : stream) {
                BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
                if (attrs.isRegularFile() && !path.getFileName().toString().endsWith(PersistentDiskCache.TEMP_FILE_SUFFIX)) {
                    actualCount++;
                    actualSize += attrs.size();
                }
            }
        }

        // Sanity-check that we actually exercised the partial-eviction path
        assertTrue("Expected some segments to survive partial eviction", actualCount > 0);
        assertTrue("Expected some segments to be evicted", actualCount < segmentCount);

        // Counters must mirror the actual disk state
        assertEquals("elementCount must match files on disk after partial eviction",
                actualCount, cache.getCacheStats().getElementCount());
        assertEquals("currentWeight must match total size on disk after partial eviction",
                actualSize, cache.getCacheStats().estimateCurrentWeight());
    }
}

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
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PersistentDiskCacheV2SyncTest extends AbstractPersistentDiskCacheV2Test {

    private static final int NO_WEIGHT_EVICTION_MB = 64;

    @Override
    protected AbstractPersistentDiskCacheV2 createCache(Path dir, int cacheMaxSizeMB,
                                                       DiskCacheIOMonitor monitor) throws IOException {
        return new PersistentDiskCacheV2Sync(dir, cacheMaxSizeMB, monitor);
    }

    @Override
    protected String expectedCacheName() {
        return PersistentDiskCacheV2Sync.NAME;
    }

    /**
     * A disk-write failure inside the loader must not fail the read: the segment was loaded successfully
     * from the backend, so {@code readSegment} must log and return the loaded buffer rather than propagating
     * the {@code IOException}. The write is forced to fail by planting a directory at the temp write path
     * ({@code $segmentId.tmp}), so the {@code FileChannel.open(WRITE, CREATE)} in {@code writeSegmentInternal}
     * cannot create the temp file.
     */
    @Test
    public void testReadReturnsLoadedBufferWhenDiskWriteFails() throws Exception {
        long[] id = randomSegmentId();
        Path path = segmentPath(cacheDir.toPath(), id);
        Files.createDirectory(path.resolveSibling(path.getFileName() + ".tmp"));

        ByteBuffer expected = createRandomBuffer(1024);
        ByteBuffer result = cache.readSegment(id[0], id[1], expected::duplicate);

        assertNotNull("read must return the loaded buffer even though the disk-cache write failed", result);
        assertBufferEquals(expected, result);

        // The write failed before the atomic rename, so no durable cache file should exist for this id.
        assertFalse("no cache file should exist after a failed write",
                Files.exists(path));
    }

    // ----- idle-based eviction (GRANITE-69901) -----
    //
    // These tests build their own cache after setting oak.segment.disk.cache.idle.eviction.seconds, because
    // the base setUp() constructs its cache before a per-test property can be applied. The cache reads the
    // property once per instance at construction, so each test lets a real, short idle window elapse; no
    // test-only hook is added to production. Idle-eviction semantics live in the base class and so are shared
    // by both variants; the synchronous variant is exercised here because its writes are on disk by the time
    // readSegment returns, making the assertions deterministic.

    @Test
    public void idleSegmentIsEvictedAfterIdleWindow() throws Exception {
        Path dir = tempFolder.newFolder("idle-evict").toPath();
        String previous = System.setProperty(AbstractPersistentDiskCacheV2.IDLE_EVICTION_SECONDS_PROP, "1");
        try (AbstractPersistentDiskCacheV2 idleCache = createCache(dir, NO_WEIGHT_EVICTION_MB)) {
            long[] id = randomSegmentId();
            ByteBuffer buffer = createRandomBuffer(1024);
            idleCache.readSegment(id[0], id[1], () -> buffer);

            assertTrue("segment should be cached right after the write", idleCache.containsSegment(id[0], id[1]));
            Path file = segmentPath(dir, id);
            assertTrue("segment file should exist right after the write", Files.exists(file));

            // Exceed the 1s idle window without accessing the segment, then force the cache to sweep expired entries.
            Thread.sleep(2000);
            idleCache.cleanUp();

            // cleanUp() applies the expiry and, with maintenance forced synchronous (see
            // CacheBuilder#asyncMaintenance), the eviction listener (file deletion, cacheSize accounting)
            // has already run by the time it returns.
            assertFalse("segment idle past the window should be evicted", idleCache.containsSegment(id[0], id[1]));
            assertFalse("the evicted segment's file should be deleted", Files.exists(file));
            assertEquals("cacheSize should return to zero after idle eviction", 0L, idleCache.cacheSize());
        } finally {
            restoreIdleEvictionProperty(previous);
        }
    }

    @Test
    public void idleEvictionDisabledKeepsSegment() throws Exception {
        Path dir = tempFolder.newFolder("idle-disabled").toPath();
        String previous = System.setProperty(AbstractPersistentDiskCacheV2.IDLE_EVICTION_SECONDS_PROP, "0");
        try (AbstractPersistentDiskCacheV2 idleCache = createCache(dir, NO_WEIGHT_EVICTION_MB)) {
            long[] id = randomSegmentId();
            ByteBuffer buffer = createRandomBuffer(1024);
            idleCache.readSegment(id[0], id[1], () -> buffer);

            Thread.sleep(1200);
            idleCache.cleanUp();

            assertTrue("with idle eviction disabled, an idle segment must remain cached",
                    idleCache.containsSegment(id[0], id[1]));
            assertEquals("cacheSize must be unchanged when idle eviction is disabled",
                    1024L, idleCache.cacheSize());
        } finally {
            restoreIdleEvictionProperty(previous);
        }
    }
    private static void restoreIdleEvictionProperty(String previous) {
        if (previous == null) {
            System.clearProperty(AbstractPersistentDiskCacheV2.IDLE_EVICTION_SECONDS_PROP);
        } else {
            System.setProperty(AbstractPersistentDiskCacheV2.IDLE_EVICTION_SECONDS_PROP, previous);
        }
    }
}

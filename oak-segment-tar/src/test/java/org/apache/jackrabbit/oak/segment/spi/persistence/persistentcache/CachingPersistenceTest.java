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
package org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.CachingSegmentReader;
import org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentGraph;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.*;

public class CachingPersistenceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    @Before
    public void setup() {
        System.setProperty("oak.segment.cache.prefetch.enabled", "true");
        System.setProperty("oak.segment.cache.prefetch.maxRefs", "8");
        System.setProperty("oak.segment.cache.threads", "2");
    }

    @AfterClass
    public static void disablePrefetch() {
        System.clearProperty("oak.segment.cache.prefetch.enabled");
        System.clearProperty("oak.segment.cache.prefetch.maxRefs");
        System.clearProperty("oak.segment.cache.threads");
    }

    @Test(expected = RepositoryNotReachableException.class)
    public void testRepositoryNotReachableWithCachingPersistence() throws IOException, InvalidFileStoreVersionException {
        FileStoreBuilder fileStoreBuilder;
        FileStore fileStore = null;
        try {

            fileStoreBuilder = getFileStoreBuilderWithCachingPersistence(false);

            fileStore = fileStoreBuilder.build();

            SegmentId id = new SegmentId(fileStore, 5, 5);
            byte[] buffer = new byte[2];
            fileStore.writeSegment(id, buffer, 0, 2);

            assertTrue(fileStore.containsSegment(id));
            //close file store so that TarReader is used to read the segment
            fileStore.close();

            fileStoreBuilder = getFileStoreBuilderWithCachingPersistence(false);
            fileStore = fileStoreBuilder.build();

            Segment segment = fileStore.readSegment(id);
            assertNotNull(segment);

            fileStore.close();

            // Construct file store that will simulate throwing RepositoryNotReachableException when reading a segment
            fileStoreBuilder = getFileStoreBuilderWithCachingPersistence(true);
            fileStore = fileStoreBuilder.build();

            try {
                fileStore.readSegment(id);
            } catch (SegmentNotFoundException e) {
                fail();
            }
        } finally {
            if (fileStore != null) {
                fileStore.close();
            }
        }
    }

    @Test
    public void prefetchOnCacheHitLoadsReferences() throws Exception {
        File dir = folder.newFolder();
        FileStore fs = fileStoreBuilder(dir).build();
        SegmentArchiveReader archiveReader = null;
        try {
            SegmentWriter writer = DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder("t").build(fs);
            // Reader for NodeState from RecordId within this FileStore
            SegmentReader sr = new CachingSegmentReader(() -> writer, null, 16, 2, NoopStats.INSTANCE);

            // Two referenced nodes in separate segments
            RecordId a1 = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();
            RecordId a2 = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();

            // Root node referencing both
            NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
            builder.setChildNode("ref1", sr.readNode(a1));
            builder.setChildNode("ref2", sr.readNode(a2));
            RecordId rootRec = writer.writeNode(builder.getNodeState());
            writer.flush();

            UUID r1 = a1.getSegmentId().asUUID();
            UUID r2 = a2.getSegmentId().asUUID();
            UUID root = rootRec.getSegmentId().asUUID();

            // Close the FileStore to ensure tar index is written and readable
            fs.close();

            fs = null;

            // Open the tar archive and fetch exact on-disk buffers
            TarPersistence tar = new TarPersistence(dir);
            SegmentArchiveManager am = tar.createArchiveManager(false, false,
                    new IOMonitorAdapter(),
                    new FileStoreMonitorAdapter(),
                    new RemoteStoreMonitorAdapter());
            List<String> archives = am.listArchives();
            assertFalse("No archives found", archives.isEmpty());
            archiveReader = am.open(archives.get(0));
            assertNotNull(archiveReader);

            Buffer rootBuffer = archiveReader.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits());
            final Buffer buf1 = archiveReader.readSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits());
            final Buffer buf2 = archiveReader.readSegment(r2.getMostSignificantBits(), r2.getLeastSignificantBits());
            assertNotNull(rootBuffer);
            assertNotNull(buf1);
            assertNotNull(buf2);

            // Instrumented cache: pre-seed root and observe writes for r1, r2
            CountDownLatch latch = new CountDownLatch(2);
            PersistentCache cache = new MemoryPersistentCache(false) {
                @Override
                public void writeSegment(long msb, long lsb, Buffer buffer) {
                    super.writeSegment(msb, lsb, buffer);
                    if ((msb == r1.getMostSignificantBits() && lsb == r1.getLeastSignificantBits()) ||
                            (msb == r2.getMostSignificantBits() && lsb == r2.getLeastSignificantBits())) {
                        latch.countDown();
                    }
                }
            };
            // seed root
            cache.writeSegment(root.getMostSignificantBits(), root.getLeastSignificantBits(), rootBuffer);

            CachingSegmentArchiveReader reader = new CachingSegmentArchiveReader(cache, archiveReader);

            // Read root. Since it's a cache hit, prefetch should be scheduled for r1 and r2.
            Buffer got = reader.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits());
            assertNotNull(got);

            boolean completed = latch.await(5, TimeUnit.SECONDS);
            assertTrue("Prefetch did not complete in time", completed);

            assertTrue(cache.containsSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits()));
            assertTrue(cache.containsSegment(r2.getMostSignificantBits(), r2.getLeastSignificantBits()));

            reader.close();
        } finally {
            if (archiveReader != null) archiveReader.close();
            if (fs != null) fs.close();
        }
    }



    @Test
    public void alreadyCachedReferencesAreNotPrefetched() throws Exception {
        File dir = folder.newFolder();
        FileStore fs = fileStoreBuilder(dir).build();
        try {
            SegmentWriter writer = DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder("t").build(fs);
            SegmentReader sr = new CachingSegmentReader(() -> writer, null, 16, 2, NoopStats.INSTANCE);

            RecordId a1 = writer.writeNode(EmptyNodeState.EMPTY_NODE); // will be preseeded in cache
            writer.flush();
            RecordId a2 = writer.writeNode(EmptyNodeState.EMPTY_NODE); // will be prefetched
            writer.flush();

            NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
            builder.setChildNode("ref1", sr.readNode(a1));
            builder.setChildNode("ref2", sr.readNode(a2));
            RecordId rootRec = writer.writeNode(builder.getNodeState());
            writer.flush();

            UUID r1 = a1.getSegmentId().asUUID();
            UUID r2 = a2.getSegmentId().asUUID();
            UUID root = rootRec.getSegmentId().asUUID();

            fs.close();
            fs = null;


            TarPersistence tar = new TarPersistence(dir);
            SegmentArchiveManager am = tar.createArchiveManager(false, false,
                    new IOMonitorAdapter(),
                    new FileStoreMonitorAdapter(),
                    new RemoteStoreMonitorAdapter());
            List<String> archives = am.listArchives();
            assertFalse("No archives found", archives.isEmpty());
            SegmentArchiveReader archiveReader = am.open(archives.get(0));
            assertNotNull(archiveReader);

            Buffer rootBuffer = archiveReader.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits());
            final Buffer buf1 = archiveReader.readSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits());
            final Buffer buf2 = archiveReader.readSegment(r2.getMostSignificantBits(), r2.getLeastSignificantBits());
            assertNotNull(rootBuffer);
            assertNotNull(buf1);
            assertNotNull(buf2);

            // Instrumented cache: count writes for r1 and r2
            AtomicInteger r1Writes = new AtomicInteger();
            AtomicInteger r2Writes = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(1); // expect only r2 to be written
            PersistentCache cache = new MemoryPersistentCache(false) {
                @Override
                public void writeSegment(long msb, long lsb, Buffer buffer) {
                    boolean isR1 = (msb == r1.getMostSignificantBits() && lsb == r1.getLeastSignificantBits());
                    boolean isR2 = (msb == r2.getMostSignificantBits() && lsb == r2.getLeastSignificantBits());
                    super.writeSegment(msb, lsb, buffer);
                    if (isR1) {
                        r1Writes.incrementAndGet();
                    }
                    if (isR2) {
                        if (r2Writes.incrementAndGet() == 1) {
                            latch.countDown();
                        }
                    }
                }
            };

            // Seed root and r1 into cache (r1 already cached => should be skipped by prefetch)
            cache.writeSegment(root.getMostSignificantBits(), root.getLeastSignificantBits(), rootBuffer);
            cache.writeSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits(), buf1);

            CachingSegmentArchiveReader reader = new CachingSegmentArchiveReader(cache, archiveReader);

            Buffer got = reader.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits());
            assertNotNull(got);

            boolean completed = latch.await(5, TimeUnit.SECONDS);
            assertTrue("Prefetch for non-cached ref did not complete in time", completed);

            assertTrue(cache.containsSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits()));
            assertTrue(cache.containsSegment(r2.getMostSignificantBits(), r2.getLeastSignificantBits()));

            assertEquals(1, r1Writes.get());
            assertEquals(1, r2Writes.get());

            reader.close();
        } finally {
            if (fs != null) fs.close();
        }
    }

    @Test
    public void prefetchDisabledDoesNotScheduleOrWrite() throws Exception {

        File dir = folder.newFolder();
        FileStore fs = fileStoreBuilder(dir).build();
        SegmentArchiveReader archiveReader = null;
        try {
            SegmentWriter writer = DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder("t").build(fs);
            SegmentReader sr = new CachingSegmentReader(() -> writer, null, 16, 2, NoopStats.INSTANCE);

            RecordId a1 = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();
            RecordId a2 = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();

            NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
            builder.setChildNode("ref1", sr.readNode(a1));
            builder.setChildNode("ref2", sr.readNode(a2));
            RecordId rootRec = writer.writeNode(builder.getNodeState());
            writer.flush();

            UUID r1 = a1.getSegmentId().asUUID();
            UUID r2 = a2.getSegmentId().asUUID();
            UUID root = rootRec.getSegmentId().asUUID();

            fs.close();
            fs = null;

            // Open the tar archive and fetch exact on-disk buffers
            TarPersistence tar = new TarPersistence(dir);
            SegmentArchiveManager am = tar.createArchiveManager(false, false,
                    new IOMonitorAdapter(),
                    new FileStoreMonitorAdapter(),
                    new RemoteStoreMonitorAdapter());
            List<String> archives = am.listArchives();
            assertFalse("No archives found", archives.isEmpty());
            archiveReader = am.open(archives.get(0));
            assertNotNull(archiveReader);

            Buffer rootBuffer = archiveReader.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits());
            final Buffer buf1 = archiveReader.readSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits());
            final Buffer buf2 = archiveReader.readSegment(r2.getMostSignificantBits(), r2.getLeastSignificantBits());
            assertNotNull(rootBuffer);
            assertNotNull(buf1);
            assertNotNull(buf2);

            AtomicInteger r1Writes = new AtomicInteger();
            AtomicInteger r2Writes = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(1);
            PersistentCache cache = new MemoryPersistentCache(false) {
                @Override
                public void writeSegment(long msb, long lsb, Buffer buffer) {
                    boolean isR1 = (msb == r1.getMostSignificantBits() && lsb == r1.getLeastSignificantBits());
                    boolean isR2 = (msb == r2.getMostSignificantBits() && lsb == r2.getLeastSignificantBits());
                    super.writeSegment(msb, lsb, buffer);
                    if ((isR1 || isR2) && latch.getCount() > 0) {
                        latch.countDown();
                    }
                }
            };

            // Seed root; with prefetch disabled, reading root should not write r1 or r2
            cache.writeSegment(root.getMostSignificantBits(), root.getLeastSignificantBits(), rootBuffer);

            String prev = System.getProperty("oak.segment.cache.prefetch.enabled");
            System.setProperty("oak.segment.cache.prefetch.enabled", "false");
            CachingSegmentArchiveReader reader = new CachingSegmentArchiveReader(cache, archiveReader);
            if (prev != null) {
                System.setProperty("oak.segment.cache.prefetch.enabled", prev);
            } else {
                System.clearProperty("oak.segment.cache.prefetch.enabled");
            }

            Buffer got = reader.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits());
            assertNotNull(got);

            boolean anyWrite = latch.await(2, TimeUnit.SECONDS);
            assertFalse("Prefetch disabled should not schedule writes", anyWrite);
            assertFalse(cache.containsSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits()));
            assertFalse(cache.containsSegment(r2.getMostSignificantBits(), r2.getLeastSignificantBits()));
            assertEquals(0, r1Writes.get());
            assertEquals(0, r2Writes.get());

            reader.close();
        } finally {
            if (archiveReader != null) archiveReader.close();
            if (fs != null) fs.close();
        }
    }

    @Test
    public void concurrentCacheHitsDeduplicatePrefetchTasks() throws Exception {
        File dir = folder.newFolder();
        FileStore fs = fileStoreBuilder(dir).build();
        SegmentArchiveReader archiveReader = null;
        try {
            SegmentWriter writer = DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder("t").build(fs);
            SegmentReader sr = new CachingSegmentReader(() -> writer, null, 16, 2, NoopStats.INSTANCE);

            RecordId a1 = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();

            NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
            builder.setChildNode("ref1", sr.readNode(a1));
            RecordId rootRec = writer.writeNode(builder.getNodeState());
            writer.flush();

            UUID r1 = a1.getSegmentId().asUUID();
            UUID root = rootRec.getSegmentId().asUUID();

            fs.close();
            fs = null;

            // Open the tar archive and fetch exact on-disk buffers
            TarPersistence tar = new TarPersistence(dir);
            SegmentArchiveManager am = tar.createArchiveManager(false, false,
                    new IOMonitorAdapter(),
                    new FileStoreMonitorAdapter(),
                    new RemoteStoreMonitorAdapter());
            List<String> archives = am.listArchives();
            assertFalse("No archives found", archives.isEmpty());
            archiveReader = am.open(archives.get(0));
            assertNotNull(archiveReader);

            Buffer rootBuffer = archiveReader.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits());
            final Buffer buf1 = archiveReader.readSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits());
            assertNotNull(rootBuffer);
            assertNotNull(buf1);

            AtomicInteger r1Writes = new AtomicInteger();
            CountDownLatch firstWrite = new CountDownLatch(1);
            PersistentCache cache = new MemoryPersistentCache(false) {
                @Override
                public void writeSegment(long msb, long lsb, Buffer buffer) {
                    boolean isR1 = (msb == r1.getMostSignificantBits() && lsb == r1.getLeastSignificantBits());
                    super.writeSegment(msb, lsb, buffer);
                    if (isR1) {
                        if (r1Writes.incrementAndGet() == 1) {
                            firstWrite.countDown();
                        }
                    }
                }
            };

            // Seed root in cache so reads are cache hits, triggering prefetch for r1
            cache.writeSegment(root.getMostSignificantBits(), root.getLeastSignificantBits(), rootBuffer);

            CachingSegmentArchiveReader reader = new CachingSegmentArchiveReader(cache, archiveReader);

            // Fire multiple concurrent reads of the same root buffer against the same reader instance
            int threads = 12;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            for (int i = 0; i < threads; i++) {
                new Thread(() -> {
                    try {
                        start.await();
                        Buffer got = reader.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits());
                        assertNotNull(got);
                    } catch (Exception e) {
                        // propagate as assertion failure
                        fail("Concurrent read failed: " + e);
                    } finally {
                        done.countDown();
                    }
                }, "t-" + i).start();
            }
            start.countDown();

            assertTrue("Concurrent reads did not finish in time", done.await(5, TimeUnit.SECONDS));
            assertTrue("Prefetch did not write target in time", firstWrite.await(5, TimeUnit.SECONDS));

            Thread.sleep(200);

            // Assert exactly one write for r1
            assertEquals("Prefetch should be deduplicated across concurrent cache hits", 1, r1Writes.get());
            assertTrue(cache.containsSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits()));

            reader.close();
        } finally {
            if (archiveReader != null) archiveReader.close();
            if (fs != null) fs.close();
        }
    }

    @Test
    public void inFlightDedupIsReleasedAfterDelegateFailure() throws Exception {
        // Build segments: root references a1 only
        File dir = folder.newFolder();
        FileStore fs = fileStoreBuilder(dir).build();
        SegmentArchiveReader archiveReader = null;
        try {
            SegmentWriter writer = DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder("t").build(fs);
            SegmentReader sr = new CachingSegmentReader(() -> writer, null, 16, 2, NoopStats.INSTANCE);

            RecordId a1 = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();

            NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
            builder.setChildNode("ref1", sr.readNode(a1));
            RecordId rootRec = writer.writeNode(builder.getNodeState());
            writer.flush();

            UUID r1 = a1.getSegmentId().asUUID();
            UUID root = rootRec.getSegmentId().asUUID();

            // Close the FileStore to ensure tar index is written and readable
            fs.close();
            fs = null;

            // Open the tar archive and fetch exact on-disk buffers
            TarPersistence tar = new TarPersistence(dir);
            SegmentArchiveManager am = tar.createArchiveManager(false, false,
                    new IOMonitorAdapter(),
                    new FileStoreMonitorAdapter(),
                    new RemoteStoreMonitorAdapter());
            List<String> archives = am.listArchives();
            assertFalse("No archives found", archives.isEmpty());
            archiveReader = am.open(archives.get(0));
            assertNotNull(archiveReader);

            Buffer rootBuffer = archiveReader.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits());
            final Buffer buf1 = archiveReader.readSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits());
            assertNotNull(rootBuffer);
            assertNotNull(buf1);

            AtomicInteger r1Writes = new AtomicInteger();
            CountDownLatch writeLatch = new CountDownLatch(1);
            PersistentCache cache = new MemoryPersistentCache(false) {
                @Override
                public void writeSegment(long msb, long lsb, Buffer buffer) {
                    boolean isR1 = (msb == r1.getMostSignificantBits() && lsb == r1.getLeastSignificantBits());
                    super.writeSegment(msb, lsb, buffer);
                    if (isR1) {
                        if (r1Writes.incrementAndGet() == 1) {
                            writeLatch.countDown();
                        }
                    }
                }
            };

            // Seed only root in cache
            cache.writeSegment(root.getMostSignificantBits(), root.getLeastSignificantBits(), rootBuffer);

            CachingSegmentArchiveReader reader = new CachingSegmentArchiveReader(cache, new FailingOnceReader(archiveReader, r1));

            // First read: schedules prefetch, delegate fails once, no write occurs, in-flight must be cleared
            Buffer got1 = reader.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits());
            assertNotNull(got1);
            // Wait a bit for async attempt to complete
            Thread.sleep(200);
            assertEquals(0, r1Writes.get());
            assertFalse(cache.containsSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits()));

            // Second read: should schedule again and now succeed
            Buffer got2 = reader.readSegment(root.getMostSignificantBits(), root.getLeastSignificantBits());
            assertNotNull(got2);
            assertTrue("Prefetch after failure did not write target in time", writeLatch.await(5, TimeUnit.SECONDS));
            assertEquals(1, r1Writes.get());
            assertTrue(cache.containsSegment(r1.getMostSignificantBits(), r1.getLeastSignificantBits()));

            reader.close();
        } finally {
            if (archiveReader != null) archiveReader.close();
            if (fs != null) fs.close();
        }
    }

    @NotNull
    private FileStoreBuilder getFileStoreBuilderWithCachingPersistence(boolean repoNotReachable) {
        FileStoreBuilder fileStoreBuilder;
        fileStoreBuilder = fileStoreBuilder(getFileStoreFolder());
        fileStoreBuilder.withSegmentCacheSize(10);

        SegmentNodeStorePersistence customPersistence = new CachingPersistence(new MemoryPersistentCache(repoNotReachable), new TarPersistence(getFileStoreFolder()));
        fileStoreBuilder.withCustomPersistence(customPersistence);
        return fileStoreBuilder;
    }

    class MemoryPersistentCache extends AbstractPersistentCache {

        private final Map<String, Buffer> segments = Collections.synchronizedMap(new HashMap<String, Buffer>());

        private boolean throwException = false;

        public MemoryPersistentCache(boolean throwException) {
            this.throwException = throwException;
            segmentCacheStats = new SegmentCacheStats(
                    "Memory Cache",
                    () -> null,
                    () -> null,
                    () -> null,
                    () -> null);
        }

        @Override
        protected Buffer readSegmentInternal(long msb, long lsb) {
            return segments.get(String.valueOf(msb) + lsb);
        }

        @Override
        public boolean containsSegment(long msb, long lsb) {
            return segments.containsKey(String.valueOf(msb) + lsb);
        }

        @Override
        public void writeSegment(long msb, long lsb, Buffer buffer) {
            segments.put(String.valueOf(msb) + lsb, buffer);
        }

        @Override
        public Buffer readSegment(long msb, long lsb, @NotNull Callable<Buffer> loader) throws RepositoryNotReachableException {
            return super.readSegment(msb, lsb, () -> {
                if (throwException) {
                    throw new RepositoryNotReachableException(null);
                }
                return loader.call();
            });
        }

        @Override
        public void cleanUp() {

        }
    }

    class FailingOnceReader implements SegmentArchiveReader {
        final SegmentArchiveReader segmentArchiveReader;
        final java.util.concurrent.atomic.AtomicBoolean failed = new java.util.concurrent.atomic.AtomicBoolean(false);
        final UUID segmentId;

        FailingOnceReader(SegmentArchiveReader d, UUID r1) {
            this.segmentArchiveReader = d;
            this.segmentId = r1;
        }

        @Override public Buffer readSegment(long msb, long lsb) throws IOException {
            if (msb == segmentId.getMostSignificantBits() && lsb == segmentId.getLeastSignificantBits()) {
                if (failed.compareAndSet(false, true)) {
                    throw new IOException("fail once");
                }
            }
            return segmentArchiveReader.readSegment(msb, lsb);
        }

        @Override public boolean containsSegment(long msb, long lsb) { return segmentArchiveReader.containsSegment(msb, lsb); }
        @Override public List<SegmentArchiveEntry> listSegments() { return segmentArchiveReader.listSegments(); }
        @Override public SegmentGraph getGraph() throws IOException { return segmentArchiveReader.getGraph(); }
        @Override public Buffer getBinaryReferences() throws IOException { return segmentArchiveReader.getBinaryReferences(); }
        @Override public long length() { return segmentArchiveReader.length(); }
        @Override public String getName() { return segmentArchiveReader.getName(); }
        @Override public void close() throws IOException { segmentArchiveReader.close(); }
        @Override public int getEntrySize(int size) { return segmentArchiveReader.getEntrySize(size); }
        @Override public boolean isRemote() { return segmentArchiveReader.isRemote(); }
    }
}

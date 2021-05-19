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
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CachingPersistenceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    @Test(expected = RepositoryNotReachableException.class)
    public void testRepositoryNotReachableWithCachingPersistence() throws IOException, InvalidFileStoreVersionException {
        FileStoreBuilder fileStoreBuilder;
        FileStore fileStore = null;
        try {

            fileStoreBuilder = createFileStoreBuilder();
            SegmentNodeStorePersistence customPersistence = new CachingPersistence(new MemoryPersistentCache(false), new TarPersistence(getFileStoreFolder()), false);
            fileStoreBuilder.withCustomPersistence(customPersistence);

            fileStore = fileStoreBuilder.build();

            SegmentId id = new SegmentId(fileStore, 5, 5);
            byte[] buffer = new byte[2];
            fileStore.writeSegment(id, buffer, 0, 2);

            assertTrue(fileStore.containsSegment(id));
            //close file store so that TarReader is used to read the segment
            fileStore.close();

            fileStoreBuilder = getFileStoreBuilderWithCachingPersistence(false, false);
            fileStore = fileStoreBuilder.build();

            Segment segment = fileStore.readSegment(id);
            assertNotNull(segment);

            fileStore.close();

            // Construct file store that will simulate throwing RepositoryNotReachableException when reading a segment
            fileStoreBuilder = getFileStoreBuilderWithCachingPersistence(true, false);

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
    public void testSegmentSavedInCacheOnWrite() throws IOException, InvalidFileStoreVersionException {
        FileStoreBuilder fileStoreBuilder;
        FileStore fileStore = null;
        try {
            //segments not persisted in cache on segment write
            fileStoreBuilder = createFileStoreBuilder();
            MemoryPersistentCache memoryPersistentCache = new MemoryPersistentCache(false);
            SegmentNodeStorePersistence customPersistence = new CachingPersistence(memoryPersistentCache, new TarPersistence(getFileStoreFolder()), false);
            fileStoreBuilder.withCustomPersistence(customPersistence);

            fileStore = fileStoreBuilder.build();

            SegmentId id = new SegmentId(fileStore, 5, 5);
            byte[] buffer = new byte[2];
            fileStore.writeSegment(id, buffer, 0, 2);

            assertTrue(fileStore.containsSegment(id));

            assertFalse("Segment should not be saved in cache", memoryPersistentCache.containsSegment(5, 5));

            fileStore.close();


            //Create new file store and on segment write, save segment in the cache
            fileStoreBuilder = createFileStoreBuilder();
            memoryPersistentCache = new MemoryPersistentCache(false);
            customPersistence = new CachingPersistence(memoryPersistentCache, new TarPersistence(getFileStoreFolder()), true);
            fileStoreBuilder.withCustomPersistence(customPersistence);

            fileStore = fileStoreBuilder.build();

            id = new SegmentId(fileStore, 6, 6);
            buffer = new byte[2];
            fileStore.writeSegment(id, buffer, 0, 2);

            assertTrue(fileStore.containsSegment(id));

            assertTrue("Segment should be saved in cache as well", memoryPersistentCache.containsSegment(6, 6));

        } finally {
            if (fileStore != null) {
                fileStore.close();
            }
        }
    }

    @Test
    public void testUpdateCacheChainOnWrite() throws IOException, InvalidFileStoreVersionException {
        FileStoreBuilder fileStoreBuilder;
        FileStore fileStore = null;
        try {
            fileStoreBuilder = createFileStoreBuilder();
            MemoryPersistentCache persistentCache1 = new MemoryPersistentCache(false);
            MemoryPersistentCache persistentCache2 = new MemoryPersistentCache(false);
            persistentCache1.linkWith(persistentCache2);
            //segments persisted in cache on segment write
            SegmentNodeStorePersistence customPersistence = new CachingPersistence(persistentCache1, new TarPersistence(getFileStoreFolder()), true);
            fileStoreBuilder.withCustomPersistence(customPersistence);

            fileStore = fileStoreBuilder.build();

            SegmentId id = new SegmentId(fileStore, 5, 5);
            byte[] buffer = new byte[2];
            fileStore.writeSegment(id, buffer, 0, 2);

            assertTrue(fileStore.containsSegment(id));

            // Segment should be written to both caches.

            assertTrue("Segment should be saved in cache", persistentCache1.containsSegment(5, 5));
            assertTrue("Segment should be saved in cache", persistentCache2.containsSegment(5, 5));

        } finally {
            if (fileStore != null) {
                fileStore.close();
            }
        }
    }

    @Test
    public void testCachesLoadedAfterMiss() {
        MemoryPersistentCache persistentCache1 = new MemoryPersistentCache(false);
        MemoryPersistentCache persistentCache2 = new MemoryPersistentCache(false);
        persistentCache1.linkWith(persistentCache2);

        byte[] buffer = new byte[2];
        Buffer segment = Buffer.wrap(buffer);

        // caches do not contain segment
        assertFalse(persistentCache1.containsSegment(5, 5));
        assertFalse(persistentCache2.containsSegment(5, 5));

        // get segment from cache
        persistentCache1.readSegment(5, 5, () -> segment);

        //verify segment loaded to both caches
        assertTrue(persistentCache1.containsSegment(5, 5));
        assertTrue(persistentCache2.containsSegment(5, 5));
    }

    /**
     * @param repoNotReachable - if set to true, {@code RepositoryNotReachableException} will be thrown when calling {@code SegmentArchiveReader}#readSegment
     * @return
     */
    @NotNull
    private FileStoreBuilder getFileStoreBuilderWithCachingPersistence(boolean repoNotReachable, boolean updateCacheAfterWrite) {
        FileStoreBuilder fileStoreBuilder;
        fileStoreBuilder = fileStoreBuilder(getFileStoreFolder());
        fileStoreBuilder.withSegmentCacheSize(10);

        SegmentNodeStorePersistence customPersistence = new CachingPersistence(new MemoryPersistentCache(repoNotReachable), new TarPersistence(getFileStoreFolder()), updateCacheAfterWrite);
        fileStoreBuilder.withCustomPersistence(customPersistence);
        return fileStoreBuilder;
    }

    @NotNull
    private FileStoreBuilder createFileStoreBuilder() {
        FileStoreBuilder fileStoreBuilder;
        fileStoreBuilder = fileStoreBuilder(getFileStoreFolder());
        fileStoreBuilder.withSegmentCacheSize(10);

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
        public void writeSegmentInternal(long msb, long lsb, Buffer buffer) {
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
}

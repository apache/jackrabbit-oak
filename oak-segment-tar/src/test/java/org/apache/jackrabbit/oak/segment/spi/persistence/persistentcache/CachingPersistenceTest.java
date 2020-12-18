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

    /**
     * @param repoNotReachable - if set to true, {@code RepositoryNotReachableException} will be thrown when calling {@code SegmentArchiveReader}#readSegment
     * @return
     */
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
}

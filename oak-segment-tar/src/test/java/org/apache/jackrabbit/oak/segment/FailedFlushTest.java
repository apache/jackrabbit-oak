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
 */
package org.apache.jackrabbit.oak.segment;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentTarManager;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentTarWriter;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FailedFlushTest {

    private DefaultSegmentWriter writer;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private final RandomStringUtils randomStrings = RandomStringUtils.insecure();
    private FileStore store;
    private boolean failAfterSegmentWrite = false;
    private boolean failBeforeSegmentWrite = false;
    private final Map<UUID, Set<Integer>> segmentId2Size = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        store = createFileStore();
        writer = defaultSegmentWriterBuilder("test").build(store);
    }

    @Test
    public void repeatedFlushFailure() throws Exception {
        for (int i = 0; i < 1000; i++) {
            writer.writeString(randomStrings.nextAlphanumeric(16));
        }

        // Repeatedly fail flush before segment write to provoke OAK-11807.
        // Without the fix, SegmentBufferWriter.flush() will end up in a
        // state where any further flush will fail with an IllegalStateException
        // saying "Too much data for a segment".
        failBeforeSegmentWrite = true;
        for (int i = 0; i < 100; i++) {
            try {
                writer.flush();
                fail("This flush must fail");
            } catch (IOException e) {
                // expected
            }
        }
        failBeforeSegmentWrite = false;

        // Without the fix for OAK-11807 this flush would continue to
        // fail with an IllegalStateException.
        writer.flush();
    }

    @Test
    public void flushTwiceAfterSegmentStored() throws Exception {
        for (int i = 0; i < 10; i++) {
            writer.writeString(randomStrings.nextAlphanumeric(16));
        }

        failAfterSegmentWrite = true;
        try {
            writer.flush();
            fail("This flush must fail");
        } catch (IOException e) {
            // expected
        }
        failAfterSegmentWrite = false;
        writer.flush();

        // expect two segments:
        // - first segment written by FileStore with initial node
        // - second segment written by this test
        // TarPersistence counts duplicate segments as one segment
        assertEquals(2, store.getSegmentCount());
        for (Map.Entry<UUID, Set<Integer>> entry : segmentId2Size.entrySet()) {
            UUID segmentId = entry.getKey();
            Set<Integer> sizes = entry.getValue();
            assertEquals("Same segment (" + segmentId + ") with different sizes: " + sizes, 1, sizes.size());
        }
    }

    private FileStore createFileStore() throws Exception {
        File dir = folder.newFolder("segment-store");
        return FileStoreBuilder.fileStoreBuilder(dir).withCustomPersistence(new TarPersistence(dir) {
            @Override
            public SegmentArchiveManager createArchiveManager(boolean memoryMapping, boolean offHeapAccess,
                                                              IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor,
                                                              RemoteStoreMonitor remoteStoreMonitor) {
                return new TestArchiveManager(dir, fileStoreMonitor, ioMonitor, memoryMapping, offHeapAccess);
            }
        }).build();
    }

    private class TestArchiveManager extends SegmentTarManager {

        private final File segmentStoreDir;
        private final FileStoreMonitor fileStoreMonitor;
        private final IOMonitor ioMonitor;

        TestArchiveManager(File segmentStoreDir, FileStoreMonitor fileStoreMonitor, IOMonitor ioMonitor,
                           boolean memoryMapping, boolean offHeapAccess) {
            super(segmentStoreDir, fileStoreMonitor, ioMonitor, memoryMapping, offHeapAccess);
            this.segmentStoreDir = segmentStoreDir;
            this.fileStoreMonitor = fileStoreMonitor;
            this.ioMonitor = ioMonitor;
        }

        @Override
        public @NotNull SegmentArchiveWriter create(String archiveName) {
            return new TestArchiveWriter(new File(segmentStoreDir, archiveName), fileStoreMonitor, ioMonitor);
        }
    }

    private class TestArchiveWriter extends SegmentTarWriter {
        TestArchiveWriter(File file, FileStoreMonitor monitor, IOMonitor ioMonitor) {
            super(file, monitor, ioMonitor);
        }

        @Override
        public void writeSegment(long msb, long lsb, byte[] data, int offset, int size,
                                 int generation, int fullGeneration, boolean compacted) throws IOException {
            if (failBeforeSegmentWrite) {
                throw new IOException("Simulated failure before segment write");
            }
            super.writeSegment(msb, lsb, data, offset, size, generation, fullGeneration, compacted);
            Buffer segmentData = Buffer.wrap(data, offset, size);
            segmentId2Size.computeIfAbsent(new UUID(msb, lsb), uuid -> new HashSet<>()).add(size);
            if (store != null) {
                SegmentId id = new SegmentId(store, msb, lsb);
                Segment s = new Segment(store.getSegmentIdProvider(), id, segmentData);
                System.out.println(s);
            }
            if (failAfterSegmentWrite) {
                throw new IOException("Simulated failure after segment write");
            }
        }
    }
}

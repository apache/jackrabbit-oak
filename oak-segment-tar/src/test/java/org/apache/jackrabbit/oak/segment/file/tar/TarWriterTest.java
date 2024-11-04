/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment.file.tar;

import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.jackrabbit.oak.segment.file.UnrecoverableArchiveException;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TarWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    protected TestFileStoreMonitor monitor = new TestFileStoreMonitor();

    @Test
    public void createNextGenerationTest() throws Exception {
        int counter = 2222;
        TarWriter t0 = new TarWriter(getSegmentArchiveManager(), counter, NoopStats.INSTANCE);

        // not dirty, will not create a new writer
        TarWriter t1 = t0.createNextGeneration();
        assertSame(t0, t1);
        assertTrue(t1.getFileName().contains(String.valueOf(counter)));

        // dirty, will create a new writer
        writeEntry(t1);

        TarWriter t2 = t1.createNextGeneration();
        assertNotSame(t1, t2);
        assertTrue(t1.isClosed());
        assertTrue(t2.getFileName().contains(String.valueOf(counter + 1)));
    }

    @Test
    public void failToClose() throws Exception {
        TarWriter tarWriter = new TarWriter(getFailingSegmentArchiveManager(), 2222, NoopStats.INSTANCE);

        writeEntry(tarWriter);

        assertThrows(UnrecoverableArchiveException.class, tarWriter::close);
    }

    private static void writeEntry(TarWriter writer) throws IOException {
        UUID id = UUID.randomUUID();
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits() & (-1 >>> 4); // OAK-1672
        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);
        writer.writeEntry(msb, lsb, data, 0, data.length, newGCGeneration(0, 0, false));
    }

    @NotNull
    protected SegmentArchiveManager getSegmentArchiveManager() throws Exception {
        IOMonitorAdapter ioMonitor = new IOMonitorAdapter();
        File segmentstoreDir = folder.newFolder();
        return new SegmentTarManager(segmentstoreDir, monitor, ioMonitor, false, false);
    }

    @NotNull
    protected SegmentArchiveManager getFailingSegmentArchiveManager() throws Exception {
        IOMonitorAdapter ioMonitor = new IOMonitorAdapter();
        File segmentstoreDir = folder.newFolder();
        return new SegmentTarManager(segmentstoreDir, monitor, ioMonitor, false, false) {
            @Override
            public @NotNull SegmentArchiveWriter create(String archiveName) {
                return new SegmentTarWriter(new File(segmentstoreDir, archiveName), monitor, ioMonitor) {
                    @Override
                    public void writeGraph(byte[] data) throws IOException {
                        throw new IOException("test");
                    }
                };
            }
        };
    }

    public static class TestFileStoreMonitor extends FileStoreMonitorAdapter {

        long written;

        @Override
        public void written(long bytes) {
            written += bytes;
        }

    }

    @Test
    public void testFileStoreMonitor() throws Exception {
        try (TarWriter writer = new TarWriter(getSegmentArchiveManager(), 0, NoopStats.INSTANCE)) {
            long sizeBefore = writer.fileLength();
            long writtenBefore = monitor.written;
            writer.writeEntry(0, 0, new byte[42], 0, 42, newGCGeneration(0, 0, false));
            long sizeAfter = writer.fileLength();
            long writtenAfter = monitor.written;
            assertEquals(sizeAfter - sizeBefore, writtenAfter - writtenBefore);
        }
    }

    @Test
    public void testTarWriterFailOnExcessiveEntryCount() throws Exception {
        final int ENTRY_LIMIT = 8;
        IOMonitorAdapter ioMonitor = new IOMonitorAdapter();
        File segmentstoreDir = folder.newFolder();
        SegmentArchiveManager archiveManager = new SegmentTarManager(
                segmentstoreDir, monitor, ioMonitor, false, false
        ) {
            @Override
            public @NotNull SegmentArchiveWriter create(String archiveName) {
                return new SegmentTarWriter(new File(segmentstoreDir, archiveName), monitor, ioMonitor) {
                    @Override
                    public int getMaxEntryCount() {
                        return ENTRY_LIMIT;
                    }
                };
            }
        };

        TarWriter tarWriter = new TarWriter(archiveManager, 0, NoopStats.INSTANCE);
        for (int i = 0; i < ENTRY_LIMIT; i++) {
            writeEntry(tarWriter);
        }
        assertThrows(IllegalStateException.class, () -> writeEntry(tarWriter));
        tarWriter.close();
    }
}

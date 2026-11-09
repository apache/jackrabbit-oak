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

import static org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.file.UnrecoverableArchiveException;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TarWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    protected TestFileStoreMonitor monitor = new TestFileStoreMonitor();

    @Test
    public final void createNextGenerationTest() throws Exception {
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
    public final void closeWritesBrfGraphAndIndex() throws Exception {
        SegmentArchiveManager segmentArchiveManager = getSegmentArchiveManager();
        String fileName;
        try (TarWriter tarWriter = new TarWriter(segmentArchiveManager, 2222, NoopStats.INSTANCE)) {
            fileName = tarWriter.getFileName();
            writeEntry(tarWriter);
            writeEntry(tarWriter);
            writeEntry(tarWriter);
        }

        try (SegmentArchiveReader reader = segmentArchiveManager.open(fileName)) {
            assertNotNull(reader);
            List<SegmentArchiveEntry> segments = reader.listSegments();
            assertEquals(3, segments.size());
            assertNotNull(reader.getGraph());
            assertNotNull(reader.getBinaryReferences());
        }
    }

    @Test
    public final void failToClose() throws Exception {
        TarWriter tarWriter = new TarWriter(getFailingSegmentArchiveManager(), 1111, NoopStats.INSTANCE);

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
    protected final SegmentArchiveManager getFailingSegmentArchiveManager() throws Exception {
        return new DelegatingSegmentArchiveManager(getSegmentArchiveManager()) {
            @Override
            public @NotNull SegmentArchiveWriter create(@NotNull String archiveName) throws IOException {
                return new DelegatingSegmentArchiveWriter(super.create(archiveName)) {
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
    public final void testFileStoreMonitor() throws Exception {
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
    public final void testTarWriterFailOnExcessiveEntryCount() throws Exception {
        final int ENTRY_LIMIT = 8;
        SegmentArchiveManager archiveManager = new DelegatingSegmentArchiveManager(getSegmentArchiveManager()) {
            @Override
            public @NotNull SegmentArchiveWriter create(@NotNull String archiveName) throws IOException {
                return new DelegatingSegmentArchiveWriter(super.create(archiveName)) {
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

    private static class DelegatingSegmentArchiveManager implements SegmentArchiveManager {

        private final SegmentArchiveManager delegate;

        protected DelegatingSegmentArchiveManager(SegmentArchiveManager delegate) {
            this.delegate = delegate;
        }

        @NotNull
        @Override
        public SegmentArchiveWriter create(@NotNull String archiveName) throws IOException {
            return delegate.create(archiveName);
        }

        @Override
        public boolean delete(@NotNull String archiveName) {
            return delegate.delete(archiveName);
        }

        @Override
        public boolean exists(@NotNull String archiveName) {
            return delegate.exists(archiveName);
        }

        @Override
        public @NotNull List<String> listArchives() throws IOException {
            return delegate.listArchives();
        }

        @Override
        public @Nullable SegmentArchiveReader open(@NotNull String archiveName) throws IOException {
            return delegate.open(archiveName);
        }

        @Override
        public @Nullable SegmentArchiveReader forceOpen(String archiveName) throws IOException {
            return delegate.forceOpen(archiveName);
        }

        @Override
        public boolean renameTo(@NotNull String from, @NotNull String to) {
            return delegate.renameTo(from, to);
        }

        @Override
        public void copyFile(@NotNull String from, @NotNull String to) throws IOException {
            delegate.copyFile(from, to);
        }

        @Override
        public void recoverEntries(@NotNull String archiveName, @NotNull LinkedHashMap<UUID, byte[]> entries) throws IOException {
            delegate.recoverEntries(archiveName, entries);
        }

        @Override
        public void backup(@NotNull String archiveName, @NotNull String backupArchiveName, @NotNull Set<UUID> recoveredEntries) throws IOException {
            delegate.backup(archiveName, backupArchiveName, recoveredEntries);
        }

        @Override
        public boolean isReadOnly(String archiveName) {
            return delegate.isReadOnly(archiveName);
        }
    }

    private static class DelegatingSegmentArchiveWriter implements SegmentArchiveWriter {

        private final SegmentArchiveWriter delegate;

        protected DelegatingSegmentArchiveWriter(SegmentArchiveWriter delegate) {
            this.delegate = delegate;
        }

        @Override
        @NotNull
        public void writeSegment(long msb, long lsb, @NotNull byte[] data, int offset, int size, int generation, int fullGeneration, boolean isCompacted) throws IOException {
            delegate.writeSegment(msb, lsb, data, offset, size, generation, fullGeneration, isCompacted);
        }

        @Override
        public @Nullable Buffer readSegment(long msb, long lsb) throws IOException {
            return delegate.readSegment(msb, lsb);
        }

        @Override
        public boolean containsSegment(long msb, long lsb) {
            return delegate.containsSegment(msb, lsb);
        }

        @Override
        public void writeGraph(@NotNull byte[] data) throws IOException {
            delegate.writeGraph(data);
        }

        @Override
        public void writeBinaryReferences(@NotNull byte[] data) throws IOException {
            delegate.writeBinaryReferences(data);
        }

        @Override
        public long getLength() {
            return delegate.getLength();
        }

        @Override
        public int getEntryCount() {
            return delegate.getEntryCount();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public boolean isCreated() {
            return delegate.isCreated();
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        @NotNull
        public String getName() {
            return delegate.getName();
        }

        @Override
        public boolean isRemote() {
            return delegate.isRemote();
        }

        @Override
        public int getMaxEntryCount() {
            return delegate.getMaxEntryCount();
        }
    }
}

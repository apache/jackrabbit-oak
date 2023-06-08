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

package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentTarManager;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentTarWriter;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileStoreTest {
    private  static final String FAILED_TO_WRITE_ON_CLOSE = "Failed to write to the archive on closing";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    @Test
    public void containsSegment() throws Exception {
        try (FileStore fileStore = fileStoreBuilder(getFileStoreFolder()).build()) {
            SegmentId id = getSegmentId(fileStore);
            if (fileStore.containsSegment(id)) {
                fileStore.readSegment(id);
            }
        }
    }

    @Test
    public void overlapping() {
        try (FileStore ignored1 = fileStoreBuilder(getFileStoreFolder()).build();
             FileStore ignored2 = fileStoreBuilder(getFileStoreFolder()).build()) {
            fail("should not be able to open 2 stores on the same path");
        } catch (Exception ex) {
            // expected
        }
    }

    @Test
    public void segmentCount() throws Exception {
        try (FileStore fileStore = fileStoreBuilder(getFileStoreFolder()).build()) {
            assertEquals(1, fileStore.getSegmentCount());

            SegmentNodeState head = fileStore.getHead();
            SegmentNodeBuilder builder = head.builder();
            builder.setProperty("a", 1);
            SegmentNodeState newHead = builder.getNodeState();

            fileStore.getRevisions().setHead(head.getRecordId(), newHead.getRecordId());
            fileStore.flush();

            assertEquals(2, fileStore.getSegmentCount());
        }
    }

    @Test
    public void writeSegment_shouldThrowUnrecoverableExceptionWhenFailToCloseArchive() throws Exception {
        File directory = getFileStoreFolder();
        TarPersistence persistence = getPersistenceThrowingUnrecoverableExceptionOnClosingArchive(directory);

        try(FileStore fileStore = fileStoreBuilder(directory)
                .withMaxFileSize(1) // max archive size = 1 MB
                .withCustomPersistence(persistence)
                .build()) {


            int size = 1024 * 1025; // Bigger than 1 MB to force closing the current archive and create a new one
            byte[] dataExceedingMaxFileSize = new byte[size];

            assertThrows(UnrecoverableArchiveException.class, () ->
                    // write to the archive but fail to close it when it's full
                    fileStore.writeSegment(getSegmentId(fileStore), dataExceedingMaxFileSize, 0, size)
            );
        }
    }

    @Test
    public void tryFlush_shouldCloseFileStoreWhenFailToCloseArchive() throws Exception {
        File directory = getFileStoreFolder();
        TarPersistence failingPersistence = getPersistenceThrowingUnrecoverableExceptionOnClosingArchive(directory);

        FileStore fileStore = fileStoreBuilder(directory)
                .withMaxFileSize(1) // max archive size = 1 MB
                .withCustomPersistence(failingPersistence)
                .build();

        // Write slightly less than 1MB
        writeData(fileStore, 1024 * 1000);
        // Write more data to exceed the archive max size, forcing to close the current archive and create a new one.
        // Data is small enough to prevent auto-flushing, so it does not fail yet
        writeData(fileStore, 1024 * 20);

        // Execute tryFlush: now actually write to the archive, which will be closed because its size > maxFileSize
        fileStore.tryFlush();

        // FileStore should have been already shut down by tryFlush because of the UnrecoverableArchiveException
        IllegalStateException closeEx = assertThrows("FileStore#tryFlush should have already shut down FileStore",
                IllegalStateException.class, fileStore::close);
        assertEquals("already shut down", closeEx.getMessage());
    }

    private static int counter = 0;

    private static void writeData(FileStore fileStore, int size) throws IOException {
        NodeBuilder nodeBuilder = EMPTY_NODE.builder();
        Blob blob = nodeBuilder.createBlob(new ZeroStream(size));
        int i = counter++;
        nodeBuilder.child("node"+ i).setProperty("prop" + i, blob);
        fileStore.getWriter().writeNode(nodeBuilder.getNodeState());
    }

    @NotNull
    private static TarPersistence getPersistenceThrowingUnrecoverableExceptionOnClosingArchive(File directory) {
        return new TarPersistence(directory) {
            @Override
            public SegmentArchiveManager createArchiveManager(boolean memoryMapping, boolean offHeapAccess, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) {
                return new SegmentTarManager(directory, fileStoreMonitor, ioMonitor, memoryMapping, offHeapAccess) {
                    @Override
                    public SegmentArchiveWriter create(String archiveName) {
                        return new SegmentTarWriter(new File(directory, archiveName), fileStoreMonitor, ioMonitor) {
                            @Override
                            public void writeGraph(byte[] data) throws IOException {
                                throw new IOException(FAILED_TO_WRITE_ON_CLOSE);
                            }
                        };
                    }
                };
            }
        };
    }

    @NotNull
    private static SegmentId getSegmentId(FileStore fileStore) {
        return new SegmentId(fileStore, 0, 0);
    }

    private static class ZeroStream extends InputStream {
        private final int size;

        private int position = 0;

        public ZeroStream(int size) {
            this.size = size;
        }

        @Override
        public int read() {
            if (position < size) {
                position++;
                return 0;
            }
            return -1;
        }
    }
}

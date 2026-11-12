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
package org.apache.jackrabbit.oak.segment.file.tar;

import static org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexWriter.newBinaryReferencesIndexWriter;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexWriter;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.InvalidBinaryReferencesIndexException;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentTarReaderTest {

    private static final String ARCHIVE = "data00000a.tar";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private SegmentArchiveManager archiveManager;

    private File archiveFile;

    private byte[] binaryReferences;

    @Before
    public void setUp() throws IOException {
        File directory = folder.newFolder();
        archiveManager = new SegmentTarManager(directory, new FileStoreMonitorAdapter(), new IOMonitorAdapter(), false, false);
        archiveFile = new File(directory, ARCHIVE);
        BinaryReferencesIndexWriter writer = newBinaryReferencesIndexWriter();
        writer.addEntry(1, 1, false, UUID.randomUUID(), "blob-id");
        binaryReferences = writer.write();
    }

    @Test
    public void binaryReferencesWithCorruptMagicFail() throws IOException {
        writeArchive();
        flipByte(endOfBinaryReferences() - 1);

        assertThrows(IOException.class, this::readBinaryReferences);
    }

    @Test
    public void binaryReferencesWithCorruptDataFailToParse() throws IOException {
        writeArchive();
        flipByte(endOfBinaryReferences() - binaryReferences.length);

        ByteBuffer corrupt = readBinaryReferences();

        assertNotNull(corrupt);
        assertThrows(InvalidBinaryReferencesIndexException.class,
                () -> BinaryReferencesIndexLoader.parseBinaryReferencesIndex(corrupt));
    }

    @Test
    public void unexpectedEntryAfterLastSegmentFails() throws IOException {
        writeArchive();
        replace(ARCHIVE + ".brf", ARCHIVE + ".xyz");

        assertThrows(IOException.class, this::readBinaryReferences);
    }

    private void writeArchive() throws IOException {
        SegmentArchiveWriter writer = archiveManager.create(ARCHIVE);
        TarFileTest.writeSegments(writer);
        writer.writeBinaryReferences(binaryReferences);
        writer.writeGraph(new SegmentGraph().write());
        writer.close();
    }

    private ByteBuffer readBinaryReferences() throws IOException {
        try (SegmentArchiveReader reader = archiveManager.open(ARCHIVE)) {
            assertNotNull(ARCHIVE + " should have a valid index", reader);
            return reader.getBinaryReferences();
        }
    }

    /**
     * The binary references entry ends where the header of the graph entry starts.
     */
    private int endOfBinaryReferences() throws IOException {
        return indexOf(ARCHIVE + ".gph");
    }

    private int indexOf(String entryName) throws IOException {
        String content = new String(Files.readAllBytes(archiveFile.toPath()), StandardCharsets.ISO_8859_1);
        int position = content.indexOf(entryName);
        assertTrue(entryName + " not found in " + ARCHIVE, position >= 0);
        return position;
    }

    private void flipByte(long position) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(archiveFile, "rw")) {
            file.seek(position);
            int value = file.read();
            file.seek(position);
            file.write(value ^ 0xff);
        }
    }

    private void replace(String entryName, String replacement) throws IOException {
        int position = indexOf(entryName);
        try (RandomAccessFile file = new RandomAccessFile(archiveFile, "rw")) {
            file.seek(position);
            file.write(replacement.getBytes(StandardCharsets.UTF_8));
        }
    }
}

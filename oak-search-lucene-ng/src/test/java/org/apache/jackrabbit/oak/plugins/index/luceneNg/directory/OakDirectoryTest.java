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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.*;

public class OakDirectoryTest {

    private NodeBuilder root;

    @Before
    public void setup() {
        root = INITIAL_CONTENT.builder();
    }

    @Test
    public void testDirectoryWritable() throws Exception {
        NodeBuilder storageBuilder = root.child("storageRoot");
        OakDirectory directory = new OakDirectory(storageBuilder, "testIndex", false);
        // In write mode the directory should accept files directly
        assertNotNull(directory.listAll());
    }

    @Test
    public void testListAllEmpty() throws Exception {
        OakDirectory directory = new OakDirectory(root.child("storageRoot"), "testIndex", false);
        String[] files = directory.listAll();
        assertNotNull(files);
        assertEquals(0, files.length);
    }

    @Test
    public void testWriteAndReadFile() throws Exception {
        NodeBuilder storageBuilder = root.child("storageRoot");
        OakDirectory directory = new OakDirectory(storageBuilder, "testIndex", false);

        // Write file
        String fileName = "testfile.txt";
        try (IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT)) {
            output.writeString("Hello Lucene 9");
            output.writeLong(123456789L);
        }

        // Verify file exists
        String[] files = directory.listAll();
        assertEquals(1, files.length);
        assertEquals(fileName, files[0]);

        // Read file back
        try (IndexInput input = directory.openInput(fileName, IOContext.DEFAULT)) {
            assertEquals("Hello Lucene 9", input.readString());
            assertEquals(123456789L, input.readLong());
        }
    }

    @Test
    public void createOutputWritesUniqueKeyToFileNode() throws Exception {
        NodeBuilder storageBuilder = root.child("storageRoot");
        OakDirectory directory = new OakDirectory(storageBuilder, "testIndex", false);

        try (IndexOutput output = directory.createOutput("test.bin", IOContext.DEFAULT)) {
            output.writeBytes(new byte[10], 10);
        }

        NodeBuilder fileNode = storageBuilder.getChildNode("test.bin");
        assertTrue("file node must exist", fileNode.exists());

        PropertyState keyProp = fileNode.getProperty(OakDirectory.PROP_UNIQUE_KEY);
        assertNotNull("PROP_UNIQUE_KEY must be set on every new file", keyProp);

        String hexKey = keyProp.getValue(Type.STRING);
        assertEquals("unique key must be " + OakDirectory.UNIQUE_KEY_SIZE + " bytes (= " +
                        OakDirectory.UNIQUE_KEY_SIZE * 2 + " hex chars)",
                OakDirectory.UNIQUE_KEY_SIZE * 2, hexKey.length());
        assertTrue("unique key must contain only hex characters", hexKey.matches("[0-9a-f]+"));
    }

    @Test
    public void uniqueKeyIsAppendedToBlobButNotReportedInFileLength() throws Exception {
        NodeBuilder storageBuilder = root.child("storageRoot");
        OakDirectory directory = new OakDirectory(storageBuilder, "testIndex", false);

        byte[] payload = new byte[100];
        try (IndexOutput output = directory.createOutput("test.bin", IOContext.DEFAULT)) {
            output.writeBytes(payload, payload.length);
        }
        directory.close();

        // Reported file length must equal exactly the bytes written
        OakDirectory readDir = new OakDirectory(storageBuilder, "testIndex", true);
        assertEquals("fileLength() must not include the uniqueKey suffix",
                payload.length, readDir.fileLength("test.bin"));
        readDir.close();

        // The blob stored in the repository must be longer by UNIQUE_KEY_SIZE
        NodeBuilder fileNode = storageBuilder.getChildNode("test.bin");
        PropertyState dataProp = fileNode.getProperty(JCR_DATA);
        assertNotNull(dataProp);
        Blob blob = dataProp.getValue(Type.BINARIES).iterator().next();
        assertEquals("blob stored in JCR_DATA must include the uniqueKey suffix",
                payload.length + OakDirectory.UNIQUE_KEY_SIZE, blob.length());
    }

    @Test
    public void deleteFileNotifiesBlobDeletionCallback() throws Exception {
        List<String> deletedBlobIds = new ArrayList<>();
        BlobDeletionCallback callback = (blobId, path) -> deletedBlobIds.add(blobId);

        // Use an identifiable BlobFactory so getContentIdentity() returns non-null.
        // In-memory Oak blobs have null content identities — we need real IDs to test the callback.
        java.util.concurrent.atomic.AtomicInteger blobCounter = new java.util.concurrent.atomic.AtomicInteger();
        BlobFactory identifiableBlobFactory = in -> {
            byte[] bytes = in.readAllBytes();
            String id = "test-blob-" + blobCounter.incrementAndGet();
            return new org.apache.jackrabbit.oak.api.Blob() {
                @Override public java.io.InputStream getNewStream() { return new java.io.ByteArrayInputStream(bytes); }
                @Override public long length() { return bytes.length; }
                @Override public String getContentIdentity() { return id; }
                @Override public String getReference() { return null; }
                @Override public boolean isInlined() { return false; }
            };
        };

        NodeBuilder storageBuilder = root.child("storageRoot");
        OakDirectory writeDir = new OakDirectory(storageBuilder, "testIndex", false,
                identifiableBlobFactory, callback);

        byte[] payload = new byte[OakBufferedIndexFile.DEFAULT_BLOB_SIZE];
        try (IndexOutput out = writeDir.createOutput("index.bin", IOContext.DEFAULT)) {
            out.writeBytes(payload, payload.length);
        }
        writeDir.close();

        assertTrue("callback must not fire before deleteFile()", deletedBlobIds.isEmpty());

        OakDirectory deleteDir = new OakDirectory(storageBuilder, "testIndex", false,
                identifiableBlobFactory, callback);
        deleteDir.deleteFile("index.bin");
        deleteDir.close();

        assertFalse("callback must fire when a file with identifiable blobs is deleted",
                deletedBlobIds.isEmpty());
        for (String id : deletedBlobIds) {
            assertNotNull(id);
            assertTrue(id.startsWith("test-blob-"));
        }
    }

    @Test
    public void deleteFileWithNoopCallbackDoesNotThrow() throws Exception {
        NodeBuilder storageBuilder = root.child("storageRoot");
        OakDirectory dir = new OakDirectory(storageBuilder, "testIndex", false);

        try (IndexOutput out = dir.createOutput("index.bin", IOContext.DEFAULT)) {
            out.writeBytes(new byte[10], 10);
        }
        dir.close();

        // Default constructor uses NOOP — deleteFile must not throw
        OakDirectory dir2 = new OakDirectory(storageBuilder, "testIndex", false);
        dir2.deleteFile("index.bin");
        dir2.close();
        assertFalse("file must be removed from listing", List.of(dir2.listAll()).contains("index.bin"));
    }

    @Test
    public void uniqueKeysDifferBetweenFiles() throws Exception {
        NodeBuilder storageBuilder = root.child("storageRoot");
        OakDirectory directory = new OakDirectory(storageBuilder, "testIndex", false);

        try (IndexOutput o1 = directory.createOutput("file1.bin", IOContext.DEFAULT)) {
            o1.writeBytes(new byte[10], 10);
        }
        try (IndexOutput o2 = directory.createOutput("file2.bin", IOContext.DEFAULT)) {
            o2.writeBytes(new byte[10], 10);
        }

        String key1 = storageBuilder.getChildNode("file1.bin")
                .getProperty(OakDirectory.PROP_UNIQUE_KEY).getValue(Type.STRING);
        String key2 = storageBuilder.getChildNode("file2.bin")
                .getProperty(OakDirectory.PROP_UNIQUE_KEY).getValue(Type.STRING);

        assertNotEquals("each file must get a distinct unique key", key1, key2);
    }
}

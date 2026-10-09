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
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.*;

/**
 * Tests for chunked I/O boundary edge cases in OakBufferedIndexFile.
 * Verifies correct behavior at 32KB chunk boundaries.
 */
public class ChunkedIOEdgeCasesTest {

    /**
     * Test 1: Write exactly one chunk (32KB) and verify read-back correctness.
     */
    @Test
    public void testWriteExactlyOneChunk() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        OakBufferedIndexFile indexFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        // Write exactly 32KB
        byte[] data = new byte[32 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        indexFile.writeBytes(data, 0, data.length);
        indexFile.flush();

        assertEquals(32 * 1024, indexFile.length());

        // Read back and verify
        indexFile.seek(0);
        byte[] readData = new byte[32 * 1024];
        indexFile.readBytes(readData, 0, readData.length);

        assertArrayEquals(data, readData);
        indexFile.close();
    }

    /**
     * Test 2: Write 80KB spanning three chunks and verify JCR_DATA has 3 blobs.
     */
    @Test
    public void testWriteSpanningThreeChunks() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        OakBufferedIndexFile indexFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        // Write 80KB (3 chunks: 32KB + 32KB + 16KB)
        int totalSize = 80 * 1024;
        byte[] data = new byte[totalSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        indexFile.writeBytes(data, 0, data.length);
        indexFile.flush();

        assertEquals(totalSize, indexFile.length());

        // Verify JCR_DATA has exactly 3 blobs
        assertEquals(3, file.getProperty(JCR_DATA).count());

        indexFile.close();
    }

    /**
     * Test 3: Write 40KB (32KB + 8KB) and verify last blob is 8KB.
     */
    @Test
    public void testWritePartialLastChunk() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        OakBufferedIndexFile indexFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        // Write 40KB (32KB + 8KB)
        int totalSize = 40 * 1024;
        byte[] data = new byte[totalSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        indexFile.writeBytes(data, 0, data.length);
        indexFile.flush();

        assertEquals(totalSize, indexFile.length());

        // Verify JCR_DATA has exactly 2 blobs
        PropertyState jcrData = file.getProperty(JCR_DATA);
        assertNotNull("JCR_DATA property should exist", jcrData);
        assertEquals("Should have 2 blobs", 2, jcrData.count());

        // Verify blob sizes: first should be 32KB, second should be 8KB
        Iterable<Blob> blobs = jcrData.getValue(Type.BINARIES);
        int blobIndex = 0;
        for (Blob blob : blobs) {
            if (blobIndex == 0) {
                assertEquals("First blob should be 32KB", 32 * 1024, blob.length());
            } else {
                assertEquals("Second blob should be 8KB", 8 * 1024, blob.length());
            }
            blobIndex++;
        }

        indexFile.close();
    }

    /**
     * Test 4: Seek to position == length (LUCENE-1196 compliance).
     * This should be allowed without throwing an exception.
     */
    @Test
    public void testSeekToEndOfFile() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        OakBufferedIndexFile indexFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        // Write some data
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        indexFile.writeBytes(data, 0, data.length);
        indexFile.flush();

        // Seek to end of file (position == length) - should not throw
        long fileLength = indexFile.length();
        indexFile.seek(fileLength);
        assertEquals(fileLength, indexFile.position());

        indexFile.close();
    }

    /**
     * Test 5: Read 8KB from position 30KB to 38KB (crosses 32KB chunk boundary).
     */
    @Test
    public void testReadAcrossChunkBoundary() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        OakBufferedIndexFile indexFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        // Write 40KB (to span into second chunk)
        int totalSize = 40 * 1024;
        byte[] data = new byte[totalSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        indexFile.writeBytes(data, 0, data.length);
        indexFile.flush();

        // Read 8KB from position 30KB to 38KB (crosses the 32KB boundary)
        int readStart = 30 * 1024;
        int readSize = 8 * 1024;
        indexFile.seek(readStart);
        byte[] readData = new byte[readSize];
        indexFile.readBytes(readData, 0, readSize);

        // Verify read data matches original data
        for (int i = 0; i < readSize; i++) {
            assertEquals("Data mismatch at position " + (readStart + i),
                data[readStart + i], readData[i]);
        }

        indexFile.close();
    }
}

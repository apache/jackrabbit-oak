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

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import java.io.IOException;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.*;

/**
 * Tests for error handling in OakBufferedIndexFile and OakIndexInput.
 * Verifies that error conditions are handled gracefully with appropriate exceptions.
 */
public class ErrorHandlingTest {

    /**
     * Test 1: Read from closed file should throw IOException.
     */
    @Test
    public void testReadFromClosedFile() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        OakBufferedIndexFile indexFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        // Write 1KB of data
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        indexFile.writeBytes(data, 0, data.length);
        indexFile.flush();

        // Close the file
        indexFile.close();

        // Attempt to read should throw IOException
        byte[] readData = new byte[100];
        try {
            indexFile.readBytes(readData, 0, 100);
            fail("Should throw IOException for closed file");
        } catch (IOException e) {
            // Expected - file is closed
        }
    }

    /**
     * Test 2: Invalid seek positions should throw IOException.
     * Note: Seek to position == length is allowed (LUCENE-1196).
     */
    @Test
    public void testInvalidSeekPositions() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        OakBufferedIndexFile indexFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        // Write 1000 bytes
        byte[] data = new byte[1000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        indexFile.writeBytes(data, 0, data.length);
        indexFile.flush();

        // Test 1: Seek to -1 should throw IOException
        try {
            indexFile.seek(-1);
            fail("Expected IOException when seeking to negative position");
        } catch (IOException e) {
            assertTrue("Error message should contain 'Invalid seek'",
                e.getMessage().contains("Invalid seek"));
        }

        // Test 2: Seek to 1001 (beyond file length) should throw IOException
        try {
            indexFile.seek(1001);
            fail("Expected IOException when seeking beyond file length");
        } catch (IOException e) {
            assertTrue("Error message should contain 'Invalid seek'",
                e.getMessage().contains("Invalid seek"));
        }

        // Test 3: Seek to 1000 (position == length) should succeed (LUCENE-1196)
        indexFile.seek(1000);
        assertEquals(1000, indexFile.position());

        indexFile.close();
    }

    /**
     * Test 3: Invalid read parameters should throw appropriate exceptions.
     */
    @Test
    public void testInvalidReadParameters() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        OakBufferedIndexFile indexFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        // Write 1000 bytes
        byte[] data = new byte[1000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        indexFile.writeBytes(data, 0, data.length);
        indexFile.flush();
        indexFile.seek(0);

        // Test 1: readBytes(null, 0, 10) should throw IllegalArgumentException
        try {
            indexFile.readBytes(null, 0, 10);
            fail("Expected IllegalArgumentException when reading into null array");
        } catch (IllegalArgumentException e) {
            // Expected
            assertTrue("Exception should indicate null array",
                e.getMessage().contains("null"));
        }

        // Test 2: readBytes(new byte[100], -1, 10) should throw IndexOutOfBoundsException
        try {
            indexFile.readBytes(new byte[100], -1, 10);
            fail("Expected IndexOutOfBoundsException for negative offset");
        } catch (IndexOutOfBoundsException e) {
            // Expected
            assertTrue("Exception should indicate invalid offset/length",
                e.getMessage().contains("Invalid offset/length"));
        }

        // Test 3: readBytes(new byte[100], 95, 10) should throw IndexOutOfBoundsException
        // (offset + length > array length: 95 + 10 = 105 > 100)
        try {
            indexFile.readBytes(new byte[100], 95, 10);
            fail("Expected IndexOutOfBoundsException when offset + length > array length");
        } catch (IndexOutOfBoundsException e) {
            // Expected
            assertTrue("Exception should indicate invalid offset/length",
                e.getMessage().contains("Invalid offset/length"));
        }

        // Test 4: readBytes(new byte[2000], 0, 2000) should throw IOException
        // (beyond file length)
        try {
            indexFile.seek(0);
            indexFile.readBytes(new byte[2000], 0, 2000);
            fail("Expected IOException when reading beyond file length");
        } catch (IOException e) {
            // Expected
            assertTrue("Error message should contain 'Invalid read'",
                e.getMessage().contains("Invalid read"));
        }

        indexFile.close();
    }

    /**
     * Test 4: IndexInput operations on closed state should throw IOException.
     */
    @Test
    public void testIndexInputClosedState() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        // Create and write data using OakBufferedIndexFile
        OakBufferedIndexFile indexFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        byte[] data = new byte[1000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        indexFile.writeBytes(data, 0, data.length);
        indexFile.flush();
        indexFile.close();

        // Open as OakIndexInput
        OakIndexInput indexInput = new OakIndexInput("test.bin", file, "/test", blobFactory);

        // Close the input
        indexInput.close();

        // Test 1: readByte() should throw IOException with "closed"
        try {
            indexInput.readByte();
            fail("Expected IOException when calling readByte() on closed IndexInput");
        } catch (IOException e) {
            assertTrue("Error message should contain 'closed'",
                e.getMessage().toLowerCase().contains("closed"));
        }

        // Test 2: seek(0) should throw IOException with "closed"
        try {
            indexInput.seek(0);
            fail("Expected IOException when calling seek() on closed IndexInput");
        } catch (IOException e) {
            assertTrue("Error message should contain 'closed'",
                e.getMessage().toLowerCase().contains("closed"));
        }

        // Test 3: length() should throw IllegalStateException with "closed"
        try {
            indexInput.length();
            fail("Expected IllegalStateException when calling length() on closed IndexInput");
        } catch (IllegalStateException e) {
            assertTrue("Error message should contain 'closed'",
                e.getMessage().toLowerCase().contains("closed"));
        }
    }

    /**
     * Test 5: Slice parameter validation should reject invalid parameters.
     */
    @Test
    public void testSliceParameterValidation() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        // Create and write data using OakBufferedIndexFile
        OakBufferedIndexFile indexFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        byte[] data = new byte[1000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        indexFile.writeBytes(data, 0, data.length);
        indexFile.flush();
        indexFile.close();

        // Open as OakIndexInput
        OakIndexInput indexInput = new OakIndexInput("test.bin", file, "/test", blobFactory);

        // Test 1: slice("test", -1, 100) should throw IllegalArgumentException
        try {
            indexInput.slice("test", -1, 100);
            fail("Expected IllegalArgumentException for negative offset");
        } catch (IllegalArgumentException e) {
            // Expected
            assertTrue("Exception message should indicate invalid slice parameters",
                e.getMessage().contains("Invalid slice"));
        }

        // Test 2: slice("test", 0, -1) should throw IllegalArgumentException
        try {
            indexInput.slice("test", 0, -1);
            fail("Expected IllegalArgumentException for negative length");
        } catch (IllegalArgumentException e) {
            // Expected
            assertTrue("Exception message should indicate invalid slice parameters",
                e.getMessage().contains("Invalid slice"));
        }

        // Test 3: slice("test", 500, 600) should throw IllegalArgumentException
        // (offset + length = 1100 > file length of 1000)
        try {
            indexInput.slice("test", 500, 600);
            fail("Expected IllegalArgumentException when offset + length > file length");
        } catch (IllegalArgumentException e) {
            // Expected
            assertTrue("Exception message should indicate invalid slice parameters",
                e.getMessage().contains("Invalid slice"));
        }

        indexInput.close();
    }
}

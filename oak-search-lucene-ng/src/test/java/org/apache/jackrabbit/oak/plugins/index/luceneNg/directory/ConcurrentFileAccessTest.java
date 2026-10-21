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
import org.apache.lucene.store.IndexInput;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.*;

/**
 * Tests for concurrent file access in OakIndexFile.
 * Verifies clone() for concurrent reads and position independence.
 */
public class ConcurrentFileAccessTest {

    /**
     * Test 1: Create original file, clone twice, read from 3 different positions
     * concurrently (0, 32KB, 48KB), verify each got correct data.
     */
    @Test
    public void testConcurrentReadsViaClone() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        // Write 64KB file with predictable pattern
        OakBufferedIndexFile writeFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        int fileSize = 64 * 1024;
        byte[] data = new byte[fileSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        writeFile.writeBytes(data, 0, data.length);
        writeFile.flush();
        writeFile.close();

        // Create original reader and two clones
        OakIndexFile original = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);
        OakIndexFile clone1 = original.clone();
        OakIndexFile clone2 = original.clone();

        // Positions to read from: 0, 32KB, 48KB
        final long pos0 = 0;
        final long pos32KB = 32 * 1024;
        final long pos48KB = 48 * 1024;

        // Thread-safe containers for results
        final AtomicReference<byte[]> result0 = new AtomicReference<>();
        final AtomicReference<byte[]> result32KB = new AtomicReference<>();
        final AtomicReference<byte[]> result48KB = new AtomicReference<>();
        final List<Exception> errors = new CopyOnWriteArrayList<>();

        // CountDownLatch to synchronize concurrent reads
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(3);

        // Thread 1: Read from position 0 using original
        Thread thread1 = new Thread(() -> {
            try {
                startLatch.await();
                original.seek(pos0);
                byte[] buffer = new byte[1024];
                original.readBytes(buffer, 0, buffer.length);
                result0.set(buffer);
            } catch (Exception e) {
                errors.add(e);
            } finally {
                doneLatch.countDown();
            }
        });

        // Thread 2: Read from position 32KB using clone1
        Thread thread2 = new Thread(() -> {
            try {
                startLatch.await();
                clone1.seek(pos32KB);
                byte[] buffer = new byte[1024];
                clone1.readBytes(buffer, 0, buffer.length);
                result32KB.set(buffer);
            } catch (Exception e) {
                errors.add(e);
            } finally {
                doneLatch.countDown();
            }
        });

        // Thread 3: Read from position 48KB using clone2
        Thread thread3 = new Thread(() -> {
            try {
                startLatch.await();
                clone2.seek(pos48KB);
                byte[] buffer = new byte[1024];
                clone2.readBytes(buffer, 0, buffer.length);
                result48KB.set(buffer);
            } catch (Exception e) {
                errors.add(e);
            } finally {
                doneLatch.countDown();
            }
        });

        // Start threads
        thread1.start();
        thread2.start();
        thread3.start();

        // Signal all threads to start reading
        startLatch.countDown();

        // Wait for all threads to complete
        assertTrue("Threads should complete within 5 seconds", doneLatch.await(5, TimeUnit.SECONDS));

        // Check for errors
        assertTrue("No errors should occur: " + errors, errors.isEmpty());

        // Verify each thread read correct data
        byte[] expected0 = new byte[1024];
        byte[] expected32KB = new byte[1024];
        byte[] expected48KB = new byte[1024];

        for (int i = 0; i < 1024; i++) {
            expected0[i] = (byte) ((pos0 + i) % 256);
            expected32KB[i] = (byte) ((pos32KB + i) % 256);
            expected48KB[i] = (byte) ((pos48KB + i) % 256);
        }

        assertArrayEquals("Data at position 0 should be correct", expected0, result0.get());
        assertArrayEquals("Data at position 32KB should be correct", expected32KB, result32KB.get());
        assertArrayEquals("Data at position 48KB should be correct", expected48KB, result48KB.get());

        // Cleanup
        original.close();
        clone1.close();
        clone2.close();
    }

    /**
     * Test 2: Create file with 10000 bytes, seek original to 5000, clone it
     * (should start at 5000), then move original to 1000 and clone to 8000,
     * verify they don't affect each other.
     */
    @Test
    public void testClonePositionIndependence() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        // Write 10000 bytes
        OakBufferedIndexFile writeFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        byte[] data = new byte[10000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        writeFile.writeBytes(data, 0, data.length);
        writeFile.flush();
        writeFile.close();

        // Create original file and seek to 5000
        OakIndexFile original = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);
        original.seek(5000);
        assertEquals("Original should be at position 5000", 5000, original.position());

        // Clone it - clone should start at 5000
        OakIndexFile clone = original.clone();
        assertEquals("Clone should start at position 5000", 5000, clone.position());

        // Move original to 1000 and clone to 8000
        original.seek(1000);
        clone.seek(8000);

        // Verify they are independent
        assertEquals("Original should be at position 1000", 1000, original.position());
        assertEquals("Clone should be at position 8000", 8000, clone.position());

        // Read from both and verify independence
        byte[] originalData = new byte[100];
        byte[] cloneData = new byte[100];

        original.readBytes(originalData, 0, 100);
        clone.readBytes(cloneData, 0, 100);

        // Verify data is from correct positions
        for (int i = 0; i < 100; i++) {
            assertEquals("Original data should be from position 1000+i",
                (byte) ((1000 + i) % 256), originalData[i]);
            assertEquals("Clone data should be from position 8000+i",
                (byte) ((8000 + i) % 256), cloneData[i]);
        }

        // Verify positions after read
        assertEquals("Original should be at position 1100", 1100, original.position());
        assertEquals("Clone should be at position 8100", 8100, clone.position());

        // Cleanup
        original.close();
        clone.close();
    }

    /**
     * Test 3: Create 64KB file with OakBufferedIndexFile, close it, open as
     * OakIndexInput, create slice from offset 10KB length 20KB, verify slice
     * pointer at 0 starts reading from offset 10KB, read 1KB from slice and
     * verify it's data from offset 10KB of original.
     */
    @Test
    public void testIndexInputSlice() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder file = builder.child("testFile");
        BlobFactory blobFactory = BlobFactory.getNodeBuilderBlobFactory(builder);

        // Write 64KB file
        OakBufferedIndexFile writeFile = new OakBufferedIndexFile(
            "test.bin", file, "/test", blobFactory);

        int fileSize = 64 * 1024;
        byte[] data = new byte[fileSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 256);
        }
        writeFile.writeBytes(data, 0, data.length);
        writeFile.flush();
        writeFile.close();

        // Open as OakIndexInput
        OakIndexInput indexInput = new OakIndexInput(
            "test.bin", file, "/test", blobFactory);

        // Create slice from offset 10KB length 20KB
        long sliceOffset = 10 * 1024;
        long sliceLength = 20 * 1024;
        IndexInput slice = indexInput.slice("test-slice", sliceOffset, sliceLength);

        // Verify slice length is 20KB
        assertEquals("Slice length should be 20KB", sliceLength, slice.length());

        // Verify slice pointer is at 0 (relative to slice, not original file)
        assertEquals("Slice pointer should be at 0", 0, slice.getFilePointer());

        // Read 1KB from slice
        byte[] sliceData = new byte[1024];
        slice.readBytes(sliceData, 0, 1024);

        // Verify it's data from offset 10KB of original
        byte[] expectedData = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            expectedData[i] = (byte) ((sliceOffset + i) % 256);
        }
        assertArrayEquals("Slice data should be from offset 10KB of original",
            expectedData, sliceData);

        // Verify slice pointer advanced by 1KB (relative to slice)
        assertEquals("Slice pointer should have advanced by 1KB", 1024, slice.getFilePointer());

        // Cleanup
        slice.close();
        indexInput.close();
    }
}

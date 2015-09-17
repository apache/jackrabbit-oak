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

package org.apache.jackrabbit.oak.spi.blob.split;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.blob.split.DefaultSplitBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class SplitBlobStoreTest {

    private static final int LENGTH = 1024;

    private final Random random = new Random();

    private File repository;

    private BlobStore oldBlobStore;

    private BlobStore newBlobStore;

    private DefaultSplitBlobStore splitBlobStore;

    private List<String> oldBlobIds;

    private List<String> newBlobIds;

    @Before
    public void setup() throws IOException {
        repository = Files.createTempDir();
        oldBlobStore = new FileBlobStore(repository.getPath() + "/old");
        newBlobStore = new FileBlobStore(repository.getPath() + "/new");
        splitBlobStore = new DefaultSplitBlobStore(repository.getPath(), oldBlobStore, newBlobStore);
        oldBlobIds = addBlobs(oldBlobStore);
        newBlobIds = addBlobs(splitBlobStore);
    }

    @After
    public void teardown() throws IOException {
        FileUtils.deleteDirectory(repository);
    }

    @Test
    public void testLength() throws IOException {
        for (String id : oldBlobIds) {
            assertEquals(LENGTH, splitBlobStore.getBlobLength(id));
        }
        for (String id : newBlobIds) {
            assertEquals(LENGTH, newBlobStore.getBlobLength(id));
            assertEquals(LENGTH, splitBlobStore.getBlobLength(id));
        }
    }

    @Test
    public void testIsMigrated() throws IOException {
        for (String id : oldBlobIds) {
            assertFalse(splitBlobStore.isMigrated(id));
        }
        for (String id : newBlobIds) {
            assertTrue(splitBlobStore.isMigrated(id));
        }
    }

    @Test
    public void testGetInputStream() throws IOException {
        for (String id : oldBlobIds) {
            assertStreamEquals(oldBlobStore.getInputStream(id), splitBlobStore.getInputStream(id));
        }
        for (String id : newBlobIds) {
            assertStreamEquals(newBlobStore.getInputStream(id), splitBlobStore.getInputStream(id));
        }
    }

    @Test
    public void testReadByte() throws IOException {
        byte[] expected = new byte[LENGTH];
        byte[] actual = new byte[LENGTH];
        for (String id : oldBlobIds) {
            oldBlobStore.readBlob(id, 0, expected, 0, LENGTH);
            splitBlobStore.readBlob(id, 0, actual, 0, LENGTH);
            assertArrayEquals(expected, actual);
        }
        for (String id : newBlobIds) {
            newBlobStore.readBlob(id, 0, expected, 0, LENGTH);
            splitBlobStore.readBlob(id, 0, actual, 0, LENGTH);
            assertArrayEquals(expected, actual);
        }
    }

    @Test
    public void testReferences() throws IOException {
        for (String id : oldBlobIds) {
            String reference = splitBlobStore.getReference(id);
            assertEquals(id, splitBlobStore.getBlobId(reference));
        }
        for (String id : newBlobIds) {
            String reference = splitBlobStore.getReference(id);
            assertEquals(id, splitBlobStore.getBlobId(reference));
        }
    }

    private List<String> addBlobs(BlobStore blobStore) throws IOException {
        List<String> ids = new ArrayList<String>();
        for (int i = 0; i < 5; i++) {
            byte[] buffer = new byte[LENGTH];
            random.nextBytes(buffer);
            InputStream bis = new ByteArrayInputStream(buffer);
            ids.add(blobStore.writeBlob(bis));
        }
        return ids;
    }

    private static void assertStreamEquals(InputStream expected, InputStream actual) throws IOException {
        while (true) {
            int expectedByte = expected.read();
            int actualByte = actual.read();
            assertEquals(expectedByte, actualByte);
            if (expectedByte == -1) {
                break;
            }
        }
    }

    private static void assertArrayEquals(byte[] expected, byte[] actual) throws IOException {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

}
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BlobIdSetTest {

    private static final String TEST_FILENAME = "test-blob-ids.txt";

    private File tempDir;
    private BlobIdSet blobIdSet;

    @Before
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("blob-id-set-test").toFile();
        blobIdSet = new BlobIdSet(tempDir.getAbsolutePath(), TEST_FILENAME);
    }

    @After
    public void cleanup() {
        File testFile = new File(tempDir, TEST_FILENAME);
        if (testFile.exists()) {
            testFile.delete();
        }
        tempDir.delete();
    }

    @Test
    public void testAddAndContains() throws IOException {
        String blobId = "testblob123";
        Assert.assertFalse("New set should not contain blob ID", blobIdSet.contains(blobId));

        blobIdSet.add(blobId);
        Assert.assertTrue("Set should contain added blob ID", blobIdSet.contains(blobId));
    }

    @Test
    public void testMultipleAddAndContains() throws IOException {
        String[] blobIds = {"blob1", "blob2", "blob3", "blob4", "blob5"};

        // Add all blob IDs
        for (String blobId : blobIds) {
            blobIdSet.add(blobId);
        }

        // Check all blob IDs are present
        for (String blobId : blobIds) {
            Assert.assertTrue("Set should contain: " + blobId, blobIdSet.contains(blobId));
        }

        // Check a non-existent blob ID
        Assert.assertFalse("Set should not contain non-existent blob ID", blobIdSet.contains("nonexistentblob"));
    }

    @Test
    public void testPersistenceAcrossInstances() throws IOException {
        String blobId = "persistenceblob";

        // Add to the first instance
        blobIdSet.add(blobId);

        // Create a new instance pointing to the same file
        BlobIdSet newSet = new BlobIdSet(tempDir.getAbsolutePath(), TEST_FILENAME);

        // Verify the new instance sees the previously added blob ID
        Assert.assertTrue("New instance should see blob ID from file", newSet.contains(blobId));
    }

    @Test
    public void testEmptyFileStore() throws IOException {
        // Create with non-existent file
        File nonExistentDir = Files.createTempDirectory("non-existent").toFile();
        BlobIdSet emptySet = new BlobIdSet(nonExistentDir.getAbsolutePath(), "empty.txt");

        // Should not contain any blob IDs
        Assert.assertFalse(emptySet.contains("anyblob"));

        // Clean up
        nonExistentDir.delete();
    }

    @Test
    public void testLargeNumberOfEntries() throws IOException {
        // Add a moderate number of entries
        int count = 1000;
        for (int i = 0; i < count; i++) {
            blobIdSet.add("blob-" + i);
        }

        // Verify all entries can be found
        for (int i = 0; i < count; i++) {
            Assert.assertTrue(blobIdSet.contains("blob-" + i));
        }

        // Non-existent entries should return false
        for (int i = 0; i < 10; i++) {
            Assert.assertFalse(blobIdSet.contains("nonexistent-blob-" + i));
        }
    }

    @Test
    public void testFileContainsAddedEntries() throws IOException {
        // Add several blob IDs
        String[] blobIds = {"a", "b", "c"};
        for (String id : blobIds) {
            blobIdSet.add(id);
        }

        // Verify the file contains the added blob IDs
        File storeFile = new File(tempDir, TEST_FILENAME);
        Assert.assertTrue("Store file should exist", storeFile.exists());

        List<String> fileContent = Files.readAllLines(storeFile.toPath());
        Assert.assertEquals("File should contain all added blob IDs", blobIds.length, fileContent.size());

        for (int i = 0; i < blobIds.length; i++) {
            Assert.assertEquals("File line should match blob ID", blobIds[i], fileContent.get(i));
        }
    }

    @Test
    public void testBloomFilterPreventsUnnecessaryFileReads() throws IOException {
        // The bloom filter should prevent checking the file for non-existent IDs

        // Add some entries to populate the bloom filter
        for (int i = 0; i < 10; i++) {
            blobIdSet.add("existing-" + i);
        }

        // Using a unique pattern for non-existent IDs to ensure they hash differently
        // than the ones we added
        for (int i = 0; i < 10; i++) {
            Assert.assertFalse(blobIdSet.contains("definitely-not-there-" + i));
        }
    }

    @Test
    public void testCachingBehavior() throws IOException {
        String blobId = "cachedblob";

        // Add the blob ID
        blobIdSet.add(blobId);

        // First check should populate cache
        Assert.assertTrue(blobIdSet.contains(blobId));

        // Even if we delete the file, the cached result should be used
        File storeFile = new File(tempDir, TEST_FILENAME);
        storeFile.delete();

        // Should still return true due to cache
        Assert.assertTrue(blobIdSet.contains(blobId));
    }

    @Test
    public void testContainsFindsExistingEntriesInFile() throws IOException {
        // Create some blob IDs
        String[] blobIds = {"fileblob1", "fileblob2", "fileblob3"};

        // Write blob IDs directly to file (not using BlobIdSet.add())
        File storeFile = new File(tempDir, TEST_FILENAME);
        try (FileWriter writer = new FileWriter(storeFile)) {
            for (String id : blobIds) {
                writer.write(id + "\n");
            }
        }

        // Create a new BlobIdSet instance (which should load from the file)
        BlobIdSet newBlobIdSet = new BlobIdSet(tempDir.getAbsolutePath(), TEST_FILENAME);

        // Verify that contains() finds all the IDs
        for (String id : blobIds) {
            Assert.assertTrue("Should contain blob ID written directly to file: " + id, newBlobIdSet.contains(id));
        }

        // Verify a non-existent ID still returns false
        Assert.assertFalse(newBlobIdSet.contains("notinfile"));
    }

    @Test
    public void testBloomFilterFalsePositiveProbabilityLessThanThreePercent() throws IOException {
        // Load the bloom filter with a significant number of entries (about 5% of configured capacity)
        final int numToAdd = 5000;

        // Add entries to the bloom filter
        for (int i = 0; i < numToAdd; i++) {
            blobIdSet.add("entry-" + i);
        }

        // Test with non-existent entries using carefully crafted IDs
        int numTests = 1000;
        int falsePositives = 0;

        // Use a distinct prefix to ensure test IDs don't conflict with added entries
        for (int i = 0; i < numTests; i++) {
            String nonExistentId = "non-existent-" + i;

            if (blobIdSet.contains(nonExistentId)) {
                falsePositives++;
            }
        }

        final double falsePositiveRate = (double) falsePositives / numTests;

        // Verify the false positive rate is below the configured 3%
        Assert.assertTrue(
                "False positive rate should be less than 3%, was: " + (falsePositiveRate * 100) + "%",
                falsePositiveRate < 0.03
        );
    }
}
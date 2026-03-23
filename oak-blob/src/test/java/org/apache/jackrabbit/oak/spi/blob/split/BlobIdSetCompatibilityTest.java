/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.blob.split;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Compatibility tests for {@link BlobIdSet}.
 * These assertions verify observable lookup semantics without depending on the
 * underlying cache implementation.
 */
public class BlobIdSetCompatibilityTest {

    private static final String TEST_FILENAME = "compat-blob-ids.txt";

    private File tempDir;
    private File storeFile;
    private BlobIdSet blobIdSet;

    @Before
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("blob-id-set-compat").toFile();
        storeFile = new File(tempDir, TEST_FILENAME);
        blobIdSet = new BlobIdSet(tempDir.getAbsolutePath(), TEST_FILENAME);
    }

    @After
    public void tearDown() {
        if (storeFile.exists()) {
            storeFile.delete();
        }
        tempDir.delete();
    }

    @Test
    public void containsReturnsTrueForEntryAddedAfterRestart() throws IOException {
        // Seed the on-disk store first, then rebuild BlobIdSet to show startup
        // rehydrates lookup state from the persisted file.
        try (FileWriter writer = new FileWriter(storeFile)) {
            writer.write("blob-from-store\n");
        }

        BlobIdSet restarted = new BlobIdSet(tempDir.getAbsolutePath(), TEST_FILENAME);

        assertTrue(restarted.contains("blob-from-store"));
    }

    @Test
    public void addMakesEntryVisibleBeforeAndAfterRestart() throws IOException {
        // Add through the public API and verify both the current instance and a
        // restarted one observe the same persisted membership result.
        blobIdSet.add("added-through-api");

        assertTrue(blobIdSet.contains("added-through-api"));

        BlobIdSet restarted = new BlobIdSet(tempDir.getAbsolutePath(), TEST_FILENAME);
        assertTrue(restarted.contains("added-through-api"));
    }
}

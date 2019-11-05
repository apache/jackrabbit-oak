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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlobType;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobContainer;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static org.junit.Assert.*;

public class AzureJournalFileTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    private AzureJournalFile journal;

    @Before
    public void setup() throws BlobStorageException, InvalidKeyException, URISyntaxException {
        container = azurite.getContainer("oak-test");
        journal = new AzureJournalFile(container.getDirectoryReference("journal"), "journal.log", 50);
    }

    @Test
    public void testSplitJournalFiles() throws IOException, URISyntaxException, BlobStorageException {
        assertFalse(journal.exists());

        int index = 0;
        index = writeNLines(index, 10); // 10
        assertTrue(journal.exists());
        assertEquals(1, countJournalBlobs());

        index = writeNLines(index, 20); // 30
        assertEquals(1, countJournalBlobs());

        index = writeNLines(index, 30); // 60
        assertEquals(2, countJournalBlobs());

        index = writeNLines(index, 100); // 160
        assertEquals(4, countJournalBlobs());

        try (JournalFileReader reader = journal.openJournalReader()) {
            for (int i = index - 1; i >= 0; i--) {
                assertEquals("line " + i, reader.readLine());
            }
        }
    }

    private int countJournalBlobs() {
        return (int) container.getDirectoryReference("journal")
                .listBlobsStartingWith("journal.log")
                .stream()
                .filter(blobItem -> blobItem.getProperties().getBlobType() == BlobType.APPEND_BLOB)
                .count();
    }


    private int writeNLines(int index, int n) throws IOException {
        try (JournalFileWriter writer = journal.openJournalWriter()) {
            for (int i = 0; i < n; i++) {
                writer.writeLine("line " + index++);
            }
        }
        return index;
    }

    @Test
    public void testTruncateJournalFile() throws IOException {
        assertFalse(journal.exists());

        JournalFileWriter writer = journal.openJournalWriter();
        for (int i = 0; i < 100; i++) {
            writer.writeLine("line " + i);
        }

        assertTrue(journal.exists());

        writer.truncate();

        assertTrue(journal.exists());
    }
}

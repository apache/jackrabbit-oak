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

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.guava.common.collect.Lists.reverse;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AzureJournalFileTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient readBlobContainerClient;

    private BlobContainerClient writeBlobContainerClient;

    private AzureJournalFile journal;

    @Before
    public void setup() throws BlobStorageException {
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
        writeBlobContainerClient = azurite.getWriteBlobContainerClient("oak-test");
        WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        journal = new AzureJournalFile(readBlobContainerClient, writeBlobContainerClient, "journal.log", writeAccessController, 50);
    }

    @Test
    public void testSplitJournalFiles() throws IOException {
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

        assertJournalEntriesCount(index);
    }

    private int countJournalBlobs() {
        ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
        listBlobsOptions.setPrefix("journal.log");

        List<BlobItem> result  = readBlobContainerClient.listBlobs(listBlobsOptions, null).stream().collect(Collectors.toList());
        return result.size();
    }

    private int writeNLines(int index, int n) throws IOException {
        try (JournalFileWriter writer = journal.openJournalWriter()) {
            for (int i = 0; i < n; i++) {
                writer.writeLine("line " + (index++));
            }
        }
        return index;
    }

    @Test
    public void testTruncateJournalFile() throws IOException {
        assertFalse(journal.exists());

        List<String> lines = buildLines(0, 100);
        try (JournalFileWriter writer = journal.openJournalWriter()) {
            writer.batchWriteLines(lines);
        }

        assertTrue(journal.exists());
        assertJournalEntriesCount(100);

        try (JournalFileWriter writer = journal.openJournalWriter()) {
            writer.truncate();
        }

        assertTrue(journal.exists());
        assertJournalEntriesCount(0);
    }

    @Test
    public void testBatchWriteLines() throws IOException {
        List<String> lines = buildLines(0, 5000);

        try (JournalFileWriter writer = journal.openJournalWriter()) {
            writer.batchWriteLines(lines);
        }

        List<String> entries = readEntriesFromJournal();
        assertEquals(lines, reverse(entries));
    }

    @Test
    public void testEnsureBatchWriteLinesIsFasterThanNaiveImplementation() throws IOException {
        List<String> lines = buildLines(0, 100);

        StopWatch watchNaiveImpl = StopWatch.createStarted();
        try (JournalFileWriter writer = journal.openJournalWriter()) {
            // Emulating previous naive implementation of 'batchWriteLines', which simply delegated to 'writeLine()'
            for (String line : lines) {
                writer.writeLine(line);
            }
        }
        watchNaiveImpl.stop();

        StopWatch watchOptimizedImpl = StopWatch.createStarted();
        try (JournalFileWriter writer = journal.openJournalWriter()) {
            writer.batchWriteLines(lines);
        }
        watchOptimizedImpl.stop();
        long optimizedImplTime = watchOptimizedImpl.getTime();
        long naiveImplTime = watchNaiveImpl.getTime();
        assertTrue("batchWriteLines() should be significantly faster (>10x) than the naive implementation, but took "
            + optimizedImplTime + "ms while naive implementation took " + naiveImplTime + "ms", optimizedImplTime < naiveImplTime / 10);
    }

    @Test
    public void testBatchWriteLines_splitJournalFile() throws Exception {
        assertFalse(journal.exists());

        try (JournalFileWriter writer = journal.openJournalWriter()) {
            writer.batchWriteLines(buildLines(0, 30)); // 30
        }
        assertTrue(journal.exists());
        assertEquals(1, countJournalBlobs());

        try (JournalFileWriter writer = journal.openJournalWriter()) {
            writer.batchWriteLines(buildLines(30, 40)); // 70
        }
        assertEquals(2, countJournalBlobs());

        try (JournalFileWriter writer = journal.openJournalWriter()) {
            writer.batchWriteLines(buildLines(70, 30)); // 100
        }
        assertEquals(2, countJournalBlobs());

        try (JournalFileWriter writer = journal.openJournalWriter()) {
            writer.batchWriteLines(buildLines(100, 1)); // 101
        }
        assertEquals(3, countJournalBlobs());

        try (JournalFileWriter writer = journal.openJournalWriter()) {
            writer.batchWriteLines(buildLines(101, 100)); // 201
        }
        assertEquals(5, countJournalBlobs());

        assertJournalEntriesCount(201);
    }

    private void assertJournalEntriesCount(int index) throws IOException {
        List<String> entries = readEntriesFromJournal();
        assertEquals(buildLines(0, index), reverse(entries));
    }

    @NotNull
    private static List<String> buildLines(int start, int count) {
        return IntStream.range(start, count + start)
            .mapToObj(i -> "line " + i)
            .collect(toList());
    }

    @NotNull
    private List<String> readEntriesFromJournal() throws IOException {
        List<String> result = new ArrayList<>();
        try (JournalFileReader reader = journal.openJournalReader()) {
            String entry;
            while ((entry = reader.readLine()) != null) {
                result.add(entry);
            }
        }
        return result;
    }
}

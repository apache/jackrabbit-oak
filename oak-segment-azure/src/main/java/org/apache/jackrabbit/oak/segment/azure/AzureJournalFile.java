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

import com.azure.storage.blob.AppendBlobClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.Metadata;
import com.azure.storage.blob.models.StorageException;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AzureJournalFile implements JournalFile {

    private static final Logger log = LoggerFactory.getLogger(AzureJournalFile.class);

    private static final int JOURNAL_LINE_LIMIT = Integer.getInteger("org.apache.jackrabbit.oak.segment.azure.journal.lines", 40_000);

    private final CloudBlobDirectory directory;

    private final String journalNamePrefix;

    private final int lineLimit;
    private static final Comparator<AppendBlobClient> BY_NAME_REVERSED = Comparator.<AppendBlobClient, String>comparing(AzureUtilities::getName).reversed();

    AzureJournalFile(CloudBlobDirectory directory, String journalNamePrefix, int lineLimit) {
        this.directory = directory;
        this.journalNamePrefix = journalNamePrefix;
        this.lineLimit = lineLimit;
    }

    public AzureJournalFile(CloudBlobDirectory directory, String journalNamePrefix) {
        this(directory, journalNamePrefix, JOURNAL_LINE_LIMIT);
    }

    @Override
    public JournalFileReader openJournalReader() {
        return new CombinedReader(getJournalBlobs());
    }

    @Override
    public JournalFileWriter openJournalWriter() {
        return new AzureJournalWriter();
    }

    @Override
    public String getName() {
        return journalNamePrefix;
    }

    @Override
    public boolean exists() {
        return !getJournalBlobs().isEmpty();
    }

    private String getJournalFileName(int index) {
        return String.format("%s.%03d", journalNamePrefix, index);
    }

    private List<AppendBlobClient> getJournalBlobs() {
        return directory
                .listBlobsFlat(new ListBlobsOptions().prefix(journalNamePrefix), null)
                .stream()
                .map(blobItem -> directory.getBlobClient(blobItem.name()))
                .map(BlobClient::asAppendBlobClient)
                .sorted(BY_NAME_REVERSED)
                .collect(Collectors.toList());
    }

    private static class AzureJournalReader implements JournalFileReader {

        private final BlobClient blob;

        private ReverseFileReader reader;

        private boolean metadataFetched;

        private boolean firstLineReturned;

        private AzureJournalReader(BlobClient blobClient) {
            this.blob = blobClient;
        }

        @Override
        public String readLine() throws IOException {
            if (reader == null) {
                try {
                    if (!metadataFetched) {
                        metadataFetched = true;
                        Metadata metadata = blob.getProperties().metadata();
                        if (metadata.containsKey(AzureBlobMetadata.METADATA_LAST_ENTRY)) {
                            firstLineReturned = true;
                            return metadata.get(AzureBlobMetadata.METADATA_LAST_ENTRY);
                        }
                    }
                    reader = new ReverseFileReader(blob);
                    if (firstLineReturned) {
                        while ("".equals(reader.readLine()))
                            ; // the first line was already returned, let's fast-forward it
                    }
                } catch (StorageException e) {
                    throw new IOException(e);
                }
            }
            return reader.readLine();
        }

        @Override
        public void close() throws IOException {
        }
    }

    private class AzureJournalWriter implements JournalFileWriter {

        private AppendBlobClient currentBlob;

        private int blockCount;

        public AzureJournalWriter() {
            List<AppendBlobClient> blobs = getJournalBlobs();
            if (blobs.isEmpty()) {
                currentBlob = directory.getBlobClient(getJournalFileName(1)).asAppendBlobClient();
                currentBlob.create();
            } else {
                currentBlob = blobs.get(0);
            }
            Integer bc = currentBlob.getProperties().committedBlockCount();
            blockCount = bc == null ? 0 : bc;
        }

        @Override
        public void truncate() throws IOException {
            for (AppendBlobClient cloudAppendBlob : getJournalBlobs()) {
                cloudAppendBlob.delete();
            }

            createNextFile(0);
        }

        @Override
        public void writeLine(String line) throws IOException {
            if (blockCount >= lineLimit) {
                int parsedSuffix = parseCurrentSuffix();
                createNextFile(parsedSuffix);
            }


            byte[] lineBytes = (line + "\n").getBytes();
            try (ByteArrayInputStream in = new ByteArrayInputStream(lineBytes); BufferedInputStream data = new BufferedInputStream(in)) {
                currentBlob.appendBlock(data, lineBytes.length);
            }
            Metadata metadata = currentBlob.getProperties().metadata();
            metadata.put("lastEntry", line);
            currentBlob.setMetadata(metadata);
            blockCount++;
        }

        private void createNextFile(int suffix) {
            currentBlob = directory.getBlobClient(getJournalFileName(suffix + 1)).asAppendBlobClient();
            currentBlob.create();
            blockCount = 0;
        }

        private int parseCurrentSuffix() {
            String name = AzureUtilities.getName(currentBlob);
            Pattern pattern = Pattern.compile(Pattern.quote(journalNamePrefix) + "\\.(\\d+)");
            Matcher matcher = pattern.matcher(name);
            int parsedSuffix;
            if (matcher.find()) {
                String suffix = matcher.group(1);
                try {
                    parsedSuffix = Integer.parseInt(suffix);
                } catch (NumberFormatException e) {
                    log.warn("Can't parse suffix for journal file {}", name);
                    parsedSuffix = 0;
                }
            } else {
                log.warn("Can't parse journal file name {}", name);
                parsedSuffix = 0;
            }
            return parsedSuffix;
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
    }

    private static class CombinedReader implements JournalFileReader {

        private final Iterator<AzureJournalReader> readers;

        private JournalFileReader currentReader;

        private CombinedReader(List<AppendBlobClient> blobs) {
            readers = blobs.stream().map(AzureJournalReader::new).iterator();
        }

        @Override
        public String readLine() throws IOException {
            String line;
            do {
                if (currentReader == null) {
                    if (!readers.hasNext()) {
                        return null;
                    }
                    currentReader = readers.next();
                }
                do {
                    line = currentReader.readLine();
                } while ("".equals(line));
                if (line == null) {
                    currentReader.close();
                    currentReader = null;
                }
            } while (line == null);
            return line;
        }

        @Override
        public void close() throws IOException {
            while (readers.hasNext()) {
                readers.next().close();
            }
            if (currentReader != null) {
                currentReader.close();
                currentReader = null;
            }
        }
    }
}
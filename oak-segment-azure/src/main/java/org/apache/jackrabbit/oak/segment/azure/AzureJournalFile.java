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

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AzureJournalFile implements JournalFile {

    private static final Logger log = LoggerFactory.getLogger(AzureJournalFile.class);

    private static final int JOURNAL_LINE_LIMIT = Integer.getInteger("org.apache.jackrabbit.oak.segment.azure.journal.lines", 40_000);

    private final CloudBlobDirectory directory;

    private final String journalNamePrefix;

    private final int lineLimit;

    AzureJournalFile(CloudBlobDirectory directory, String journalNamePrefix, int lineLimit) {
        this.directory = directory;
        this.journalNamePrefix = journalNamePrefix;
        this.lineLimit = lineLimit;
    }

    public AzureJournalFile(CloudBlobDirectory directory, String journalNamePrefix) {
        this(directory, journalNamePrefix, JOURNAL_LINE_LIMIT);
    }

    @Override
    public JournalFileReader openJournalReader() throws IOException {
        return new CombinedReader(getJournalBlobs());
    }

    @Override
    public JournalFileWriter openJournalWriter() throws IOException {
        return new AzureJournalWriter();
    }

    @Override
    public String getName() {
        return journalNamePrefix;
    }

    @Override
    public boolean exists() {
        try {
            return !getJournalBlobs().isEmpty();
        } catch (IOException e) {
            log.error("Can't check if the file exists", e);
            return false;
        }
    }

    private String getJournalFileName(int index) {
        return String.format("%s.%03d", journalNamePrefix, index);
    }

    private List<CloudAppendBlob> getJournalBlobs() throws IOException {
        try {
            List<CloudAppendBlob> result = new ArrayList<>();
            for (ListBlobItem b : directory.listBlobs(journalNamePrefix)) {
                if (b instanceof CloudAppendBlob) {
                    result.add((CloudAppendBlob) b);
                } else {
                    log.warn("Invalid blob type: {} {}", b.getUri(), b.getClass());
                }
            }
            result.sort(Comparator.<CloudAppendBlob, String>comparing(AzureUtilities::getName).reversed());
            return result;
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    private static class AzureJournalReader implements JournalFileReader {

        private final CloudBlob blob;

        private ReverseFileReader reader;

        private AzureJournalReader(CloudBlob blob) {
            this.blob = blob;
        }

        @Override
        public String readLine() throws IOException {
            if (reader == null) {
                try {
                    reader = new ReverseFileReader(blob);
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

        private CloudAppendBlob currentBlob;

        private int blockCount;

        public AzureJournalWriter() throws IOException {
            List<CloudAppendBlob> blobs = getJournalBlobs();
            if (blobs.isEmpty()) {
                try {
                    currentBlob = directory.getAppendBlobReference(getJournalFileName(1));
                    currentBlob.createOrReplace();
                } catch (URISyntaxException | StorageException e) {
                    throw new IOException(e);
                }
            } else {
                currentBlob = blobs.get(0);
            }
            Integer bc = currentBlob.getProperties().getAppendBlobCommittedBlockCount();
            blockCount = bc == null ? 0 : bc;
        }

        @Override
        public void truncate() throws IOException {
            try {
                for (CloudAppendBlob cloudAppendBlob : getJournalBlobs()) {
                    cloudAppendBlob.delete();
                }

                createNextFile(0);
            } catch (StorageException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void writeLine(String line) throws IOException {
            if (blockCount >= lineLimit) {
                int parsedSuffix = parseCurrentSuffix();
                createNextFile(parsedSuffix);
            }
            try {
                currentBlob.appendText(line + "\n");
                blockCount++;
            } catch (StorageException e) {
                throw new IOException(e);
            }
        }

        private void createNextFile(int suffix) throws IOException {
            try {
                currentBlob = directory.getAppendBlobReference(getJournalFileName(suffix + 1));
                currentBlob.createOrReplace();
                blockCount = 0;
            } catch (URISyntaxException | StorageException e) {
                throw new IOException(e);
            }
        }

        private int parseCurrentSuffix() {
            String name = AzureUtilities.getName(currentBlob);
            Pattern pattern = Pattern.compile(Pattern.quote(journalNamePrefix) + "\\.(\\d+)" );
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

        private CombinedReader(List<CloudAppendBlob> blobs) {
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
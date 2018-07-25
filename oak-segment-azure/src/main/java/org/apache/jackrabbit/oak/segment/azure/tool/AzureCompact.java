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

package org.apache.jackrabbit.oak.segment.azure.tool;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_ACCOUNT_NAME;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_DIR;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.KEY_STORAGE_URI;
import static org.apache.jackrabbit.oak.segment.azure.util.AzureConfigurationParserUtils.parseAzureConfigurationFromUri;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.azure.AzureJournalFile;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzureUtilities;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.tool.Compact;

import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;

/**
 * Perform an offline compaction of an existing Azure Segment Store.
 */
public class AzureCompact {

    /**
     * Create a builder for the {@link Compact} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link Compact} command.
     */
    public static class Builder {

        private String path;

        private boolean force;

        private long gcLogInterval = 150000;

        private int segmentCacheSize = DEFAULT_SEGMENT_CACHE_MB;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The path (URI) to an existing segment store. This parameter is required.
         *
         * @param path
         *            the path to an existing segment store.
         * @return this builder.
         */
        public Builder withPath(String path) {
            this.path = checkNotNull(path);
            return this;
        }

        /**
         * Whether to fail if run on an older version of the store of force upgrading
         * its format.
         *
         * @param force
         *            upgrade iff {@code true}
         * @return this builder.
         */
        public Builder withForce(boolean force) {
            this.force = force;
            return this;
        }

        /**
         * The size of the segment cache in MB. The default of
         * {@link SegmentCache#DEFAULT_SEGMENT_CACHE_MB} when this method is not
         * invoked.
         *
         * @param segmentCacheSize
         *            cache size in MB
         * @return this builder
         * @throws IllegalArgumentException
         *             if {@code segmentCacheSize} is not a positive integer.
         */
        public Builder withSegmentCacheSize(int segmentCacheSize) {
            checkArgument(segmentCacheSize > 0, "segmentCacheSize must be strictly positive");
            this.segmentCacheSize = segmentCacheSize;
            return this;
        }

        /**
         * The number of nodes after which an update about the compaction process is
         * logged. Set to a negative number to disable progress logging. If not
         * specified, it defaults to 150,000 nodes.
         *
         * @param gcLogInterval
         *            The log interval.
         * @return this builder.
         */
        public Builder withGCLogInterval(long gcLogInterval) {
            this.gcLogInterval = gcLogInterval;
            return this;
        }

        /**
         * Create an executable version of the {@link Compact} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public AzureCompact build() {
            checkNotNull(path);
            return new AzureCompact(this);
        }

    }

    private static String printableStopwatch(Stopwatch s) {
        return String.format("%s (%ds)", s, s.elapsed(TimeUnit.SECONDS));
    }

    private final String path;

    private final int segmentCacheSize;

    private final boolean strictVersionCheck;

    private final long gcLogInterval;

    private AzureCompact(Builder builder) {
        this.path = builder.path;
        this.segmentCacheSize = builder.segmentCacheSize;
        this.strictVersionCheck = !builder.force;
        this.gcLogInterval = builder.gcLogInterval;
    }

    public int run() {
        Stopwatch watch = Stopwatch.createStarted();
        CloudBlobDirectory cloudBlobDirectory = null;
        try {
            cloudBlobDirectory = createCloudBlobDirectory();
        } catch (URISyntaxException | StorageException e1) {
            throw new IllegalArgumentException(
                    "Could not connect to the Azure Storage. Please verify the path provided!");
        }

        SegmentNodeStorePersistence persistence = new AzurePersistence(cloudBlobDirectory);
        SegmentArchiveManager archiveManager = null;
        try {
            archiveManager = persistence.createArchiveManager(false, new IOMonitorAdapter(),
                    new FileStoreMonitorAdapter());
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Could not access the Azure Storage. Please verify the path provided!");
        }

        System.out.printf("Compacting %s\n", path);
        System.out.printf("    before\n");
        List<String> beforeArchives = Collections.emptyList();
        try {
            beforeArchives = archiveManager.listArchives();
        } catch (IOException e) {
            System.err.println(e);
        }

        printArchives(System.out, beforeArchives);
        System.out.printf("    -> compacting\n");

        try (FileStore store = newFileStore(persistence)) {
            if (!store.compactFull()) {
                System.out.printf("Compaction cancelled after %s.\n", printableStopwatch(watch));
                return 1;
            }
            System.out.printf("    -> cleaning up\n");
            store.cleanup();
            JournalFile journal = new AzureJournalFile(cloudBlobDirectory, "journal.log");
            String head;
            try (JournalReader journalReader = new JournalReader(journal)) {
                head = String.format("%s root %s\n", journalReader.next().getRevision(), System.currentTimeMillis());
            }

            try (JournalFileWriter journalWriter = journal.openJournalWriter()) {
                System.out.printf("    -> writing new %s: %s\n", journal.getName(), head);
                journalWriter.truncate();
                journalWriter.writeLine(head);
            }
        } catch (Exception e) {
            watch.stop();
            e.printStackTrace(System.err);
            System.out.printf("Compaction failed after %s.\n", printableStopwatch(watch));
            return 1;
        }

        watch.stop();
        System.out.printf("    after\n");
        List<String> afterArchives = Collections.emptyList();
        try {
            afterArchives = archiveManager.listArchives();
        } catch (IOException e) {
            System.err.println(e);
        }
        printArchives(System.out, afterArchives);
        System.out.printf("Compaction succeeded in %s.\n", printableStopwatch(watch));
        return 0;
    }

    private static void printArchives(PrintStream s, List<String> archives) {
        for (String a : archives) {
            s.printf("        %s\n", a);
        }
    }

    private FileStore newFileStore(SegmentNodeStorePersistence persistence)
            throws IOException, InvalidFileStoreVersionException, URISyntaxException, StorageException {
        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(Files.createTempDir())
                .withCustomPersistence(persistence).withMemoryMapping(false).withStrictVersionCheck(strictVersionCheck)
                .withSegmentCacheSize(segmentCacheSize)
                .withGCOptions(defaultGCOptions().setOffline().setGCLogInterval(gcLogInterval));

        return builder.build();
    }

    private CloudBlobDirectory createCloudBlobDirectory() throws URISyntaxException, StorageException {
        Map<String, String> config = parseAzureConfigurationFromUri(path);

        String accountName = config.get(KEY_ACCOUNT_NAME);
        String key = System.getenv("AZURE_SECRET_KEY");
        StorageCredentials credentials = new StorageCredentialsAccountAndKey(accountName, key);

        String uri = config.get(KEY_STORAGE_URI);
        String dir = config.get(KEY_DIR);

        return AzureUtilities.cloudBlobDirectoryFrom(credentials, uri, dir);
    }
}

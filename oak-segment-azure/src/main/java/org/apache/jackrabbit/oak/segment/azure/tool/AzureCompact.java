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
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.createArchiveManager;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newFileStore;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newSegmentNodeStorePersistence;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.printableStopwatch;

import com.google.common.base.Stopwatch;
import com.google.common.io.Files;

import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.SegmentStoreType;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.tool.Compact;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

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
        SegmentNodeStorePersistence persistence = newSegmentNodeStorePersistence(SegmentStoreType.AZURE, path);
        SegmentArchiveManager archiveManager = createArchiveManager(persistence);

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

        try (FileStore store = newFileStore(persistence, Files.createTempDir(), strictVersionCheck, segmentCacheSize,
                gcLogInterval)) {
            if (!store.compactFull()) {
                System.out.printf("Compaction cancelled after %s.\n", printableStopwatch(watch));
                return 1;
            }
            System.out.printf("    -> cleaning up\n");
            store.cleanup();
            JournalFile journal = persistence.getJournalFile();
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
}

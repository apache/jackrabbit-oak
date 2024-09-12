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

import com.google.common.io.Files;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.azure.v8.AzurePersistenceV8;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureStorageCredentialManagerV8;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.tool.Check;
import org.apache.jackrabbit.oak.segment.tool.check.CheckHelper;

import java.io.File;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

public class AzureCheck {
    /**
     * Create a builder for the {@link Check} command.
     *
     * @return an instance of {@link Check.Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link Check} command.
     */
    public static class Builder {

        private String path;

        private String journal;

        private long debugInterval = Long.MAX_VALUE;

        private boolean checkBinaries;

        private boolean checkHead;

        private Integer revisionsCount;

        private Set<String> checkpoints;

        private Set<String> filterPaths;

        private boolean ioStatistics;

        private Check.RepositoryStatistics repoStatistics;

        private PrintWriter outWriter;

        private PrintWriter errWriter;

        private boolean failFast;

        private String persistentCachePath;

        private Integer persistentCacheSizeGb;

        private CloudBlobDirectory cloudBlobDirectory;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The path to an existing segment store. This parameter is required.
         *
         * @param path the path to an existing segment store.
         * @return this builder.
         */
        public Builder withPath(String path) {
            this.path = requireNonNull(path);
            return this;
        }

        /**
         * The path to the journal of the segment store. This parameter is
         * optional. If not provided, the journal in the default location is
         * used.
         *
         * @param journal the path to the journal of the segment store.
         * @return this builder.
         */
        public Builder withJournal(String journal) {
            this.journal = requireNonNull(journal);
            return this;
        }

        /**
         * Number of seconds between successive debug print statements. This
         * parameter is not required and defaults to an arbitrary large number.
         *
         * @param debugInterval number of seconds between successive debug print
         *                      statements. It must be positive.
         * @return this builder.
         */
        public Builder withDebugInterval(long debugInterval) {
            checkArgument(debugInterval >= 0);
            this.debugInterval = debugInterval;
            return this;
        }

        /**
         * Instruct the command to scan the full content of binary properties.
         * This parameter is not required and defaults to {@code false}.
         *
         * @param checkBinaries {@code true} if binary properties should be
         *                      scanned, {@code false} otherwise.
         * @return this builder.
         */
        public Builder withCheckBinaries(boolean checkBinaries) {
            this.checkBinaries = checkBinaries;
            return this;
        }

        /**
         * Instruct the command to check head state.
         * This parameter is not required and defaults to {@code true}.
         * @param checkHead if {@code true}, will check the head state.
         * @return this builder.
         */
        public Builder withCheckHead(boolean checkHead) {
            this.checkHead = checkHead;
            return this;
        }

        /**
         * Instruct the command to check only the last {@code revisionsCount} revisions.
         * This parameter is not required and defaults to {@code 1}.
         * @param revisionsCount number of revisions to check.
         * @return this builder.
         */
        public Builder withRevisionsCount(Integer revisionsCount){
            this.revisionsCount = revisionsCount;
            return this;
        }

        /**
         * Instruct the command to check specified checkpoints.
         * This parameter is not required and defaults to "/checkpoints",
         * i.e. will check all checkpoints when not explicitly overridden.
         *
         * @param checkpoints   checkpoints to be checked
         * @return this builder.
         */
        public Builder withCheckpoints(Set<String> checkpoints) {
            this.checkpoints = checkpoints;
            return this;
        }

        /**
         * Content paths to be checked. This parameter is not required and
         * defaults to "/".
         *
         * @param filterPaths
         *            paths to be checked
         * @return this builder.
         */
        public Builder withFilterPaths(Set<String> filterPaths) {
            this.filterPaths = filterPaths;
            return this;
        }

        /**
         * Instruct the command to print statistics about I/O operations
         * performed during the check. This parameter is not required and
         * defaults to {@code false}.
         *
         * @param ioStatistics {@code true} if I/O statistics should be
         *                     provided, {@code false} otherwise.
         * @return this builder.
         */
        public Builder withIOStatistics(boolean ioStatistics) {
            this.ioStatistics = ioStatistics;
            return this;
        }

        /**
         * Attach a repository statistics instance to collect info on nodes
         * and properties checked on head.
         *
         * @param repoStatistics instance to collect statistics
         * @return this builder.
         */
        public Builder withRepositoryStatistics(Check.RepositoryStatistics repoStatistics) {
            this.repoStatistics = repoStatistics;
            return this;
        }

        /**
         * The text output stream writer used to print normal output.
         * @param outWriter the output writer.
         * @return this builder.
         */
        public Builder withOutWriter(PrintWriter outWriter) {
            this.outWriter = outWriter;

            return this;
        }

        /**
         * The text error stream writer used to print erroneous output.
         * @param errWriter the error writer.
         * @return this builder.
         */
        public Builder withErrWriter(PrintWriter errWriter) {
            this.errWriter = errWriter;

            return this;
        }

        /**
         * Instruct the command to fail fast if the first path/revision checked
         * is inconsistent. This parameter is not required and defaults to
         * {@code false}.
         *
         * @param failFast {@code true} if the command should fail fast,
         *                 {@code false} otherwise.
         * @return this builder.
         */
        public Builder withFailFast(boolean failFast) {
            this.failFast = failFast;
            return this;
        }

        /**
         * The path where segments in the persistent cache will be stored.
         *
         * @param persistentCachePath
         *             the path to the persistent cache.
         * @return this builder
         */
        public Builder withPersistentCachePath(String persistentCachePath) {
            this.persistentCachePath = requireNonNull(persistentCachePath);
            return this;
        }

        /**
         * The maximum size in GB of the persistent disk cache.
         *
         * @param persistentCacheSizeGb
         *             the maximum size of the persistent cache.
         * @return this builder
         */
        public Builder withPersistentCacheSizeGb(Integer persistentCacheSizeGb) {
            this.persistentCacheSizeGb = requireNonNull(persistentCacheSizeGb);
            return this;
        }

        /**
         * The Azure blob directory to connect to.
         * @param cloudBlobDirectory
         *          the Azure blob directory.
         * @return this builder
         */
        public Builder withCloudBlobDirectory(CloudBlobDirectory cloudBlobDirectory) {
            this.cloudBlobDirectory = requireNonNull(cloudBlobDirectory);
            return this;
        }

        /**
         * Create an executable version of the {@link Check} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public AzureCheck build() {
            if (cloudBlobDirectory == null) {
                requireNonNull(path);
            }
            return new AzureCheck(this);
        }

    }

    private static class StatisticsIOMonitor extends IOMonitorAdapter {

        AtomicLong ops = new AtomicLong(0);

        AtomicLong bytes = new AtomicLong(0);

        AtomicLong time = new AtomicLong(0);

        @Override
        public void afterSegmentRead(File file, long msb, long lsb, int length, long elapsed) {
            ops.incrementAndGet();
            bytes.addAndGet(length);
            time.addAndGet(elapsed);
        }

    }

    private final String path;

    private final String journal;

    private final long debugInterval;

    private final boolean checkBinaries;

    private final boolean checkHead;

    private final Integer revisionsCount;

    private final Set<String> requestedCheckpoints;

    private final Set<String> filterPaths;

    private final boolean ioStatistics;

    private Check.RepositoryStatistics repoStatistics;

    private final PrintWriter out;

    private final PrintWriter err;

    private final boolean failFast;

    private final String persistentCachePath;

    private final Integer persistentCacheSizeGb;

    private final CloudBlobDirectory cloudBlobDirectory;
    private final AzureStorageCredentialManagerV8 azureStorageCredentialManagerV8;

    private AzureCheck(Builder builder) {
        this.path = builder.path;
        this.debugInterval = builder.debugInterval;
        this.checkHead = builder.checkHead;
        this.checkBinaries = builder.checkBinaries;
        this.requestedCheckpoints = builder.checkpoints;
        this.filterPaths = builder.filterPaths;
        this.ioStatistics = builder.ioStatistics;
        this.repoStatistics = builder.repoStatistics;
        this.out = builder.outWriter;
        this.err = builder.errWriter;
        this.journal = builder.journal;
        this.revisionsCount = revisionsToCheckCount(builder.revisionsCount);
        this.failFast = builder.failFast;
        this.persistentCachePath = builder.persistentCachePath;
        this.persistentCacheSizeGb = builder.persistentCacheSizeGb;
        this.cloudBlobDirectory = builder.cloudBlobDirectory;
        this.azureStorageCredentialManagerV8 = new AzureStorageCredentialManagerV8();
    }

    private static Integer revisionsToCheckCount(Integer revisionsCount) {
        return revisionsCount != null ? revisionsCount : Integer.MAX_VALUE;
    }

    public int run() {
        StatisticsIOMonitor ioMonitor = new StatisticsIOMonitor();
        SegmentNodeStorePersistence persistence;

        if (cloudBlobDirectory != null) {
            persistence = new AzurePersistenceV8(cloudBlobDirectory);
        } else {
            persistence = ToolUtils.newSegmentNodeStorePersistence(ToolUtils.SegmentStoreType.AZURE, path, azureStorageCredentialManagerV8);
        }

        if (persistentCachePath != null) {
            persistence = ToolUtils.decorateWithCache(persistence, persistentCachePath, persistentCacheSizeGb);
        }

        FileStoreBuilder builder = fileStoreBuilder(Files.createTempDir()).withCustomPersistence(persistence);

        if (ioStatistics) {
            builder.withIOMonitor(ioMonitor);
        }

        CheckHelper checkHelper = CheckHelper.builder()
                .withCheckBinaries(checkBinaries)
                .withCheckpoints(requestedCheckpoints)
                .withCheckHead(checkHead)
                .withDebugInterval(debugInterval)
                .withFailFast(failFast)
                .withFilterPaths(filterPaths)
                .withRevisionsCount(revisionsCount)
                .withErrWriter(err)
                .withOutWriter(out)
                .build();

        try (
                ReadOnlyFileStore store = builder.buildReadOnly();
                JournalReader journal = new JournalReader(persistence.getJournalFile())
        ) {
            int result = checkHelper.run(store, journal);

            if (ioStatistics) {
                print("[I/O] Segment read: Number of operations: {0}", ioMonitor.ops.get());
                print("[I/O] Segment read: Total size: {0} ({1} bytes)", humanReadableByteCount(ioMonitor.bytes.get()), ioMonitor.bytes.get());
                print("[I/O] Segment read: Total time: {0} ns", ioMonitor.time.get());
            }

            if (repoStatistics != null) {
                repoStatistics.setHeadNodeCount(checkHelper.getHeadNodeCount());
                repoStatistics.setHeadPropertyCount(checkHelper.getHeadPropertyCount());
            }

            return result;
        } catch (Exception e) {
            e.printStackTrace(err);
            return 1;
        } finally {
            azureStorageCredentialManagerV8.close();
        }
    }

    private void print(String format, Object... arguments) {
        out.println(MessageFormat.format(format, arguments));
    }
}

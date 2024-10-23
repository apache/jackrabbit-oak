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

package org.apache.jackrabbit.oak.segment.tool;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.LocalJournalFile;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.tool.check.CheckHelper;

/**
 * Perform a consistency check on an existing segment store.
 */
public class Check {

    /**
     * Create a builder for the {@link Check} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link Check} command.
     */
    public static class Builder {

        private File path;

        private boolean mmap;

        private File journal;

        private long debugInterval = Long.MAX_VALUE;

        private boolean checkBinaries;

        private boolean checkHead;

        private Integer revisionsCount;

        private Set<String> checkpoints;

        private Set<String> filterPaths;

        private boolean ioStatistics;

        private RepositoryStatistics repoStatistics;

        private PrintWriter outWriter;

        private PrintWriter errWriter;

        private boolean failFast;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The path to an existing segment store. This parameter is required.
         *
         * @param path the path to an existing segment store.
         * @return this builder.
         */
        public Builder withPath(File path) {
            this.path = requireNonNull(path);
            return this;
        }

        /**
         * Whether to use memory mapped access or file access.
         *
         * @param mmap {@code true} for memory mapped access, {@code false} for
         *             file access {@code null} to determine the access mode
         *             from the system architecture: memory mapped on 64 bit
         *             systems, file access on  32 bit systems.
         * @return this builder.
         */
        public Builder withMmap(boolean mmap) {
            this.mmap = mmap;
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
        public Builder withJournal(File journal) {
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
        public Builder withRepositoryStatistics(RepositoryStatistics repoStatistics) {
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
         * Create an executable version of the {@link Check} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Check build() {
            requireNonNull(path);
            return new Check(this);
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

    public static class RepositoryStatistics {
        int headNodeCount;
        int headPropertyCount;

        public void setHeadPropertyCount(int headPropertyCount) {
            this.headPropertyCount = headPropertyCount;
        }

        public void setHeadNodeCount(int headNodeCount) {
            this.headNodeCount = headNodeCount;
        }

        public int getHeadNodeCount() {
            return headNodeCount;
        }

        public int getHeadPropertyCount() {
            return headPropertyCount;
        }
    }

    private final File path;

    private final boolean mmap;

    private final File journal;

    private final long debugInterval;

    private final boolean checkBinaries;

    private final boolean checkHead;

    private final Integer revisionsCount;

    private final Set<String> requestedCheckpoints;

    private final Set<String> filterPaths;

    private final boolean ioStatistics;

    private RepositoryStatistics repoStatistics;

    private final PrintWriter out;

    private final PrintWriter err;

    private final boolean failFast;

    private Check(Builder builder) {
        this.path = builder.path;
        this.mmap = builder.mmap;
        this.debugInterval = builder.debugInterval;
        this.checkHead = builder.checkHead;
        this.checkBinaries = builder.checkBinaries;
        this.requestedCheckpoints = builder.checkpoints;
        this.filterPaths = builder.filterPaths;
        this.ioStatistics = builder.ioStatistics;
        this.repoStatistics = builder.repoStatistics;
        this.out = builder.outWriter;
        this.err = builder.errWriter;
        this.journal = journalPath(builder.path, builder.journal);
        this.revisionsCount = revisionsToCheckCount(builder.revisionsCount);
        this.failFast = builder.failFast;
    }

    private static File journalPath(File segmentStore, File journal) {
        if (journal == null) {
            return new File(segmentStore, "journal.log");
        }
        return journal;
    }

    private static Integer revisionsToCheckCount(Integer revisionsCount) {
        return revisionsCount != null ? revisionsCount : Integer.MAX_VALUE;
    }

    public int run() {
        StatisticsIOMonitor ioMonitor = new StatisticsIOMonitor();

        FileStoreBuilder builder = fileStoreBuilder(path)
            .withMemoryMapping(mmap)
            .withCustomPersistence(new TarPersistence(this.path, this.journal));

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
            JournalReader journal = new JournalReader(new LocalJournalFile(this.journal))
        ) {
            int result = checkHelper.run(store, journal);

            if (ioStatistics) {
                print("[I/O] Segment read: Number of operations: {0}", ioMonitor.ops.get());
                print("[I/O] Segment read: Total size: {0} ({1} bytes)", humanReadableByteCount(ioMonitor.bytes.get()), ioMonitor.bytes.get());
                print("[I/O] Segment read: Total time: {0} ns", ioMonitor.time.get());
            }

            if (repoStatistics != null) {
                repoStatistics.headNodeCount = checkHelper.getHeadNodeCount();
                repoStatistics.headPropertyCount = checkHelper.getHeadPropertyCount();
            }

            return result;
        } catch (Exception e) {
            e.printStackTrace(err);
            return 1;
        }
    }

    private void print(String format, Object... arguments) {
        out.println(MessageFormat.format(format, arguments));
    }
}

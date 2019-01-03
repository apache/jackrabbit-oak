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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.text.DateFormat.getDateTimeInstance;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.LocalJournalFile;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.file.tooling.ConsistencyChecker;
import org.apache.jackrabbit.oak.segment.file.tooling.ConsistencyChecker.ConsistencyCheckResult;
import org.apache.jackrabbit.oak.segment.file.tooling.ConsistencyChecker.Revision;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;

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

        private File journal;

        private long debugInterval = Long.MAX_VALUE;

        private boolean checkBinaries;
        
        private boolean checkHead;
        
        private Set<String> checkpoints;
        
        private Set<String> filterPaths;

        private boolean ioStatistics;
        
        private PrintWriter outWriter;
        
        private PrintWriter errWriter;

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
            this.path = checkNotNull(path);
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
            this.journal = checkNotNull(journal);
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
         * Create an executable version of the {@link Check} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Check build() {
            checkNotNull(path);
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

    private final File path;

    private final File journal;

    private final long debugInterval;

    private final boolean checkBinaries;
    
    private final boolean checkHead;

    private final Set<String> requestedCheckpoints;
    
    private final Set<String> filterPaths;

    private final boolean ioStatistics;

    private final PrintWriter out;

    private final PrintWriter err;

    private int nodeCount;

    private int propertyCount;

    private long lastDebugEvent;

    private Check(Builder builder) {
        this.path = builder.path;
        this.debugInterval = builder.debugInterval;
        this.checkHead = builder.checkHead;
        this.checkBinaries = builder.checkBinaries;
        this.requestedCheckpoints = builder.checkpoints;
        this.filterPaths = builder.filterPaths;
        this.ioStatistics = builder.ioStatistics;
        this.out = builder.outWriter;
        this.err = builder.errWriter;
        this.journal = journalPath(builder.path, builder.journal);
    }

    private static File journalPath(File segmentStore, File journal) {
        if (journal == null) {
            return new File(segmentStore, "journal.log");
        }
        return journal;
    }

    public int run() {
        StatisticsIOMonitor ioMonitor = new StatisticsIOMonitor();

        FileStoreBuilder builder = fileStoreBuilder(path)
            .withCustomPersistence(new TarPersistence(this.path, this.journal));

        if (ioStatistics) {
            builder.withIOMonitor(ioMonitor);
        }

        try (
            ReadOnlyFileStore store = builder.buildReadOnly();
            JournalReader journal = new JournalReader(new LocalJournalFile(this.journal))
        ) {
            run(store, journal);

            if (ioStatistics) {
                print("[I/O] Segment read: Number of operations: {0}", ioMonitor.ops.get());
                print("[I/O] Segment read: Total size: {0} ({1} bytes)", humanReadableByteCount(ioMonitor.bytes.get()), ioMonitor.bytes.get());
                print("[I/O] Segment read: Total time: {0} ns", ioMonitor.time.get());
            }

            return 0;
        } catch (Exception e) {
            e.printStackTrace(err);
            return 1;
        }
    }

    private void run(ReadOnlyFileStore store, JournalReader journal) {
        Set<String> checkpoints = requestedCheckpoints;

        if (requestedCheckpoints.contains("all")) {
            checkpoints = Sets.newLinkedHashSet(SegmentNodeStoreBuilders.builder(store).build().checkpoints());
        }

        ConsistencyCheckResult result = newConsistencyChecker().checkConsistency(
            store,
            journal,
            checkHead,
            checkpoints,
            filterPaths,
            checkBinaries
        );

        print("\nSearched through {0} revisions and {1} checkpoints", result.getCheckedRevisionsCount(), checkpoints.size());

        if (hasAnyRevision(result)) {
            if (checkHead) {
                print("\nHead");
                for (Entry<String, Revision> e : result.getHeadRevisions().entrySet()) {
                    printRevision(0, e.getKey(), e.getValue());
                }
            }
            if (checkpoints.size() > 0) {
                print("\nCheckpoints");
                for (String checkpoint : result.getCheckpointRevisions().keySet()) {
                    print("- {0}", checkpoint);
                    for (Entry<String, Revision> e : result.getCheckpointRevisions().get(checkpoint).entrySet()) {
                        printRevision(2, e.getKey(), e.getValue());
                    }

                }
            }
            print("\nOverall");
            printOverallRevision(result.getOverallRevision());
        } else {
            print("No good revision found");
        }
    }

    private ConsistencyChecker newConsistencyChecker() {
        return new ConsistencyChecker() {

            @Override
            protected void onCheckRevision(String revision) {
                print("\nChecking revision {0}", revision);
            }

            @Override
            protected void onCheckHead() {
                print("\nChecking head\n");
            }

            @Override
            protected void onCheckChekpoints() {
                print("\nChecking checkpoints");
            }

            @Override
            protected void onCheckCheckpoint(String checkpoint) {
                print("\nChecking checkpoint {0}", checkpoint);
            }

            @Override
            protected void onCheckpointNotFoundInRevision(String checkpoint) {
                printError("Checkpoint {0} not found in this revision!", checkpoint);
            }

            @Override
            protected void onCheckRevisionError(String revision, Exception e) {
                printError("Skipping invalid record id {0}: {1}", revision, e);
            }

            @Override
            protected void onConsistentPath(String path) {
                print("Path {0} is consistent", path);
            }

            @Override
            protected void onPathNotFound(String path) {
                printError("Path {0} not found", path);
            }

            @Override
            protected void onCheckTree(String path) {
                nodeCount = 0;
                propertyCount = 0;
                print("Checking {0}", path);
            }

            @Override
            protected void onCheckTreeEnd() {
                print("Checked {0} nodes and {1} properties", nodeCount, propertyCount);
            }

            @Override
            protected void onCheckNode(String path) {
                debug("Traversing {0}", path);
                nodeCount++;
            }

            @Override
            protected void onCheckProperty() {
                propertyCount++;
            }

            @Override
            protected void onCheckPropertyEnd(String path, PropertyState property) {
                debug("Checked {0}/{1}", path, property);
            }

            @Override
            protected void onCheckNodeError(String path, Exception e) {
                printError("Error while traversing {0}: {1}", path, e);
            }

            @Override
            protected void onCheckTreeError(String path, Exception e) {
                printError("Error while traversing {0}: {1}", path, e.getMessage());
            }

        };
    }

    private void print(String format, Object... arguments) {
        out.println(MessageFormat.format(format, arguments));
    }

    private void printError(String format, Object... args) {
        err.println(MessageFormat.format(format, args));
    }

    private void debug(String format, Object... arg) {
        if (debug()) {
            print(format, arg);
        }
    }

    private boolean debug() {
        // Avoid calling System.currentTimeMillis(), which is slow on some systems.
        if (debugInterval == Long.MAX_VALUE) {
            return false;
        }

        if (debugInterval == 0) {
            return true;
        }

        long t = System.currentTimeMillis();
        if ((t - this.lastDebugEvent) / 1000 > debugInterval) {
            this.lastDebugEvent = t;
            return true;
        } else {
            return false;
        }
    }

    private static boolean hasAnyRevision(ConsistencyCheckResult result) {
        return hasAnyHeadRevision(result) || hasAnyCheckpointRevision(result);
    }

    private static boolean hasAnyHeadRevision(ConsistencyCheckResult result) {
        return result.getHeadRevisions()
            .values()
            .stream()
            .anyMatch(Objects::nonNull);
    }

    private static boolean hasAnyCheckpointRevision(ConsistencyCheckResult result) {
        return result.getCheckpointRevisions()
            .values()
            .stream()
            .flatMap(m -> m.values().stream())
            .anyMatch(Objects::nonNull);
    }

    private void printRevision(int indent, String path, Revision revision) {
        Optional<Revision> r = Optional.ofNullable(revision);
        print(
            "{0}Latest good revision for path {1} is {2} from {3}",
            Strings.repeat(" ", indent),
            path,
            r.map(Revision::getRevision).orElse("none"),
            r.map(Revision::getTimestamp).map(Check::timestampToString).orElse("unknown time")
        );
    }

    private void printOverallRevision(Revision revision) {
        Optional<Revision> r = Optional.ofNullable(revision);
        print(
            "Latest good revision for paths and checkpoints checked is {0} from {1}",
            r.map(Revision::getRevision).orElse("none"),
            r.map(Revision::getTimestamp).map(Check::timestampToString).orElse("unknown time")
        );
    }

    private static String timestampToString(long timestamp) {
        return getDateTimeInstance().format(new Date(timestamp));
    }

}

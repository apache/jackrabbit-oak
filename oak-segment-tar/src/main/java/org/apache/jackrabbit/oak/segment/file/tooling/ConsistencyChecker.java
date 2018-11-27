/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment.file.tooling;

import static java.text.DateFormat.getDateTimeInstance;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
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
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.LocalJournalFile;
import org.apache.jackrabbit.oak.segment.file.tooling.ConsistencyCheckerTemplate.ConsistencyCheckResult;
import org.apache.jackrabbit.oak.segment.file.tooling.ConsistencyCheckerTemplate.Revision;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;

/**
 * Utility for checking the files of a
 * {@link FileStore} for inconsistency and
 * reporting that latest consistent revision.
 */
public class ConsistencyChecker implements Closeable {

    private static class StatisticsIOMonitor extends IOMonitorAdapter {

        private final AtomicLong ioOperations = new AtomicLong(0);

        private final AtomicLong readBytes = new AtomicLong(0);

        private final AtomicLong readTime = new AtomicLong(0);

        @Override
        public void afterSegmentRead(File file, long msb, long lsb, int length, long elapsed) {
            ioOperations.incrementAndGet();
            readBytes.addAndGet(length);
            readTime.addAndGet(elapsed);
        }

    }

    private final StatisticsIOMonitor statisticsIOMonitor = new StatisticsIOMonitor();

    private final ReadOnlyFileStore store;

    private final long debugInterval;
    
    private final PrintWriter outWriter;
    
    private final PrintWriter errWriter;

    private int nodeCount;
    
    private int propertyCount;

    /**
     * Create a new consistency checker instance
     *
     * @param directory     directory containing the tar files
     * @param debugInterval number of seconds between printing progress
     *                      information to the console during the full traversal
     *                      phase.
     * @param ioStatistics  if {@code true} prints I/O statistics gathered while
     *                      consistency check was performed
     * @param outWriter     text output stream writer
     * @param errWriter     text error stream writer
     * @throws IOException
     */
    public ConsistencyChecker(File directory, long debugInterval, boolean ioStatistics, PrintWriter outWriter,
        PrintWriter errWriter) throws IOException, InvalidFileStoreVersionException {
        FileStoreBuilder builder = fileStoreBuilder(directory);
        if (ioStatistics) {
            builder.withIOMonitor(statisticsIOMonitor);
        }
        this.store = builder.buildReadOnly();
        this.debugInterval = debugInterval;
        this.outWriter = outWriter;
        this.errWriter = errWriter;
    }

    /**
     * Run a full traversal consistency check.
     *
     * @param directory       directory containing the tar files
     * @param journalFileName name of the journal file containing the revision
     *                        history
     * @param checkBinaries   if {@code true} full content of binary properties
     *                        will be scanned
     * @param checkHead       if {@code true} will check the head
     * @param checkpointsToCheck     collection of checkpoints to be checked
     * @param filterPaths     collection of repository paths to be checked
     * @param ioStatistics    if {@code true} prints I/O statistics gathered
     *                        while consistency check was performed
     */
    public void checkConsistency(
        File directory,
        String journalFileName,
        boolean checkBinaries,
        boolean checkHead,
        Set<String> checkpointsToCheck,
        Set<String> filterPaths,
        boolean ioStatistics
    ) throws IOException {
        try (JournalReader journal = new JournalReader(new LocalJournalFile(directory, journalFileName))) {
            ConsistencyCheckerTemplate template = new ConsistencyCheckerTemplate() {

                @Override
                void onCheckRevision(String revision) {
                    print("\nChecking revision {0}", revision);
                }

                @Override
                void onCheckHead() {
                    print("\nChecking head\n");
                }

                @Override
                void onCheckChekpoints() {
                    print("\nChecking checkpoints");
                }

                @Override
                void onCheckCheckpoint(String checkpoint) {
                    print("\nChecking checkpoint {0}", checkpoint);
                }

                @Override
                void onCheckpointNotFoundInRevision(String checkpoint) {
                    printError("Checkpoint {0} not found in this revision!", checkpoint);
                }

                @Override
                void onCheckRevisionError(String revision, Exception e) {
                    printError("Skipping invalid record id {0}: {1}", revision, e);
                }

                @Override
                void onConsistentPath(String path) {
                    print("Path {0} is consistent", path);
                }

                @Override
                void onPathNotFound(String path) {
                    printError("Path {0} not found", path);
                }

                @Override
                void onCheckTree(String path) {
                    nodeCount = 0;
                    propertyCount = 0;
                    print("Checking {0}", path);
                }

                @Override
                void onCheckTreeEnd() {
                    print("Checked {0} nodes and {1} properties", nodeCount, propertyCount);
                }

                @Override
                void onCheckNode(String path) {
                    debug("Traversing {0}", path);
                    nodeCount++;
                }

                @Override
                void onCheckProperty() {
                    propertyCount++;
                }

                @Override
                void onCheckPropertyEnd(String path, PropertyState property) {
                    debug("Checked {0}/{1}", path, property);
                }

                @Override
                void onCheckNodeError(String path, Exception e) {
                    printError("Error while traversing {0}: {1}", path, e);
                }

                @Override
                void onCheckTreeError(String path, Exception e) {
                    printError("Error while traversing {0}: {1}", path, e.getMessage());
                }

            };

            Set<String> checkpoints = checkpointsToCheck;
            if (checkpointsToCheck.contains("all")) {
                checkpoints = Sets.newLinkedHashSet(SegmentNodeStoreBuilders.builder(store).build().checkpoints());
            }

            ConsistencyCheckResult result = template.checkConsistency(
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

            if (ioStatistics) {
                print(
                    "[I/O] Segment read: Number of operations: {0}",
                    statisticsIOMonitor.ioOperations.get()
                );
                print(
                    "[I/O] Segment read: Total size: {0} ({1} bytes)",
                    humanReadableByteCount(statisticsIOMonitor.readBytes.get()),
                    statisticsIOMonitor.readBytes.get()
                );
                print(
                    "[I/O] Segment read: Total time: {0} ns",
                    statisticsIOMonitor.readTime.get()
                );
            }
        }
    }

    private void printRevision(int indent, String path, Revision revision) {
        Optional<Revision> r = Optional.ofNullable(revision);
        print(
            "{0}Latest good revision for path {1} is {2} from {3}",
            Strings.repeat(" ", indent),
            path,
            r.map(Revision::getRevision).orElse("none"),
            r.map(Revision::getTimestamp).map(ConsistencyChecker::timestampToString).orElse("unknown time")
        );
    }

    private void printOverallRevision(Revision revision) {
        Optional<Revision> r = Optional.ofNullable(revision);
        print(
            "Latest good revision for paths and checkpoints checked is {0} from {1}",
            r.map(Revision::getRevision).orElse("none"),
            r.map(Revision::getTimestamp).map(ConsistencyChecker::timestampToString).orElse("unknown time")
        );
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

    @Override
    public void close() {
        store.close();
    }

    private void print(String format, Object... arguments) {
        outWriter.println(MessageFormat.format(format, arguments));
    }

    private void printError(String format, Object... args) {
        errWriter.println(MessageFormat.format(format, args));
    }

    private long ts;

    private void debug(String format, Object... arg) {
        if (debug()) {
            print(format, arg);
        }
    }

    private boolean debug() {
        // Avoid calling System.currentTimeMillis(), which is slow on some systems.
        if (debugInterval == Long.MAX_VALUE) {
            return false;
        } else if (debugInterval == 0) {
            return true;
        }

        long ts = System.currentTimeMillis();
        if ((ts - this.ts) / 1000 > debugInterval) {
            this.ts = ts;
            return true;
        } else {
            return false;
        }
    }

    private static String timestampToString(long timestamp) {
        return getDateTimeInstance().format(new Date(timestamp));
    }

}

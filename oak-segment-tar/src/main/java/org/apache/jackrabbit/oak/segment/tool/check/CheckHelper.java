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
package org.apache.jackrabbit.oak.segment.tool.check;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tooling.ConsistencyChecker;
import org.apache.jackrabbit.oak.segment.tool.Check;

import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.Set;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.Date;

import static java.text.DateFormat.getDateTimeInstance;
import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

public class CheckHelper {
    /**
     * Create a builder for the {@link CheckHelper}.
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

        private long debugInterval = Long.MAX_VALUE;

        private boolean checkBinaries;

        private boolean checkHead;

        private Integer revisionsCount;

        private Set<String> checkpoints;

        private Set<String> filterPaths;

        private PrintWriter outWriter;

        private PrintWriter errWriter;

        private boolean failFast;

        private Builder() {
            // Prevent external instantiation.
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

        public Builder withFailFast(boolean failFast) {
            this.failFast = failFast;
            return this;
        }

        /**
         * Create an executable version of the {@link CheckHelper} command.
         *
         * @return an instance of {@link CheckHelper}.
         */
        public CheckHelper build() {
            return new CheckHelper(this);
        }

    }

    private final boolean checkBinaries;

    private final boolean checkHead;

    private final Integer revisionsCount;

    private final Set<String> requestedCheckpoints;

    private final Set<String> filterPaths;

    private final boolean failFast;

    private final long debugInterval;

    private final PrintWriter out;

    private final PrintWriter err;

    private int currentNodeCount;

    private int currentPropertyCount;

    private int headNodeCount;

    private int headPropertyCount;

    private long lastDebugEvent;

    private CheckHelper(Builder builder) {
        this.debugInterval = builder.debugInterval;
        this.checkHead = builder.checkHead;
        this.checkBinaries = builder.checkBinaries;
        this.requestedCheckpoints = builder.checkpoints;
        this.filterPaths = builder.filterPaths;
        this.out = builder.outWriter;
        this.err = builder.errWriter;
        this.failFast = builder.failFast;
        this.revisionsCount = builder.revisionsCount;
    }

    public int run(ReadOnlyFileStore store, JournalReader journal) {
        Set<String> checkpoints = requestedCheckpoints;

        if (requestedCheckpoints.contains("all")) {
            checkpoints = CollectionUtils.toLinkedSet(SegmentNodeStoreBuilders.builder(store).build().checkpoints());
        }

        ConsistencyChecker.ConsistencyCheckResult result = newConsistencyChecker().checkConsistency(
                store,
                journal,
                checkHead,
                checkpoints,
                filterPaths,
                checkBinaries,
                revisionsCount,
                failFast
        );

        print("\nSearched through {0} revisions and {1} checkpoints", result.getCheckedRevisionsCount(), checkpoints.size());

        if (isGoodRevisionFound(result)) {
            if (checkHead) {
                print("\nHead");
                for (Map.Entry<String, ConsistencyChecker.Revision> e : result.getHeadRevisions().entrySet()) {
                    printRevision(0, e.getKey(), e.getValue());
                }
            }
            if (!checkpoints.isEmpty()) {
                print("\nCheckpoints");
                for (String checkpoint : result.getCheckpointRevisions().keySet()) {
                    print("- {0}", checkpoint);
                    for (Map.Entry<String, ConsistencyChecker.Revision> e : result.getCheckpointRevisions().get(checkpoint).entrySet()) {
                        printRevision(2, e.getKey(), e.getValue());
                    }

                }
            }
            print("\nOverall");
            printOverallRevision(result.getOverallRevision());
            return 0;
        } else {
            print("No good revision found");
            return 1;
        }
    }

    public int getHeadNodeCount() {
        return headNodeCount;
    }

    public int getHeadPropertyCount() {
        return headPropertyCount;
    }

    private boolean isGoodRevisionFound(ConsistencyChecker.ConsistencyCheckResult result) {
        return failFast ? hasAllRevision(result) : hasAnyRevision(result);
    }

    private ConsistencyChecker newConsistencyChecker() {
        return new ConsistencyChecker() {

            @Override
            protected void onCheckRevision(String revision) {
                print("\nChecking revision {0}", revision);
            }

            @Override
            protected void onCheckHead() {
                headNodeCount = 0;
                headPropertyCount = 0;
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
            protected void onCheckTree(String path, boolean head) {
                currentNodeCount = 0;
                currentPropertyCount = 0;
                print("Checking {0}", path);
            }

            @Override
            protected void onCheckTreeEnd(boolean head) {
                if (head) {
                    headNodeCount += currentNodeCount;
                    headPropertyCount += currentPropertyCount;
                }

                print("Checked {0} nodes and {1} properties", currentNodeCount, currentPropertyCount);
            }

            @Override
            protected void onCheckNode(String path) {
                debug("Traversing {0}", path);
                currentNodeCount++;
            }

            @Override
            protected void onCheckProperty() {
                currentPropertyCount++;
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

    private static boolean hasAnyRevision(ConsistencyChecker.ConsistencyCheckResult result) {
        return hasAnyHeadRevision(result) || hasAnyCheckpointRevision(result);
    }

    private static boolean hasAllRevision(ConsistencyChecker.ConsistencyCheckResult result) {
        return hasAnyHeadRevision(result) && hasAllCheckpointRevision(result);
    }

    private static boolean hasAnyHeadRevision(ConsistencyChecker.ConsistencyCheckResult result) {
        return result.getHeadRevisions()
                .values()
                .stream()
                .anyMatch(Objects::nonNull);
    }

    private static boolean hasAnyCheckpointRevision(ConsistencyChecker.ConsistencyCheckResult result) {
        return result.getCheckpointRevisions()
                .values()
                .stream()
                .flatMap(m -> m.values().stream())
                .anyMatch(Objects::nonNull);
    }

    private static boolean hasAllCheckpointRevision(ConsistencyChecker.ConsistencyCheckResult result) {
        return result.getCheckpointRevisions()
                .values()
                .stream()
                .flatMap(m -> m.values().stream())
                .allMatch(Objects::nonNull);
    }

    private void printRevision(int indent, String path, ConsistencyChecker.Revision revision) {
        Optional<ConsistencyChecker.Revision> r = Optional.ofNullable(revision);
        print(
                "{0}Latest good revision for path {1} is {2} from {3}",
                " ".repeat(indent),
                path,
                r.map(ConsistencyChecker.Revision::getRevision).orElse("none"),
                r.map(ConsistencyChecker.Revision::getTimestamp).map(CheckHelper::timestampToString).orElse("unknown time")
        );
    }

    private void printOverallRevision(ConsistencyChecker.Revision revision) {
        Optional<ConsistencyChecker.Revision> r = Optional.ofNullable(revision);
        print(
                "Latest good revision for paths and checkpoints checked is {0} from {1}",
                r.map(ConsistencyChecker.Revision::getRevision).orElse("none"),
                r.map(ConsistencyChecker.Revision::getTimestamp).map(CheckHelper::timestampToString).orElse("unknown time")
        );
    }

    private static String timestampToString(long timestamp) {
        return getDateTimeInstance().format(new Date(timestamp));
    }

    private void printError(String format, Object... args) {
        err.println(MessageFormat.format(format, args));
    }

    private void print(String format, Object... arguments) {
        out.println(MessageFormat.format(format, arguments));
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
}

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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.Lists.reverse;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.segment.RecordId.fromString;

import java.io.File;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentIdProvider;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Shows the differences between two head states.
 */
public class Diff {

    /**
     * Create a builder for the {@link Diff} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link Diff} command.
     */
    public static class Builder {

        private String path;

        private ReadOnlyFileStore store;

        private String interval;

        private boolean incremental;

        private File out;

        private String filter;

        private boolean ignoreMissingSegments;

        private Revisions.RevisionsProcessor processor;

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
         * The read-only filestore at path
         * @param store the read-only filestore to diff
         * @return this builder.
         */
        public Builder withReadOnlyFileStore(ReadOnlyFileStore store) {
            this.store = requireNonNull(store);
            return this;
        }
        /**
         * The two node records to diff specified as a record ID interval. This
         * parameter is required.
         * <p>
         * The interval is specified as two record IDs separated by two full
         * stops ({@code ..}). In example, {@code 333dc24d-438f-4cca-8b21-3ebf67c05856:12345..46116fda-7a72-4dbc-af88-a09322a7753a:67890}.
         * Instead of using a full record ID, it is possible to use the special
         * placeholder {@code head}. This placeholder is translated to the
         * record ID of the most recent head state.
         *
         * @param interval an interval between two node record IDs.
         * @return this builder.
         */
        public Builder withInterval(String interval) {
            this.interval = requireNonNull(interval);
            return this;
        }

        /**
         * Set whether or not to perform an incremental diff of the specified
         * interval. An incremental diff shows every change between the two
         * records at every revision available to the segment store. This
         * parameter is not mandatory and defaults to {@code false}.
         *
         * @param incremental {@code true} to perform an incremental diff,
         *                    {@code false} otherwise.
         * @return this builder.
         */
        public Builder withIncremental(boolean incremental) {
            this.incremental = incremental;
            return this;
        }

        /**
         * The file where the output of this command is stored. this parameter
         * is mandatory.
         *
         * @param file the output file.
         * @return this builder.
         */
        public Builder withOutput(File file) {
            this.out = requireNonNull(file);
            return this;
        }

        /**
         * The path to a subtree. If specified, this parameter allows to
         * restrict the diff to the specified subtree. This parameter is not
         * mandatory and defaults to the entire tree.
         *
         * @param filter a path used as as filter for the resulting diff.
         * @return this builder.
         */
        public Builder withFilter(String filter) {
            this.filter = requireNonNull(filter);
            return this;
        }

        /**
         * Whether to ignore exceptions caused by missing segments in the
         * segment store. This parameter is not mandatory and defaults to {@code
         * false}.
         *
         * @param ignoreMissingSegments {@code true} to ignore exceptions caused
         *                              by missing segments, {@code false}
         *                              otherwise.
         * @return this builder.
         */
        public Builder withIgnoreMissingSegments(boolean ignoreMissingSegments) {
            this.ignoreMissingSegments = ignoreMissingSegments;
            return this;
        }

        /**
         * The revisions processor used to list revisions
         * @param processor a revisions processor
         * @return this builder.
         */
        public Builder withRevisionsProcessor(Revisions.RevisionsProcessor processor) {
            this.processor = processor;
            return this;
        }

        /**
         * Create an executable version of the {@link Diff} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Diff build() {
            requireNonNull(path);
            requireNonNull(interval);
            requireNonNull(out);
            requireNonNull(filter);
            return new Diff(this);
        }

    }

    private final String path;

    private final String interval;

    private final boolean incremental;

    private final File out;

    private final String filter;

    private final boolean ignoreMissingSegments;

    private final ReadOnlyFileStore store;

    private final Revisions.RevisionsProcessor processor;

    private Diff(Builder builder) {
        this.path = builder.path;
        this.store = builder.store;
        this.interval = builder.interval;
        this.incremental = builder.incremental;
        this.out = builder.out;
        this.filter = builder.filter;
        this.ignoreMissingSegments = builder.ignoreMissingSegments;
        this.processor = builder.processor;
    }

    public int run() {
        try {
            diff();
            return 0;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            return 1;
        }
    }

    private void diff() throws Exception {
        System.out.println("Store " + path);
        System.out.println("Writing diff to " + out);

        String[] tokens = interval.trim().split("\\.\\.");

        if (tokens.length != 2) {
            System.out.println("Error parsing revision interval '" + interval + "'.");
            return;
        }

        SegmentIdProvider idProvider = store.getSegmentIdProvider();
        RecordId idL;

        try {
            if (tokens[0].equalsIgnoreCase("head")) {
                idL = store.getRevisions().getHead();
            } else {
                idL = fromString(idProvider, tokens[0]);
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid left endpoint for interval " + interval);
            return;
        }

        RecordId idR;

        try {
            if (tokens[1].equalsIgnoreCase("head")) {
                idR = store.getRevisions().getHead();
            } else {
                idR = fromString(idProvider, tokens[1]);
            }
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid left endpoint for interval " + interval);
            return;
        }

        long start = System.currentTimeMillis();

        try (PrintWriter pw = new PrintWriter(out)) {
            if (incremental) {
                List<String> revs = processor.process(path);
                System.out.println("Generating diff between " + idL + " and " + idR + " incrementally. Found " + revs.size() + " revisions.");

                int s = revs.indexOf(idL.toString10());
                int e = revs.indexOf(idR.toString10());
                if (s == -1 || e == -1) {
                    System.out.println("Unable to match input revisions with FileStore.");
                    return;
                }
                List<String> revDiffs = revs.subList(Math.min(s, e), Math.max(s, e) + 1);
                if (s > e) {
                    // reverse list
                    revDiffs = reverse(revDiffs);
                }
                if (revDiffs.size() < 2) {
                    System.out.println("Nothing to diff: " + revDiffs);
                    return;
                }
                Iterator<String> revDiffsIt = revDiffs.iterator();
                RecordId idLt = fromString(idProvider, revDiffsIt.next());
                while (revDiffsIt.hasNext()) {
                    RecordId idRt = fromString(idProvider, revDiffsIt.next());
                    boolean good = diff(store, idLt, idRt, pw);
                    idLt = idRt;
                    if (!good && !ignoreMissingSegments) {
                        break;
                    }
                }
            } else {
                System.out.println("Generating diff between " + idL + " and " + idR);
                diff(store, idL, idR, pw);
            }
        }

        long dur = System.currentTimeMillis() - start;
        System.out.println("Finished in " + dur + " ms.");
    }

    private boolean diff(ReadOnlyFileStore store, RecordId idL, RecordId idR, PrintWriter pw) {
        pw.println("rev " + idL + ".." + idR);
        try {
            NodeState before = store.getReader().readNode(idL).getChildNode("root");
            NodeState after = store.getReader().readNode(idR).getChildNode("root");
            for (String name : elements(filter)) {
                before = before.getChildNode(name);
                after = after.getChildNode(name);
            }
            after.compareAgainstBaseState(before, new PrintingDiff(pw, filter));
            return true;
        } catch (SegmentNotFoundException ex) {
            System.out.println(ex.getMessage());
            pw.println("#SNFE " + ex.getSegmentId());
            return false;
        }
    }

}

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

import java.io.File;
import java.util.Iterator;

import org.apache.jackrabbit.oak.segment.file.tooling.RevisionHistory;
import org.apache.jackrabbit.oak.segment.file.tooling.RevisionHistory.HistoryElement;

/**
 * Prints the revision history of an existing segment store. Optionally, it can
 * narrow to the output to the history of a single node.
 */
public class History implements Runnable {

    /**
     * Create a builder for the {@link History} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link History} command.
     */
    public static class Builder {

        private File path;

        private File journal;

        private String node;

        private int depth;

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
         * The path to the journal. This parameter is required.
         *
         * @param journal the path to the journal.
         * @return this builder.
         */
        public Builder withJournal(File journal) {
            this.journal = checkNotNull(journal);
            return this;
        }

        /**
         * A path to a node. If specified, the history will be restricted to the
         * subtree pointed to by this node. This parameter is not mandatory and
         * defaults to the entire tree.
         *
         * @param node a path to a node.
         * @return this builder.
         */
        public Builder withNode(String node) {
            this.node = checkNotNull(node);
            return this;
        }

        /**
         * Maximum depth of the history. If specified, this command will print
         * information about the history of every node at or below the provided
         * depth. This parameter is not mandatory and defaults to zero.
         *
         * @param depth the depth of the subtree.
         * @return this builder.
         */
        public Builder withDepth(int depth) {
            checkArgument(depth >= 0);
            this.depth = depth;
            return this;
        }

        /**
         * Create an executable version of the {@link History} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Runnable build() {
            checkNotNull(path);
            checkNotNull(journal);
            checkNotNull(node);
            return new History(this);
        }

    }

    private final File path;

    private final File journal;

    private final String node;

    private final int depth;

    private History(Builder builder) {
        this.path = builder.path;
        this.journal = builder.journal;
        this.node = builder.node;
        this.depth = builder.depth;
    }

    @Override
    public void run() {
        try {
            run(new RevisionHistory(path).getHistory(journal, node));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void run(Iterator<HistoryElement> history) {
        while (history.hasNext()) {
            System.out.println(history.next().toString(depth));
        }
    }

}

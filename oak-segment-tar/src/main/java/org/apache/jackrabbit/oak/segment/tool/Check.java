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

import org.apache.jackrabbit.oak.segment.file.tooling.ConsistencyChecker;

/**
 * Perform a consistency check on an existing segment store.
 */
public class Check implements Runnable {

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

        private String journal;

        private boolean fullTraversal;

        private long debugInterval = Long.MAX_VALUE;

        private long minimumBinaryLength;

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
         * required.
         *
         * @param journal the path to the journal of the segment store.
         * @return this builder.
         */
        public Builder withJournal(String journal) {
            this.journal = checkNotNull(journal);
            return this;
        }

        /**
         * Should a full traversal of the segment store be performed? This
         * parameter is not required and defaults to {@code false}.
         *
         * @param fullTraversal {@code true} if a full traversal should be
         *                      performed, {@code false} otherwise.
         * @return this builder.
         */
        public Builder withFullTraversal(boolean fullTraversal) {
            this.fullTraversal = fullTraversal;
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
         * Minimum amount of bytes to read from binary properties. This
         * parameter is not required and defaults to zero.
         *
         * @param minimumBinaryLength minimum amount of bytes to read from
         *                            binary properties. If this parameter is
         *                            set to {@code -1}, every binary property
         *                            is read in its entirety.
         * @return
         */
        public Builder withMinimumBinaryLength(long minimumBinaryLength) {
            this.minimumBinaryLength = minimumBinaryLength;
            return this;
        }

        /**
         * Create an executable version of the {@link Check} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Runnable build() {
            checkNotNull(path);
            checkNotNull(journal);
            return new Check(this);
        }

    }

    private final File path;

    private final String journal;

    private final boolean fullTraversal;

    private final long debugInterval;

    private final long minimumBinaryLength;

    private Check(Builder builder) {
        this.path = builder.path;
        this.journal = builder.journal;
        this.fullTraversal = builder.fullTraversal;
        this.debugInterval = builder.debugInterval;
        this.minimumBinaryLength = builder.minimumBinaryLength;
    }

    @Override
    public void run() {
        try {
            ConsistencyChecker.checkConsistency(path, journal, fullTraversal, debugInterval, minimumBinaryLength);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

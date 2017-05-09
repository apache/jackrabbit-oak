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
import java.io.PrintWriter;
import java.util.Set;

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

        private long debugInterval = Long.MAX_VALUE;

        private boolean checkBinaries;
        
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
        public Runnable build() {
            checkNotNull(path);
            checkNotNull(journal);
            return new Check(this);
        }

    }

    private final File path;

    private final String journal;

    private final long debugInterval;

    private final boolean checkBinaries;
    
    private final Set<String> filterPaths;

    private final boolean ioStatistics;
    
    private final PrintWriter outWriter;
    
    private final PrintWriter errWriter;

    private Check(Builder builder) {
        this.path = builder.path;
        this.journal = builder.journal;
        this.debugInterval = builder.debugInterval;
        this.checkBinaries = builder.checkBinaries;
        this.filterPaths = builder.filterPaths;
        this.ioStatistics = builder.ioStatistics;
        this.outWriter = builder.outWriter;
        this.errWriter = builder.errWriter;
    }

    @Override
    public void run() {
        try {
            ConsistencyChecker.checkConsistency(path, journal, debugInterval, checkBinaries, filterPaths, ioStatistics, outWriter, errWriter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

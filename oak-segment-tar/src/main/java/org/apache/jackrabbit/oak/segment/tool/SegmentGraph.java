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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.SegmentGraph.writeSegmentGraph;
import static org.apache.jackrabbit.oak.segment.tool.Utils.openReadOnlyFileStore;

import java.io.File;
import java.io.OutputStream;
import java.util.Date;

import org.apache.jackrabbit.oak.segment.file.FileStore.ReadOnlyStore;

/**
 * Generates a segment collection generation graph. The graph is written in <a
 * href="https://gephi.github.io/users/supported-graph-formats/gdf-format/">the
 * Guess GDF format</a>, which is easily imported into <a
 * href="https://gephi.github.io/">Gephi</a>.
 */
public class SegmentGraph implements Runnable {

    /**
     * Create a builder for the {@link SegmentGraph} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link SegmentGraph} command.
     */
    public static class Builder {

        private File path;

        private Date epoch;

        private String filter;

        private OutputStream out;

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
         * Filter out segments that were created before the specified epoch.
         * This parameter is not mandatory and by default is not specified, thus
         * including every segment in the output.
         *
         * @param epoch the minimum creation time of the reported segments.
         * @return this builder.
         */
        public Builder withEpoch(Date epoch) {
            this.epoch = checkNotNull(epoch);
            return this;
        }

        /**
         * A regular expression that can be used to select a specific subset of
         * the segments.
         *
         * @param filter a regular expression.
         * @return this builder.
         */
        public Builder withFilter(String filter) {
            this.filter = checkNotNull(filter);
            return this;
        }

        /**
         * The destination of the output of the command. This parameter is
         * mandatory.
         *
         * @param out the destination of the output of the command.
         * @return this builder.
         */
        public Builder withOutput(OutputStream out) {
            this.out = checkNotNull(out);
            return this;
        }

        public Runnable build() {
            checkNotNull(path);
            checkNotNull(out);
            return new SegmentGraph(this);
        }

    }

    private final File path;

    private final OutputStream out;

    private final Date epoch;

    private final String filter;

    private SegmentGraph(Builder builder) {
        this.path = builder.path;
        this.out = builder.out;
        this.epoch = builder.epoch;
        this.filter = builder.filter;
    }

    @Override
    public void run() {
        try (ReadOnlyStore store = openReadOnlyFileStore(path)) {
            writeSegmentGraph(store, out, epoch, filter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

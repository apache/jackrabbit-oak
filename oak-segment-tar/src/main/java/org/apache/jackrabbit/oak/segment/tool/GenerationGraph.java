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
import static org.apache.jackrabbit.oak.segment.SegmentGraph.writeGCGraph;
import static org.apache.jackrabbit.oak.segment.tool.Utils.openReadOnlyFileStore;

import java.io.File;
import java.io.OutputStream;

import org.apache.jackrabbit.oak.segment.file.FileStore.ReadOnlyStore;

/**
 * Generates a garbage collection generation graph. The graph is written in <a
 * href="https://gephi.github.io/users/supported-graph-formats/gdf-format/">the
 * Guess GDF format</a>, which is easily imported into <a
 * href="https://gephi.github.io/">Gephi</a>.
 */
public class GenerationGraph implements Runnable {

    /**
     * Create a builder for the {@link GenerationGraph} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link GenerationGraph} command.
     */
    public static class Builder {

        private File path;

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

        /**
         * Create an executable version of the {@link GenerationGraph} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Runnable build() {
            checkNotNull(path);
            checkNotNull(out);
            return new GenerationGraph(this);
        }

    }

    private final File path;

    private final OutputStream out;

    private GenerationGraph(Builder builder) {
        this.path = builder.path;
        this.out = builder.out;
    }

    @Override
    public void run() {
        try (ReadOnlyStore store = openReadOnlyFileStore(path)) {
            writeGCGraph(store, out);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

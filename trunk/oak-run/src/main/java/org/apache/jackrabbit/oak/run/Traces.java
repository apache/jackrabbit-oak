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
 *
 */

package org.apache.jackrabbit.oak.run;

import static java.nio.file.Files.readAllLines;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.tool.iotrace.IOTracer.newIOTracer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.tool.iotrace.BreadthFirstTrace;
import org.apache.jackrabbit.oak.segment.tool.iotrace.DepthFirstTrace;
import org.apache.jackrabbit.oak.segment.tool.iotrace.IOTracer;
import org.apache.jackrabbit.oak.segment.tool.iotrace.RandomAccessTrace;
import org.apache.jackrabbit.oak.segment.tool.iotrace.Trace;
import org.jetbrains.annotations.NotNull;

/**
 * Utility class for running the various {@link Trace} implementations.
 */
public enum Traces {

    /**
     * Utility for running a {@link DepthFirstTrace depth first trace}.
     */
    DEPTH {
        @NotNull
        private String path = DEFAULT_PATH;

        private int depth = DEFAULT_DEPTH;

        @Override
        void setPath(@NotNull String path) {
            this.path = path;
        }

        @Override
        void setDepth(int depth) {
            this.depth = depth;
        }

        @Override
        String getDescription() {
            return String.format("depth first traversal of %d levels starting from %s", depth, path);
        }

        @Override
        void collectIOTrace(
                @NotNull File segmentStore,
                boolean mmap,
                int segmentCacheSize,
                @NotNull File output)
        throws IOException {
            try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output, true)))) {
                Function<IOMonitor, FileStore> factory = ioMonitor ->
                        newFileStore(fileStoreBuilder(segmentStore)
                                             .withMemoryMapping(mmap)
                                             .withSegmentCacheSize(segmentCacheSize)
                                             .withIOMonitor(ioMonitor));

                IOTracer ioTracer = newIOTracer(factory, out, DepthFirstTrace.CONTEXT_SPEC);
                ioTracer.collectTrace(
                        new DepthFirstTrace(depth, path, ioTracer::setContext));
            }
        }

    },

    /**
     * Utility for running a {@link BreadthFirstTrace breadth first trace}.
     */
    BREADTH {
        @NotNull
        private String path = DEFAULT_PATH;

        private int depth = DEFAULT_DEPTH;

        @Override
        void setPath(@NotNull String path) {
            this.path = path;
        }

        @Override
        void setDepth(int depth) {
            this.depth = depth;
        }

        @Override
        String getDescription() {
            return String.format("breadth first traversal of %d levels starting from %s", depth, path);
        }

        @Override
        void collectIOTrace(
                @NotNull File segmentStore,
                boolean mmap,
                int segmentCacheSize,
                @NotNull File output)
        throws IOException {
            try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output, true)))) {
                Function<IOMonitor, FileStore> factory = ioMonitor ->
                        newFileStore(fileStoreBuilder(segmentStore)
                                             .withMemoryMapping(mmap)
                                             .withSegmentCacheSize(segmentCacheSize)
                                             .withIOMonitor(ioMonitor));

                IOTracer ioTracer = newIOTracer(factory, out, BreadthFirstTrace.CONTEXT_SPEC);
                ioTracer.collectTrace(
                        new BreadthFirstTrace(depth, path, ioTracer::setContext));
            }
        }
    },

    /**
     * Utility for running a {@link RandomAccessTrace random access trace}.
     */
    RANDOM {
        @NotNull
        private List<String> paths = emptyList();
        private long seed = DEFAULT_SEED;
        private int count = DEFAULT_COUNT;

        @Override
        void setPaths(@NotNull File paths) throws IOException {
            this.paths = readAllLines(Paths.get(paths.toURI()));
        }

        @Override
        void setSeed(long seed) {
            this.seed = seed;
        }

        @Override
        void setCount(int count) {
            this.count = count;
        }

        @Override
        String getDescription() {
            return String.format("random access of %d paths (seed=%d)", count, seed);
        }

        @Override
        void collectIOTrace(@NotNull File segmentStore, boolean mmap, int segmentCacheSize,
                                   @NotNull File output)
        throws IOException {
            try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output, true)))) {
                Function<IOMonitor, FileStore> factory = ioMonitor ->
                        newFileStore(fileStoreBuilder(segmentStore)
                                             .withMemoryMapping(mmap)
                                             .withSegmentCacheSize(segmentCacheSize)
                                             .withIOMonitor(ioMonitor));

                IOTracer ioTracer = newIOTracer(factory, out, RandomAccessTrace.CONTEXT_SPEC);
                ioTracer.collectTrace(
                        new RandomAccessTrace(paths, seed, count, ioTracer::setContext));
            }
        }
    };

    /**
     * Default path to the node from where to start tracing.
     */
    @NotNull
    static final String DEFAULT_PATH = "/root";

    /**
     * Default maximal depth
     */
    static final int DEFAULT_DEPTH = 5;

    /**
     * Default seed for picking paths randomly
     */
    static final long DEFAULT_SEED = 0;

    /**
     * Default number of paths to trace
     */
    static final int DEFAULT_COUNT = 1000;

    /**
     * Set the path to the node from where to start tracing.
     * @param path
     * @see BreadthFirstTrace
     * @see DepthFirstTrace
     */
    void setPath(@NotNull String path) {}

    /**
     * Set the maximal depth
     * @param depth
     * @see BreadthFirstTrace
     * @see DepthFirstTrace
     */
    void setDepth(int depth) {}

    /**
     * Set the paths to trace
     * @param paths  file containing a paths per line
     * @throws IOException
     * @see RandomAccessTrace
     */
    void setPaths(@NotNull File paths) throws IOException {}

    /**
     * Set the seed used to randomly pick paths
     * @param seed
     */
    void setSeed(long seed) {}

    /**
     * Set the number of paths to trace
     * @param count
     */
    void setCount(int count) {}

    /**
     * @return a human readable description of the trace
     */
    abstract String getDescription();

    /**
     * Collect an IO trace on a segment store.
     * @param segmentStore       path to the segment store
     * @param mmap               whether to enable memory mapping
     * @param segmentCacheSize   size of the segment cache in MB
     * @param output             output file where the trace is written to
     * @throws IOException
     */
    abstract void collectIOTrace(
            @NotNull File segmentStore,
            boolean mmap,
            int segmentCacheSize,
            @NotNull File output)
    throws IOException, InvalidFileStoreVersionException;

    @NotNull
    private static FileStore newFileStore(FileStoreBuilder fileStoreBuilder) {
        try {
            return fileStoreBuilder.build();
        } catch (InvalidFileStoreVersionException | IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

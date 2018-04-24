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

package org.apache.jackrabbit.oak.segment.tool.iotrace;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.tool.iotrace.IOTracer.newIOTracer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;

/**
 * Utility class for running the various {@link Trace} implementations.
 */
public enum Traces {

    /**
     * Utility for running a {@link DepthFirstTrace depth first trace}.
     */
    DEPTH {

        /**
         * Collect an IO trace on a segment store using a {@link DepthFirstTrace depth first trace}.
         * @param segmentStore       path to the segment store
         * @param mmap               whether to enable memory mapping
         * @param segmentCacheSize   size of the segment cache in MB
         * @param path               path to the root node (starting from the super root) from where
         *                           to start traversing
         * @param depth              maximal number of levels to traverse
         * @param output             output file where the trace is written to
         * @throws IOException
         */
        @Override
        public void collectIOTrace(
                @Nonnull File segmentStore,
                boolean mmap,
                int segmentCacheSize,
                @Nonnull String path,
                int depth,
                @Nonnull File output)
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

        /**
         * Collect an IO trace on a segment store using a {@link BreadthFirstTrace breadth first trace}.
         * @param segmentStore       path to the segment store
         * @param mmap               whether to enable memory mapping
         * @param segmentCacheSize   size of the segment cache in MB
         * @param path               path to the root node (starting from the super root) from where
         *                           to start traversing
         * @param depth              maximal number of levels to traverse
         * @param output             output file where the trace is written to
         * @throws IOException
         */
        @Override
        public void collectIOTrace(
                @Nonnull File segmentStore,
                boolean mmap,
                int segmentCacheSize,
                @Nonnull String path,
                int depth,
                @Nonnull File output)
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
    };

    public abstract void collectIOTrace(
            @Nonnull File segmentStore,
            boolean mmap,
            int segmentCacheSize,
            @Nonnull String path,
            int depth,
            @Nonnull File output)
    throws IOException, InvalidFileStoreVersionException;

    @Nonnull
    private static FileStore newFileStore(FileStoreBuilder fileStoreBuilder) {
        try {
            return fileStoreBuilder.build();
        } catch (InvalidFileStoreVersionException | IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

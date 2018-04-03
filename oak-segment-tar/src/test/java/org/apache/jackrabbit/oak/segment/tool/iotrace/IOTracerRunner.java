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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.getInteger;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.tool.iotrace.IOTracer.newIOTracer;
import static org.junit.Assume.assumeTrue;

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
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.junit.Before;
import org.junit.Test;

/**
 * This test case can be used to collect {@link IOTracer io traces}. It is disabled
 * by default and needs to be enabled via {@code -Dtest=IOTraceRunner}.
 * <br>
 * The test accepts the following properties:<br>
 * {@code -Dinput=/path/to/segmentstore}. Required.<br>
 * {{@code -Dtrace=breadth|depth}}. Required. <br>
 * {@code -Doutput=/path/to/trace.cvs}. Default: {@code iotrace.csv}<br>
 * {@code -Dmmap=true|false}. Default {@code true}<br>
 * {@code -Dsegment-cache=n}. Default {@code 256}<br>
 * {@code -Ddepth=n}. Default {@code 5}<br>
 * {@code -Dpath=/path/to/start/node}. Default {@code /root}
 * <p>
 * FIXME OAK-5655: Turn this into a development tool and move to the right place.
 */
public class IOTracerRunner extends IOMonitorAdapter {
    private static final boolean ENABLED =
            IOTracerRunner.class.getSimpleName().equals(getProperty("test"));

    private static final String INPUT = getProperty("segmentstore");

    private static final String TRACE = getProperty("trace");

    private static final String OUTPUT = getProperty("output", "iotrace.csv");

    private static final boolean MMAP = parseBoolean(getProperty("mmap", "true"));

    private static final int SEGMENT_CACHE = getInteger("segment-cache", 256);

    private static final int DEPTH = getInteger("depth", 5);

    private static final String PATH = getProperty("path", "/root");

    @Before
    public void setup() throws Exception {
        assumeTrue(
            format("Test disabled. Specify -Dtest=%s to enable it", IOTracerRunner.class.getSimpleName()),
            ENABLED);
    }

    @Test
    public void collectTrace() throws IOException, InvalidFileStoreVersionException {
        checkArgument(INPUT != null, "No segment store directory specified");
        checkArgument("breadth".equalsIgnoreCase(TRACE) || "depth".equalsIgnoreCase(TRACE),
                  "No trace specified");

        System.out.println(format(
                "%s first traversing %d levels of %s starting at %s", TRACE, DEPTH, INPUT, PATH));
        System.out.println(
                format("mmap=%b, segment cache=%d", MMAP, SEGMENT_CACHE));
        System.out.println(format("Writing trace to %s", OUTPUT));

        if ("depth".equals(TRACE)) {
            collectDepthFirstTrace(INPUT, MMAP, SEGMENT_CACHE, PATH, DEPTH, OUTPUT);
        } else if ("breadth".equals(TRACE)) {
            collectBreadthFirstTrace(INPUT, MMAP, SEGMENT_CACHE, PATH, DEPTH, OUTPUT);
        } else {
            throw new IllegalStateException();
        }
    }

    public void collectBreadthFirstTrace(
            @Nonnull String segmentStore,
            boolean mmap,
            int segmentCacheSize,
            @Nonnull String path,
            int depth,
            @Nonnull String output)
    throws IOException, InvalidFileStoreVersionException {
        checkNotNull(segmentStore);
        checkNotNull(path);
        checkNotNull(output);

        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output, true)))) {
            Function<IOMonitor, FileStore> factory = ioMonitor ->
                    newFileStore(
                            fileStoreBuilder(new File(segmentStore))
                                    .withMemoryMapping(mmap)
                                    .withSegmentCacheSize(segmentCacheSize)
                                    .withIOMonitor(ioMonitor));

            IOTracer ioTracer = newIOTracer(factory, out, BreadthFirstTrace.CONTEXT_SPEC);
            ioTracer.collectTrace(
                    new BreadthFirstTrace(depth, path, ioTracer::setContext));
        }
    }

    public void collectDepthFirstTrace(
            @Nonnull String segmentStore,
            boolean mmap,
            int segmentCacheSize,
            @Nonnull String path,
            int depth,
            @Nonnull String output)
    throws IOException, InvalidFileStoreVersionException {
        checkNotNull(segmentStore);
        checkNotNull(path);
        checkNotNull(output);

        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(output, true)))) {
            Function<IOMonitor, FileStore> factory = ioMonitor ->
                    newFileStore(
                            fileStoreBuilder(new File(segmentStore))
                                    .withMemoryMapping(mmap)
                                    .withSegmentCacheSize(segmentCacheSize)
                                    .withIOMonitor(ioMonitor));

            IOTracer ioTracer = newIOTracer(factory, out, DepthFirstTrace.CONTEXT_SPEC);
            ioTracer.collectTrace(
                    new DepthFirstTrace(depth, path, ioTracer::setContext));
        }
    }


    @Nonnull
    private static FileStore newFileStore(FileStoreBuilder fileStoreBuilder) {
        try {
            return fileStoreBuilder.build();
        } catch (InvalidFileStoreVersionException | IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

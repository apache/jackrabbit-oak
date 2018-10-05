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

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.tool.iotrace.IOTracer.newIOTracer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IOTracerTest extends IOMonitorAdapter {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException {
        try (FileStore fileStore = fileStoreBuilder(folder.getRoot()).build()) {
            SegmentNodeState currentHead = fileStore.getHead();
            SegmentNodeBuilder root = currentHead.builder();
            root.setChildNode("1a");
            root.setChildNode("1b");
            NodeBuilder builder = root.setChildNode("1c");

            builder.setChildNode("2d");
            builder.setChildNode("2e");
            builder = builder.setChildNode("2f");

            builder.setChildNode("3g");
            builder.setChildNode("3h");
            builder.setChildNode("3i").setChildNode("4j");
            SegmentNodeState newHead = root.getNodeState();
            fileStore.getRevisions().setHead(currentHead.getRecordId(), newHead.getRecordId());
        }
    }

    @NotNull
    private FileStore createFileStore(IOMonitor ioMonitor) {
        try {
            return fileStoreBuilder(folder.getRoot())
                    .withSegmentCacheSize(0)
                    .withIOMonitor(ioMonitor).build();
        } catch (InvalidFileStoreVersionException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void collectBreadthFirstTrace() throws IOException {
        try (StringWriter out = new StringWriter()) {
            Function<IOMonitor, FileStore> factory = this::createFileStore;

            IOTracer ioTracer = newIOTracer(factory, out, BreadthFirstTrace.CONTEXT_SPEC);
            ioTracer.collectTrace(
                    new BreadthFirstTrace(2, "/", ioTracer::setContext));

            try (BufferedReader reader = new BufferedReader(new StringReader(out.toString()))) {
                Optional<String> header = reader.lines().findFirst();
                List<String[]> entries = reader.lines()
                    .map(line -> line.split(","))
                    .collect(toList());

                assertTrue(header.isPresent());
                assertEquals("timestamp,file,segmentId,length,elapsed,depth,count", header.get());

                long now = currentTimeMillis();
                assertTrue("The timestamps of all entries must be in the past",
                    entries.stream()
                        .map(row -> parseLong(row[0]))  // ts
                        .allMatch(ts -> ts <= now));

                assertEquals("Expected depths 0, 1 and 2",
                        ImmutableSet.of(0, 1, 2),
                        entries.stream()
                            .map(row -> parseInt(row[5])) // depth
                            .distinct().collect(toSet()));

                assertEquals("Expected max 10 nodes",
                        Optional.of(true),
                        entries.stream()
                            .map(row -> parseInt(row[6])) // count
                            .max(Comparator.naturalOrder())
                            .map(max -> max <= 10));
            }
        }
    }

    @Test
    public void collectDepthFirstTrace() throws IOException {
        try (StringWriter out = new StringWriter()) {
            Function<IOMonitor, FileStore> factory = this::createFileStore;

            IOTracer ioTracer = newIOTracer(factory, out, DepthFirstTrace.CONTEXT_SPEC);
            ioTracer.collectTrace(
                    new DepthFirstTrace(2, "/", ioTracer::setContext));

            try (BufferedReader reader = new BufferedReader(new StringReader(out.toString()))) {
                Optional<String> header = reader.lines().findFirst();
                List<String[]> entries = reader.lines()
                    .map(line -> line.split(","))
                    .collect(toList());

                assertTrue(header.isPresent());
                assertEquals("timestamp,file,segmentId,length,elapsed,depth,count,path", header.get());

                long now = currentTimeMillis();
                assertTrue("The timestamps of all entries must be in the past",
                    entries.stream()
                        .map(row -> parseLong(row[0]))  // ts
                        .allMatch(ts -> ts <= now));

                assertEquals("Expected depths 0 and 1",
                        ImmutableSet.of(0, 1),
                        entries.stream()
                            .map(row -> parseInt(row[5])) // depth
                            .distinct().collect(toSet()));

                assertEquals("Expected max 10 nodes",
                        Optional.of(true),
                        entries.stream()
                            .map(row -> parseInt(row[6])) // count
                            .max(Comparator.naturalOrder())
                            .map(max -> max <= 10));
            }
        }
    }

    @Test
    public void collectRandomAccessTrace() throws IOException {
        try (StringWriter out = new StringWriter()) {
            Function<IOMonitor, FileStore> factory = this::createFileStore;

            IOTracer ioTracer = newIOTracer(factory, out, RandomAccessTrace.CONTEXT_SPEC);
            ImmutableList<String> paths = ImmutableList.of("/1a", "/1b", "/foo");
            ioTracer.collectTrace(
                    new RandomAccessTrace(paths, 0, 10, ioTracer::setContext));

            try (BufferedReader reader = new BufferedReader(new StringReader(out.toString()))) {
                Optional<String> header = reader.lines().findFirst();
                List<String[]> entries = reader.lines()
                    .map(line -> line.split(","))
                    .collect(toList());

                assertTrue(header.isPresent());
                assertEquals("timestamp,file,segmentId,length,elapsed,path", header.get());

                long now = currentTimeMillis();
                assertTrue("The timestamps of all entries must be in the past",
                    entries.stream()
                        .map(row -> parseLong(row[0]))  // ts
                        .allMatch(ts -> ts <= now));

                assertTrue("The path of all entries must be in " + paths,
                    entries.stream()
                       .map(row -> row[5])  // path
                       .allMatch(paths::contains));
            }
        }
    }

}

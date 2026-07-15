/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.segment.file.CompactedNodeState;
import org.apache.jackrabbit.oak.segment.file.CompactionWriter;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.gc.LoggingGCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import static org.apache.jackrabbit.oak.segment.SegmentNodeStore.ROOT;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CompactToDifferentNodeStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(CompactToDifferentNodeStoreTest.class);

    private final CompactorFactory compactorFactory;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    public CompactToDifferentNodeStoreTest(@SuppressWarnings({"unused", "java:S1172"}) String name, CompactorFactory compactorFactory) {
        this.compactorFactory = compactorFactory;
    }

    public interface CompactorFactory {
        Compactor createCompactor(GCMonitor gcListener, CompactionWriter writer, GCNodeWriteMonitor compactionMonitor);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> compactorFactories() {
        return Arrays.asList(
                new Object[] { "ClassicCompactor", (CompactorFactory) (gcListener, writer, compactionMonitor) -> new ClassicCompactor(writer, compactionMonitor) },
                new Object[] { "CheckpointCompactor", (CompactorFactory) (gcListener, writer, compactionMonitor) -> new CheckpointCompactor(gcListener, new ClassicCompactor(writer, compactionMonitor)) },
                new Object[] { "CheckpointCompactor-parallel", (CompactorFactory) (gcListener, writer, compactionMonitor) -> new CheckpointCompactor(gcListener, new ParallelCompactor(gcListener, writer, compactionMonitor, 4)) }
        );
    }

    @Test
    public void testCompactCopy() throws InvalidFileStoreVersionException, IOException, CommitFailedException {
        File sourceFolder = folder.newFolder("source");
        createGarbage(sourceFolder);

        File targetFolder = folder.newFolder("target");

        try (ReadOnlyFileStore compactionSource = fileStoreBuilder(sourceFolder).buildReadOnly()) {

            CompactedNodeState compactedNodeState;
            try (FileStore compactionTarget = fileStoreBuilder(targetFolder).build()) {
                compactedNodeState = compact(compactionSource, compactionTarget);
            }

            try (ReadOnlyFileStore result = fileStoreBuilder(targetFolder).buildReadOnly()) {
                SegmentNodeStore s = SegmentNodeStoreBuilders.builder(compactionSource).build();
                SegmentNodeStore t = SegmentNodeStoreBuilders.builder(result).build();

                assertEquals("value100", compactedNodeState.getChildNode(ROOT).getChildNode("test").getProperty("property").getValue(Type.STRING));
                assertEquals(
                        s.getRoot().getChildNode("garbage").getProperty("binary"),
                        compactedNodeState.getChildNode(ROOT).getChildNode("garbage").getProperty("binary"));
                assertEquals(
                        s.getRoot().getChildNode("garbage").getProperty("binary"),
                        t.getRoot().getChildNode("garbage").getProperty("binary"));
            }
        }
    }

    private CompactedNodeState compact(ReadOnlyFileStore source, FileStore target) {
        GCGeneration headGeneration = target.getHead().getRecordId().getSegment().getGcGeneration();
        CompactionWriter writer = new CompactionWriter(target.getReader(), target.getBlobStore(), headGeneration, target.getWriter());
        GCMonitor gcListener = new LoggingGCMonitor(LOG);
        GCNodeWriteMonitor compactionMonitor = new GCNodeWriteMonitor(1, gcListener);
        Compactor compactor = compactorFactory.createCompactor(gcListener, writer, compactionMonitor);
        try {
            CompactedNodeState compactedNodeState = compactor.compactUp(target.getHead(), source.getHead(), Canceller.newCanceller());
            if (compactedNodeState != null) {
                target.getRevisions().setHead(target.getHead().getRecordId(), compactedNodeState.getRecordId());
            }
            return compactedNodeState;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void createGarbage(File source) throws InvalidFileStoreVersionException, IOException, CommitFailedException {
        try (FileStore store = fileStoreBuilder(source).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(store).build();
            for (int i = 0; i <= 100; i++) {
                NodeBuilder builder = nodeStore.getRoot().builder();
                builder.child("test").setProperty("property", "value" + i);
                if (builder.hasChildNode("garbage")) {
                    builder.getChildNode("garbage").remove();
                } else {
                    builder.child("garbage").setProperty(BinaryPropertyState.binaryProperty("binary", RandomStringUtils.insecure().nextPrint(10_000)));
                }
                nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                store.flush();
            }
        }
    }

}

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

package org.apache.jackrabbit.oak.segment;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.CompactorTestUtils.SimpleCompactor;
import static org.apache.jackrabbit.oak.segment.CompactorTestUtils.SimpleCompactorFactory;
import static org.apache.jackrabbit.oak.segment.CompactorTestUtils.addTestContent;
import static org.apache.jackrabbit.oak.segment.CompactorTestUtils.assertSameRecord;
import static org.apache.jackrabbit.oak.segment.CompactorTestUtils.assertSameStableId;
import static org.apache.jackrabbit.oak.segment.CompactorTestUtils.checkGeneration;
import static org.apache.jackrabbit.oak.segment.CompactorTestUtils.getCheckpoint;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.jackrabbit.oak.segment.file.CompactedNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.GCIncrement;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

public abstract class AbstractCompactorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore fileStore;

    private SegmentNodeStore nodeStore;

    private Compactor compactor;

    private SimpleCompactor simpleCompactor;

    private final SimpleCompactorFactory compactorFactory;

    private GCNodeWriteMonitor compactionMonitor;

    private GCGeneration baseGeneration;
    private GCGeneration partialGeneration;
    private GCGeneration targetGeneration;

    @Parameterized.Parameters
    public static List<SimpleCompactorFactory> compactorFactories() {
        return Arrays.asList(
                compactor -> compactor::compactUp,
                compactor -> (node, canceller) -> compactor.compactDown(node, canceller, canceller),
                compactor -> (node, canceller) -> compactor.compact(EMPTY_NODE, node, EMPTY_NODE, canceller)
        );
    }

    public AbstractCompactorTest(@NotNull SimpleCompactorFactory compactorFactory) {
        this.compactorFactory = compactorFactory;
    }

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException {
        fileStore = fileStoreBuilder(folder.getRoot()).build();
        nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

        baseGeneration = fileStore.getHead().getGcGeneration();
        partialGeneration = baseGeneration.nextPartial();
        targetGeneration = baseGeneration.nextFull();
        GCIncrement increment = new GCIncrement(baseGeneration, partialGeneration, targetGeneration);

        compactionMonitor = new GCNodeWriteMonitor(-1, GCMonitor.EMPTY);
        compactor = createCompactor(fileStore, increment, compactionMonitor);
        simpleCompactor = compactorFactory.newSimpleCompactor(compactor);
    }

    protected abstract Compactor createCompactor(
            @NotNull FileStore fileStore,
            @NotNull GCIncrement increment,
            @NotNull GCNodeWriteMonitor compactionMonitor);

    @After
    public void tearDown() {
        fileStore.close();
    }

    @Test
    public void testCompact() throws Exception {
        addTestContent("cp1", nodeStore, 42);
        String cp1 = nodeStore.checkpoint(DAYS.toMillis(1));
        addTestContent("cp2", nodeStore, 42);
        String cp2 = nodeStore.checkpoint(DAYS.toMillis(1));

        SegmentNodeState uncompacted1 = fileStore.getHead();
        SegmentNodeState compacted1 = simpleCompactor.compact(uncompacted1, Canceller.newCanceller());
        assertNotNull(compacted1);
        assertNotSame(uncompacted1, compacted1);
        checkGeneration(compacted1, targetGeneration);

        assertSameStableId(uncompacted1, compacted1);
        assertSameStableId(getCheckpoint(uncompacted1, cp1), getCheckpoint(compacted1, cp1));
        assertSameStableId(getCheckpoint(uncompacted1, cp2), getCheckpoint(compacted1, cp2));
        assertSameRecord(getCheckpoint(compacted1, cp2), compacted1.getChildNode("root"));

        // Simulate a 2nd compaction cycle
        addTestContent("cp3", nodeStore, 42);
        String cp3 = nodeStore.checkpoint(DAYS.toMillis(1));
        addTestContent("cp4", nodeStore, 42);
        String cp4 = nodeStore.checkpoint(DAYS.toMillis(1));

        SegmentNodeState uncompacted2 = fileStore.getHead();
        SegmentNodeState compacted2 = compactor.compact(uncompacted1, uncompacted2, compacted1, Canceller.newCanceller());
        assertNotNull(compacted2);
        assertNotSame(uncompacted2, compacted2);
        checkGeneration(compacted2, targetGeneration);

        assertTrue(fileStore.getRevisions().setHead(uncompacted2.getRecordId(), compacted2.getRecordId()));

        assertEquals(uncompacted2, compacted2);
        assertSameStableId(uncompacted2, compacted2);
        assertSameStableId(getCheckpoint(uncompacted2, cp1), getCheckpoint(compacted2, cp1));
        assertSameStableId(getCheckpoint(uncompacted2, cp2), getCheckpoint(compacted2, cp2));
        assertSameStableId(getCheckpoint(uncompacted2, cp3), getCheckpoint(compacted2, cp3));
        assertSameStableId(getCheckpoint(uncompacted2, cp4), getCheckpoint(compacted2, cp4));
        assertSameRecord(getCheckpoint(compacted1, cp1), getCheckpoint(compacted2, cp1));
        assertSameRecord(getCheckpoint(compacted1, cp2), getCheckpoint(compacted2, cp2));
        assertSameRecord(getCheckpoint(compacted2, cp4), compacted2.getChildNode("root"));
    }

    @Test
    public void testHardCancellation() throws Exception {
        for (int i = 1; i < 25; i++) {
            addTestContent("cp" + i, nodeStore, 42);
        }

        Canceller canceller = Canceller.newCanceller()
                .withCondition(null, () -> (compactionMonitor.getCompactedNodes() >= 10));

        SegmentNodeState uncompacted1 = fileStore.getHead();
        SegmentNodeState compacted1 = simpleCompactor.compact(uncompacted1, canceller);
        assertNull(compacted1);
    }

    @Test
    public void testSoftCancellation() throws Exception {
        for (int i = 1; i < 25; i++) {
            addTestContent("cp" + i, nodeStore, 42);
        }

        Canceller nullCanceller = Canceller.newCanceller();
        Canceller softCanceller = Canceller.newCanceller()
                .withCondition(null, () -> (compactionMonitor.getCompactedNodes() >= 10));

        SegmentNodeState uncompacted1 = fileStore.getHead();
        CompactedNodeState compacted1 = compactor.compactDown(uncompacted1, nullCanceller, softCanceller);
        System.out.println(compactionMonitor.getCompactedNodes());

        assertNotNull(compacted1);
        assertFalse(compacted1.isComplete());
        assertEquals(uncompacted1, compacted1);
        checkGeneration(uncompacted1, baseGeneration);
        assertNotEquals(targetGeneration, compacted1.getGcGeneration());

        CompactedNodeState compacted2 = simpleCompactor.compact(compacted1, nullCanceller);
        System.out.println(compactionMonitor.getCompactedNodes());

        assertNotNull(compacted2);
        assertTrue(compacted2.isComplete());
        checkGeneration(compacted2, targetGeneration);
    }
}

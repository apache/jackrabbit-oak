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

package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.segment.CompactorTestUtils;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentBufferWriterPool;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentWriterFactory;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.apache.jackrabbit.oak.segment.CompactorTestUtils.addTestContent;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class FullCompactionStrategyTest {

    private static final Throwable MARKER_THROWABLE =
            new RuntimeException("We pretend that something went horribly wrong.");

    @Test
    public void compactionIsAbortedOnAnyThrowable() throws IOException {
        MemoryStore store = new MemoryStore();
        CompactionStrategy.Context throwingContext = Mockito.mock(CompactionStrategy.Context.class);
        when(throwingContext.getGCListener()).thenReturn(Mockito.mock(GCListener.class));
        when(throwingContext.getRevisions()).thenReturn(store.getRevisions());
        when(throwingContext.getGCOptions()).thenThrow(MARKER_THROWABLE);

        try {
            final CompactionResult compactionResult = new FullCompactionStrategy().compact(throwingContext);
            assertThat("Compaction should be properly aborted.", compactionResult.isSuccess(), is(false));
        } catch (Throwable e) {
            if (e == MARKER_THROWABLE) {
                fail("The marker throwable was not caught by the CompactionStrategy and therefore not properly aborted.");
            } else {
                throw new IllegalStateException("The test likely needs to be adjusted.", e);
            }
        }
    }

    private CompactionStrategy.Context getMockedCompactionContext(MemoryStore store) {
        CompactionStrategy.Context mockedContext = Mockito.mock(CompactionStrategy.Context.class);
        when(mockedContext.getGCListener()).thenReturn(Mockito.mock(GCListener.class));
        when(mockedContext.getTarFiles()).thenReturn(Mockito.mock(TarFiles.class));
        when(mockedContext.getSuccessfulCompactionListener()).thenReturn(Mockito.mock(SuccessfulCompactionListener.class));
        when(mockedContext.getGCOptions()).thenReturn(SegmentGCOptions.defaultGCOptions());
        when(mockedContext.getFlusher()).thenReturn(() -> {});

        GCJournal mockedJournal = Mockito.mock(GCJournal.class);
        when(mockedContext.getGCJournal()).thenReturn(mockedJournal);
        when(mockedJournal.read()).thenReturn(Mockito.mock(GCJournal.GCJournalEntry.class));

        when(mockedContext.getRevisions()).thenReturn(store.getRevisions());
        when(mockedContext.getSegmentReader()).thenReturn(store.getReader());

        SegmentWriterFactory writerFactory = generation -> defaultSegmentWriterBuilder("c")
                .withGeneration(generation)
                .withWriterPool(SegmentBufferWriterPool.PoolType.THREAD_SPECIFIC)
                .build(store);
        when(mockedContext.getSegmentWriterFactory()).thenReturn(writerFactory);

        return mockedContext;
    }

    @Test
    public void testIntermediateStateSave() throws Exception {
        MemoryStore store = new MemoryStore();
        NodeStore nodeStore = SegmentNodeStoreBuilders.builder(store).build();

        for (int i = 1; i < 100; i++) {
            addTestContent("node" + i, nodeStore, 42);
        }

        CompactionStrategy.Context mockedContext = getMockedCompactionContext(store);
        GCNodeWriteMonitor gcMonitor = new GCNodeWriteMonitor(-1, GCMonitor.EMPTY);
        when(mockedContext.getCompactionMonitor()).thenReturn(gcMonitor);

        when(mockedContext.getStateSaveTriggerSupplier()).thenReturn(
                () -> {
                    long compactedNodes = gcMonitor.getCompactedNodes();
                    return Canceller.newCanceller().withCondition("10 more nodes compacted",
                            () -> gcMonitor.getCompactedNodes() >= compactedNodes + 10);
                }
        );
        when(mockedContext.getHardCanceller()).thenReturn(
                Canceller.newCanceller().withCondition("50 total nodes compacted",
                        () -> gcMonitor.getCompactedNodes() >= 50)
        );
        when(mockedContext.getSoftCanceller()).thenReturn(Canceller.newCanceller());

        RecordId initialHead = store.getRevisions().getHead();

        FullCompactionStrategy strategy = new FullCompactionStrategy();
        CompactionResult result = strategy.compact(mockedContext);

        assertFalse(result.isSuccess());
        assertNotEquals(initialHead, store.getRevisions().getHead());
    }

    @Test
    public void testIncrementalCompaction() throws Exception {
        MemoryStore store = new MemoryStore();
        NodeStore nodeStore = SegmentNodeStoreBuilders.builder(store).build();

        for (int i = 1; i < 100; i++) {
            addTestContent("node" + i, nodeStore, 42);
        }

        CompactionStrategy.Context mockedContext = getMockedCompactionContext(store);
        GCNodeWriteMonitor gcMonitor = new GCNodeWriteMonitor(-1, GCMonitor.EMPTY);
        when(mockedContext.getCompactionMonitor()).thenReturn(gcMonitor);

        when(mockedContext.getStateSaveTriggerSupplier()).thenReturn(
                () -> {
                    long compactedNodes = gcMonitor.getCompactedNodes();
                    return Canceller.newCanceller().withCondition("10 more nodes compacted",
                            () -> gcMonitor.getCompactedNodes() >= compactedNodes + 10);
                }
        );
        when(mockedContext.getHardCanceller()).thenReturn(Canceller.newCanceller());
        when(mockedContext.getSoftCanceller()).thenReturn(Canceller.newCanceller());

        SegmentNodeState initialHeadState = store.getReader().readNode(store.getRevisions().getHead());
        GCGeneration baseGeneration = initialHeadState.getGcGeneration();
        CompactorTestUtils.checkGeneration(initialHeadState, baseGeneration);

        FullCompactionStrategy strategy = new FullCompactionStrategy();
        CompactionResult result = strategy.compact(mockedContext);
        assertTrue(result.isSuccess());

        SegmentNodeState headState = store.getReader().readNode(store.getRevisions().getHead());
        CompactorTestUtils.checkGeneration(headState, baseGeneration.nextFull());
    }
}
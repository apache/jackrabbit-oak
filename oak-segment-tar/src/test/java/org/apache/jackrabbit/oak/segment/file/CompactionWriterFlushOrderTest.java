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
package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.segment.CompactorTestUtils.addTestContent;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.segment.ClassicCompactor;
import org.apache.jackrabbit.oak.segment.Compactor;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentWriterFactory;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * {@link CompactionWriter#flush()} flushes the partial writer before the target writer:
 * <pre>
 * public void flush() throws IOException {
 *     partialWriter.flush();
 *     targetWriter.flush();
 * }
 * </pre>
 * A partially compacted node references the fully compacted nodes below it, so this
 * persists a segment before the segment it depends on. Two consequences:
 * <ul>
 * <li>A crash between the two flushes leaves a persisted node referencing records that
 *     were never written.</li>
 * <li>The segment holding the partial root is the compacted root of a
 *     {@link CompactionResult#partiallySucceeded partially succeeded} compaction, and
 *     {@link DefaultCleanupContext#shouldReclaim} reclaims every compacted segment
 *     persisted <em>after</em> the compacted root ("dangling future segments"). The
 *     target segment flushed afterwards matches that description while still being
 *     referenced from the head.</li>
 * </ul>
 * This only matters for soft cancellation: without it the partial writer has nothing
 * buffered and the flush order is unobservable.
 */
public class CompactionWriterFlushOrderTest {

    private final List<SegmentId> persisted = new ArrayList<>();

    private RecordingStore store;

    @BeforeEach
    public void setUp() throws IOException {
        store = new RecordingStore();
    }

    /**
     * Drives a {@link ClassicCompactor} with a soft canceller, which routes the completed
     * subtrees to the target writer and the spine above the cancellation point to the
     * partial writer. Asserts that the segment holding the compacted root does not
     * reference any segment that is persisted after it.
     */
    @Test
    @Disabled("OAK-12404: reproduces the bug, currently fails. "
            + "Fix: swap the two statements in CompactionWriter#flush().")
    public void softCancellationPersistsTheSpineBeforeTheContentItReferences() throws Exception {
        NodeStore nodeStore = SegmentNodeStoreBuilders.builder(store).build();
        for (int i = 0; i < 100; i++) {
            addTestContent("node" + i, nodeStore, 42);
        }

        SegmentNodeState head = store.getReader().readHeadState(store.getRevisions());
        GCGeneration base = head.getGcGeneration();
        GCIncrement increment = new GCIncrement(base, base.nextPartial(), base.nextFull());

        SegmentWriterFactory writerFactory = generation -> defaultSegmentWriterBuilder("c")
                .withGeneration(generation)
                .build(store);
        CompactionWriter writer = new CompactionWriter(store.getReader(), null, increment, writerFactory);

        GCNodeWriteMonitor monitor = new GCNodeWriteMonitor(-1, GCMonitor.EMPTY);
        Compactor compactor = new ClassicCompactor(writer, monitor);

        Canceller softCanceller = Canceller.newCanceller()
                .withCondition("40 nodes compacted", () -> monitor.getCompactedNodes() >= 40);
        CompactedNodeState compacted =
                compactor.compactDown(head, Canceller.newCanceller(), softCanceller);

        assertNotNull(compacted, "compaction should not have been hard cancelled");
        assertFalse(compacted.isComplete(), "compaction should have been soft cancelled");

        SegmentId rootSegment = compacted.getRecordId().getSegmentId();
        assertEquals(base.nextPartial(), rootSegment.getGcGeneration(),
                "the partial root should be at the partial generation");

        writer.flush();

        int rootIndex = persisted.indexOf(rootSegment);
        assertTrue(rootIndex >= 0, "the partial root segment should have been persisted");

        // The segment reference table is what TarReader#mark builds the cleanup graph from.
        Set<UUID> referencedByRoot = referencedSegmentIds(store.readSegment(rootSegment));

        // Segments the spine depends on, yet which reach the store only after it. They are
        // at the target generation, i.e. compacted, which is also what
        // DefaultCleanupContext#isDanglingFutureSegment reclaims without consulting
        // reachability: it assumes compacted segments are unused iff they are persisted
        // after the last compacted root.
        List<SegmentId> referencedButLater = persisted.subList(rootIndex + 1, persisted.size())
                .stream()
                .filter(id -> referencedByRoot.contains(id.asUUID()))
                .collect(Collectors.toList());

        assertTrue(referencedButLater.isEmpty(), String.format(
                "the segment holding the compacted root %s (%s) references %d segment(s) that "
                        + "are only persisted after it: %s",
                rootSegment, rootSegment.getGcGeneration(), referencedButLater.size(),
                referencedButLater.stream()
                        .map(id -> id + " " + id.getGcGeneration())
                        .collect(Collectors.toList())));
    }

    private static Set<UUID> referencedSegmentIds(Segment segment) {
        Set<UUID> referenced = new HashSet<>();
        for (int i = 0; i < segment.getReferencedSegmentIdCount(); i++) {
            referenced.add(segment.getReferencedSegmentId(i));
        }
        return referenced;
    }

    /**
     * Records the order in which segments reach the underlying store. The recording list
     * belongs to the enclosing instance because {@link MemoryStore#MemoryStore()} already
     * flushes a segment, before any field of this subclass could be initialised.
     */
    private class RecordingStore extends MemoryStore {

        RecordingStore() throws IOException {
            super();
        }

        @Override
        public void writeSegment(SegmentId id, byte[] data, int offset, int length) throws IOException {
            persisted.add(id);
            super.writeSegment(id, data, offset, length);
        }
    }
}

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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.file.CompactedNodeState;
import org.apache.jackrabbit.oak.segment.file.CompactionWriter;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.GCIncrement;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.apache.jackrabbit.oak.segment.test.FileStoreParameterResolver;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Unit test to replicate the behavior of {@code org.apache.jackrabbit.oak.segment.file.AbstractCompactionStrategy}
 * when writes happen during compaction. The {@link Compactor#compact(NodeState, NodeState, NodeState, Canceller)}
 * method is called with the previous "head" state as "before" state and the new "head" state as "after" state. The
 * diff between the two is written to the "compacted" state of the previous compaction cycle.
 * <p>
 * The test verifies that new snapshots created concurrently to a compaction cycle are correctly included into the
 * compacted state. In particular, the tests ensure that the same records are re-used for unchanged nodes. Not doing
 * so would create unnecessary copies of the content and thus
 * <ul>
 *     <li>increase the size of the segment store,
 *     <li>increase the time taken for compaction and
 *     <li>increase the chance of incurring additional compaction cycles.
 * </ul>
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-12134">OAK-12134</a>
 */
class CheckpointCompactorEfficiencyTest {

    private static final int WIDTH = 1000;

    // Nodes a correct retry may compact beyond the changed children (their ancestor spine plus the
    // concurrent-checkpoint structure).
    private static final int RETRY_SPINE_OVERHEAD = 32;

    @RegisterExtension
    FileStoreParameterResolver fileStoreParameterResolver = new FileStoreParameterResolver(b -> b.withSegmentCacheSize(4));

    interface CompactionStrategy {
        CompactedNodeState compact(Compactor compactor, NodeState before, NodeState after) throws IOException;
    }

    @SuppressWarnings("deprecation")
    static Stream<Arguments> scenarios() {
        Named<CompactionStrategy> tailCompaction = named("tail compaction", (compactor, before, after) ->
                compactor.compactDown(before, after, Canceller.newCanceller(), Canceller.newCanceller()));
        Named<CompactionStrategy> fullCompaction = named("full compaction", (compactor, before, after) ->
                compactor.compactUp(before, after, Canceller.newCanceller()));
        return Stream.of(CheckpointCompactor.class, LegacyCheckpointCompactor.class)
                .flatMap(clazz -> Stream.of(tailCompaction, fullCompaction)
                        .map(compactionStrategy -> arguments(clazz, compactionStrategy)));
    }

    @BeforeEach
    void setUp(NodeStore nodeStore) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.child("unchanged").setProperty("foo", "bar");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @ParameterizedTest
    @MethodSource("scenarios")
    void checkpointsReplayedInCreationOrderWithConcurrentWrites(Class<? extends Compactor> classUnderTest, CompactionStrategy compactionStrategy, FileStore fileStore, NodeStore nodeStore) throws CommitFailedException, IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Compactor compactor = createCompactor(classUnderTest, fileStore);
        SegmentNodeState headBeforeChanges = fileStore.getHead();
        updateContentAndCreateCheckpoint(nodeStore, 1, 5);
        assertCheckpointsShareUnchangedNodeStateWithRoot(nodeStore, getCheckpoints(nodeStore));

        // run compaction
        SegmentNodeState head = fileStore.getHead();
        CompactedNodeState partiallyCompacted = compactionStrategy.compact(compactor, headBeforeChanges, head);

        // simulate concurrent write during compaction
        updateContentAndCreateCheckpoint(nodeStore, 6, 6);
        assertNotNull(partiallyCompacted);
        assertFalse(fileStore.getRevisions().setHead(head.getRecordId(), partiallyCompacted.getRecordId()));

        // just like in AbstractCompactionStrategy, invoke #compact(head, newHead, partiallyCompacted) once setting the new head has failed
        SegmentNodeState newHead = fileStore.getHead();
        CompactedNodeState compacted = compactor.compact(head, newHead, partiallyCompacted, Canceller.newCanceller());
        assertNotNull(compacted);
        assertTrue(fileStore.getRevisions().setHead(newHead.getRecordId(), compacted.getRecordId()));
        assertCheckpointsShareUnchangedNodeStateWithRoot(nodeStore, getCheckpoints(nodeStore));
        assertEquals(compacted, newHead, "Expected the compacted state to be identical to the new head, " +
                "as all changes during compaction should have been included into the compacted state");
    }

    @ParameterizedTest
    @MethodSource("scenarios")
    void checkpointDeletedByConcurrentWrite(Class<? extends Compactor> classUnderTest, CompactionStrategy compactionStrategy, FileStore fileStore, NodeStore nodeStore)
            throws IOException, CommitFailedException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Compactor compactor = createCompactor(classUnderTest, fileStore);
        SegmentNodeState headBeforeChanges = fileStore.getHead();
        updateContentAndCreateCheckpoint(nodeStore, 1, 5);
        assertCheckpointsShareUnchangedNodeStateWithRoot(nodeStore, getCheckpoints(nodeStore));

        // run compaction
        SegmentNodeState head = fileStore.getHead();
        CompactedNodeState partiallyCompacted = compactionStrategy.compact(compactor, headBeforeChanges, head);

        // delete first checkpoint
        nodeStore.release(getCheckpoints(nodeStore).get(0));

        assertNotNull(partiallyCompacted);
        assertFalse(fileStore.getRevisions().setHead(head.getRecordId(), partiallyCompacted.getRecordId()));

        // just like in AbstractCompactionStrategy, invoke #compact(head, newHead, partiallyCompacted) once setting the new head has failed
        SegmentNodeState newHead = fileStore.getHead();
        CompactedNodeState compacted = compactor.compact(head, newHead, partiallyCompacted, Canceller.newCanceller());
        assertNotNull(compacted);
        assertTrue(fileStore.getRevisions().setHead(newHead.getRecordId(), compacted.getRecordId()));
        List<String> checkpoints = getCheckpoints(nodeStore);
        assertEquals(4, checkpoints.size(), "Expected one checkpoint to be deleted during compaction");
        assertCheckpointsShareUnchangedNodeStateWithRoot(nodeStore, checkpoints.subList(1, checkpoints.size()));
        assertEquals(compacted, newHead, "Expected the compacted state to be identical to the new head, " +
                "as all changes during compaction should have been included into the compacted state");
    }

    @ParameterizedTest
    @MethodSource("scenarios")
    void retryAfterConcurrentCheckpointProcessesOnlyTheDelta(Class<? extends Compactor> classUnderTest, CompactionStrategy compactionStrategy, FileStore fileStore, NodeStore nodeStore)
            throws CommitFailedException, IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        int changedChildren = 3;
        long retryCompactedNodes = runRetryCycle(classUnderTest, compactionStrategy, fileStore, nodeStore, changedChildren, true);
        assertTrue(retryCompactedNodes <= changedChildren + RETRY_SPINE_OVERHEAD,
                classUnderTest.getSimpleName() + " compacted " + retryCompactedNodes + " node states in a single retry "
                        + "cycle after " + changedChildren + " children changed under a " + WIDTH + "-wide node "
                        + "(expected <= " + (changedChildren + RETRY_SPINE_OVERHEAD) + "). A checkpoint was created "
                        + "during the cycle, so the live root is the 2nd super-root and its diff base is the compacted "
                        + "state - a different GC generation than the live root - which defeats MapRecord record-id "
                        + "bucket pruning and re-compacts the whole map.");
    }

    @ParameterizedTest
    @MethodSource("scenarios")
    void retryWithoutConcurrentCheckpointProcessesOnlyTheDelta(Class<? extends Compactor> classUnderTest, CompactionStrategy compactionStrategy, FileStore fileStore, NodeStore nodeStore)
            throws CommitFailedException, IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        int changedChildren = 3;
        long retryCompactedNodes = runRetryCycle(classUnderTest, compactionStrategy, fileStore, nodeStore, changedChildren, false);
        assertTrue(retryCompactedNodes <= changedChildren + RETRY_SPINE_OVERHEAD,
                classUnderTest.getSimpleName() + " compacted " + retryCompactedNodes + " node states in a retry cycle "
                        + "with no concurrently-created checkpoint (expected <= " + (changedChildren + RETRY_SPINE_OVERHEAD)
                        + "). Without an added checkpoint the live root is the first super-root and its diff base is "
                        + "same-generation, so pruning must apply regardless of compactor.");
    }

    @ParameterizedTest
    @MethodSource("scenarios")
    void everyRetryCycleProcessesOnlyTheDelta(Class<? extends Compactor> classUnderTest, CompactionStrategy compactionStrategy, FileStore fileStore, NodeStore nodeStore)
            throws CommitFailedException, IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        int retryCycles = 4;

        NodeBuilder builder = nodeStore.getRoot().builder();
        NodeBuilder wide = builder.child("wide");
        for (int i = 0; i < WIDTH; i++) {
            wide.child("c" + i).setProperty("v", (long) i);
        }
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        GCNodeWriteMonitor monitor = new GCNodeWriteMonitor(-1, GCMonitor.EMPTY);
        Compactor compactor = createCompactor(classUnderTest, fileStore, monitor);

        SegmentNodeState headBeforeChanges = fileStore.getHead();
        int idx = 0;
        touch(nodeStore, idx++);
        nodeStore.checkpoint(60_000, Map.of("name", "cp0"));
        SegmentNodeState head = fileStore.getHead();
        CompactedNodeState compacted = compactionStrategy.compact(compactor, headBeforeChanges, head);
        assertNotNull(compacted);

        // mirror the AbstractCompactionStrategy retry loop: compact(head, newHead, compacted), advance head.
        // Each cycle changes 2 children with a checkpoint in between.
        int changedPerCycle = 2;
        long[] perCycle = new long[retryCycles];
        for (int c = 0; c < retryCycles; c++) {
            touch(nodeStore, idx++);
            nodeStore.checkpoint(60_000, Map.of("name", "cp" + (c + 1)));
            touch(nodeStore, idx++);
            SegmentNodeState newHead = fileStore.getHead();

            long before = monitor.getCompactedNodes();
            compacted = compactor.compact(head, newHead, compacted, Canceller.newCanceller());
            assertNotNull(compacted);
            perCycle[c] = monitor.getCompactedNodes() - before;
            head = newHead;
        }

        for (int c = 0; c < retryCycles; c++) {
            assertTrue(perCycle[c] <= changedPerCycle + RETRY_SPINE_OVERHEAD,
                    classUnderTest.getSimpleName() + " per-cycle node counts " + Arrays.toString(perCycle) + ": cycle "
                            + (c + 1) + " compacted " + perCycle[c] + " node states (expected <= "
                            + (changedPerCycle + RETRY_SPINE_OVERHEAD) + " each). A count that stays flat near the "
                            + WIDTH + "-wide map width means the retry cycles never converge - each re-reads the whole "
                            + "tree because the diff base is in a different GC generation than the live root.");
        }
    }

    private long runRetryCycle(Class<? extends Compactor> classUnderTest, CompactionStrategy compactionStrategy,
                               FileStore fileStore, NodeStore nodeStore, int changedChildren, boolean checkpointDuringCycle)
            throws CommitFailedException, IOException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        NodeBuilder wide = builder.child("wide");
        for (int i = 0; i < WIDTH; i++) {
            wide.child("c" + i).setProperty("v", (long) i);
        }
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        GCNodeWriteMonitor monitor = new GCNodeWriteMonitor(-1, GCMonitor.EMPTY);
        Compactor compactor = createCompactor(classUnderTest, fileStore, monitor);

        SegmentNodeState headBeforeChanges = fileStore.getHead();
        touch(nodeStore, WIDTH - 1);
        nodeStore.checkpoint(60_000, Map.of("name", "before"));
        SegmentNodeState head = fileStore.getHead();
        CompactedNodeState partiallyCompacted = compactionStrategy.compact(compactor, headBeforeChanges, head);
        assertNotNull(partiallyCompacted);

        // concurrent changes during compaction: touch `changedChildren` distinct children. When a checkpoint is
        // created mid-way, the children touched after it make the live root differ from that checkpoint, so the
        // live root becomes a 2nd super-root whose diff base is the (target-generation) compacted state.
        int beforeCheckpoint = checkpointDuringCycle ? (changedChildren + 1) / 2 : changedChildren;
        for (int i = 0; i < beforeCheckpoint; i++) {
            touch(nodeStore, i);
        }
        if (checkpointDuringCycle) {
            nodeStore.checkpoint(60_000, Map.of("name", "concurrent"));
        }
        for (int i = beforeCheckpoint; i < changedChildren; i++) {
            touch(nodeStore, i);
        }

        assertFalse(fileStore.getRevisions().setHead(head.getRecordId(), partiallyCompacted.getRecordId()));
        SegmentNodeState newHead = fileStore.getHead();

        long compactedBefore = monitor.getCompactedNodes();
        CompactedNodeState compacted = compactor.compact(head, newHead, partiallyCompacted, Canceller.newCanceller());
        long retryCompactedNodes = monitor.getCompactedNodes() - compactedBefore;

        assertNotNull(compacted);
        assertTrue(fileStore.getRevisions().setHead(newHead.getRecordId(), compacted.getRecordId()));
        assertEquals(compacted, newHead, "retry compaction must fully reconcile the concurrent writes");

        return retryCompactedNodes;
    }

    private static void touch(NodeStore nodeStore, int childIndex) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.child("wide").child("c" + childIndex).setProperty("v", 1000L + childIndex);
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static @NotNull Compactor createCompactor(Class<? extends Compactor> classUnderTest, FileStore fileStore)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        return createCompactor(classUnderTest, fileStore, new GCNodeWriteMonitor(-1, GCMonitor.EMPTY));
    }

    private static @NotNull Compactor createCompactor(Class<? extends Compactor> classUnderTest, FileStore fileStore, GCNodeWriteMonitor compactionMonitor)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        GCGeneration baseGeneration = fileStore.getHead().getGcGeneration();
        GCGeneration partialGeneration = baseGeneration.nextPartial();
        GCGeneration targetGeneration = baseGeneration.nextFull();
        GCIncrement increment = new GCIncrement(baseGeneration, partialGeneration, targetGeneration);
        SegmentWriterFactory writerFactory = generation -> defaultSegmentWriterBuilder("c")
                .withGeneration(generation)
                .build(fileStore);
        CompactionWriter compactionWriter = new CompactionWriter(fileStore.getReader(), fileStore.getBlobStore(), increment, writerFactory);
        Constructor<? extends Compactor> declaredConstructor = classUnderTest.getDeclaredConstructor(GCMonitor.class, ClassicCompactor.class);
        return declaredConstructor.newInstance(GCMonitor.EMPTY, new ClassicCompactor(compactionWriter, compactionMonitor));
    }

    private void updateContentAndCreateCheckpoint(NodeStore nodeStore, int start, int end) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        for (int i = start; i <= end; i++) {
            builder.child("changed").setProperty("checkpoint", "checkpoint-" + i);
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            nodeStore.checkpoint(60_000, Map.of("checkpoint", "checkpoint-" + i));
        }
    }

    private static void assertCheckpointsShareUnchangedNodeStateWithRoot(NodeStore nodeStore, List<String> checkpoints) {
        SegmentNodeState unchanged = (SegmentNodeState) nodeStore.getRoot().getChildNode("unchanged");
        for (String name : checkpoints) {
            NodeState checkpoint = nodeStore.retrieve(name);
            assertNotNull(checkpoint);
            SegmentNodeState unchangedInCheckpoint = (SegmentNodeState) checkpoint.getChildNode("unchanged");
            assertEquals("bar", unchangedInCheckpoint.getString("foo"));
            String checkpointName = checkpoint.getChildNode("changed").getString("checkpoint");
            assertEquals(unchanged.getRecordId(), unchangedInCheckpoint.getRecordId(), "Expected the same record to be reused for the unchanged node in " + checkpointName);
        }
    }

    private static @NotNull List<String> getCheckpoints(NodeStore nodeStore) {
        return StreamSupport.stream(nodeStore.checkpoints().spliterator(), false)
                .sorted(Comparator.comparing(name -> nodeStore.checkpointInfo(name).get("checkpoint")))
                .toList();
    }
}

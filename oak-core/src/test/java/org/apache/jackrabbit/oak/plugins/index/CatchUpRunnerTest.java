/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.CatchUpCapable.CATCH_UP_FROM_START;
import static org.apache.jackrabbit.oak.plugins.index.CatchUpCapable.CATCH_UP_TRACKING_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CatchUpRunnerTest {

    private static final String INDEX_NAME = "testIndex";
    private static final String TARGET_TYPE = "testTarget";
    private static final String AFTER_CHECKPOINT = "after-cp-1";

    private MemoryNodeStore store;
    private RecordingProvider recordingProvider;
    private CatchUpRunner runner;

    @Before
    public void setUp() throws Exception {
        store = new MemoryNodeStore();
        recordingProvider = new RecordingProvider();
        runner = new CatchUpRunner(store, recordingProvider);

        // Create a base index definition with tracking node
        NodeBuilder builder = store.getRoot().builder();
        builder.child("oak:index").child(INDEX_NAME)
               .setProperty("type", TARGET_TYPE);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    /**
     * Test 1: INITIAL → full traversal (MISSING_NODE as before), tracking advanced.
     */
    @Test
    public void initialTrackingTriggerFullTraversal() throws Exception {
        // Write INITIAL to tracking node
        NodeBuilder b = store.getRoot().builder();
        b.child("oak:index").child(INDEX_NAME)
         .child(CATCH_UP_TRACKING_NODE)
         .setProperty(TARGET_TYPE, CATCH_UP_FROM_START);
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState after = store.getRoot();
        runner.run(after, after, AFTER_CHECKPOINT);

        // Tracking property should be updated to AFTER_CHECKPOINT
        NodeState tracking = store.getRoot().getChildNode("oak:index")
                .getChildNode(INDEX_NAME).getChildNode(CATCH_UP_TRACKING_NODE);
        assertEquals(AFTER_CHECKPOINT, tracking.getString(TARGET_TYPE));

        // Editor was called with MISSING_NODE as before-state
        assertNotNull(recordingProvider.lastBeforeState);
        assertTrue(recordingProvider.lastBeforeState.equals(
                org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE));
    }

    /**
     * Test 2: Valid checkpoint → incremental diff.
     */
    @Test
    public void validCheckpointTriggersIncrementalDiff() throws Exception {
        // Create a real checkpoint in the store
        String catchUpCheckpoint = store.checkpoint(Long.MAX_VALUE);
        assertNotNull(catchUpCheckpoint);

        // Add some content after the checkpoint (to create a diff)
        NodeBuilder b = store.getRoot().builder();
        b.child("content").setProperty("updated", true);
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Write checkpoint to tracking node
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("oak:index").child(INDEX_NAME)
          .child(CATCH_UP_TRACKING_NODE)
          .setProperty(TARGET_TYPE, catchUpCheckpoint);
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState after = store.getRoot();
        
        runner.run(after, after, AFTER_CHECKPOINT);

        // Tracking property advanced to AFTER_CHECKPOINT
        NodeState tracking = store.getRoot().getChildNode("oak:index")
                .getChildNode(INDEX_NAME).getChildNode(CATCH_UP_TRACKING_NODE);
        assertEquals(AFTER_CHECKPOINT, tracking.getString(TARGET_TYPE));

        // Before-state was the checkpoint state, not MISSING_NODE
        assertNotNull(recordingProvider.lastBeforeState);
        assertFalse(recordingProvider.lastBeforeState.equals(
                org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE));
    }

    /**
     * Test 3: Expired/invalid checkpoint → falls back to MISSING_NODE, tracking advanced.
     */
    @Test
    public void expiredCheckpointFallsBackToFullTraversal() throws Exception {
        // Write a bogus checkpoint string that doesn't exist in the store
        NodeBuilder b = store.getRoot().builder();
        b.child("oak:index").child(INDEX_NAME)
         .child(CATCH_UP_TRACKING_NODE)
         .setProperty(TARGET_TYPE, "nonexistent-checkpoint-xyz");
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState after = store.getRoot();
        
        runner.run(after, after, AFTER_CHECKPOINT);

        // Tracking property advanced (fell back to full traversal)
        NodeState tracking = store.getRoot().getChildNode("oak:index")
                .getChildNode(INDEX_NAME).getChildNode(CATCH_UP_TRACKING_NODE);
        assertEquals(AFTER_CHECKPOINT, tracking.getString(TARGET_TYPE));

        // Before-state was MISSING_NODE (fallback)
        assertNotNull(recordingProvider.lastBeforeState);
        assertTrue(recordingProvider.lastBeforeState.equals(
                org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE));
    }

    /**
     * Test 4: Diff failure → tracking property NOT updated.
     */
    @Test
    public void diffFailurePreservesTrackingProperty() throws Exception {
        recordingProvider.shouldFailDiff = true;

        NodeBuilder b = store.getRoot().builder();
        b.child("oak:index").child(INDEX_NAME)
         .child(CATCH_UP_TRACKING_NODE)
         .setProperty(TARGET_TYPE, CATCH_UP_FROM_START);
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState after = store.getRoot();
        
        runner.run(after, after, AFTER_CHECKPOINT);

        // Tracking property stays at INITIAL (not updated)
        NodeState tracking = store.getRoot().getChildNode("oak:index")
                .getChildNode(INDEX_NAME).getChildNode(CATCH_UP_TRACKING_NODE);
        assertNotNull("Tracking property must be preserved on failure",
                tracking.getProperty(TARGET_TYPE));
        assertEquals(CATCH_UP_FROM_START, tracking.getString(TARGET_TYPE));
    }

    /**
     * Test 5: No CatchUpCapable provider → runner does nothing.
     */
    @Test
    public void nonCatchUpCapableProviderIsIgnored() throws Exception {
        // Provider that is NOT CatchUpCapable
        IndexEditorProvider plainProvider = (type, builder, root, callback) -> null;
        CatchUpRunner plainRunner = new CatchUpRunner(store, plainProvider);

        // Write INITIAL
        NodeBuilder b = store.getRoot().builder();
        b.child("oak:index").child(INDEX_NAME)
         .child(CATCH_UP_TRACKING_NODE)
         .setProperty(TARGET_TYPE, CATCH_UP_FROM_START);
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Should not throw
        plainRunner.run(store.getRoot(), store.getRoot(), AFTER_CHECKPOINT);

        // Tracking property unchanged
        NodeState tracking = store.getRoot().getChildNode("oak:index")
                .getChildNode(INDEX_NAME).getChildNode(CATCH_UP_TRACKING_NODE);
        assertEquals(CATCH_UP_FROM_START, tracking.getString(TARGET_TYPE));
    }

    /**
     * Test 6: getIndexEditor returns null for this targetType → skip silently.
     */
    @Test
    public void nullEditorSkipsTarget() throws Exception {
        recordingProvider.returnNullEditor = true;

        NodeBuilder b = store.getRoot().builder();
        b.child("oak:index").child(INDEX_NAME)
         .child(CATCH_UP_TRACKING_NODE)
         .setProperty(TARGET_TYPE, CATCH_UP_FROM_START);
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Should not throw
        runner.run(store.getRoot(), store.getRoot(), AFTER_CHECKPOINT);

        // Tracking property unchanged (no editor → no advance)
        NodeState tracking = store.getRoot().getChildNode("oak:index")
                .getChildNode(INDEX_NAME).getChildNode(CATCH_UP_TRACKING_NODE);
        assertEquals(CATCH_UP_FROM_START, tracking.getString(TARGET_TYPE));
    }

    /**
     * Test 7: No tracking node → skip that index, no NPE.
     */
    @Test
    public void missingTrackingNodeSkipsIndex() {
        // No tracking child — plain index definition with no tracking
        runner.run(store.getRoot(), store.getRoot(), AFTER_CHECKPOINT);
        // If we get here without NPE, the test passes
    }

    /**
     * Test 8: Runner skips indexes whose {@code async} property does not match the lane.
     */
    @Test
    public void skipsIndexOnDifferentLane() throws Exception {
        // Recreate runner with a lane name
        runner = new CatchUpRunner(store, recordingProvider, "async");

        // Index belongs to fulltext-async, not async
        NodeBuilder b = store.getRoot().builder();
        NodeBuilder idx = b.child("oak:index").child(INDEX_NAME);
        idx.setProperty("async", "fulltext-async");
        idx.child(CATCH_UP_TRACKING_NODE).setProperty(TARGET_TYPE, CATCH_UP_FROM_START);
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState after = store.getRoot();
        runner.run(after, after, AFTER_CHECKPOINT);

        // Tracking property must remain unchanged — runner should have skipped this index
        NodeState tracking = store.getRoot().getChildNode("oak:index")
                .getChildNode(INDEX_NAME).getChildNode(CATCH_UP_TRACKING_NODE);
        assertEquals(CATCH_UP_FROM_START, tracking.getString(TARGET_TYPE));
    }

    /**
     * Test 9: Runner processes indexes whose {@code async} property matches the lane.
     */
    @Test
    public void processesIndexOnOwnLane() throws Exception {
        runner = new CatchUpRunner(store, recordingProvider, "async");

        NodeBuilder b = store.getRoot().builder();
        NodeBuilder idx = b.child("oak:index").child(INDEX_NAME);
        idx.setProperty("async", "async");
        idx.child(CATCH_UP_TRACKING_NODE).setProperty(TARGET_TYPE, CATCH_UP_FROM_START);
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState after = store.getRoot();
        runner.run(after, after, AFTER_CHECKPOINT);

        NodeState tracking = store.getRoot().getChildNode("oak:index")
                .getChildNode(INDEX_NAME).getChildNode(CATCH_UP_TRACKING_NODE);
        assertEquals(AFTER_CHECKPOINT, tracking.getString(TARGET_TYPE));
    }

    // ---- Helper classes ----

    /**
     * A CatchUpCapable + IndexEditorProvider that records call arguments
     * and can be configured to fail or return null.
     */
    static class RecordingProvider implements IndexEditorProvider, CatchUpCapable {

        NodeState lastBeforeState;
        boolean shouldFailDiff = false;
        boolean returnNullEditor = false;

        @Override
        public Editor getIndexEditor(String type, NodeBuilder builder,
                                     NodeState root, IndexUpdateCallback callback) {
            // Handle both normal indexing and catch-up
            if (returnNullEditor) return null;
            return new RecordingEditor(this, shouldFailDiff);
        }
    }

    /**
     * An editor that records the before-state it receives and optionally fails.
     */
    static class RecordingEditor implements Editor {

        private final RecordingProvider provider;
        private final boolean shouldFail;

        RecordingEditor(RecordingProvider provider, boolean shouldFail) {
            this.provider = provider;
            this.shouldFail = shouldFail;
        }

        @Override
        public void enter(NodeState before, NodeState after) throws CommitFailedException {
            provider.lastBeforeState = before;
            if (shouldFail) {
                throw new CommitFailedException("Test", 1, "Simulated diff failure");
            }
        }

        @Override public void leave(NodeState before, NodeState after) {}
        @Override public void propertyAdded(org.apache.jackrabbit.oak.api.PropertyState after) {}
        @Override public void propertyChanged(org.apache.jackrabbit.oak.api.PropertyState before, org.apache.jackrabbit.oak.api.PropertyState after) {}
        @Override public void propertyDeleted(org.apache.jackrabbit.oak.api.PropertyState before) {}
        @Override public Editor childNodeAdded(String name, NodeState after) { return null; }
        @Override public Editor childNodeChanged(String name, NodeState before, NodeState after) { return null; }
        @Override public Editor childNodeDeleted(String name, NodeState before) { return null; }
    }
}

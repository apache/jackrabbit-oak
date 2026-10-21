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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.jackrabbit.oak.plugins.index.CatchUpCapable.CATCH_UP_FROM_START;
import static org.apache.jackrabbit.oak.plugins.index.CatchUpCapable.CATCH_UP_TRACKING_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Tests for the catch-up integration points in AsyncIndexUpdate:
 * - detectNewTargets
 * - graduateTargets
 */
public class AsyncIndexUpdateCatchUpTest {

    private static final String INDEX_NAME = "testIndex";
    private static final String TARGET_A = "providerA";
    private static final String TARGET_B = "providerB";

    private MemoryNodeStore store;
    private AsyncIndexUpdate asyncUpdate;

    @Before
    public void setUp() throws Exception {
        store = new MemoryNodeStore();
        IndexEditorProvider noopProvider = (type, builder, root, callback) -> null;
        asyncUpdate = new AsyncIndexUpdate("async", store, noopProvider);
    }

    /**
     * Adding TARGET_B to storeTargets on an existing index should write INITIAL
     * to the tracking node for TARGET_B, and leave TARGET_A untouched.
     */
    @Test
    public void newTargetGetsInitialTrackingProperty() throws Exception {
        // Set up: existing index with storeTargets=[A] at "before" state
        NodeBuilder b = store.getRoot().builder();
        NodeBuilder idx = b.child("oak:index").child(INDEX_NAME);
        idx.setProperty("storeTargets", Collections.singletonList(TARGET_A), Type.STRINGS);
        idx.setProperty("async", "async");
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState before = store.getRoot();

        // Add TARGET_B to storeTargets in "after" state
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("oak:index").child(INDEX_NAME)
          .setProperty("storeTargets", Arrays.asList(TARGET_A, TARGET_B), Type.STRINGS);
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState after = store.getRoot();

        // Call detectNewTargets
        NodeBuilder rootBuilder = store.getRoot().builder();
        asyncUpdate.detectNewTargets(rootBuilder, before, after);

        // TARGET_B should have INITIAL; TARGET_A should have no tracking property
        NodeState tracking = rootBuilder.getNodeState()
                .getChildNode("oak:index").getChildNode(INDEX_NAME)
                .getChildNode(CATCH_UP_TRACKING_NODE);
        assertEquals(CATCH_UP_FROM_START, tracking.getString(TARGET_B));
        assertNull(tracking.getProperty(TARGET_A));
    }

    /**
     * A brand-new index (not in before-state) should NOT get tracking properties —
     * reindex handles new indexes.
     */
    @Test
    public void brandNewIndexIsNotMarkedForCatchUp() throws Exception {
        NodeState before = store.getRoot(); // no index yet

        NodeBuilder b = store.getRoot().builder();
        b.child("oak:index").child(INDEX_NAME)
         .setProperty("storeTargets", Collections.singletonList(TARGET_A), Type.STRINGS);
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState after = store.getRoot();

        NodeBuilder rootBuilder = store.getRoot().builder();
        asyncUpdate.detectNewTargets(rootBuilder, before, after);

        // No tracking node should be created
        assertFalse(rootBuilder.getNodeState()
                .getChildNode("oak:index").getChildNode(INDEX_NAME)
                .hasChildNode(CATCH_UP_TRACKING_NODE));
    }

    /**
     * Target whose tracking checkpoint has the same content state as the current
     * checkpoint should be graduated (property removed).
     */
    @Test
    public void targetCaughtUpIsGraduated() throws Exception {
        // Create a checkpoint with some content
        NodeBuilder b = store.getRoot().builder();
        b.child("content").setProperty("foo", "bar");
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        String checkpoint1 = store.checkpoint(Long.MAX_VALUE);

        // Create tracking property pointing to checkpoint1
        NodeBuilder b2 = store.getRoot().builder();
        NodeBuilder idx = b2.child("oak:index").child(INDEX_NAME);
        idx.setProperty("async", "async");
        idx.child(CATCH_UP_TRACKING_NODE).setProperty(TARGET_A, checkpoint1);
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create another checkpoint with NO content changes (only index changes)
        NodeBuilder b3 = store.getRoot().builder();
        b3.child("oak:index").child(INDEX_NAME).setProperty("someIndexProp", "value");
        store.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        String checkpoint2 = store.checkpoint(Long.MAX_VALUE);

        // Graduate with checkpoint2 - should succeed because content is the same
        NodeState after = store.getRoot();
        NodeBuilder rootBuilder = store.getRoot().builder();
        asyncUpdate.graduateTargets(rootBuilder, checkpoint2, after);

        // Property should be removed (graduated)
        assertFalse(rootBuilder.getNodeState()
                .getChildNode("oak:index").getChildNode(INDEX_NAME)
                .getChildNode(CATCH_UP_TRACKING_NODE)
                .hasProperty(TARGET_A));

        // Tracking node itself should be removed when last property is graduated
        assertFalse(rootBuilder.getNodeState()
                .getChildNode("oak:index").getChildNode(INDEX_NAME)
                .hasChildNode(CATCH_UP_TRACKING_NODE));
    }

    /**
     * Target whose tracking checkpoint has different content than the current
     * checkpoint must NOT be graduated.
     */
    @Test
    public void targetBehindCurrentCheckpointIsNotGraduated() throws Exception {
        // Create checkpoint1 with some content
        NodeBuilder b = store.getRoot().builder();
        b.child("content").setProperty("foo", "bar");
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        String checkpoint1 = store.checkpoint(Long.MAX_VALUE);

        // Create tracking property pointing to checkpoint1
        NodeBuilder b2 = store.getRoot().builder();
        NodeBuilder idx2 = b2.child("oak:index").child(INDEX_NAME);
        idx2.setProperty("async", "async");
        idx2.child(CATCH_UP_TRACKING_NODE).setProperty(TARGET_A, checkpoint1);
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Create checkpoint2 with NEW content (target is behind)
        NodeBuilder b3 = store.getRoot().builder();
        b3.child("content").setProperty("foo", "baz"); // content changed!
        store.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        String checkpoint2 = store.checkpoint(Long.MAX_VALUE);

        // Graduate with checkpoint2 - should NOT succeed because content differs
        NodeState after = store.getRoot();
        NodeBuilder rootBuilder = store.getRoot().builder();
        asyncUpdate.graduateTargets(rootBuilder, checkpoint2, after);

        // Property must remain (not graduated)
        assertEquals(checkpoint1, rootBuilder.getNodeState()
                .getChildNode("oak:index").getChildNode(INDEX_NAME)
                .getChildNode(CATCH_UP_TRACKING_NODE)
                .getString(TARGET_A));
    }

    /**
     * When migrating from legacy 'type' property to 'storeTargets', the target
     * that matches the old 'type' should NOT be marked for catch-up since it
     * was already being indexed. Only truly new targets should get INITIAL.
     */
    @Test
    public void legacyTypeMigrationDoesNotMarkExistingTargetForCatchUp() throws Exception {
        // Set up: existing index with type=TARGET_A (legacy style)
        NodeBuilder b = store.getRoot().builder();
        NodeBuilder idx = b.child("oak:index").child(INDEX_NAME);
        idx.setProperty("type", TARGET_A);
        idx.setProperty("async", "async");
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState before = store.getRoot();

        // Migrate to storeTargets=[TARGET_A, TARGET_B]
        NodeBuilder b2 = store.getRoot().builder();
        NodeBuilder indexDef = b2.child("oak:index").child(INDEX_NAME);
        indexDef.removeProperty("type");
        indexDef.setProperty("storeTargets", Arrays.asList(TARGET_A, TARGET_B), Type.STRINGS);
        indexDef.setProperty("activeTarget", TARGET_A);
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState after = store.getRoot();

        // Call detectNewTargets
        NodeBuilder rootBuilder = store.getRoot().builder();
        asyncUpdate.detectNewTargets(rootBuilder, before, after);

        // Only TARGET_B should have INITIAL; TARGET_A should NOT (it was already indexed)
        NodeState tracking = rootBuilder.getNodeState()
                .getChildNode("oak:index").getChildNode(INDEX_NAME)
                .getChildNode(CATCH_UP_TRACKING_NODE);
        assertEquals(CATCH_UP_FROM_START, tracking.getString(TARGET_B));
        assertNull("TARGET_A was already indexed via 'type' property, should not need catch-up",
                tracking.getProperty(TARGET_A));
    }

    /**
     * detectNewTargets must only process indexes whose {@code async} property
     * contains the current lane name. An index belonging to a different lane
     * must be left untouched even when its storeTargets change.
     */
    @Test
    public void detectNewTargetsSkipsIndexOnDifferentLane() throws Exception {
        // Set up: index with async=fulltext-async, storeTargets=[TARGET_A]
        NodeBuilder b = store.getRoot().builder();
        NodeBuilder idx = b.child("oak:index").child(INDEX_NAME);
        idx.setProperty("storeTargets", Collections.singletonList(TARGET_A), Type.STRINGS);
        idx.setProperty("async", "fulltext-async");
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState before = store.getRoot();

        // Add TARGET_B to storeTargets
        NodeBuilder b2 = store.getRoot().builder();
        b2.child("oak:index").child(INDEX_NAME)
          .setProperty("storeTargets", Arrays.asList(TARGET_A, TARGET_B), Type.STRINGS);
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState after = store.getRoot();

        // asyncUpdate is on lane "async" — should NOT mark this fulltext-async index
        NodeBuilder rootBuilder = store.getRoot().builder();
        asyncUpdate.detectNewTargets(rootBuilder, before, after);

        assertFalse("Index on different lane must not get tracking node",
                rootBuilder.getNodeState()
                        .getChildNode("oak:index").getChildNode(INDEX_NAME)
                        .hasChildNode(CATCH_UP_TRACKING_NODE));
    }

    /**
     * detectNewTargets processes an index whose {@code async} property matches
     * the lane.
     */
    @Test
    public void detectNewTargetsProcessesIndexOnOwnLane() throws Exception {
        NodeBuilder b = store.getRoot().builder();
        NodeBuilder idx = b.child("oak:index").child(INDEX_NAME);
        idx.setProperty("storeTargets", Collections.singletonList(TARGET_A), Type.STRINGS);
        idx.setProperty("async", "async");
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState before = store.getRoot();

        NodeBuilder b2 = store.getRoot().builder();
        b2.child("oak:index").child(INDEX_NAME)
          .setProperty("storeTargets", Arrays.asList(TARGET_A, TARGET_B), Type.STRINGS);
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState after = store.getRoot();

        NodeBuilder rootBuilder = store.getRoot().builder();
        asyncUpdate.detectNewTargets(rootBuilder, before, after);

        assertEquals(CATCH_UP_FROM_START, rootBuilder.getNodeState()
                .getChildNode("oak:index").getChildNode(INDEX_NAME)
                .getChildNode(CATCH_UP_TRACKING_NODE).getString(TARGET_B));
    }

    /**
     * graduateTargets must skip indexes on a different lane.
     */
    @Test
    public void graduateTargetsSkipsIndexOnDifferentLane() throws Exception {
        NodeBuilder b = store.getRoot().builder();
        b.child("content").setProperty("foo", "bar");
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        String checkpoint1 = store.checkpoint(Long.MAX_VALUE);

        NodeBuilder b2 = store.getRoot().builder();
        NodeBuilder idx = b2.child("oak:index").child(INDEX_NAME);
        idx.setProperty("async", "fulltext-async");
        idx.child(CATCH_UP_TRACKING_NODE).setProperty(TARGET_A, checkpoint1);
        store.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        String checkpoint2 = store.checkpoint(Long.MAX_VALUE);
        NodeState after = store.getRoot();
        NodeBuilder rootBuilder = store.getRoot().builder();
        asyncUpdate.graduateTargets(rootBuilder, checkpoint2, after);

        // Must NOT graduate — wrong lane
        assertEquals("Tracking property must remain on different-lane index",
                checkpoint1,
                rootBuilder.getNodeState()
                        .getChildNode("oak:index").getChildNode(INDEX_NAME)
                        .getChildNode(CATCH_UP_TRACKING_NODE).getString(TARGET_A));
    }

    /**
     * After a target graduates (tracking property removed), detectNewTargets should
     * NOT re-create the tracking property on subsequent runs. The target should
     * continue to be indexed normally without catch-up.
     */
    @Test
    public void graduatedTargetIsNotMarkedForCatchUpAgain() throws Exception {
        // Set up: index with storeTargets=[TARGET_A, TARGET_B]
        NodeBuilder b = store.getRoot().builder();
        b.child("oak:index").child(INDEX_NAME)
         .setProperty("storeTargets", Arrays.asList(TARGET_A, TARGET_B), Type.STRINGS);
        store.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState before = store.getRoot();

        // Same state after (no changes to storeTargets)
        NodeState after = before;

        // Call detectNewTargets - should NOT create any tracking properties
        // because storeTargets hasn't changed
        NodeBuilder rootBuilder = store.getRoot().builder();
        asyncUpdate.detectNewTargets(rootBuilder, before, after);

        // No tracking node should be created
        assertFalse("No tracking node should be created when storeTargets hasn't changed",
                rootBuilder.getNodeState()
                        .getChildNode("oak:index").getChildNode(INDEX_NAME)
                        .hasChildNode(CATCH_UP_TRACKING_NODE));
    }
}

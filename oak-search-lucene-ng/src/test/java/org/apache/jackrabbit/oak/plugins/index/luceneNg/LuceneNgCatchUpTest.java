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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.CatchUpCapable;
import org.apache.jackrabbit.oak.plugins.index.CatchUpRunner;
import org.apache.jackrabbit.oak.plugins.index.ContextAwareCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexingContext;
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

import static org.apache.jackrabbit.oak.plugins.index.CatchUpCapable.CATCH_UP_TRACKING_NODE;
import static org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexConstants.TYPE_LUCENE9;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the CatchUpCapable implementation in LuceneNgIndexEditorProvider.
 */
public class LuceneNgCatchUpTest {

    private MemoryNodeStore store;
    private LuceneNgIndexTracker tracker;
    private LuceneNgIndexEditorProvider provider;

    @Before
    public void setUp() throws Exception {
        store = new MemoryNodeStore();
        tracker = new LuceneNgIndexTracker();
        provider = new LuceneNgIndexEditorProvider(tracker);
    }

    @Test
    public void providerImplementsCatchUpCapable() {
        assertTrue(provider instanceof CatchUpCapable);
    }

    /**
     * Normal lane indexing (type != lucene9) is blocked while catch-up is pending.
     */
    @Test
    public void getIndexEditorReturnsNullWhenCatchUpPending() throws Exception {
        NodeBuilder rootBuilder = store.getRoot().builder();
        NodeBuilder definition = buildDefinitionWithStoreTargets(rootBuilder, TYPE_LUCENE9);
        definition.child(CATCH_UP_TRACKING_NODE)
                  .setProperty(TYPE_LUCENE9, CatchUpCapable.CATCH_UP_FROM_START);

        // Call with type="lucene" (the lane's activeTarget) — the null-guard must fire
        Editor editor = provider.getIndexEditor("lucene", definition, store.getRoot(),
                contextCallback("/oak:index/testIndex", false, rootBuilder));

        assertNull("getIndexEditor must return null for non-catch-up type while catch-up is pending", editor);
    }

    /**
     * When no catch-up tracking is present the null-guard does not fire and
     * the provider returns an editor for lucene9.
     */
    @Test
    public void getIndexEditorReturnsEditorWhenNoCatchUpPending() throws Exception {
        NodeBuilder rootBuilder = store.getRoot().builder();
        NodeBuilder definition = buildDefinitionWithStoreTargets(rootBuilder, TYPE_LUCENE9);

        Editor editor = provider.getIndexEditor(TYPE_LUCENE9, definition, store.getRoot(),
                contextCallback("/oak:index/testIndex", false, rootBuilder));

        assertNotNull("getIndexEditor must return an editor when no catch-up is pending", editor);
    }

    /**
     * getIndexEditor returns null when shouldWrite() returns false (wrong type).
     */
    @Test
    public void getIndexEditorReturnsNullWhenShouldWriteFalse() throws Exception {
        NodeBuilder rootBuilder = store.getRoot().builder();
        NodeBuilder definition = rootBuilder.child("oak:index").child("noTargetsDef");
        definition.setProperty("type", "property");

        Editor editor = provider.getIndexEditor(TYPE_LUCENE9, definition, store.getRoot(),
                contextCallback("/oak:index/noTargetsDef", false, rootBuilder));

        assertNull("getIndexEditor must return null when shouldWrite() returns false", editor);
    }

    /**
     * Catch-up call (type=lucene9 while tracking is present) bypasses the null-guard.
     */
    @Test
    public void catchUpCallIgnoresTrackingProperty() throws Exception {
        NodeBuilder rootBuilder = store.getRoot().builder();
        NodeBuilder definition = buildDefinitionWithStoreTargets(rootBuilder, TYPE_LUCENE9);
        definition.child(CATCH_UP_TRACKING_NODE)
                  .setProperty(TYPE_LUCENE9, CatchUpCapable.CATCH_UP_FROM_START);

        // type=lucene9 is treated as the catch-up call — must not be blocked
        Editor editor = provider.getIndexEditor(TYPE_LUCENE9, definition, store.getRoot(),
                contextCallback("/oak:index/testIndex", false, rootBuilder));

        assertNotNull("catch-up call (type=lucene9) must not be blocked by tracking property", editor);
    }

    /**
     * Full integration: existing content → storeTargets extended → null-guard fires →
     * CatchUpRunner indexes historical content → tracking advanced → graduation removes tracking.
     */
    @Test
    public void fullCatchUpFlow() throws Exception {
        // T0: existing content
        NodeBuilder b0 = store.getRoot().builder();
        b0.child("content").child("page1").setProperty("title", "Hello");
        b0.child("content").child("page2").setProperty("title", "World");
        store.merge(b0, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // T1: add lucene9 to storeTargets; simulate INITIAL tracking property
        NodeBuilder b1 = store.getRoot().builder();
        b1.child("oak:index").child("testIndex")
          .setProperty("storeTargets", Arrays.asList("lucene", TYPE_LUCENE9), Type.STRINGS)
          .setProperty("activeTarget", "lucene");
        b1.child("oak:index").child("testIndex")
          .child(CATCH_UP_TRACKING_NODE)
          .setProperty(TYPE_LUCENE9, CatchUpCapable.CATCH_UP_FROM_START);
        store.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // T2: lane runs — null-guard fires because tracking property is present
        NodeBuilder laneRootBuilder = store.getRoot().builder();
        NodeBuilder defBuilder = laneRootBuilder.child("oak:index").child("testIndex");
        Editor laneEditor = provider.getIndexEditor("lucene", defBuilder, store.getRoot(),
                contextCallback("/oak:index/testIndex", false, laneRootBuilder));
        assertNull("Lane must skip lucene9 while catch-up tracking is present", laneEditor);

        // T3: CatchUpRunner runs — full traversal
        String afterCheckpoint = store.checkpoint(Long.MAX_VALUE);
        NodeState after = store.retrieve(afterCheckpoint);
        assertNotNull(after);

        CatchUpRunner runner = new CatchUpRunner(store, provider);
        runner.run(store.getRoot(), after, afterCheckpoint);

        // Tracking property must now be afterCheckpoint
        NodeState tracking = store.getRoot()
                .getChildNode("oak:index").getChildNode("testIndex")
                .getChildNode(CATCH_UP_TRACKING_NODE);
        assertEquals("Tracking property must be advanced to afterCheckpoint",
                afterCheckpoint, tracking.getString(TYPE_LUCENE9));

        // T4: graduation — remove tracking property
        NodeBuilder b4 = store.getRoot().builder();
        b4.child("oak:index").child("testIndex")
          .child(CATCH_UP_TRACKING_NODE).removeProperty(TYPE_LUCENE9);
        store.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // T5: null-guard no longer fires
        assertFalse("Tracking property must be absent after graduation",
                store.getRoot().getChildNode("oak:index").getChildNode("testIndex")
                        .getChildNode(CATCH_UP_TRACKING_NODE).hasProperty(TYPE_LUCENE9));
    }

    // ---- Helpers ----

    private NodeBuilder buildDefinitionWithStoreTargets(NodeBuilder root, String target) {
        NodeBuilder def = root.child("oak:index").child("testIndex");
        def.setProperty("storeTargets", Collections.singletonList(target), Type.STRINGS);
        def.setProperty("activeTarget", target);
        return def;
    }

    private ContextAwareCallback contextCallback(String indexPath, boolean reindex,
                                                 NodeBuilder rootBuilder) {
        IndexingContext ctx = mock(IndexingContext.class);
        when(ctx.getIndexPath()).thenReturn(indexPath);
        when(ctx.isReindexing()).thenReturn(reindex);

        ContextAwareCallback callback = mock(ContextAwareCallback.class);
        when(callback.getIndexingContext()).thenReturn(ctx);
        return callback;
    }
}

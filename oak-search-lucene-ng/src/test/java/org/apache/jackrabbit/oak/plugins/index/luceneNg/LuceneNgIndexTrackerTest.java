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

import org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgIndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.*;

public class LuceneNgIndexTrackerTest {

    private NodeState root;
    private NodeBuilder builder;

    @Before
    public void setup() {
        root = INITIAL_CONTENT;
        builder = root.builder();

        // Create index definition
        NodeBuilder oakIndex = builder.child("oak:index");
        NodeBuilder testIndex = oakIndex.child("testIndex");
        testIndex.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        testIndex.setProperty("async", "async");
    }

    @Test
    public void testTrackerCreation() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        assertNotNull(tracker);
    }

    @Test
    public void testUpdate() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        NodeState after = builder.getNodeState();

        tracker.update(after);
        // Should not throw exception
    }

    @Test
    public void testGetIndexNode() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        NodeState after = builder.getNodeState();
        tracker.update(after);

        // With no index data written yet, openIndex() returns null (FulltextIndexTracker's
        // documented "index can be null" contract), so the path is not yet held in the
        // tracker's map of live index nodes...
        assertFalse(tracker.getIndexNodePaths().contains("/oak:index/testIndex"));
        // ...and acquireIndexNode returns null until index data is written.
        assertNull(tracker.acquireIndexNode("/oak:index/testIndex"));
    }

    @Test
    public void testGetNonExistentIndex() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        NodeState after = builder.getNodeState();
        tracker.update(after);

        LuceneNgIndexNode indexNode = tracker.acquireIndexNode("/oak:index/nonexistent");
        assertNull(indexNode);
    }

    /**
     * Regression test for the tracker-lookup half of the fix in OAK-12089 Task A1: unlike the
     * pre-Task-A1 tracker, which only ever called {@code root.getChildNode("oak:index")} (a
     * hardcoded top-level lookup) and so could never resolve an index below it, the shared
     * {@code FulltextIndexTracker}'s {@code findIndexNode} walks the given path
     * segment-by-segment with no depth restriction. This proves {@link LuceneNgIndexTracker
     * #acquireIndexNode(String)} now resolves a {@code lucene9} index at any nesting depth, once
     * given its exact path.
     *
     * <p>This does NOT prove that a real query can use such an index: {@code
     * LuceneNgQueryIndexProvider#getQueryIndexes()} still only enumerates direct children of
     * {@code /oak:index} and so would never hand this deeper path to the tracker in the first
     * place (see README, "Index discovery").</p>
     */
    @Test
    public void discoversIndexDefinitionsNestedDeeperThanOakIndex() throws Exception {
        // Index definition nested two levels deeper than the conventional
        // "/oak:index/<name>": it lives under "/content/dam/oak:index/damAssets".
        NodeBuilder nestedRootBuilder = INITIAL_CONTENT.builder();
        NodeBuilder defnBuilder = nestedRootBuilder.child("content").child("dam")
                .child("oak:index").child("damAssets");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.noAsync();
        idb.indexRule("nt:unstructured").property("title").propertyIndex();
        // IndexDefinitionBuilder defaults "type" to "fulltext"; the tracker only recognizes
        // "lucene9", so it must be set explicitly (same as LuceneNgIndexEditorProviderTest).
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Index one node (under /content/dam, the subtree the nested definition covers) so the
        // definition has real Lucene segment data (hasSearcher() == true); otherwise
        // acquireIndexNode() would legitimately return null regardless of nesting depth, and the
        // test would prove nothing. Driven as a real commit so the nested index is populated the
        // same way production does.
        NodeBuilder asset = nestedRootBuilder.child("content").child("dam").child("asset1");
        asset.setProperty("jcr:primaryType", "nt:unstructured");
        asset.setProperty("title", "hello");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(nestedRootBuilder.getNodeState());

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(indexed);

        LuceneNgIndexNode indexNode = tracker.acquireIndexNode("/content/dam/oak:index/damAssets");
        assertNotNull(
                "Tracker should resolve a lucene9 index at any nesting depth once given its exact path",
                indexNode);
    }
}

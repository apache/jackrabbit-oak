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

import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
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
     * Proves that {@link LuceneNgIndexTracker#acquireIndexNode(String)} resolves a {@code lucene9}
     * index at any nesting depth, once given its exact path: the shared {@code
     * FulltextIndexTracker}'s {@code findIndexNode} walks the given path segment-by-segment with
     * no depth restriction.
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

    /**
     * Black-box proof that {@code tracker.update()} reopens the index node in response to a real
     * content change, driven entirely through the real editor/context commit path
     * ({@link LuceneNgEditorCommitUtil}) rather than a hand-built {@code LuceneNgIndexNode}.
     *
     * <p>The inherited {@code FulltextIndexTracker} default {@code isUpdateNeeded} only inspects
     * {@code :status}/{@code :index-definition} at the index node itself — see the tracker's class
     * javadoc for why that is sufficient to catch every real content change.</p>
     */
    @Test
    public void updateAfterRealContentChangeReopensTheIndexNode() throws Exception {
        String indexPath = "/oak:index/testIndex";

        NodeBuilder rootBuilder = INITIAL_CONTENT.builder();
        NodeBuilder defnBuilder = rootBuilder.child("oak:index").child("testIndex");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.noAsync();
        idb.indexRule("nt:unstructured").property("title").propertyIndex();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder node1 = rootBuilder.child("node1");
        node1.setProperty("jcr:primaryType", "nt:unstructured");
        node1.setProperty("title", "hello");

        // Commit 1: reindex, indexing node1. Real editor/context path -> real Lucene segments.
        NodeState afterFirst = LuceneNgEditorCommitUtil.reindex(rootBuilder.getNodeState());

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(afterFirst);
        LuceneNgIndexNode firstNode = tracker.acquireIndexNode(indexPath);
        assertNotNull("Index node must be resolvable after the first commit", firstNode);
        int firstIndexNodeId = firstNode.getIndexNodeId();
        firstNode.release();

        // Commit 2: a real content change (a second indexed node), via the same real commit path.
        NodeBuilder b2 = afterFirst.builder();
        NodeBuilder node2 = b2.child("node2");
        node2.setProperty("jcr:primaryType", "nt:unstructured");
        node2.setProperty("title", "world");
        NodeState afterSecond = LuceneNgEditorCommitUtil.commit(afterFirst, b2.getNodeState());

        tracker.update(afterSecond);
        LuceneNgIndexNode secondNode = tracker.acquireIndexNode(indexPath);
        assertNotNull("Index node must be resolvable after the second commit", secondNode);
        int secondIndexNodeId = secondNode.getIndexNodeId();
        secondNode.release();

        assertNotEquals(
                "A real content change must cause tracker.update() to reopen the index node "
                        + "(new getIndexNodeId()), proving the tracker detected the change",
                firstIndexNodeId, secondIndexNodeId);
    }

    /**
     * Verifies that a reindex matching <b>zero</b> documents still reopens the index node.
     *
     * <p>{@code LuceneNgFulltextIndexWriter}'s {@code indexUpdated} dirty-tracking flag is
     * <em>not</em> what makes this case safe: on reindex, {@code LuceneNgFulltextIndexWriterFactory}
     * opens the {@code IndexWriter} with {@code OpenMode.CREATE} and {@code close()} always calls
     * {@code indexWriter.commit()} regardless of whether any document was ever written, so if the
     * reindex matches nothing, {@code updateDocument}/{@code deleteDocumentTree}/{@code
     * deleteDocument} are never called and {@code indexUpdated} stays {@code false}. The actual
     * safety net is upstream, in {@code oak-core}'s {@code IndexUpdate.removeIndexState()}, which
     * unconditionally strips the index definition's hidden child nodes (including {@code :status}
     * and {@code :index-definition}) before every reindex — independent of anything this module's
     * writer does — so the inherited {@code FulltextIndexTracker} default's {@code
     * isStatusChanged}/{@code isIndexDefinitionChanged} checks still see a real diff.</p>
     */
    @Test
    public void reindexMatchingZeroDocumentsStillReopensTheIndexNode() throws Exception {
        String indexPath = "/oak:index/testIndex";

        NodeBuilder rootBuilder = INITIAL_CONTENT.builder();
        NodeBuilder defnBuilder = rootBuilder.child("oak:index").child("testIndex");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.noAsync();
        idb.indexRule("nt:unstructured").property("title").propertyIndex();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        NodeBuilder node1 = rootBuilder.child("node1");
        node1.setProperty("jcr:primaryType", "nt:unstructured");
        node1.setProperty("title", "hello");

        // Commit 1: reindex, indexing node1 -- real Lucene segment data exists for the index.
        NodeState afterFirst = LuceneNgEditorCommitUtil.reindex(rootBuilder.getNodeState());
        assertEquals("Sanity check: commit 1 must actually index something",
                1, LuceneNgEditorCommitUtil.numDocs(afterFirst, indexPath));

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(afterFirst);
        LuceneNgIndexNode firstNode = tracker.acquireIndexNode(indexPath);
        assertNotNull("Index node must be resolvable after the first commit", firstNode);
        int firstIndexNodeId = firstNode.getIndexNodeId();
        firstNode.release();

        // Commit 2: force a reindex (explicit "reindex" flag, mirroring an admin-triggered
        // reindex or a rule/config change) whose matching content set is empty -- node1 (the
        // only content that ever matched the rule) is removed in the same commit.
        NodeBuilder b2 = afterFirst.builder();
        b2.child("node1").remove();
        b2.child("oak:index").child("testIndex").setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        NodeState afterSecond = LuceneNgEditorCommitUtil.commit(afterFirst, b2.getNodeState());
        assertEquals("Sanity check: commit 2 must be a reindex that matches nothing, or this "
                        + "test doesn't exercise the edge case it's meant to",
                0, LuceneNgEditorCommitUtil.numDocs(afterSecond, indexPath));

        tracker.update(afterSecond);
        LuceneNgIndexNode secondNode = tracker.acquireIndexNode(indexPath);
        try {
            assertNotNull("Index node must still be resolvable after a reindex-to-empty "
                    + "(the definition and its storage still exist, just with no documents)", secondNode);
            assertNotEquals(
                    "A reindex matching zero documents must still cause tracker.update() to reopen "
                            + "the index node (new getIndexNodeId()): the old segments are stale and must "
                            + "not keep being served, even though LuceneNgFulltextIndexWriter's own "
                            + "indexUpdated flag never got set to true for this commit",
                    firstIndexNodeId, secondNode.getIndexNodeId());
        } finally {
            if (secondNode != null) {
                secondNode.release();
            }
        }
    }
}

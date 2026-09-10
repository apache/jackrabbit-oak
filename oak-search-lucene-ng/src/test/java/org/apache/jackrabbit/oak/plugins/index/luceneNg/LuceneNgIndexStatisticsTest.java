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
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * The shared {@code oak-search} planner's {@code FulltextIndexPlanner#defaultPlan()} returns
 * {@code null} -- no plan, ever -- whenever {@code indexNode.getIndexStatistics() == null}, so
 * {@link LuceneNgIndexNode#getIndexStatistics()} must return a real, non-null
 * {@link org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.LuceneNgIndexStatistics}.
 *
 * <p>Lives in this package (not {@code .internal}, alongside the class under test) so it can reuse
 * {@link LuceneNgEditorCommitUtil}, the established real-commit test helper, which is
 * package-private to this package -- matching {@link LuceneNgIndexNodeTest} and
 * {@link LuceneNgIndexTrackerTest}, which do the same for {@code internal.LuceneNgIndexNode}.</p>
 */
public class LuceneNgIndexStatisticsTest {

    private static final String INDEX_PATH = "/oak:index/testIndex";

    @Test
    public void numDocsReflectsActualIndexedDocumentCount() throws Exception {
        int n = 5;

        NodeBuilder rootBuilder = INITIAL_CONTENT.builder();
        NodeBuilder defnBuilder = rootBuilder.child("oak:index").child("testIndex");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.noAsync();
        idb.indexRule("nt:unstructured").property("title").propertyIndex();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        for (int i = 0; i < n; i++) {
            NodeBuilder node = rootBuilder.child("node" + i);
            node.setProperty("jcr:primaryType", "nt:unstructured");
            node.setProperty("title", "hello " + i);
        }

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(rootBuilder.getNodeState());

        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        tracker.update(indexed);
        LuceneNgIndexNode node = tracker.acquireIndexNode(INDEX_PATH);
        assertNotNull("Index node must be resolvable once data has been indexed", node);
        try {
            IndexStatistics stats = node.getIndexStatistics();
            assertNotNull("getIndexStatistics() must return a real object once the index has a "
                    + "searcher -- a null result here makes FulltextIndexPlanner#defaultPlan() "
                    + "refuse to produce a plan", stats);
            assertEquals("numDocs() must reflect the actual number of indexed documents",
                    n, stats.numDocs());
        } finally {
            node.release();
        }
    }

    @Test
    public void getIndexStatisticsReturnsNullWhenNoDataYet() throws Exception {
        NodeBuilder rootBuilder = INITIAL_CONTENT.builder();
        NodeBuilder defnBuilder = rootBuilder.child("oak:index").child("testIndex");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.noAsync();
        idb.indexRule("nt:unstructured").property("title").propertyIndex();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);

        // Never committed/indexed: no lucene9 storage node exists yet, so the LuceneNgIndexNode
        // built directly over this definition has hasSearcher() == false.
        NodeState root = rootBuilder.getNodeState();
        NodeState indexState = root.getChildNode("oak:index").getChildNode("testIndex");
        LuceneNgIndexNode node = new LuceneNgIndexNode(INDEX_PATH, root, indexState);

        assertFalse("Sanity check: this node must not have a searcher yet, or this test doesn't "
                + "exercise the not-yet-populated case it's meant to", node.hasSearcher());
        assertNull("getIndexStatistics() must still return null when the index has no data yet",
                node.getIndexStatistics());
    }
}

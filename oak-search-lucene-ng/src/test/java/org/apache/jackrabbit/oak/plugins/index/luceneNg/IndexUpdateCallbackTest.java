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

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;

/**
 * Verifies that the Lucene 9 index editor emits exactly one index update per successfully indexed
 * document.
 *
 * <p>Task B4 note: the old assertions counted {@code IndexUpdateCallback} invocations by
 * constructing {@code LuceneNgIndexEditor} with a hand-supplied callback. The collapsed editor no
 * longer owns that callback — the shared framework fires {@code context.indexUpdate()} once per
 * written document, one-to-one with the callback fire. So the observable equivalent, asserted here
 * after a real commit, is the number of documents that end up in the index (and their
 * addition/removal). This preserves the original intent — "one update per indexed document" — while
 * asserting on the committed index rather than the editor's internal callback wiring.</p>
 */
public class IndexUpdateCallbackTest {

    private static final String IDX = "/oak:index/test";

    private static IndexDefinitionBuilder lucene9(NodeBuilder rootBuilder) {
        NodeBuilder defnBuilder = rootBuilder.child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.noAsync();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        return idb;
    }

    private static NodeBuilder node(NodeBuilder root, String name, String primaryType) {
        NodeBuilder b = root.child(name);
        b.setProperty("jcr:primaryType", primaryType);
        return b;
    }

    @Test
    public void oneUpdatePerIndexedDocument() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("title").propertyIndex();

        node(root, "page1", "nt:unstructured").setProperty("title", "alpha");
        node(root, "page2", "nt:unstructured").setProperty("title", "beta");
        // Node whose type has no rule -> must not be indexed (no update).
        node(root, "page3", "nt:folder");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals("exactly one indexed document per matching node (page3 excluded)",
                2, LuceneNgEditorCommitUtil.numDocs(indexed, IDX));
    }

    @Test
    public void noUpdateWhenNoPropertiesIndexed() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("title").propertyIndex();
        // Node matches the rule's type but carries no configured property.
        node(root, "page1", "nt:unstructured").setProperty("description", "no title here");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals("no indexed document when no configured property matched",
                0, LuceneNgEditorCommitUtil.numDocs(indexed, IDX));
    }

    @Test
    public void documentRemovedOnChildNodeDeletion() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("title").propertyIndex();
        node(root, "page1", "nt:unstructured").setProperty("title", "alpha");

        NodeState base = LuceneNgEditorCommitUtil.reindex(root.getNodeState());

        NodeBuilder b2 = base.builder();
        b2.child("page1").remove();
        NodeState indexed = LuceneNgEditorCommitUtil.commit(base, b2.getNodeState());

        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals("deleting the node must remove its indexed document", 0,
                    searcher.search(new TermQuery(new Term(FieldNames.PATH, "/page1")), 10).totalHits.value);
        }
    }
}

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
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.jupiter.api.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Functional tests for the Lucene 9 index editor covering real-world indexing scenarios, driven
 * through real commits via {@link LuceneNgIndexEditorProvider} (see {@link LuceneNgEditorCommitUtil}).
 */
public class IndexingFunctionalTest {

    private static final String IDX = "/oak:index/test";

    private static IndexDefinitionBuilder luceneNg(NodeBuilder rootBuilder) {
        NodeBuilder defnBuilder = rootBuilder.child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.noAsync();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE_NG);
        return idb;
    }

    private static NodeBuilder node(NodeBuilder parent, String name) {
        NodeBuilder b = parent.child(name);
        b.setProperty("jcr:primaryType", "nt:unstructured");
        return b;
    }

    @Test
    public void emptyNodeWithOnlyHiddenPropertiesIsNotIndexed() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        luceneNg(root).indexRule("nt:unstructured").property("title").propertyIndex();
        // Only a hidden property -> no visible primaryType, no indexable property.
        root.child("emptyNode").setProperty(":primaryType", "nt:base");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals(0,
                LuceneNgEditorCommitUtil.numDocs(indexed, IDX), "node with only hidden properties must not be indexed and must not error");
    }

    @Test
    public void deepHierarchyIsIndexedWithoutError() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        luceneNg(root).indexRule("nt:unstructured").property("title").propertyIndex();

        NodeBuilder current = node(root, "level0");
        current.setProperty("title", "Level 0");
        for (int i = 1; i < 10; i++) {
            current = node(current, "level" + i);
            current.setProperty("title", "Level " + i);
        }

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals(10,
                LuceneNgEditorCommitUtil.numDocs(indexed, IDX), "every node in the 10-level hierarchy must be indexed");
    }

    @Test
    public void largePropertyValueIsHandledWithoutError() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        // nodeScopeIndex (fulltext, tokenized) has no single-term length limit, unlike a StringField.
        luceneNg(root).indexRule("nt:unstructured").property("largeText").nodeScopeIndex();

        StringBuilder largeText = new StringBuilder(100 * 1024);
        for (int i = 0; i < 100 * 1024; i++) {
            largeText.append((char) ('a' + (i % 26)));
        }
        node(root, "largeNode").setProperty("largeText", largeText.toString());

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals(1, searcher.search(new TermQuery(new Term(FieldNames.PATH, "/largeNode")), 10).totalHits.value,
                    "large fulltext value must be indexed without OOM/errors");
        }
    }

    @Test
    public void specialCharactersAreHandledWithoutError() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        luceneNg(root).indexRule("nt:unstructured")
                .property("unicode").propertyIndex()
                .property("newlines").propertyIndex()
                .property("quotes").propertyIndex()
                .property("symbols").propertyIndex();

        NodeBuilder n = node(root, "specialNode");
        n.setProperty("unicode", "Hello 世界 🌍");
        n.setProperty("newlines", "Line 1\nLine 2\nLine 3");
        n.setProperty("quotes", "She said \"hello\" and 'goodbye'");
        n.setProperty("symbols", "!@#$%^&*()_+-={}[]|\\:;<>?,./");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals(1,
                LuceneNgEditorCommitUtil.numDocs(indexed, IDX), "special characters must be indexed without errors");
    }

    @Test
    public void mixedPropertyTypesAreHandledWithoutError() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        luceneNg(root).indexRule("nt:unstructured")
                .property("stringProp").propertyIndex()
                .property("longProp").propertyIndex()
                .property("booleanProp").propertyIndex()
                .property("doubleProp").propertyIndex();

        NodeBuilder n = node(root, "mixedNode");
        n.setProperty("stringProp", "Some text");
        n.setProperty("longProp", 12345L);
        n.setProperty("booleanProp", true);
        n.setProperty("doubleProp", 3.14159);

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals(1,
                LuceneNgEditorCommitUtil.numDocs(indexed, IDX), "mixed property types must be indexed without errors");
    }

    @Test
    public void hiddenPropertiesAreExcluded() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        luceneNg(root).indexRule("nt:unstructured").property("normalProp").propertyIndex();

        NodeBuilder n = node(root, "hiddenPropsNode");
        n.setProperty("normalProp", "This should be indexed");
        n.setProperty(":hiddenProp", "This should be skipped");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals(1, searcher.search(new TermQuery(new Term(FieldNames.PATH, "/hiddenPropsNode")), 10).totalHits.value);
            assertNull(reader.leaves().get(0).reader().getFieldInfos().fieldInfo(":hiddenProp"),
                    "hidden ':hiddenProp' must never become a Lucene field");
        }
    }

    @Test
    public void nodeUpdateReplacesDocument() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        luceneNg(root).indexRule("nt:unstructured").property("title").propertyIndex();
        node(root.child("content"), "page1").setProperty("title", "Original Title");

        NodeState base = LuceneNgEditorCommitUtil.reindex(root.getNodeState());

        NodeBuilder b2 = base.builder();
        b2.child("content").child("page1").setProperty("title", "Updated Title");
        NodeState indexed = LuceneNgEditorCommitUtil.commit(base, b2.getNodeState());

        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new TermQuery(new Term(FieldNames.PATH, "/content/page1")), 10);
            assertEquals(1, hits.totalHits.value, "Should have exactly one document, not a duplicate");
        }
    }

    @Test
    public void nodeDeletionRemovesDocument() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        luceneNg(root).indexRule("nt:unstructured").property("title").propertyIndex();
        NodeBuilder contentNode = root.child("content");
        node(contentNode, "keep").setProperty("title", "Keep me");
        node(contentNode, "remove").setProperty("title", "Delete me");

        NodeState base = LuceneNgEditorCommitUtil.reindex(root.getNodeState());

        NodeBuilder b2 = base.builder();
        b2.child("content").child("remove").remove();
        NodeState indexed = LuceneNgEditorCommitUtil.commit(base, b2.getNodeState());

        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs keepHits = searcher.search(new TermQuery(new Term(FieldNames.PATH, "/content/keep")), 10);
            TopDocs removeHits = searcher.search(new TermQuery(new Term(FieldNames.PATH, "/content/remove")), 10);
            assertEquals(1, keepHits.totalHits.value, "keep should still be indexed");
            assertEquals(0, removeHits.totalHits.value, "remove should be deleted");
        }
    }

    @Test
    public void manyPropertiesAreHandledWithoutError() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        luceneNg(root).indexRule("nt:unstructured").property("prop.*", true).propertyIndex();

        NodeBuilder n = node(root, "manyPropsNode");
        for (int i = 0; i < 100; i++) {
            n.setProperty("prop" + i, "Value for property " + i);
        }

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals(1, searcher.search(new TermQuery(new Term(FieldNames.PATH, "/manyPropsNode")), 10).totalHits.value,
                    "a node with 100 indexed properties must be indexed without errors");
        }
    }
}

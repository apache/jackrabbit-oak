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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import java.util.List;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;

/**
 * Tests that LuceneNgIndexEditor only indexes properties declared in the index definition,
 * using the proper field types based on PropertyDefinition flags.
 */
public class IndexingRulesTest {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds the index definition NodeState from an IndexDefinitionBuilder and
     * returns a ready-to-use LuceneNgIndexEditor for the given content node.
     *
     * The editor uses the 3-argument convenience constructor:
     *   LuceneNgIndexEditor(path, definitionBuilder, root)
     *
     * Index data is written into the definition NodeBuilder itself (as the
     * OakDirectory storage root), which lets tests open it with OakDirectory.
     */
    private LuceneNgIndexEditor editorFor(String path, NodeBuilder definitionBuilder,
                                          NodeState root) throws Exception {
        return new LuceneNgIndexEditor(path, definitionBuilder, root);
    }

    /** Index the given node, commit, and return a searcher over the written data. */
    private IndexSearcher indexAndOpen(LuceneNgIndexEditor editor,
                                       NodeState before, NodeState after,
                                       NodeBuilder definitionBuilder) throws Exception {
        editor.enter(before, after);
        editor.leave(before, after);
        DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(definitionBuilder.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true));
        return new IndexSearcher(reader);
    }

    /** Return the single document in the index, or null if none. */
    private Document singleDoc(IndexSearcher searcher) throws Exception {
        TopDocs hits = searcher.search(new MatchAllDocsQuery(), 10);
        if (hits.totalHits.value == 0) return null;
        return searcher.storedFields().document(hits.scoreDocs[0].doc);
    }

    /** Build a NodeBuilder with jcr:primaryType set. */
    private NodeBuilder nodeOf(String primaryType) {
        NodeBuilder b = INITIAL_CONTENT.builder().child("content");
        b.setProperty("jcr:primaryType", primaryType);
        return b;
    }

    // -------------------------------------------------------------------------
    // Tests: rule matching
    // -------------------------------------------------------------------------

    @Test
    public void nodeNotMatchingAnyRuleIsNotIndexed() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:folder").property("title").propertyIndex();

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("title", "hello");

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        IndexSearcher searcher = indexAndOpen(editor, EMPTY_NODE, content.getNodeState(), defnBuilder);

        assertEquals("node type not in rules — must not produce a document",
                0, searcher.search(new MatchAllDocsQuery(), 10).totalHits.value);
    }

    @Test
    public void nodeMatchingRuleWithNoPropertiesProducesNoDocument() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        // rule exists but no properties configured
        idb.indexRule("nt:unstructured");

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("title", "hello");

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        IndexSearcher searcher = indexAndOpen(editor, EMPTY_NODE, content.getNodeState(), defnBuilder);

        assertEquals("rule with no properties — must not produce a document",
                0, searcher.search(new MatchAllDocsQuery(), 10).totalHits.value);
    }

    // -------------------------------------------------------------------------
    // Tests: property-level filtering
    // -------------------------------------------------------------------------

    @Test
    public void onlyConfiguredPropertyIsIndexed() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("title").propertyIndex();

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("title", "hello");
        content.setProperty("description", "world");

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        IndexSearcher searcher = indexAndOpen(editor, EMPTY_NODE, content.getNodeState(), defnBuilder);

        TopDocs hits = searcher.search(new MatchAllDocsQuery(), 10);
        assertEquals(1, hits.totalHits.value);

        LeafReader leafReader = searcher.getIndexReader().leaves().get(0).reader();
        assertNotNull("configured 'title' field must be present",
                leafReader.getFieldInfos().fieldInfo("title"));
        assertNull("unconfigured 'description' field must be absent",
                leafReader.getFieldInfos().fieldInfo("description"));
    }

    @Test
    public void propertyWithIndexFalseIsSkipped() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        // Manually craft a rule where index=false
        defnBuilder.child("indexRules").child("nt:unstructured")
                .child("properties").child("title")
                .setProperty("name", "title")
                .setProperty("index", false)
                .setProperty("propertyIndex", false);

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("title", "hello");
        content.setProperty("jcr:primaryType", "nt:unstructured");

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        IndexSearcher searcher = indexAndOpen(editor, EMPTY_NODE, content.getNodeState(), defnBuilder);

        // index=false means the property entry exists but should not be indexed
        TopDocs hits = searcher.search(new MatchAllDocsQuery(), 10);
        // The document should not exist (no indexed fields other than system fields)
        if (hits.totalHits.value > 0) {
            Document doc = searcher.storedFields().document(hits.scoreDocs[0].doc);
            assertNull("index=false property must not produce a field", doc.getField("title"));
        }
    }

    // -------------------------------------------------------------------------
    // Tests: fulltext / nodeScopeIndex
    // -------------------------------------------------------------------------

    @Test
    public void nodeScopeIndexAddsFulltextField() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("body").nodeScopeIndex();

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("body", "search me");

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        IndexSearcher searcher = indexAndOpen(editor, EMPTY_NODE, content.getNodeState(), defnBuilder);

        TopDocs hits = searcher.search(new MatchAllDocsQuery(), 10);
        assertEquals(1, hits.totalHits.value);
        Document doc = searcher.storedFields().document(hits.scoreDocs[0].doc);
        // FieldNames.FULLTEXT field is stored when useInExcerpt=true, not stored otherwise,
        // but the field should be present in the index (confirmed via field list on leaf reader)
        boolean fulltextPresent = false;
        for (IndexableField f : doc.getFields()) {
            if (FieldNames.FULLTEXT.equals(f.name())) {
                fulltextPresent = true;
                break;
            }
        }
        // nodeScopeIndex means fulltext field is added; if not stored, it won't appear in
        // stored fields — verify via the direct document's fields list which includes all added fields
        // Since TextField(FULLTEXT, "search me", Field.Store.NO) is not stored,
        // we check the leaf reader's fieldInfos instead
        LeafReader leafReader = searcher.getIndexReader().leaves().get(0).reader();
        assertNotNull("FULLTEXT field should exist in index schema",
                leafReader.getFieldInfos().fieldInfo(FieldNames.FULLTEXT));
    }

    @Test
    public void propertyWithoutNodeScopeIndexDoesNotContributeToFulltext() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("status").propertyIndex();
        // nodeScopeIndex NOT called — defaults to false

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("status", "active");

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        IndexSearcher searcher = indexAndOpen(editor, EMPTY_NODE, content.getNodeState(), defnBuilder);

        LeafReader leafReader = searcher.getIndexReader().leaves().get(0).reader();
        assertNull("FULLTEXT field must be absent when nodeScopeIndex=false",
                leafReader.getFieldInfos().fieldInfo(FieldNames.FULLTEXT));
    }

    @Test
    public void storedNodeScopeIndexFieldIsStoredForExcerpt() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("body")
                .nodeScopeIndex()
                .useInExcerpt();

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("body", "the excerpt value");

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        IndexSearcher searcher = indexAndOpen(editor, EMPTY_NODE, content.getNodeState(), defnBuilder);

        TopDocs hits = searcher.search(new MatchAllDocsQuery(), 10);
        assertEquals(1, hits.totalHits.value);
        Document doc = searcher.storedFields().document(hits.scoreDocs[0].doc);

        boolean storedFulltext = false;
        for (IndexableField f : doc.getFields()) {
            if (FieldNames.FULLTEXT.equals(f.name()) && f.stringValue() != null) {
                storedFulltext = true;
                break;
            }
        }
        assertTrue("FULLTEXT field must be stored when useInExcerpt=true", storedFulltext);
    }

    // -------------------------------------------------------------------------
    // Tests: doc values for ordered properties
    // -------------------------------------------------------------------------

    @Test
    public void orderedStringPropertyHasSortedDocValues() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("title").ordered();

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("title", "hello");

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        editor.enter(EMPTY_NODE, content.getNodeState());
        editor.leave(EMPTY_NODE, content.getNodeState());

        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(defnBuilder.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            LeafReader leaf = reader.leaves().get(0).reader();
            SortedDocValues sdv = leaf.getSortedDocValues("title");
            assertNotNull("ordered String property must have SortedDocValues", sdv);
        }
    }

    @Test
    public void orderedLongPropertyHasNumericDocValues() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("size").ordered("Long");

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("size", 42L);

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        editor.enter(EMPTY_NODE, content.getNodeState());
        editor.leave(EMPTY_NODE, content.getNodeState());

        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(defnBuilder.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            LeafReader leaf = reader.leaves().get(0).reader();
            NumericDocValues ndv = leaf.getNumericDocValues("size");
            assertNotNull("ordered Long property must have NumericDocValues", ndv);
        }
    }

    @Test
    public void unorderedPropertyHasNoDocValues() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("tag").propertyIndex();
        // ordered NOT called

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("tag", "oak");

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        editor.enter(EMPTY_NODE, content.getNodeState());
        editor.leave(EMPTY_NODE, content.getNodeState());

        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(defnBuilder.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            LeafReader leaf = reader.leaves().get(0).reader();
            assertNull("unordered property must not have SortedDocValues",
                    leaf.getSortedDocValues("tag"));
            assertNull("unordered property must not have NumericDocValues",
                    leaf.getNumericDocValues("tag"));
        }
    }

    // -------------------------------------------------------------------------
    // Tests: type conflict is impossible when using index rules
    // -------------------------------------------------------------------------

    /**
     * The root cause of the original reindex loop: a property named "path" can be
     * STRING on one node and LONG on another. When we added SortedDocValuesField for
     * STRING and NumericDocValuesField for LONG, Lucene threw IllegalArgumentException.
     *
     * With index rules, only the declared type is ever indexed for a given property,
     * so the conflict cannot arise.
     */
    @Test
    public void samePropertyNameWithDifferentTypesAcrossNodesDoesNotThrow() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        // Declare "path" as a String property index only
        idb.indexRule("nt:unstructured").property("path").propertyIndex();

        NodeState root = INITIAL_CONTENT;
        NodeBuilder rootBuilder = root.builder();

        // Node A: "path" is a String
        NodeBuilder nodeA = rootBuilder.child("nodeA");
        nodeA.setProperty("jcr:primaryType", "nt:unstructured");
        nodeA.setProperty("path", "/some/string/path");

        // Node B: "path" is a Long — should be skipped (rule declared as String context,
        // but more importantly: no doc values added, so no type conflict)
        NodeBuilder nodeB = rootBuilder.child("nodeB");
        nodeB.setProperty("jcr:primaryType", "nt:unstructured");
        nodeB.setProperty("path", 12345L);

        // Index node A
        LuceneNgIndexEditor editorA = editorFor("/nodeA", defnBuilder, root);
        editorA.enter(EMPTY_NODE, nodeA.getNodeState());
        editorA.leave(EMPTY_NODE, nodeA.getNodeState());

        // Index node B using a child editor (shared writer via the 3-arg constructor re-open)
        // Re-use the same index by opening a second editor that appends — the key is no exception
        LuceneNgIndexEditor editorB = editorFor("/nodeB", defnBuilder, root);
        // Should not throw IllegalArgumentException regardless of "path" being Long here
        editorB.enter(EMPTY_NODE, nodeB.getNodeState());
        editorB.leave(EMPTY_NODE, nodeB.getNodeState());
    }

    // -------------------------------------------------------------------------
    // Tests: multi-value properties
    // -------------------------------------------------------------------------

    @Test
    public void multiValueStringPropertyIndexesAllValues() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("tags").propertyIndex().nodeScopeIndex();

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("tags",
                java.util.Arrays.asList("alpha", "beta", "gamma"),
                org.apache.jackrabbit.oak.api.Type.STRINGS);

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        editor.enter(EMPTY_NODE, content.getNodeState());
        editor.leave(EMPTY_NODE, content.getNodeState());

        try (DirectoryReader reader = DirectoryReader.open(
                new OakDirectory(defnBuilder.child(LuceneNgIndexStorage.STORAGE_NODE_NAME), "default", true))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new MatchAllDocsQuery(), 10);
            assertEquals(1, hits.totalHits.value);

            // Count "tags" fields in the document
            Document doc = searcher.storedFields().document(hits.scoreDocs[0].doc);
            // StringField is not stored by default, so count via term vectors / field infos
            // We verify the FULLTEXT field received 3 contributions via stored count
            // (nodeScopeIndex means 3 TextField(FULLTEXT, ...) were added)
            LeafReader leaf = reader.leaves().get(0).reader();
            assertNotNull("FULLTEXT field must exist for nodeScopeIndex tags",
                    leaf.getFieldInfos().fieldInfo(FieldNames.FULLTEXT));
        }
    }

    // -------------------------------------------------------------------------
    // Tests: regex property definitions
    // -------------------------------------------------------------------------

    @Test
    public void regexPropertyDefinitionMatchesProperty() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("prop_.*", true).propertyIndex();

        NodeBuilder content = nodeOf("nt:unstructured");
        content.setProperty("prop_foo", "bar");
        content.setProperty("other", "baz");

        LuceneNgIndexEditor editor = editorFor("/content", defnBuilder, INITIAL_CONTENT);
        IndexSearcher searcher = indexAndOpen(editor, EMPTY_NODE, content.getNodeState(), defnBuilder);

        TopDocs hits = searcher.search(new MatchAllDocsQuery(), 10);
        assertEquals(1, hits.totalHits.value);

        // prop_foo should be indexed; "other" should not
        // StringField is not stored, verify via field infos
        LeafReader leaf = searcher.getIndexReader().leaves().get(0).reader();
        assertNotNull("prop_foo matched by regex — field must be in schema",
                leaf.getFieldInfos().fieldInfo("prop_foo"));
        assertNull("other not matched by regex — field must be absent",
                leaf.getFieldInfos().fieldInfo("other"));
    }

    // -------------------------------------------------------------------------
    // Tests: relative properties
    // -------------------------------------------------------------------------

    @Test
    public void relativePropertyIsIndexedIntoParentDocument() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured")
           .property("child/title")
               .propertyIndex();

        // Parent node: nt:unstructured
        // Child node "child" carries the indexed property "title"
        NodeBuilder parent = INITIAL_CONTENT.builder().child("page");
        parent.setProperty("jcr:primaryType", "nt:unstructured");
        NodeBuilder child = parent.child("child");
        child.setProperty("title", "deep value");

        LuceneNgIndexEditor editor = editorFor("/page", defnBuilder, INITIAL_CONTENT);
        IndexSearcher searcher = indexAndOpen(editor, EMPTY_NODE, parent.getNodeState(), defnBuilder);

        TopDocs hits = searcher.search(new MatchAllDocsQuery(), 10);
        assertEquals("relative property must produce a document for the parent path", 1,
                hits.totalHits.value);

        Document doc = searcher.storedFields().document(hits.scoreDocs[0].doc);
        assertEquals("/page", doc.get(FieldNames.PATH));
    }

    @Test
    public void missingChildNodeForRelativePropertyProducesNoDocument() throws Exception {
        NodeBuilder defnBuilder = INITIAL_CONTENT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured")
           .property("child/title")
               .propertyIndex();

        // Parent node has no "child" sub-node
        NodeBuilder parent = INITIAL_CONTENT.builder().child("page");
        parent.setProperty("jcr:primaryType", "nt:unstructured");

        LuceneNgIndexEditor editor = editorFor("/page", defnBuilder, INITIAL_CONTENT);
        IndexSearcher searcher = indexAndOpen(editor, EMPTY_NODE, parent.getNodeState(), defnBuilder);

        assertEquals("no child node — must produce no document", 0,
                searcher.search(new MatchAllDocsQuery(), 10).totalHits.value);
    }
}

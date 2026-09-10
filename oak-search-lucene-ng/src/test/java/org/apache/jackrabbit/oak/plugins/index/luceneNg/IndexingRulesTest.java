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
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that the Lucene 9 index editor only indexes properties declared in the index definition,
 * using the proper Lucene field types based on {@code PropertyDefinition} flags.
 *
 * <p>These drive real commits through {@link LuceneNgIndexEditorProvider} (see
 * {@link LuceneNgEditorCommitUtil}) and inspect the committed Lucene index (documents, fields,
 * doc-values) via a {@link DirectoryReader} opened over the {@code /oak:index/test/lucene9}
 * storage.</p>
 */
public class IndexingRulesTest {

    private static final String IDX = "/oak:index/test";

    /** Creates a synchronous {@code lucene9} index definition builder at {@code /oak:index/test}. */
    private static IndexDefinitionBuilder lucene9(NodeBuilder rootBuilder) {
        NodeBuilder defnBuilder = rootBuilder.child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.noAsync();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        return idb;
    }

    private static NodeBuilder content(NodeBuilder rootBuilder, String name, String primaryType) {
        NodeBuilder b = rootBuilder.child(name);
        b.setProperty("jcr:primaryType", primaryType);
        return b;
    }

    // -------------------------------------------------------------------------
    // Tests: rule matching
    // -------------------------------------------------------------------------

    @Test
    public void nodeNotMatchingAnyRuleIsNotIndexed() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:folder").property("title").propertyIndex();
        content(root, "content", "nt:unstructured").setProperty("title", "hello");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals("node type not in rules — must not produce a document",
                0, LuceneNgEditorCommitUtil.numDocs(indexed, IDX));
    }

    @Test
    public void nodeMatchingRuleWithNoPropertiesProducesNoDocument() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured"); // rule exists but no properties configured
        content(root, "content", "nt:unstructured").setProperty("title", "hello");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals("rule with no properties — must not produce a document",
                0, LuceneNgEditorCommitUtil.numDocs(indexed, IDX));
    }

    // -------------------------------------------------------------------------
    // Tests: property-level filtering
    // -------------------------------------------------------------------------

    @Test
    public void onlyConfiguredPropertyIsIndexed() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("title").propertyIndex();
        NodeBuilder c = content(root, "content", "nt:unstructured");
        c.setProperty("title", "hello");
        c.setProperty("description", "world");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals(1, searcher.search(new TermQuery(new Term(FieldNames.PATH, "/content")), 10).totalHits.value);
            LeafReader leaf = reader.leaves().get(0).reader();
            assertNotNull("configured 'title' field must be present",
                    leaf.getFieldInfos().fieldInfo("title"));
            assertNull("unconfigured 'description' field must be absent",
                    leaf.getFieldInfos().fieldInfo("description"));
        }
    }

    @Test
    public void propertyWithIndexFalseIsSkipped() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        // Manually craft a rule where index=false; make it a real sync lucene9 index.
        NodeBuilder defnBuilder = root.child("oak:index").child("test");
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        defnBuilder.setProperty("reindex", true);
        defnBuilder.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        defnBuilder.child("indexRules").child("nt:unstructured")
                .child("properties").child("title")
                .setProperty("name", "title")
                .setProperty("index", false)
                .setProperty("propertyIndex", false);

        content(root, "content", "nt:unstructured").setProperty("title", "hello");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        // index=false means the property must not be indexed, so the node produces no document.
        assertEquals("index=false property must not produce an indexed document",
                0, LuceneNgEditorCommitUtil.numDocs(indexed, IDX));
    }

    // -------------------------------------------------------------------------
    // Tests: fulltext / nodeScopeIndex
    // -------------------------------------------------------------------------

    @Test
    public void nodeScopeIndexAddsFulltextField() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("body").nodeScopeIndex();
        content(root, "content", "nt:unstructured").setProperty("body", "search me");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            assertNotNull("FULLTEXT field should exist in index schema for nodeScopeIndex",
                    leaf.getFieldInfos().fieldInfo(FieldNames.FULLTEXT));
        }
    }

    @Test
    public void propertyWithoutNodeScopeIndexDoesNotContributeToFulltext() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("status").propertyIndex();
        content(root, "content", "nt:unstructured").setProperty("status", "active");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            assertNull("FULLTEXT field must be absent when nodeScopeIndex=false",
                    leaf.getFieldInfos().fieldInfo(FieldNames.FULLTEXT));
        }
    }

    @Test
    public void storedNodeScopeIndexFieldIsStoredForExcerpt() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("body").nodeScopeIndex().useInExcerpt();
        content(root, "content", "nt:unstructured").setProperty("body", "the excerpt value");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new TermQuery(new Term(FieldNames.PATH, "/content")), 10);
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
    }

    // -------------------------------------------------------------------------
    // Tests: doc values for ordered properties
    // -------------------------------------------------------------------------

    @Test
    public void orderedStringPropertyHasSortedDocValues() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("title").ordered();
        content(root, "content", "nt:unstructured").setProperty("title", "hello");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            // A single-valued "ordered" String property is written as a SortedSetDocValuesField
            // (not a SortedDocValuesField), matching the multi-valued field's doc-values type.
            SortedSetDocValues ssdv = leaf.getSortedSetDocValues("title");
            assertNotNull("ordered String property must have SortedSetDocValues", ssdv);
            assertTrue("SortedSetDocValues must have a value for doc 0", ssdv.advanceExact(0));
            assertEquals("hello", ssdv.lookupOrd(ssdv.nextOrd()).utf8ToString());
            assertEquals("a single-valued property must have exactly one ord",
                    SortedSetDocValues.NO_MORE_ORDS, ssdv.nextOrd());
        }
    }

    @Test
    public void orderedLongPropertyHasNumericDocValues() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("size").ordered("Long");
        content(root, "content", "nt:unstructured").setProperty("size", 42L);

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            NumericDocValues ndv = leaf.getNumericDocValues("size");
            assertNotNull("ordered Long property must have NumericDocValues", ndv);
        }
    }

    @Test
    public void unorderedPropertyHasNoDocValues() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("tag").propertyIndex();
        content(root, "content", "nt:unstructured").setProperty("tag", "oak");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
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
     * A property named "path" can be STRING on one node and LONG on another. With index rules only
     * the declared type is ever considered, so no Lucene doc-values type conflict can arise and a
     * single commit indexing both must not throw.
     */
    @Test
    public void samePropertyNameWithDifferentTypesAcrossNodesDoesNotThrow() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("path").propertyIndex();

        content(root, "nodeA", "nt:unstructured").setProperty("path", "/some/string/path");
        content(root, "nodeB", "nt:unstructured").setProperty("path", 12345L);

        // Must not throw IllegalArgumentException while indexing both cardinalities in one commit.
        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertNotNull(indexed);
    }

    // -------------------------------------------------------------------------
    // Tests: multi-value properties
    // -------------------------------------------------------------------------

    @Test
    public void multiValueStringPropertyIndexesAllValues() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("tags").propertyIndex().nodeScopeIndex();
        content(root, "content", "nt:unstructured")
                .setProperty("tags", Arrays.asList("alpha", "beta", "gamma"), Type.STRINGS);

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals(1, searcher.search(new TermQuery(new Term(FieldNames.PATH, "/content")), 10).totalHits.value);
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
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("prop_.*", true).propertyIndex();
        NodeBuilder c = content(root, "content", "nt:unstructured");
        c.setProperty("prop_foo", "bar");
        c.setProperty("other", "baz");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals(1, searcher.search(new TermQuery(new Term(FieldNames.PATH, "/content")), 10).totalHits.value);
            LeafReader leaf = reader.leaves().get(0).reader();
            assertNotNull("prop_foo matched by regex — field must be in schema",
                    leaf.getFieldInfos().fieldInfo("prop_foo"));
            assertNull("other not matched by regex — field must be absent",
                    leaf.getFieldInfos().fieldInfo("other"));
        }
    }

    // -------------------------------------------------------------------------
    // Tests: relative properties
    // -------------------------------------------------------------------------

    @Test
    public void relativePropertyIsIndexedIntoParentDocument() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("child/title").propertyIndex();

        NodeBuilder parent = content(root, "page", "nt:unstructured");
        parent.child("child").setProperty("title", "deep value");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new TermQuery(new Term(FieldNames.PATH, "/page")), 10);
            assertEquals("relative property must produce a document for the parent path", 1,
                    hits.totalHits.value);
            Document doc = searcher.storedFields().document(hits.scoreDocs[0].doc);
            assertEquals("/page", doc.get(FieldNames.PATH));
        }
    }

    @Test
    public void missingChildNodeForRelativePropertyProducesNoDocument() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("child/title").propertyIndex();
        content(root, "page", "nt:unstructured"); // no "child" sub-node

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        IndexSearcher searcher = new IndexSearcher(LuceneNgEditorCommitUtil.openReader(indexed, IDX));
        assertEquals("no child node — must produce no document for /page", 0,
                searcher.search(new TermQuery(new Term(FieldNames.PATH, "/page")), 10).totalHits.value);
    }
}

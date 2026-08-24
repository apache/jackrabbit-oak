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

import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Verifies type-safe field creation in the Lucene 9 index editor.
 *
 * <p>When an index definition declares a property with an explicit type (Long, Double, Date), the
 * Lucene field type must be driven by that declaration — not by the actual Oak property type. This
 * prevents Lucene 9's field-schema consistency constraint from firing when different nodes store the
 * same property with different value types.</p>
 *
 * <p>Task B4 migrated these to drive real commits through {@link LuceneNgIndexEditorProvider} (see
 * {@link LuceneNgEditorCommitUtil}); assertions still inspect the committed index via a
 * {@link DirectoryReader}.</p>
 */
public class TypeSafeIndexingTest {

    private static final String IDX = "/oak:index/test";

    private static IndexDefinitionBuilder lucene9(NodeBuilder rootBuilder) {
        NodeBuilder defnBuilder = rootBuilder.child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.noAsync();
        defnBuilder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        return idb;
    }

    private static NodeBuilder node(NodeBuilder rootBuilder, String name) {
        NodeBuilder b = rootBuilder.child(name);
        b.setProperty("jcr:primaryType", "nt:unstructured");
        return b;
    }

    @Test
    public void stringValueWithDeclaredLongTypeIsConvertedToLongPoint() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("size").propertyIndex().type("Long");
        node(root, "asset").setProperty("size", "1234"); // stored as String, declared Long

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals("Convertible string '1234' with Long declaration must produce a document",
                1, LuceneNgEditorCommitUtil.numDocs(indexed, IDX));
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            FieldInfo fi = leaf.getFieldInfos().fieldInfo("size");
            assertNotNull("'size' field must be present", fi);
            assertEquals("declared Long must produce a point field (NONE index options)",
                    IndexOptions.NONE, fi.getIndexOptions());
        }
    }

    @Test
    public void unconvertibleStringWithDeclaredLongTypeIsSkipped() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("size").propertyIndex().type("Long");
        node(root, "asset").setProperty("size", "not-a-number");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        // The unparseable value is skipped: no LongPoint is written, so the property is not
        // queryable as a Long. (The shared framework marks the node "dirty" because the declared
        // property is present, so a path-only document may exist; the observable contract is that
        // nothing is indexed under "size", which the range query below asserts.)
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            assertEquals("Un-parseable string with declared Long type must not be queryable as a Long",
                    0, searcher.search(LongPoint.newRangeQuery("size", Long.MIN_VALUE, Long.MAX_VALUE), 10).totalHits.value);
        }
    }

    @Test
    public void stringValueWithDeclaredDoubleTypeIsConvertedToDoublePoint() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("score").propertyIndex().type("Double");
        node(root, "asset").setProperty("score", "3.14");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals("String '3.14' with declared Double type must produce a document",
                1, LuceneNgEditorCommitUtil.numDocs(indexed, IDX));
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            FieldInfo fi = leaf.getFieldInfos().fieldInfo("score");
            assertNotNull("'score' field must be present", fi);
            assertEquals("declared Double must produce a point field (NONE index options)",
                    IndexOptions.NONE, fi.getIndexOptions());
        }
    }

    @Test
    public void longValueWithDefaultStringTypeProducesStringField() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        // No .type() call -> PropertyDefinition.isTypeDefined() == false -> defaults to STRING
        lucene9(root).indexRule("nt:unstructured").property("count").propertyIndex();
        node(root, "node").setProperty("count", 42L);

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals("LONG value with no declared type must still produce a document",
                1, LuceneNgEditorCommitUtil.numDocs(indexed, IDX));
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            FieldInfo fi = leaf.getFieldInfos().fieldInfo("count");
            assertNotNull("'count' field must be present", fi);
            assertEquals("undeclared type defaults to String field (DOCS index options)",
                    IndexOptions.DOCS, fi.getIndexOptions());
        }
    }

    /**
     * The exact AEM scenario: dam:size is declared Long but some nodes store it as a String. A full
     * traversal (all nodes indexed in one commit) must not throw and must index all convertible values.
     */
    @Test
    public void fullTraversalWithMixedValueTypesForDeclaredLongDoesNotThrow() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("dam:size").propertyIndex().type("Long");

        for (int i = 0; i < 10; i++) {
            NodeBuilder n = node(root, "asset" + i);
            if (i % 2 == 0) {
                n.setProperty("dam:size", (long) (i + 1) * 1000L);          // Long
            } else {
                n.setProperty("dam:size", String.valueOf((i + 1) * 1000L)); // String
            }
        }

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals("All 10 nodes must be indexed (all string values are parseable longs)",
                10, LuceneNgEditorCommitUtil.numDocs(indexed, IDX));
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            FieldInfo fi = leaf.getFieldInfos().fieldInfo("dam:size");
            assertNotNull("dam:size field must exist", fi);
            assertEquals("All dam:size documents must use point fields (NONE)",
                    IndexOptions.NONE, fi.getIndexOptions());
        }
    }

    @Test
    public void booleanValueWithNoExplicitTypeProducesStringField() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("active").propertyIndex();
        node(root, "node").setProperty("active", true);

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals(1, LuceneNgEditorCommitUtil.numDocs(indexed, IDX));
        try (DirectoryReader reader = LuceneNgEditorCommitUtil.openReader(indexed, IDX)) {
            LeafReader leaf = reader.leaves().get(0).reader();
            FieldInfo fi = leaf.getFieldInfos().fieldInfo("active");
            assertNotNull("'active' boolean field must be present", fi);
            assertEquals("boolean must produce a StringField (DOCS index options)",
                    IndexOptions.DOCS, fi.getIndexOptions());
        }
    }

    /**
     * A commit that indexes a node must complete cleanly (any RuntimeException surfaced from Lucene
     * is wrapped as CommitFailedException, not leaked). This just verifies the normal path commits.
     */
    @Test
    public void indexingCompletesWithoutUncheckedException() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();
        lucene9(root).indexRule("nt:unstructured").property("title").propertyIndex();
        node(root, "node").setProperty("title", "hello");

        NodeState indexed = LuceneNgEditorCommitUtil.reindex(root.getNodeState());
        assertEquals(1, LuceneNgEditorCommitUtil.numDocs(indexed, IDX));
    }
}

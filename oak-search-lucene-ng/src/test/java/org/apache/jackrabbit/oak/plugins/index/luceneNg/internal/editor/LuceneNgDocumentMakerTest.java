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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.editor;

import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Direct-hook unit tests for {@link LuceneNgDocumentMaker}: build a minimal index definition
 * with {@link IndexDefinitionBuilder}, drive a single node through {@code makeDocument}, and
 * assert the resulting Lucene {@link Document} fields — no repository / editor context needed.
 *
 * <p>The end-to-end proof that aggregation folds a child's text into the parent's fulltext via a
 * real commit belongs to Task B4 (once {@code LuceneNgIndexEditorContext} exists to build a
 * {@code LuceneNgDocumentMaker} through the full framework), and is intentionally not here.</p>
 */
public class LuceneNgDocumentMakerTest {

    private static final NodeState ROOT = INITIAL_CONTENT;

    private LuceneNgIndexDefinition definitionWith(IndexDefinitionBuilder idb, NodeBuilder defnBuilder) {
        return new LuceneNgIndexDefinition(ROOT, defnBuilder.getNodeState(), "/oak:index/test");
    }

    private NodeState contentNode(String... props) {
        NodeBuilder b = ROOT.builder().child("content");
        b.setProperty("jcr:primaryType", "nt:unstructured");
        for (int i = 0; i + 1 < props.length; i += 2) {
            b.setProperty(props[i], props[i + 1]);
        }
        return b.getNodeState();
    }

    @Test
    public void facetPropertyIsWrittenAsSortedSetDocValuesFacetField() throws Exception {
        NodeBuilder defnBuilder = ROOT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("tags").propertyIndex().facets();

        LuceneNgIndexDefinition def = definitionWith(idb, defnBuilder);
        IndexingRule rule = def.getApplicableIndexingRule("nt:unstructured");
        assertNotNull(rule);

        // FacetsConfig registered the way the editor context builds it for facet properties.
        FacetsConfig facetsConfig = new FacetsConfig();
        facetsConfig.setIndexFieldName("tags", FieldNames.createFacetFieldName("tags"));
        facetsConfig.setMultiValued("tags", true);

        LuceneNgDocumentMaker maker = new LuceneNgDocumentMaker(null, def, rule, "/content", facetsConfig);
        Document doc = maker.makeDocument(contentNode("tags", "red"));

        assertNotNull("a facet-enabled property must produce a document", doc);
        // finalizeDoc runs FacetsConfig.build, materializing the SortedSetDocValuesFacetField into
        // the configured facet index field.
        assertNotNull("facet field must be present after FacetsConfig.build",
                doc.getField(FieldNames.createFacetFieldName("tags")));
    }

    @Test
    public void nodeScopeIndexedStringIsAddedToFulltext() throws Exception {
        NodeBuilder defnBuilder = ROOT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").property("body").nodeScopeIndex();

        LuceneNgIndexDefinition def = definitionWith(idb, defnBuilder);
        IndexingRule rule = def.getApplicableIndexingRule("nt:unstructured");
        assertNotNull(rule);

        LuceneNgDocumentMaker maker = new LuceneNgDocumentMaker(null, def, rule, "/content", new FacetsConfig());
        Document doc = maker.makeDocument(contentNode("body", "search me"));

        assertNotNull("a nodeScopeIndex property must produce a document", doc);
        boolean found = false;
        for (IndexableField f : doc.getFields(FieldNames.FULLTEXT)) {
            if ("search me".equals(f.stringValue())) {
                found = true;
                break;
            }
        }
        assertTrue("nodeScopeIndex property value must be added to the :fulltext field", found);
    }

    @Test
    public void nodeNameIndexingWritesTheStrippedLocalName() throws Exception {
        NodeBuilder defnBuilder = ROOT.builder().child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured").indexNodeName();

        LuceneNgIndexDefinition def = definitionWith(idb, defnBuilder);
        IndexingRule rule = def.getApplicableIndexingRule("nt:unstructured");
        assertNotNull(rule);
        assertTrue("rule must have node-name indexing enabled", rule.isNodeNameIndexed());

        // Path leaf is "jcr:foo"; the framework strips the namespace prefix before indexNodeName.
        LuceneNgDocumentMaker maker = new LuceneNgDocumentMaker(null, def, rule, "/a/jcr:foo", new FacetsConfig());
        Document doc = maker.makeDocument(contentNode());

        assertNotNull("node-name indexing must produce a document", doc);
        IndexableField nodeName = doc.getField(FieldNames.NODE_NAME);
        assertNotNull("a :nodeName field must be present", nodeName);
        assertEquals("local name must be indexed with the namespace prefix stripped",
                "foo", nodeName.stringValue());
    }
}

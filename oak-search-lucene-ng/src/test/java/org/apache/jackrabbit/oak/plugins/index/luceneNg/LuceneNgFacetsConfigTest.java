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
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;

/**
 * Verifies that FacetsConfig is built once per indexing session and correctly
 * handles multi-valued facet properties across multiple documents.
 */
public class LuceneNgFacetsConfigTest {

    @Test
    public void multivaluedFacetPropertiesIndexedCorrectlyAcrossDocuments() throws Exception {
        NodeBuilder root = INITIAL_CONTENT.builder();

        // Index definition with a multi-valued facet property
        NodeBuilder defnBuilder = root.child("oak:index").child("test");
        IndexDefinitionBuilder idb = new IndexDefinitionBuilder(defnBuilder);
        idb.indexRule("nt:unstructured")
           .property("color").propertyIndex().facets();

        // Three nodes: two with multi-valued color, one with single-valued
        NodeBuilder node1 = root.child("node1");
        node1.setProperty("jcr:primaryType", "nt:unstructured");
        node1.setProperty("color", Arrays.asList("red", "blue"), Type.STRINGS);

        NodeBuilder node2 = root.child("node2");
        node2.setProperty("jcr:primaryType", "nt:unstructured");
        node2.setProperty("color", Arrays.asList("green", "red"), Type.STRINGS);

        NodeBuilder node3 = root.child("node3");
        node3.setProperty("jcr:primaryType", "nt:unstructured");
        node3.setProperty("color", "green", Type.STRING);

        NodeState rootState = root.getNodeState();

        LuceneNgIndexEditor editor = new LuceneNgIndexEditor("/", defnBuilder, rootState);
        editor.childNodeAdded("node1", node1.getNodeState()).enter(EMPTY_NODE, node1.getNodeState());
        editor.childNodeAdded("node2", node2.getNodeState()).enter(EMPTY_NODE, node2.getNodeState());
        editor.childNodeAdded("node3", node3.getNodeState()).enter(EMPTY_NODE, node3.getNodeState());
        editor.leave(EMPTY_NODE, rootState);

        // Read back the index and verify facet counts
        NodeState indexState = root.getNodeState().getChildNode("oak:index").getChildNode("test");
        NodeState storageState = LuceneNgIndexStorage.storageState(indexState);
        NodeBuilder storageBuilder = root.child("oak:index").child("test")
                .child(LuceneNgIndexStorage.STORAGE_NODE_NAME);

        String luceneFacetField = FieldNames.createFacetFieldName("color");

        try (OakDirectory dir = new OakDirectory(storageBuilder, "test", true);
             DirectoryReader reader = DirectoryReader.open(dir)) {

            assertEquals("Three documents must be indexed", 3, reader.numDocs());

            IndexSearcher searcher = new IndexSearcher(reader);
            FacetsCollector fc = new FacetsCollector();
            FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, fc);

            DefaultSortedSetDocValuesReaderState state =
                    new DefaultSortedSetDocValuesReaderState(reader, luceneFacetField);
            Facets facets = new SortedSetDocValuesFacetCounts(state, fc);
            FacetResult result = facets.getTopChildren(10, "color");

            assertNotNull("Facet result for 'color' must not be null", result);

            java.util.Map<String, Integer> counts = new java.util.HashMap<>();
            for (org.apache.lucene.facet.LabelAndValue lv : result.labelValues) {
                counts.put(lv.label, lv.value.intValue());
            }

            assertEquals("'red' appears in node1 and node2", 2, (int) counts.getOrDefault("red", 0));
            assertEquals("'green' appears in node2 and node3", 2, (int) counts.getOrDefault("green", 0));
            assertEquals("'blue' appears only in node1", 1, (int) counts.getOrDefault("blue", 0));
        }
    }
}

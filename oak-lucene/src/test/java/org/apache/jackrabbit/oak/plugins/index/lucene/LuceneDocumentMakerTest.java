/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.FieldInfo;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LuceneDocumentMakerTest {
    private final NodeState root = INITIAL_CONTENT;

    @Test
    public void excludeSingleProperty() throws Exception {
        LuceneIndexDefinitionBuilder builder = new LuceneIndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("foo")
                .propertyIndex()
                .analyzed()
                .valueExcludedPrefixes("/jobs");

        LuceneIndexDefinition defn = LuceneIndexDefinition.newLuceneBuilder(root, builder.build(), "/foo").build();
        LuceneDocumentMaker docMaker = new LuceneDocumentMaker(defn,
                defn.getApplicableIndexingRule("nt:base"), "/x");

        NodeBuilder test = EMPTY_NODE.builder();
        test.setProperty("foo", "bar");

        assertNotNull(docMaker.makeDocument(test.getNodeState()));

        test.setProperty("foo", "/jobs/a");
        assertNull(docMaker.makeDocument(test.getNodeState()));

        test.setProperty("foo", List.of("/a", "/jobs/a"), Type.STRINGS);
        assertNotNull(docMaker.makeDocument(test.getNodeState()));

        test.setProperty("foo", List.of("/jobs/a"), Type.STRINGS);
        assertNull(docMaker.makeDocument(test.getNodeState()));
    }

    @Test
    public void similarityTagMaxLengthFiltering() throws Exception {
        LuceneIndexDefinitionBuilder builder = new LuceneIndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("jcr:primaryType")
                .propertyIndex();
        builder.indexRule("nt:base")
                .property("tag")
                .similarityTags(true);

        builder.getBuilderTree().setProperty(FulltextIndexConstants.MAX_TAG_LENGTH, 10);

        LuceneIndexDefinition defn = LuceneIndexDefinition.newLuceneBuilder(root, builder.build(), "/foo").build();
        LuceneDocumentMaker docMaker = new LuceneDocumentMaker(defn,
                defn.getApplicableIndexingRule("nt:base"), "/x");

        NodeBuilder test = EMPTY_NODE.builder();
        test.setProperty("tag", "short");
        Document doc = docMaker.makeDocument(test.getNodeState());
        assertNotNull(doc);
        assertEquals("short", doc.get(FieldNames.SIMILARITY_TAGS));

        test = EMPTY_NODE.builder();
        test.setProperty("tag", "exactly10!");
        doc = docMaker.makeDocument(test.getNodeState());
        assertNotNull(doc);
        assertEquals("exactly10!", doc.get(FieldNames.SIMILARITY_TAGS));

        test = EMPTY_NODE.builder();
        test.setProperty("tag", "this is too long");
        doc = docMaker.makeDocument(test.getNodeState());
        assertNotNull(doc);
        assertNull(doc.get(FieldNames.SIMILARITY_TAGS));
    }

    @Test
    public void similarityTagCountLimit() throws Exception {
        LuceneIndexDefinitionBuilder builder = new LuceneIndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("jcr:primaryType")
                .propertyIndex();
        builder.indexRule("nt:base")
                .property("tag1")
                .similarityTags(true);
        builder.indexRule("nt:base")
                .property("tag2")
                .similarityTags(true);
        builder.indexRule("nt:base")
                .property("tag3")
                .similarityTags(true);
        builder.indexRule("nt:base")
                .property("tag4")
                .similarityTags(true);
        builder.indexRule("nt:base")
                .property("tag5")
                .similarityTags(true);

        builder.getBuilderTree().setProperty(FulltextIndexConstants.MAX_SIMILARITY_TAGS_COUNT, 3);

        LuceneIndexDefinition defn = LuceneIndexDefinition.newLuceneBuilder(root, builder.build(), "/foo").build();
        LuceneDocumentMaker docMaker = new LuceneDocumentMaker(defn,
                defn.getApplicableIndexingRule("nt:base"), "/x");

        NodeBuilder test = EMPTY_NODE.builder();
        test.setProperty("tag1", "value1");
        test.setProperty("tag2", "value2");
        test.setProperty("tag3", "value3");
        test.setProperty("tag4", "value4");
        test.setProperty("tag5", "value5");
        Document doc = docMaker.makeDocument(test.getNodeState());
        assertNotNull(doc);

        String[] tags = doc.getValues(FieldNames.SIMILARITY_TAGS);
        assertEquals(3, tags.length);
    }

    // OAK-12372: a facet property value long enough that dimension + value + 1 exceeds
    // Lucene's FacetLabel.MAX_CATEGORY_PATH_LENGTH (8191 bytes) should be ignored (with a warning) instead
    // of triggering the exception at Lucene level
    @Test
    public void facetPropertyValueExceedingMaxCategoryPathLength() throws IOException {
        LuceneIndexDefinitionBuilder builder = new LuceneIndexDefinitionBuilder();
        builder.indexRule("nt:base")
                .property("foo")
                .propertyIndex()
                .facets();
        builder.indexRule("nt:base")
                .property("bars")
                .propertyIndex()
                .facets();

        LuceneIndexDefinition defn = LuceneIndexDefinition.newLuceneBuilder(root, builder.build(), "/foo").build();
        FacetsConfig facetsConfig = new FacetsConfig();
        LuceneDocumentMaker docMaker = new LuceneDocumentMaker(null, () -> facetsConfig, null, defn,
                defn.getApplicableIndexingRule("nt:base"), "/x");

        NodeBuilder test = EMPTY_NODE.builder();
        test.setProperty("foo", "a".repeat(8191 - 3)); // Max allowed path length + 1

        test.setProperty("bars", List.of("abc", "a".repeat(10000)), Type.STRINGS);

        boolean originalFtValue = LuceneDocumentMaker.FT_OAK_12372_DISABLE.get();

        try {
            LuceneDocumentMaker.FT_OAK_12372_DISABLE.set(false); // default value --> ignore long facet properties
            Document doc = docMaker.makeDocument(test.getNodeState());
            assertNotNull(doc);
            // "foo" is too long on its own and is dropped entirely
            assertEquals(0, facetValueCount(doc, "foo"));
            // "bars" keeps its short value and drops the too-long one
            assertEquals(1, facetValueCount(doc, "bars"));

            LuceneDocumentMaker.FT_OAK_12372_DISABLE.set(true); // legacy mode --> let Lucene throw exception
            assertThrows(IllegalArgumentException.class, () -> docMaker.makeDocument(test.getNodeState()));
        } finally {
            LuceneDocumentMaker.FT_OAK_12372_DISABLE.set(originalFtValue);
        }
    }

    private static long facetValueCount(Document doc, String pname) {
        return Arrays.stream(doc.getFields(FieldNames.createFacetFieldName(pname)))
                .filter(field -> field.fieldType().docValueType() == FieldInfo.DocValuesType.SORTED_SET)
                .count();
    }

    @Test
    public void ftOak12372ToggleShouldBeRemoved() {
        // Time-bombed: if this test fails, the feature toggle FT_OAK_12372 and its guard in
        // LuceneDocumentMaker should be cleaned up — the fix has been in production long enough.
        assertTrue("Feature toggle " + LuceneDocumentMaker.FT_OAK_12372 + " is overdue for removal",
                LocalDate.now().isBefore(LocalDate.of(2027, 8, 25)));
    }
}

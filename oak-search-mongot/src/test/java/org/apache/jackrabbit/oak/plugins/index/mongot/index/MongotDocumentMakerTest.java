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
package org.apache.jackrabbit.oak.plugins.index.mongot.index;

import java.nio.ByteBuffer;
import java.util.List;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.bson.Document;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MongotDocumentMakerTest {

    @Test
    public void mapsOakIndexingRulesToTypedSearchDocument() throws Exception {
        MongotIndexDefinition definition = newDefinition();
        NodeBuilder node = EmptyNodeState.EMPTY_NODE.builder();
        node.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        node.setProperty(JCR_MIXINTYPES, List.of("mix:referenceable"), Type.NAMES);
        node.setProperty("jcr:title", "MongoDB Oak");
        node.setProperty("price", 42L);
        node.setProperty("present", "yes");
        NodeState nodeState = node.getNodeState();
        IndexDefinition.IndexingRule rule = definition.getDefinedRules().get(0);
        assertNotNull(rule);

        MongoDocument result = new MongotDocumentMaker(null, definition, rule, "/content/a")
                .makeDocument(nodeState);
        Document bson = result.toBson();

        assertEquals("nt:unstructured", bson.getString(MongoFieldNames.PRIMARY_TYPE));
        assertEquals(List.of("mix:referenceable"), bson.getList(MongoFieldNames.MIXIN_TYPES, String.class));
        assertEquals("MongoDB Oak", bson.get(MongoFieldNames.TYPED, Document.class)
                .get(MongoFieldNames.encodeProperty("jcr:title")));
        assertEquals(42L, bson.get(MongoFieldNames.TYPED, Document.class)
                .get(MongoFieldNames.encodeProperty("price")));
        assertEquals(42L, bson.get(MongoFieldNames.ORDERED, Document.class)
                .get(MongoFieldNames.encodeProperty("price")));
        assertEquals(List.of("MongoDB Oak"), bson.get(MongoFieldNames.ANALYZED, Document.class)
                .getList(MongoFieldNames.encodeProperty("jcr:title"), String.class));
        assertEquals(List.of("MongoDB Oak", "a"), bson.getList(MongoFieldNames.FULLTEXT, String.class));
        assertEquals(List.of("MongoDB Oak"), bson.getList(MongoFieldNames.SUGGEST, String.class));
        assertEquals(List.of("MongoDB Oak"), bson.getList(MongoFieldNames.SPELLCHECK, String.class));
        assertEquals(List.of(MongoFieldNames.encodeProperty("optional")),
                bson.getList(MongoFieldNames.NULL_PROPERTIES, String.class));
        assertEquals(List.of(MongoFieldNames.encodeProperty("present")),
                bson.getList(MongoFieldNames.NOT_NULL_PROPERTIES, String.class));
    }

    @Test
    public void flattensDynamicBoostTokensAndMaximumConfidence() {
        MongoDocument document = new MongoDocument("/content/a");
        document.addDynamicBoost("tag1", "Plant", 0.2d);
        document.addDynamicBoost("tag2", "Plant", 0.9d);

        Document bson = document.toBson();
        assertEquals(List.of("Plant"),
                bson.getList(MongoFieldNames.DYNAMIC_BOOST_TOKENS, String.class));
        assertEquals(0.9d, bson.get(MongoFieldNames.DYNAMIC_BOOST_SCORES, Document.class)
                .getDouble(MongoFieldNames.encodeProperty("plant")), 0.0d);
    }

    @Test
    public void excludesEmbeddedIndexDefinitions() throws Exception {
        MongotIndexDefinition definition = newDefinition();
        IndexDefinition.IndexingRule rule = definition.getDefinedRules().get(0);
        NodeBuilder node = EmptyNodeState.EMPTY_NODE.builder();
        node.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        node.setProperty("jcr:title", "Internal definition");

        assertNull(new MongotDocumentMaker(null, definition, rule,
                "/content/oak:index/search/indexRules/nt:base").makeDocument(node.getNodeState()));
    }

    @Test
    public void keepsEmptyAggregateRootDocumentForStaleTextReplacement() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder() {
            @Override
            protected String getIndexType() {
                return MongotIndexDefinition.TYPE_MONGOT;
            }
        };
        builder.indexRule("nt:unstructured").property("text")
                .propertyIndex().analyzed().nodeScopeIndex();
        builder.aggregateRule("nt:unstructured", "jcr:content");
        MongotIndexDefinition definition = new MongotIndexDefinition(
                EmptyNodeState.EMPTY_NODE, builder.build(), "/oak:index/aggregate");
        NodeBuilder node = EmptyNodeState.EMPTY_NODE.builder();
        node.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);

        MongoDocument document = new MongotDocumentMaker(null, definition,
                definition.getDefinedRules().get(0), "/content/article")
                .makeDocument(node.getNodeState(), true, List.of());

        assertNotNull(document);
        assertEquals(List.of("article"),
                document.toBson().getList(MongoFieldNames.FULLTEXT, String.class));
    }

    @Test
    public void indexesSimilarityVector() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder() {
            @Override
            protected String getIndexType() {
                return MongotIndexDefinition.TYPE_MONGOT;
            }
        };
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:unstructured");
        indexRule.property("title").analyzed().nodeScopeIndex();
        indexRule.property("fv").type(PropertyType.TYPENAME_BINARY)
                .useInSimilarity(true).similaritySearchDenseVectorSize(3);
        MongotIndexDefinition definition = new MongotIndexDefinition(
                EmptyNodeState.EMPTY_NODE, builder.build(), "/oak:index/vector");

        NodeBuilder node = EmptyNodeState.EMPTY_NODE.builder();
        node.setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        node.setProperty("title", "vector candidate");
        ByteBuffer vector = ByteBuffer.allocate(3 * Float.BYTES)
                .putFloat(0.25f).putFloat(0.5f).putFloat(0.75f);
        node.setProperty("fv", new ArrayBasedBlob(vector.array()), Type.BINARY);

        MongoDocument document = new MongotDocumentMaker(null, definition,
                definition.getDefinedRules().get(0), "/content/vector")
                .makeDocument(node.getNodeState());
        String fieldName = FieldNames.createSimilarityFieldName(
                MongoFieldNames.encodeProperty("fv"));

        assertNotNull(document);
        assertEquals(List.of(0.25f, 0.5f, 0.75f), document.toBson().getList(
                fieldName, Float.class));
    }

    private static MongotIndexDefinition newDefinition() {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder() {
            @Override
            protected String getIndexType() {
                return MongotIndexDefinition.TYPE_MONGOT;
            }
        };
        builder.evaluatePathRestrictions();
        IndexDefinitionBuilder.IndexRule rule = builder.indexRule("nt:unstructured");
        rule.property("jcr:title")
                .propertyIndex()
                .analyzed()
                .nodeScopeIndex()
                .useInSuggest()
                .useInSpellcheck();
        rule.property("price").propertyIndex().ordered(PropertyType.TYPENAME_LONG);
        rule.property("optional").propertyIndex().nullCheckEnabled();
        rule.property("present").propertyIndex().notNullCheckEnabled();

        NodeState definitionState = builder.build();
        return new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE, definitionState, "/oak:index/siteSearch");
    }
}

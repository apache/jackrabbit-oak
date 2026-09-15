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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.mongot.util.MongotIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class MongotIndexDefinitionTest {

    @Test
    public void identifiesMongotBackendResourcesFromOakIndexPath() {
        MongotIndexDefinition definition = newDefinition("siteSearch");

        assertEquals("mongot", MongotIndexDefinition.TYPE_MONGOT);
        assertEquals("/oak:index/siteSearch", definition.getIndexPath());
        assertEquals("oak_aeda7eb399971cf7e5f0c1c8cc1b55539b757575d2268baef3cdaa95da106b0c",
                definition.getCollectionName());
        assertEquals("search_aeda7eb399971cf7e5f0c1c8cc1b55539b757575d2268baef3cdaa95da106b0c",
                definition.getSearchIndexName());
    }

    @Test
    public void distinguishesDynamicScoringFromFullTextRecall() {
        MongotIndexDefinitionBuilder builder = new MongotIndexDefinitionBuilder();
        builder.indexRule("nt:base").property("tags").getBuilderTree()
                .setProperty(FulltextIndexConstants.PROP_DYNAMIC_BOOST, true);
        builder.indexRule("nt:base").property("tags").getBuilderTree()
                .setProperty("useInFullTextQuery", false);
        MongotIndexDefinition definition = new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE,
                builder.build(), "/oak:index/dynamic");

        assertTrue(definition.hasDynamicBoost());
        assertFalse(definition.hasFullTextDynamicBoost());
    }

    @Test
    public void acceptsLegacyOakCompatibilityDefinitions() {
        MongotIndexDefinitionBuilder builder = new MongotIndexDefinitionBuilder();
        builder.getBuilderTree().setProperty(FulltextIndexConstants.COMPAT_MODE, 1L);

        MongotIndexDefinition definition = new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE,
                builder.build(), "/oak:index/legacy");

        assertEquals(IndexFormatVersion.V2, definition.getVersion());
        assertEquals(1L, definition.getDefinitionNodeState()
                .getLong(FulltextIndexConstants.COMPAT_MODE));
    }

    @Test
    public void resolvesThePublishedCollectionGenerationFromTheIndexDefinition() {
        NodeBuilder root = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder definitionBuilder = root.child(INDEX_DEFINITIONS_NAME).child("siteSearch");
        definitionBuilder.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        definitionBuilder.setProperty(TYPE_PROPERTY_NAME, MongotIndexDefinition.TYPE_MONGOT);
        definitionBuilder.setProperty(MongotIndexDefinition.PROP_COLLECTION_SEED, 42L);

        MongotIndexDefinition definition = new MongotIndexDefinition(root.getNodeState(),
                definitionBuilder.getNodeState(), "/oak:index/siteSearch");

        assertEquals(42L, definition.getCollectionSeed());
        assertEquals("oak_aeda7eb399971cf7e5f0c1c8cc1b55539b757575d2268baef3cdaa95da106b0c__2a",
                definition.getCollectionName());
        assertEquals("oak_aeda7eb399971cf7e5f0c1c8cc1b55539b757575d2268baef3cdaa95da106b0c_synonyms",
                definition.getSynonymCollectionName());
    }

    @Test
    public void usesElasticCompatibleQueryBatchAndTimeoutProperties() {
        MongotIndexDefinition defaults = newDefinition("defaults");
        assertEquals(java.util.List.of(10, 100, 1000),
                java.util.Arrays.stream(defaults.getQueryFetchSizes()).boxed().toList());
        assertEquals(60_000L, defaults.getQueryTimeoutMillis());

        NodeBuilder root = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder definitionBuilder = root.child(INDEX_DEFINITIONS_NAME).child("configured");
        definitionBuilder.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        definitionBuilder.setProperty(TYPE_PROPERTY_NAME, MongotIndexDefinition.TYPE_MONGOT);
        definitionBuilder.setProperty(MongotIndexDefinition.QUERY_FETCH_SIZES,
                java.util.List.of(3L, 9L), Type.LONGS);
        definitionBuilder.setProperty(MongotIndexDefinition.QUERY_TIMEOUT_MS, 1_234L);

        MongotIndexDefinition configured = new MongotIndexDefinition(root.getNodeState(),
                definitionBuilder.getNodeState(), "/oak:index/configured");
        assertEquals(java.util.List.of(3, 9),
                java.util.Arrays.stream(configured.getQueryFetchSizes()).boxed().toList());
        assertEquals(1_234L, configured.getQueryTimeoutMillis());
    }

    @Test
    public void rejectsUnusableQueryBatchAndTimeoutProperties() {
        assertThrows(IllegalArgumentException.class,
                () -> configuredDefinition(java.util.List.of(), 1_000));
        assertThrows(IllegalArgumentException.class,
                () -> configuredDefinition(java.util.List.of(0L), 1_000));
        assertThrows(IllegalArgumentException.class,
                () -> configuredDefinition(java.util.List.of(10L), 0));
    }

    static MongotIndexDefinition newDefinition(String name) {
        NodeBuilder root = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder definition = root.child(INDEX_DEFINITIONS_NAME).child(name);
        definition.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        definition.setProperty(TYPE_PROPERTY_NAME, MongotIndexDefinition.TYPE_MONGOT);
        return new MongotIndexDefinition(root.getNodeState(), definition.getNodeState(),
                "/" + INDEX_DEFINITIONS_NAME + "/" + name);
    }

    private static MongotIndexDefinition configuredDefinition(java.util.List<Long> fetchSizes,
                                                             long timeoutMillis) {
        NodeBuilder root = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder definition = root.child(INDEX_DEFINITIONS_NAME).child("configured");
        definition.setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        definition.setProperty(TYPE_PROPERTY_NAME, MongotIndexDefinition.TYPE_MONGOT);
        definition.setProperty(MongotIndexDefinition.QUERY_FETCH_SIZES, fetchSizes, Type.LONGS);
        definition.setProperty(MongotIndexDefinition.QUERY_TIMEOUT_MS, timeoutMillis);
        return new MongotIndexDefinition(root.getNodeState(), definition.getNodeState(),
                "/oak:index/configured");
    }
}

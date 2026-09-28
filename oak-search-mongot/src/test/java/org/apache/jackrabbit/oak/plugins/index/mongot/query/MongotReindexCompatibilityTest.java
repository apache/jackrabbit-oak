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
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotSearchConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotTestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoDocument;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.bson.Document;
import org.junit.ClassRule;
import org.junit.Test;

import static com.mongodb.client.model.Filters.eq;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_USE_IN_EXCERPT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MongotReindexCompatibilityTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @Test
    public void fullReindexRemovesStaleDocumentsAndUsesTheCurrentDefinition() throws Exception {
        MongotTestRepositoryBuilder builder = titleBuilder();
        content(builder.root(), "keep", "current-title");
        content(builder.root(), "remove", "removed-title");

        try (MongotTestRepositoryBuilder.Fixture fixture = builder.build()) {
            MongoDocument ghost = new MongoDocument("/ghost");
            ghost.addTypedProperty("jcr:title", "ghost-title");
            ghost.addAnalyzedProperty("jcr:title", "ghost-title");
            ghost.addFulltext("ghost-title");
            fixture.collection().insertOne(ghost.toBson());

            NodeState replacement = definitionWithCategory().build().builder()
                    .setProperty(REINDEX_PROPERTY_NAME, true)
                    .getNodeState();
            fixture.mutate(root -> {
                root.getChildNode("content").getChildNode("remove").remove();
                root.getChildNode("content").getChildNode("keep")
                        .setProperty("category", "current");
                root.getChildNode("oak:index").setChildNode("mongoSearch", replacement);
            });
            fixture.index();

            assertEquals(0, fixture.collection().countDocuments(eq(MongoFieldNames.ID, "/ghost")));
            assertEquals(0, fixture.collection().countDocuments(
                    eq(MongoFieldNames.ID, "/content/remove")));
            fixture.awaitPaths(titleQuery("current-title"), "JCR-SQL2", List.of("/content/keep"));
            fixture.assertMongotPlan(categoryQuery(), "JCR-SQL2");
            fixture.awaitPaths(categoryQuery(), "JCR-SQL2", List.of("/content/keep"));
        }
    }

    @Test
    public void emptyContentFullReindexClearsAllContentDocuments() throws Exception {
        MongotTestRepositoryBuilder builder = titleBuilder();
        content(builder.root(), "stale", "stale-title");

        try (MongotTestRepositoryBuilder.Fixture fixture = builder.build()) {
            fixture.mutate(root -> {
                root.getChildNode("content").remove();
                root.getChildNode("oak:index").getChildNode("mongoSearch")
                        .setProperty(REINDEX_PROPERTY_NAME, true);
            });
            fixture.index();

            assertEquals(0, fixture.collection().countDocuments(
                    eq(MongoFieldNames.ID, "/content/stale")));
            assertEquals(0, fixture.collection().countDocuments(
                    eq(MongoFieldNames.ANCESTORS, "/content")));
        }
    }

    @Test
    public void storedSourceTakesEffectOnlyWhenAReindexPublishesIt() throws Exception {
        MongotTestRepositoryBuilder builder = titleBuilder();
        content(builder.root(), "keep", "current-title");
        String query = titleQuery("current-title") + " order by [jcr:path]";

        try (MongotTestRepositoryBuilder.Fixture fixture = builder.build()) {
            fixture.mutate(root -> {
                root.getChildNode("oak:index").getChildNode("mongoSearch")
                        .setProperty(MongotIndexDefinition.STORED_SOURCE, true);
                content(root, "added", "current-title");
            });
            fixture.index();

            // An ordinary indexing cycle keeps serving the generation built without stored fields.
            assertFalse(servedDefinition(fixture).containsKey("storedSource"));
            assertFalse(plan(fixture, query).contains("returnStoredSource"));
            fixture.awaitPaths(query, "JCR-SQL2", List.of("/content/added", "/content/keep"));

            fixture.mutate(root -> root.getChildNode("oak:index").getChildNode("mongoSearch")
                    .setProperty(REINDEX_PROPERTY_NAME, true));
            fixture.index();

            assertTrue(servedDefinition(fixture).containsKey("storedSource"));
            assertTrue(plan(fixture, query).contains("returnStoredSource=true"));
            fixture.awaitPaths(query, "JCR-SQL2", List.of("/content/added", "/content/keep"));
        }
    }

    @Test
    public void fullTextStorageFollowsThePublishedGenerationWhenExcerptRulesChange()
            throws Exception {
        MongotTestRepositoryBuilder builder = titleBuilder();
        content(builder.root(), "keep", "current-title");
        String query = "select [jcr:path], [rep:excerpt(.)] from [nt:unstructured] as s "
                + "where contains(s.[jcr:title], 'current-title') order by [jcr:path]";

        try (MongotTestRepositoryBuilder.Fixture fixture = builder.build()) {
            fixture.mutate(root -> mongoSearch(root)
                    .setProperty(MongotIndexDefinition.STORED_SOURCE, true)
                    .setProperty(REINDEX_PROPERTY_NAME, true));
            fixture.index();
            assertNotNull(fullTextMapping(fixture));

            // Mongot rejects highlighting an unstored field, and resubmitting the mapping makes
            // it rebuild the index, so new excerpt rules wait for the next reindex.
            Object version = latestVersion(fixture);
            fixture.mutate(root -> {
                titleRule(root).setProperty(PROP_USE_IN_EXCERPT, true);
                content(root, "added", "current-title");
            });
            fixture.index();
            assertEquals(version, latestVersion(fixture));
            assertNotNull(fullTextMapping(fixture));
            fixture.awaitPaths(query, "JCR-SQL2", List.of("/content/added", "/content/keep"));
            assertNull(excerpt(fixture, query));

            fixture.mutate(root -> mongoSearch(root).setProperty(REINDEX_PROPERTY_NAME, true));
            fixture.index();
            assertNull(fullTextMapping(fixture));
            fixture.awaitPaths(query, "JCR-SQL2", List.of("/content/added", "/content/keep"));
            assertTrue(excerpt(fixture, query), excerpt(fixture, query).contains("<strong>"));

            // Removing the rule keeps the served full-text copy until the next reindex too.
            version = latestVersion(fixture);
            fixture.mutate(root -> {
                titleRule(root).removeProperty(PROP_USE_IN_EXCERPT);
                content(root, "later", "current-title");
            });
            fixture.index();
            assertEquals(version, latestVersion(fixture));
            assertNull(fullTextMapping(fixture));
            fixture.awaitPaths(query, "JCR-SQL2",
                    List.of("/content/added", "/content/keep", "/content/later"));

            fixture.mutate(root -> mongoSearch(root).setProperty(REINDEX_PROPERTY_NAME, true));
            fixture.index();
            assertNotNull(fullTextMapping(fixture));
            fixture.awaitPaths(query, "JCR-SQL2",
                    List.of("/content/added", "/content/keep", "/content/later"));
        }
    }

    private static NodeBuilder mongoSearch(NodeBuilder root) {
        return root.getChildNode("oak:index").getChildNode("mongoSearch");
    }

    private static NodeBuilder titleRule(NodeBuilder root) {
        NodeBuilder properties = mongoSearch(root).getChildNode("indexRules")
                .getChildNode("nt:unstructured").getChildNode("properties");
        for (String name : properties.getChildNodeNames()) {
            if ("jcr:title".equals(properties.getChildNode(name).getString("name"))) {
                return properties.getChildNode(name);
            }
        }
        throw new AssertionError("no jcr:title property rule");
    }

    private static Document fullTextMapping(MongotTestRepositoryBuilder.Fixture fixture) {
        return servedDefinition(fixture).get("mappings", Document.class)
                .get("fields", Document.class).get(MongoFieldNames.FULLTEXT, Document.class);
    }

    private static Object latestVersion(MongotTestRepositoryBuilder.Fixture fixture) {
        Document index = fixture.collection().listSearchIndexes().first();
        assertEquals(index.toJson(), "READY", index.getString("status"));
        assertNotNull(index.toJson(), index.get("latestVersion"));
        return index.get("latestVersion");
    }

    private static String excerpt(MongotTestRepositoryBuilder.Fixture fixture, String query)
            throws Exception {
        PropertyValue excerpt = fixture.query(query, "JCR-SQL2").getRows().iterator().next()
                .getValue("rep:excerpt(.)");
        return excerpt == null ? null : excerpt.getValue(Type.STRING);
    }

    private static Document servedDefinition(MongotTestRepositoryBuilder.Fixture fixture) {
        return fixture.collection().listSearchIndexes().first()
                .get("latestDefinition", Document.class);
    }

    private static String plan(MongotTestRepositoryBuilder.Fixture fixture, String query)
            throws Exception {
        return fixture.query("explain " + query, "JCR-SQL2").getRows().iterator().next()
                .getValue("plan").getValue(Type.STRING);
    }

    private static MongotTestRepositoryBuilder titleBuilder() {
        MongotTestRepositoryBuilder builder = new MongotTestRepositoryBuilder(mongo);
        configureTitle(builder.definition());
        return builder;
    }

    private static IndexDefinitionBuilder definitionWithCategory() {
        IndexDefinitionBuilder definition = new IndexDefinitionBuilder() {
            @Override
            protected String getIndexType() {
                return MongotIndexDefinition.TYPE_MONGOT;
            }
        };
        configureTitle(definition);
        definition.indexRule("nt:unstructured").property("category").propertyIndex();
        return definition;
    }

    private static void configureTitle(IndexDefinitionBuilder definition) {
        definition.indexRule("nt:unstructured").property("jcr:title")
                .propertyIndex().analyzed().nodeScopeIndex();
    }

    private static void content(NodeBuilder root, String name, String title) {
        root.child("content").child(name)
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", title);
    }

    private static String titleQuery(String term) {
        return "select [jcr:path] from [nt:unstructured] as s where "
                + "contains(s.[jcr:title], '" + term + "')";
    }

    private static String categoryQuery() {
        return "select [jcr:path] from [nt:unstructured] as s where s.[category] = 'current'";
    }
}

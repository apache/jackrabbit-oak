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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotSearchConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotTestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoDocument;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.ClassRule;
import org.junit.Test;

import static com.mongodb.client.model.Filters.eq;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.junit.Assert.assertEquals;

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

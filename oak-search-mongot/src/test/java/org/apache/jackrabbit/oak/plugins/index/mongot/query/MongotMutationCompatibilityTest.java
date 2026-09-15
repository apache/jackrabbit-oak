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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotSearchConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotTestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.ClassRule;
import org.junit.Test;

import static com.mongodb.client.model.Filters.eq;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class MongotMutationCompatibilityTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @Test
    public void propertyUpdateAndRemovalReplaceTheQueryableDocument() throws Exception {
        try (MongotTestRepositoryBuilder.Fixture fixture = fixtureWithContent()) {
            fixture.assertMongotPlan(titleQuery("oldterm"), "JCR-SQL2");
            assertEquals(List.of("/content/a"),
                    fixture.paths(titleQuery("oldterm"), "JCR-SQL2"));

            fixture.mutate(root -> root.getChildNode("content").getChildNode("a")
                    .setProperty("jcr:title", "newterm"));
            fixture.index();
            fixture.awaitPaths(titleQuery("oldterm"), "JCR-SQL2", List.of());
            fixture.awaitPaths(titleQuery("newterm"), "JCR-SQL2", List.of("/content/a"));

            fixture.mutate(root -> root.getChildNode("content").getChildNode("a")
                    .removeProperty("jcr:title"));
            fixture.index();
            fixture.awaitPaths(titleQuery("newterm"), "JCR-SQL2", List.of());
        }
    }

    @Test
    public void incrementalUpdateIsSearchableWithinOneSecondLocally() throws Exception {
        try (MongotTestRepositoryBuilder.Fixture fixture = fixtureWithContent()) {
            fixture.mutate(root -> root.getChildNode("content").getChildNode("a")
                    .setProperty("jcr:title", "latency-target"));
            long started = System.nanoTime();
            fixture.index();
            fixture.awaitPaths(titleQuery("latency-target"), "JCR-SQL2", List.of("/content/a"));
            Duration elapsed = Duration.ofNanos(System.nanoTime() - started);

            assertTrue("Local update-to-search latency was " + elapsed.toMillis() + " ms",
                    elapsed.compareTo(Duration.ofSeconds(1)) < 0);
        }
    }

    @Test
    public void subtreeDeletionRemovesOnlyTheDeletedTree() throws Exception {
        MongotTestRepositoryBuilder builder = titleBuilder();
        NodeBuilder content = builder.root().child("content");
        content.child("tree")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "delete-target")
                .child("child")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "delete-target");
        content.child("tree-sibling")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "delete-target");

        try (MongotTestRepositoryBuilder.Fixture fixture = builder.build()) {
            fixture.awaitPaths(titleQuery("delete-target") + " order by [jcr:path]", "JCR-SQL2",
                    List.of("/content/tree", "/content/tree-sibling", "/content/tree/child"));

            fixture.mutate(root -> root.getChildNode("content").getChildNode("tree").remove());
            fixture.index();

            fixture.awaitPaths(titleQuery("delete-target") + " order by [jcr:path]", "JCR-SQL2",
                    List.of("/content/tree-sibling"));
            assertEquals(0, fixture.collection().countDocuments(eq(MongoFieldNames.ID, "/content/tree")));
            assertEquals(0, fixture.collection().countDocuments(
                    eq(MongoFieldNames.ANCESTORS, "/content/tree")));
        }
    }

    @Test
    public void moveRemovesOldPathsAndIndexesNewPaths() throws Exception {
        MongotTestRepositoryBuilder builder = titleBuilder();
        builder.root().child("content").child("source")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "move-target")
                .child("child")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "move-target");

        try (MongotTestRepositoryBuilder.Fixture fixture = builder.build()) {
            fixture.mutate(root -> {
                NodeBuilder content = root.getChildNode("content");
                assertTrue(content.getChildNode("source").moveTo(content, "target"));
            });
            fixture.index();

            fixture.awaitPaths(titleQuery("move-target") + " order by [jcr:path]", "JCR-SQL2",
                    List.of("/content/target", "/content/target/child"));
            assertEquals(0, fixture.collection().countDocuments(eq(MongoFieldNames.ID, "/content/source")));
            assertEquals(0, fixture.collection().countDocuments(
                    eq(MongoFieldNames.ANCESTORS, "/content/source")));
        }
    }

    @Test
    public void losingAnIndexRuleDeletesOnlyTheExactDocument() throws Exception {
        MongotTestRepositoryBuilder builder = titleBuilder();
        builder.root().child("content").child("a")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "type-target")
                .child("child")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "type-target");

        try (MongotTestRepositoryBuilder.Fixture fixture = builder.build()) {
            fixture.mutate(root -> root.getChildNode("content").getChildNode("a")
                    .setProperty(JCR_PRIMARYTYPE, "nt:folder", Type.NAME));
            fixture.index();

            fixture.awaitPaths(titleQuery("type-target"), "JCR-SQL2",
                    List.of("/content/a/child"));
            assertEquals(0, fixture.collection().countDocuments(eq(MongoFieldNames.ID, "/content/a")));
            assertEquals(1, fixture.collection().countDocuments(
                    eq(MongoFieldNames.ID, "/content/a/child")));
        }
    }

    @Test
    public void aggregateChildUpdateAndDeletionRefreshTheAggregateRoot() throws Exception {
        MongotTestRepositoryBuilder builder = new MongotTestRepositoryBuilder(mongo);
        IndexDefinitionBuilder.IndexRule rule = builder.definition().indexRule("nt:unstructured");
        rule.property("text").propertyIndex().analyzed().nodeScopeIndex();
        builder.definition().aggregateRule("nt:unstructured", "jcr:content");
        builder.root().child("content").child("article")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .child("jcr:content")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("text", "walrus");

        try (MongotTestRepositoryBuilder.Fixture fixture = builder.build()) {
            String walrus = aggregateQuery("walrus");
            String badger = aggregateQuery("badger");
            fixture.awaitPaths(walrus, "JCR-SQL2", List.of("/content/article"));

            fixture.mutate(root -> root.getChildNode("content").getChildNode("article")
                    .getChildNode("jcr:content").setProperty("text", "badger"));
            fixture.index();
            fixture.awaitPaths(walrus, "JCR-SQL2", List.of());
            fixture.awaitPaths(badger, "JCR-SQL2", List.of("/content/article"));

            fixture.mutate(root -> root.getChildNode("content").getChildNode("article")
                    .getChildNode("jcr:content").remove());
            fixture.index();
            fixture.awaitPaths(badger, "JCR-SQL2", List.of());
        }
    }

    @Test
    public void missingSearchIndexFailsInsteadOfReturningAFalseEmptyResult() throws Exception {
        try (MongotTestRepositoryBuilder.Fixture fixture = fixtureWithContent()) {
            fixture.awaitPaths(titleQuery("oldterm"), "JCR-SQL2", List.of("/content/a"));
            String indexName = fixture.indexDefinition().getSearchIndexName();

            fixture.collection().dropSearchIndex(indexName);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (fixture.collection().listSearchIndexes().into(new ArrayList<>()).stream()
                    .anyMatch(index -> indexName.equals(index.getString("name")))
                    && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(25);
            }

            String propertyQuery = "select [jcr:path] from [nt:unstructured] as s where "
                    + "s.[jcr:title] = 'oldterm'";
            assertEquals(List.of("/content/a"), fixture.paths(propertyQuery, "JCR-SQL2"));

            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> fixture.paths(titleQuery("oldterm"), "JCR-SQL2"));
            assertTrue(failure.getMessage(), failure.getMessage().contains(indexName));
        }
    }

    @Test
    public void longOakPathRoundTripsThroughSearch() throws Exception {
        String nodeName = "x".repeat(33_000);
        String path = "/content/" + nodeName;
        MongotTestRepositoryBuilder builder = titleBuilder();
        builder.root().child("content").child(nodeName)
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "long-path-target");

        try (MongotTestRepositoryBuilder.Fixture fixture = builder.build()) {
            fixture.awaitPaths(titleQuery("long-path-target"), "JCR-SQL2", List.of(path));
            assertEquals(1, fixture.collection().countDocuments(eq(MongoFieldNames.PATH, path)));
            assertEquals(0, fixture.collection().countDocuments(eq(MongoFieldNames.ID, path)));
        }
    }

    private static MongotTestRepositoryBuilder.Fixture fixtureWithContent() throws Exception {
        MongotTestRepositoryBuilder builder = titleBuilder();
        builder.root().child("content").child("a")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "oldterm");
        return builder.build();
    }

    private static MongotTestRepositoryBuilder titleBuilder() {
        MongotTestRepositoryBuilder builder = new MongotTestRepositoryBuilder(mongo);
        IndexDefinitionBuilder.IndexRule rule = builder.definition().indexRule("nt:unstructured");
        rule.property("jcr:title").propertyIndex().analyzed().nodeScopeIndex();
        return builder;
    }

    private static String titleQuery(String term) {
        return "select [jcr:path] from [nt:unstructured] as s where "
                + "contains(s.[jcr:title], '" + term + "')";
    }

    private static String aggregateQuery(String term) {
        return "select [jcr:path] from [nt:unstructured] as s where "
                + "contains(s.*, '" + term + "') and issamenode(s, '/content/article')";
    }
}

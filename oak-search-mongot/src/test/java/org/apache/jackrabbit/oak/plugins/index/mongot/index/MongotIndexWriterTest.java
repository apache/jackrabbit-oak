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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.MongoCollection;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotSearchConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.bson.Document;
import org.junit.ClassRule;
import org.junit.Test;

import static com.mongodb.client.model.Filters.eq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class MongotIndexWriterTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @Test
    public void replacesAndDeletesDocumentsByOakPath() throws Exception {
        MongotIndexDefinition definition = newDefinition("/oak:index/writer");
        try (MongoConnection connection = connection()) {
            MongoCollection<Document> collection = connection.getCollection(definition);
            MongotIndexWriter writer = new MongotIndexWriter(connection, definition, false);

            writer.updateDocument("/content/a", document("/content/a", "first"));
            writer.updateDocument("/content/a", document("/content/a", "replacement"));
            writer.updateDocument("/content/a/child", document("/content/a/child", "child"));
            writer.updateDocument("/other", document("/other", "other"));

            assertEquals(3, collection.countDocuments());
            assertEquals(List.of("replacement"), collection.find(eq("_id", "/content/a")).first()
                    .getList(MongoFieldNames.FULLTEXT, String.class));

            writer.deleteDocument("/content/a");
            assertEquals(0, collection.countDocuments(eq("_id", "/content/a")));
            assertEquals(1, collection.countDocuments(eq("_id", "/content/a/child")));

            writer.updateDocument("/content", document("/content", "root"));
            writer.deleteDocumentTree("/content");
            assertEquals(0, collection.countDocuments(eq(MongoFieldNames.ANCESTORS, "/content")));
            assertEquals(0, collection.countDocuments(eq("_id", "/content")));
            assertEquals(1, collection.countDocuments(eq("_id", "/other")));
            assertTrue(writer.close(0));
        }
    }

    @Test
    public void replacesAndDeletesDocumentsWithLongOakPaths() throws Exception {
        MongotIndexDefinition definition = newDefinition("/oak:index/writer-long-path");
        String path = "/content/" + "x".repeat(33_000);
        try (MongoConnection connection = connection()) {
            MongoCollection<Document> collection = connection.getCollection(definition);
            MongotIndexWriter writer = new MongotIndexWriter(connection, definition, false);

            writer.updateDocument(path, document(path, "first"));
            writer.updateDocument(path, document(path, "replacement"));

            assertEquals(1, collection.countDocuments(eq(MongoFieldNames.PATH, path)));
            Document stored = collection.find(eq(MongoFieldNames.PATH, path)).first();
            assertEquals(List.of("replacement"), stored.getList(MongoFieldNames.FULLTEXT, String.class));
            assertFalse(path.equals(stored.getString(MongoFieldNames.ID)));

            writer.deleteDocument(path);
            assertEquals(0, collection.countDocuments(eq(MongoFieldNames.PATH, path)));
        }
    }

    @Test
    public void createsOneQueryableSearchIndex() throws Exception {
        MongotIndexDefinition definition = newDefinition("/oak:index/search-lifecycle");
        try (MongoConnection connection = connection()) {
            MongotIndexWriter writer = new MongotIndexWriter(connection, definition, false);
            writer.updateDocument("/content/a", document("/content/a", "native connector"));
            MongotSearchIndexManager manager = new MongotSearchIndexManager(connection);

            manager.ensureSearchIndex(definition);
            manager.ensureSearchIndex(definition);

            MongoCollection<Document> collection = connection.getCollection(definition);
            mongo.awaitSearchIndexReady(collection, definition.getSearchIndexName(), Duration.ofMinutes(2));
            long namedIndexes = collection.listSearchIndexes().into(new ArrayList<>()).stream()
                    .filter(index -> definition.getSearchIndexName().equals(index.getString("name")))
                    .count();
            assertEquals(1, namedIndexes);

            Document search = new Document("$search", new Document("index", definition.getSearchIndexName())
                    .append("text", new Document("path", MongoFieldNames.FULLTEXT)
                            .append("query", "connector")));
            assertEquals(List.of("/content/a"), collection.aggregate(List.of(search)).into(new ArrayList<>())
                    .stream().map(result -> result.getString("_id")).toList());
            assertTrue(writer.close(0));
        }
    }

    @Test
    public void updatesSearchIndexWhenOakAnalyzerChanges() throws Exception {
        String path = "/oak:index/search-analyzer-update";
        MongotIndexDefinition initial = newDefinition(path);
        MongotIndexDefinition english = newDefinition(path, builder -> builder.getBuilderTree()
                .addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT)
                .setProperty(FulltextIndexConstants.ANL_CLASS,
                        "org.apache.lucene.analysis.en.EnglishAnalyzer"));
        try (MongoConnection connection = connection()) {
            MongotSearchIndexManager manager = new MongotSearchIndexManager(connection);
            manager.ensureSearchIndex(initial);
            MongoCollection<Document> collection = connection.getCollection(initial);
            collection.insertOne(document("/content/a", "jumping").toBson());
            mongo.awaitSearchIndexReady(collection, initial.getSearchIndexName(), Duration.ofMinutes(2));

            manager.ensureSearchIndex(english);
            mongo.awaitSearchIndexReady(collection, english.getSearchIndexName(), Duration.ofMinutes(2));

            Document search = new Document("$search", new Document("index", english.getSearchIndexName())
                    .append("text", new Document("path", MongoFieldNames.FULLTEXT)
                            .append("query", "jump")));
            assertEquals(List.of("/content/a"), collection.aggregate(List.of(search))
                    .into(new ArrayList<>()).stream()
                    .map(result -> result.getString(MongoFieldNames.ID)).toList());
        }
    }

    @Test
    public void closeReportsNoChangesForUnusedWriter() throws Exception {
        try (MongoConnection connection = connection()) {
            MongotIndexWriter writer = new MongotIndexWriter(
                    connection, newDefinition("/oak:index/unused"), false);
            assertFalse(writer.close(0));
        }
    }

    @Test
    public void realTimeCloseReturnsOnlyAfterTheMutationIsSearchable() throws Exception {
        MongotIndexDefinition definition = newDefinition("/oak:index/real-time");
        NodeBuilder definitionBuilder = definition.getDefinitionNodeState().builder();
        try (MongoConnection connection = connection()) {
            MongotIndexWriter writer = new MongotIndexWriter(
                    connection, definition, definitionBuilder, false, true);
            writer.updateDocument("/content/visible",
                    document("/content/visible", "immediate visibility"));

            assertTrue(writer.close(0));

            Document search = new Document("$search",
                    new Document("index", definition.getSearchIndexName())
                            .append("text", new Document("path", MongoFieldNames.FULLTEXT)
                                    .append("query", "immediate")));
            assertEquals(List.of("/content/visible"), connection.getCollection(definition)
                    .aggregate(List.of(search)).into(new ArrayList<>()).stream()
                    .map(result -> result.getString(MongoFieldNames.PATH)).toList());
        }
    }

    @Test
    public void reindexBuildsAnIsolatedGenerationAndPublishesItOnClose() throws Exception {
        MongotIndexDefinition definition = newDefinition("/oak:index/reindex");
        NodeBuilder definitionBuilder = definition.getDefinitionNodeState().builder();
        try (MongoConnection connection = connection()) {
            MongoCollection<Document> liveCollection = connection.getCollection(definition);
            MongotIndexWriter incremental = new MongotIndexWriter(connection, definition, false);
            incremental.updateDocument("/stale", document("/stale", "stale"));
            incremental.close(0);
            mongo.awaitSearchIndexReady(liveCollection, definition.getSearchIndexName(), Duration.ofMinutes(2));

            MongotIndexWriter reindex = new MongotIndexWriter(
                    connection, definition, definitionBuilder, true, false);
            reindex.updateDocument("/current", document("/current", "current"));

            assertFalse(definitionBuilder.hasProperty(MongotIndexDefinition.PROP_COLLECTION_SEED));
            assertEquals(List.of("/stale"), liveCollection.find().into(new ArrayList<>()).stream()
                    .map(value -> value.getString(MongoFieldNames.ID)).toList());
            assertEquals(List.of("/stale"), search(liveCollection, definition, "stale"));

            assertTrue(reindex.close(0));
            MongotIndexDefinition published = new MongotIndexDefinition(
                    EmptyNodeState.EMPTY_NODE, definitionBuilder.getNodeState(), definition.getIndexPath());
            assertNotEquals(0L, published.getCollectionSeed());
            assertNotEquals(definition.getCollectionName(), published.getCollectionName());
            assertEquals(List.of("/current"), connection.getCollection(published).find()
                    .into(new ArrayList<>()).stream()
                    .map(value -> value.getString(MongoFieldNames.ID)).toList());
            assertEquals(List.of("/current"), search(connection.getCollection(published), published, "current"));
            assertEquals(List.of("/stale"), search(liveCollection, definition, "stale"));
        }
    }

    @Test
    public void emptyReindexPublishesAnEmptyGenerationWithoutDestroyingTheLiveOne() throws Exception {
        MongotIndexDefinition definition = newDefinition("/oak:index/empty-reindex");
        NodeBuilder definitionBuilder = definition.getDefinitionNodeState().builder();
        try (MongoConnection connection = connection()) {
            MongotIndexWriter incremental = new MongotIndexWriter(connection, definition, false);
            incremental.updateDocument("/stale", document("/stale", "stale"));
            incremental.close(0);

            MongotIndexWriter reindex = new MongotIndexWriter(
                    connection, definition, definitionBuilder, true, false);

            assertTrue(reindex.close(0));
            MongotIndexDefinition published = new MongotIndexDefinition(
                    EmptyNodeState.EMPTY_NODE, definitionBuilder.getNodeState(), definition.getIndexPath());
            assertEquals(1, connection.getCollection(definition).countDocuments());
            assertEquals(0, connection.getCollection(published).countDocuments());
        }
    }

    @Test
    public void emptyInitialReindexCreatesAQueryableCollection() throws Exception {
        MongotIndexDefinition definition = newDefinition("/oak:index/initial-empty-reindex");
        NodeBuilder definitionBuilder = definition.getDefinitionNodeState().builder();
        try (MongoConnection connection = connection()) {
            MongotIndexWriter reindex = new MongotIndexWriter(
                    connection, definition, definitionBuilder, true, false);

            assertTrue(reindex.close(0));
            MongotIndexDefinition published = new MongotIndexDefinition(
                    EmptyNodeState.EMPTY_NODE, definitionBuilder.getNodeState(), definition.getIndexPath());
            MongoCollection<Document> collection = connection.getCollection(published);
            mongo.awaitSearchIndexReady(collection, published.getSearchIndexName(), Duration.ofMinutes(2));
            assertEquals(0, collection.countDocuments());
        }
    }

    private static MongoConnection connection() {
        return MongoConnection.create(mongo.getConnectionString(), mongo.getDatabaseName());
    }

    private static MongoDocument document(String path, String text) {
        MongoDocument document = new MongoDocument(path);
        document.addFulltext(text);
        return document;
    }

    private static List<String> search(MongoCollection<Document> collection,
                                       MongotIndexDefinition definition,
                                       String text) {
        Document search = new Document("$search",
                new Document("index", definition.getSearchIndexName())
                        .append("text", new Document("path", MongoFieldNames.FULLTEXT)
                                .append("query", text)));
        return collection.aggregate(List.of(search)).into(new ArrayList<>()).stream()
                .map(result -> result.getString(MongoFieldNames.PATH)).toList();
    }

    private static MongotIndexDefinition newDefinition(String indexPath) {
        return newDefinition(indexPath, builder -> { });
    }

    private static MongotIndexDefinition newDefinition(
            String indexPath, java.util.function.Consumer<IndexDefinitionBuilder> customize) {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder() {
            @Override
            protected String getIndexType() {
                return MongotIndexDefinition.TYPE_MONGOT;
            }
        };
        builder.indexRule("nt:base").property("jcr:title").propertyIndex();
        customize.accept(builder);
        NodeState definitionState = builder.build();
        return new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE, definitionState, indexPath);
    }
}

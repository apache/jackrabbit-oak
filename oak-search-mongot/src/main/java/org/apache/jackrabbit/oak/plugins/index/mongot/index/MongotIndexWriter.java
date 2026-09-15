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

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;

public final class MongotIndexWriter implements FulltextIndexWriter<MongoDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(MongotIndexWriter.class);
    private static final long SYNC_TIMEOUT_MILLIS = Long.getLong(
            "oak.mongot.syncTimeoutMillis", 60_000L);
    private static final long SYNC_RETRY_MILLIS = 25L;

    private final MongoCollection<Document> collection;
    private final NodeBuilder definitionBuilder;
    private final MongotIndexDefinition definition;
    private final MongotSearchIndexManager searchIndexManager;
    private final String collectionName;
    private final long collectionSeed;
    private final boolean reindex;
    private final boolean realTime;
    private boolean updated;

    public MongotIndexWriter(MongoConnection connection,
                            MongotIndexDefinition definition,
                            boolean reindex) {
        this(connection, definition, definition.getDefinitionNodeState().builder(),
                reindex, false);
    }

    public MongotIndexWriter(MongoConnection connection,
                            MongotIndexDefinition definition,
                            NodeBuilder definitionBuilder,
                            boolean reindex,
                            boolean realTime) {
        this.definitionBuilder = definitionBuilder;
        this.definition = definition;
        this.searchIndexManager = new MongotSearchIndexManager(connection);
        this.reindex = reindex;
        this.realTime = realTime;
        if (reindex) {
            this.collectionSeed = newCollectionSeed(connection, definition);
            this.collectionName = definition.getCollectionName(collectionSeed);
            updated = true;
        } else {
            this.collectionSeed = definition.getCollectionSeed();
            this.collectionName = definition.getCollectionName();
        }
        this.collection = connection.getDatabase().getCollection(collectionName);
    }

    @Override
    public void updateDocument(String path, MongoDocument document) throws IOException {
        collection.replaceOne(eq(MongoFieldNames.ID, MongoDocumentId.fromPath(path)), document.toBson(),
                new ReplaceOptions().upsert(true));
        updated = true;
    }

    @Override
    public void deleteDocumentTree(String path) throws IOException {
        collection.deleteMany(or(eq(MongoFieldNames.PATH, path), eq(MongoFieldNames.ANCESTORS, path)));
        updated = true;
    }

    @Override
    public void deleteDocument(String path) throws IOException {
        collection.deleteOne(eq(MongoFieldNames.ID, MongoDocumentId.fromPath(path)));
        updated = true;
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        if (updated) {
            searchIndexManager.ensureSearchIndex(definition, collectionName);
            if (reindex) {
                searchIndexManager.awaitSearchIndexReady(definition, collectionName);
            }
            if (realTime) {
                awaitSearchIndexCatchUp();
            }
            if (reindex) {
                definitionBuilder.setProperty(
                        MongotIndexDefinition.PROP_COLLECTION_SEED, collectionSeed);
            }
        }
        return updated;
    }

    private static long newCollectionSeed(MongoConnection connection,
                                          MongotIndexDefinition definition) {
        List<String> existing = connection.getDatabase().listCollectionNames()
                .into(new java.util.ArrayList<>());
        for (int attempt = 0; attempt < 10; attempt++) {
            long seed = UUID.randomUUID().getMostSignificantBits();
            if (seed != 0 && !existing.contains(definition.getCollectionName(seed))) {
                return seed;
            }
        }
        throw new IllegalStateException("Cannot allocate a MongoDB collection generation for "
                + definition.getIndexPath());
    }

    private void awaitSearchIndexCatchUp() throws IOException {
        String token = UUID.randomUUID().toString();
        String markerId = "oak-sync-" + token;
        Document marker = new Document(MongoFieldNames.ID, markerId)
                .append(MongoFieldNames.SYNC_TOKEN, token);
        collection.replaceOne(eq(MongoFieldNames.ID, markerId), marker,
                new ReplaceOptions().upsert(true));
        Document search = new Document("$search",
                new Document("index", definition.getSearchIndexName())
                        .append("equals", new Document("path", MongoFieldNames.SYNC_TOKEN)
                                .append("value", token)));
        List<Document> pipeline = List.of(search, new Document("$limit", 1));
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(SYNC_TIMEOUT_MILLIS);
        RuntimeException lastFailure = null;
        try {
            while (System.nanoTime() < deadline) {
                try {
                    if (collection.aggregate(pipeline).first() != null) {
                        return;
                    }
                } catch (RuntimeException e) {
                    lastFailure = e;
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(SYNC_RETRY_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for Mongot", e);
                }
            }
            throw new IOException("Mongot did not expose the commit within "
                    + SYNC_TIMEOUT_MILLIS + " ms", lastFailure);
        } finally {
            try {
                collection.deleteOne(eq(MongoFieldNames.ID, markerId));
            } catch (RuntimeException e) {
                LOG.warn("Unable to remove Mongot synchronization marker {}", markerId, e);
            }
        }
    }
}

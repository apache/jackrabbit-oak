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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.bson.Document;

public final class MongotSearchIndexManager {

    private static final long BUILD_TIMEOUT_MILLIS = Long.getLong(
            "oak.mongot.searchIndexBuildTimeoutMillis", 120_000L);
    private static final long BUILD_RETRY_MILLIS = 250L;

    private final MongoConnection connection;

    public MongotSearchIndexManager(MongoConnection connection) {
        this.connection = connection;
    }

    public void ensureSearchIndex(MongotIndexDefinition definition) {
        ensureSearchIndex(definition, definition.getCollectionName());
    }

    void ensureSearchIndex(MongotIndexDefinition definition, String collectionName) {
        if (!connection.getDatabase().listCollectionNames().into(new ArrayList<>())
                .contains(collectionName)) {
            connection.getDatabase().createCollection(collectionName);
        }
        ensureSynonyms(definition);
        MongoCollection<Document> collection = connection.getDatabase().getCollection(collectionName);
        Document desiredDefinition = MongotSearchIndexDefinitionBuilder.build(definition);
        for (Document index : collection.listSearchIndexes()) {
            if (definition.getSearchIndexName().equals(index.getString("name"))) {
                Document latestDefinition = index.get("latestDefinition", Document.class);
                if (latestDefinition == null || !containsDefinition(latestDefinition, desiredDefinition)) {
                    collection.updateSearchIndex(definition.getSearchIndexName(), desiredDefinition);
                }
                return;
            }
        }
        collection.createSearchIndex(definition.getSearchIndexName(), desiredDefinition);
    }

    void awaitSearchIndexReady(MongotIndexDefinition definition, String collectionName)
            throws IOException {
        MongoCollection<Document> collection = connection.getDatabase().getCollection(collectionName);
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(BUILD_TIMEOUT_MILLIS);
        Document lastState = null;
        while (System.nanoTime() < deadline) {
            for (Document index : collection.listSearchIndexes()) {
                if (!definition.getSearchIndexName().equals(index.getString("name"))) {
                    continue;
                }
                lastState = index;
                if (Boolean.TRUE.equals(index.getBoolean("queryable"))) {
                    return;
                }
                if ("FAILED".equals(index.getString("status"))) {
                    throw new IOException("Mongot index failed to build: " + index.toJson());
                }
            }
            try {
                TimeUnit.MILLISECONDS.sleep(BUILD_RETRY_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for Mongot index", e);
            }
        }
        throw new IOException("Mongot index '" + definition.getSearchIndexName()
                + "' on collection '" + collectionName + "' did not become queryable within "
                + BUILD_TIMEOUT_MILLIS + " ms. Last state: " + lastState);
    }

    private void ensureSynonyms(MongotIndexDefinition definition) {
        if (!definition.hasSynonyms()) {
            return;
        }
        String collectionName = definition.getSynonymCollectionName();
        if (!connection.getDatabase().listCollectionNames().into(new ArrayList<>())
                .contains(collectionName)) {
            connection.getDatabase().createCollection(collectionName);
        }
        MongoCollection<Document> collection = connection.getDatabase().getCollection(collectionName);
        List<Document> desired = MongotSearchIndexDefinitionBuilder.synonymDocuments(definition);
        List<Document> existing = collection.find().sort(Sorts.ascending("_id"))
                .into(new ArrayList<>());
        if (existing.equals(desired)) {
            return;
        }
        collection.deleteMany(new Document());
        collection.insertMany(desired);
    }

    private static boolean containsDefinition(Object actual, Object expected) {
        if (expected instanceof Map<?, ?> expectedMap) {
            if (!(actual instanceof Map<?, ?> actualMap)) {
                return false;
            }
            for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
                if (!actualMap.containsKey(entry.getKey())
                        || !containsDefinition(actualMap.get(entry.getKey()), entry.getValue())) {
                    return false;
                }
            }
            return true;
        }
        if (expected instanceof List<?> expectedList) {
            if (!(actual instanceof List<?> actualList) || actualList.size() != expectedList.size()) {
                return false;
            }
            for (int i = 0; i < expectedList.size(); i++) {
                if (!containsDefinition(actualList.get(i), expectedList.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return Objects.equals(actual, expected);
    }
}

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

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.rules.ExternalResource;

public class MongotSearchConnectionRule extends ExternalResource {

    static final String CONNECTION_STRING_PROPERTY = "mongoSearchConnectionString";

    private final String configuredConnectionString;

    private MongoClient client;
    private MongoConnection searchConnection;
    private String connectionString;
    private String databaseName;

    public MongotSearchConnectionRule() {
        this(System.getProperty(CONNECTION_STRING_PROPERTY));
    }

    MongotSearchConnectionRule(String connectionString) {
        this.configuredConnectionString = connectionString == null || connectionString.isBlank()
                ? null
                : connectionString;
    }

    @Override
    protected void before() {
        connectionString = configuredConnectionString;
        if (connectionString == null) {
            connectionString = MongotSearchTestServer.getTestServer().getConnectionString();
        }

        databaseName = newDatabaseName();
        try {
            client = MongoClients.create(connectionString);
            client.getDatabase("admin").runCommand(new Document("ping", 1));
        } catch (RuntimeException e) {
            closeClient();
            throw new IllegalStateException("Unable to connect to the Mongot test deployment", e);
        }
    }

    public MongoDatabase getDatabase() {
        if (client == null || databaseName == null) {
            throw new IllegalStateException("MongotSearchConnectionRule has not been started");
        }
        return client.getDatabase(databaseName);
    }

    public String getConnectionString() {
        ensureStarted();
        return connectionString;
    }

    public String getDatabaseName() {
        ensureStarted();
        return databaseName;
    }

    public synchronized MongoConnection getSearchConnection() {
        ensureStarted();
        if (searchConnection == null) {
            searchConnection = MongoConnection.create(connectionString, databaseName);
        }
        return searchConnection;
    }

    public synchronized void useFreshDatabase() {
        ensureStarted();
        if (searchConnection != null) {
            searchConnection.close();
            searchConnection = null;
        }
        client.getDatabase(databaseName).drop();
        databaseName = newDatabaseName();
    }

    public void awaitSearchIndexReady(MongoCollection<Document> collection, String indexName, Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("Search index readiness timeout must be positive");
        }

        long deadline = System.nanoTime() + timeout.toNanos();
        Document lastState = null;
        while (System.nanoTime() < deadline) {
            for (Document index : collection.listSearchIndexes()) {
                if (!indexName.equals(index.getString("name"))) {
                    continue;
                }
                lastState = index;
                String status = index.getString("status");
                if ("READY".equals(status) && Boolean.TRUE.equals(index.getBoolean("queryable"))) {
                    return;
                }
                if ("FAILED".equals(status)) {
                    throw new IllegalStateException("Mongot index failed to build: " + index.toJson());
                }
            }

            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                break;
            }
            try {
                TimeUnit.NANOSECONDS.sleep(Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(500)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for Mongot index readiness", e);
            }
        }
        throw new IllegalStateException("Mongot index '" + indexName
                + "' did not become ready within " + timeout + ". Last state: " + lastState);
    }

    @Override
    protected void after() {
        try {
            try {
                if (searchConnection != null) {
                    searchConnection.close();
                    searchConnection = null;
                }
            } finally {
                if (client != null && databaseName != null) {
                    client.getDatabase(databaseName).drop();
                }
            }
        } finally {
            closeClient();
        }
    }

    private void closeClient() {
        if (client != null) {
            client.close();
            client = null;
        }
        connectionString = null;
        databaseName = null;
    }

    private void ensureStarted() {
        if (client == null || connectionString == null || databaseName == null) {
            throw new IllegalStateException("MongotSearchConnectionRule has not been started");
        }
    }

    private static String newDatabaseName() {
        return "oak_search_test_" + UUID.randomUUID().toString().replace("-", "");
    }
}

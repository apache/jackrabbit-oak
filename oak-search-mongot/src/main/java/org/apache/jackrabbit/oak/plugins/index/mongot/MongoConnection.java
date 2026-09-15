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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public final class MongoConnection implements AutoCloseable {

    private final MongoClient client;
    private final MongoDatabase database;

    private MongoConnection(MongoClient client, String databaseName) {
        this.client = client;
        this.database = client.getDatabase(databaseName);
    }

    public static MongoConnection create(String connectionString, String databaseName) {
        return new MongoConnection(MongoClients.create(connectionString), databaseName);
    }

    public MongoDatabase getDatabase() {
        return database;
    }

    public MongoCollection<Document> getCollection(MongotIndexDefinition definition) {
        return database.getCollection(definition.getCollectionName());
    }

    @Override
    public void close() {
        client.close();
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ConnectionString;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.connection.ClusterDescription;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;

class MongoTestClient implements MongoClient {

    private AtomicReference<String> beforeQueryException = new AtomicReference<>();
    private AtomicReference<String> beforeUpdateException = new AtomicReference<>();
    private AtomicReference<String> afterUpdateException = new AtomicReference<>();

    private final MongoClient client;

    MongoTestClient(String uri) {
        client = MongoClients.create(
                MongoConnection.getDefaultBuilder()
                        .applyConnectionString(new ConnectionString(uri))
                        .build());
    }

    @NotNull
    @Override
    public MongoDatabase getDatabase(String databaseName) {
        return new MongoTestDatabase(client.getDatabase(databaseName),
                beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @Override
    public ClientSession startSession() {
        return client.startSession();
    }

    @Override
    public ClientSession startSession(ClientSessionOptions clientSessionOptions) {
        return client.startSession(clientSessionOptions);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
        return client.listDatabaseNames();
    }

    @Override
    public MongoIterable<String> listDatabaseNames(ClientSession clientSession) {
        return client.listDatabaseNames(clientSession);
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return client.listDatabases();
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(ClientSession clientSession) {
        return client.listDatabases(clientSession);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(Class<TResult> var1) {
        return client.listDatabases(var1);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(ClientSession clientSession, Class<TResult> var1) {
        return client.listDatabases(clientSession, var1);
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return client.watch();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(Class<TResult> var1) {
        return client.watch(var1);
    }

    @Override
    public ChangeStreamIterable<Document> watch(List<? extends Bson> list) {
        return client.watch(list);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(List<? extends Bson> list, Class<TResult> var1) {
        return client.watch(list, var1);
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession) {
        return client.watch(clientSession);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, Class<TResult> var1) {
        return client.watch(clientSession, var1);
    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession, List<? extends Bson> list) {
        return client.watch(clientSession, list);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, List<? extends Bson> list, Class<TResult> var1) {
        return client.watch(clientSession, list, var1);
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return client.getClusterDescription();
    }

    void setExceptionBeforeQuery(String msg) {
        beforeQueryException.set(msg);
    }

    void setExceptionBeforeUpdate(String msg) {
        beforeUpdateException.set(msg);
    }

    void setExceptionAfterUpdate(String msg) {
        afterUpdateException.set(msg);
    }
}

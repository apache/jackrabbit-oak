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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;

class MongoTestDatabase implements MongoDatabase {

    private final MongoDatabase db;

    private final AtomicReference<String> beforeQueryException;
    private final AtomicReference<String> beforeUpdateException;
    private final AtomicReference<String> afterUpdateException;

    MongoTestDatabase(MongoDatabase db,
                      AtomicReference<String> beforeQueryException,
                      AtomicReference<String> beforeUpdateException,
                      AtomicReference<String> afterUpdateException) {
        this.db = db;
        this.beforeQueryException = beforeQueryException;
        this.beforeUpdateException = beforeUpdateException;
        this.afterUpdateException = afterUpdateException;
    }

    @NotNull
    @Override
    public String getName() {
        return db.getName();
    }

    @NotNull
    @Override
    public CodecRegistry getCodecRegistry() {
        return db.getCodecRegistry();
    }

    @NotNull
    @Override
    public ReadPreference getReadPreference() {
        return db.getReadPreference();
    }

    @NotNull
    @Override
    public WriteConcern getWriteConcern() {
        return db.getWriteConcern();
    }

    @NotNull
    @Override
    public ReadConcern getReadConcern() {
        return db.getReadConcern();
    }

    @NotNull
    @Override
    public MongoDatabase withCodecRegistry(@NotNull CodecRegistry codecRegistry) {
        return new MongoTestDatabase(db.withCodecRegistry(codecRegistry), beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @NotNull
    @Override
    public MongoDatabase withReadPreference(@NotNull ReadPreference readPreference) {
        return new MongoTestDatabase(db.withReadPreference(readPreference), beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @NotNull
    @Override
    public MongoDatabase withWriteConcern(@NotNull WriteConcern writeConcern) {
        return new MongoTestDatabase(db.withWriteConcern(writeConcern), beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @NotNull
    @Override
    public MongoDatabase withReadConcern(@NotNull ReadConcern readConcern) {
        return new MongoTestDatabase(db.withReadConcern(readConcern), beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @NotNull
    @Override
    public MongoCollection<Document> getCollection(@NotNull String collectionName) {
        return new MongoTestCollection<>(db.getCollection(collectionName), beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @NotNull
    @Override
    public <TDocument> MongoCollection<TDocument> getCollection(@NotNull String collectionName,
                                                                @NotNull Class<TDocument> tDocumentClass) {
        return new MongoTestCollection<>(db.getCollection(collectionName, tDocumentClass), beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @NotNull
    @Override
    public Document runCommand(@NotNull Bson command) {
        return db.runCommand(command);
    }

    @NotNull
    @Override
    public Document runCommand(@NotNull Bson command,
                               @NotNull ReadPreference readPreference) {
        return db.runCommand(command, readPreference);
    }

    @NotNull
    @Override
    public <TResult> TResult runCommand(@NotNull Bson command,
                                        @NotNull Class<TResult> tResultClass) {
        return db.runCommand(command, tResultClass);
    }

    @NotNull
    @Override
    public <TResult> TResult runCommand(@NotNull Bson command,
                                        @NotNull ReadPreference readPreference,
                                        @NotNull Class<TResult> tResultClass) {
        return db.runCommand(command, readPreference, tResultClass);
    }

    @NotNull
    @Override
    public Document runCommand(@NotNull ClientSession clientSession,
                               @NotNull Bson command) {
        return db.runCommand(clientSession, command);
    }

    @NotNull
    @Override
    public Document runCommand(@NotNull ClientSession clientSession,
                               @NotNull Bson command,
                               @NotNull ReadPreference readPreference) {
        return db.runCommand(clientSession, command, readPreference);
    }

    @NotNull
    @Override
    public <TResult> TResult runCommand(@NotNull ClientSession clientSession,
                                        @NotNull Bson command,
                                        @NotNull Class<TResult> tResultClass) {
        return db.runCommand(clientSession, command, tResultClass);
    }

    @NotNull
    @Override
    public <TResult> TResult runCommand(@NotNull ClientSession clientSession,
                                        @NotNull Bson command,
                                        @NotNull ReadPreference readPreference,
                                        @NotNull Class<TResult> tResultClass) {
        return db.runCommand(clientSession, command, readPreference, tResultClass);
    }

    @Override
    public void drop() {
        db.drop();
    }

    @Override
    public void drop(@NotNull ClientSession clientSession) {
        db.drop(clientSession);
    }

    @NotNull
    @Override
    public MongoIterable<String> listCollectionNames() {
        return db.listCollectionNames();
    }

    @NotNull
    @Override
    public ListCollectionsIterable<Document> listCollections() {
        return db.listCollections();
    }

    @NotNull
    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(@NotNull Class<TResult> tResultClass) {
        return db.listCollections(tResultClass);
    }

    @NotNull
    @Override
    public MongoIterable<String> listCollectionNames(@NotNull ClientSession clientSession) {
        return db.listCollectionNames(clientSession);
    }

    @NotNull
    @Override
    public ListCollectionsIterable<Document> listCollections(@NotNull ClientSession clientSession) {
        return db.listCollections(clientSession);
    }

    @NotNull
    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(
            @NotNull ClientSession clientSession,
            @NotNull Class<TResult> tResultClass) {
        return db.listCollections(clientSession, tResultClass);
    }

    @Override
    public void createCollection(@NotNull String collectionName) {
        db.createCollection(collectionName);
    }

    @Override
    public void createCollection(@NotNull String collectionName,
                                 @NotNull CreateCollectionOptions createCollectionOptions) {
        db.createCollection(collectionName, createCollectionOptions);
    }

    @Override
    public void createCollection(@NotNull ClientSession clientSession,
                                 @NotNull String collectionName) {
        db.createCollection(clientSession, collectionName);
    }

    @Override
    public void createCollection(@NotNull ClientSession clientSession,
                                 @NotNull String collectionName,
                                 @NotNull CreateCollectionOptions createCollectionOptions) {
        db.createCollection(clientSession, collectionName, createCollectionOptions);
    }

    @Override
    public void createView(@NotNull String viewName,
                           @NotNull String viewOn,
                           @NotNull List<? extends Bson> pipeline) {
        db.createView(viewName, viewOn, pipeline);
    }

    @Override
    public void createView(@NotNull String viewName,
                           @NotNull String viewOn,
                           @NotNull List<? extends Bson> pipeline,
                           @NotNull CreateViewOptions createViewOptions) {
        db.createView(viewName, viewOn, pipeline, createViewOptions);
    }

    @Override
    public void createView(@NotNull ClientSession clientSession,
                           @NotNull String viewName,
                           @NotNull String viewOn,
                           @NotNull List<? extends Bson> pipeline) {
        db.createView(clientSession, viewName, viewOn, pipeline);
    }

    @Override
    public void createView(@NotNull ClientSession clientSession,
                           @NotNull String viewName,
                           @NotNull String viewOn,
                           @NotNull List<? extends Bson> pipeline,
                           @NotNull CreateViewOptions createViewOptions) {
        db.createView(clientSession, viewName, viewOn, pipeline, createViewOptions);
    }

    @NotNull
    @Override
    public ChangeStreamIterable<Document> watch() {
        return db.watch();
    }

    @NotNull
    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(@NotNull Class<TResult> tResultClass) {
        return db.watch(tResultClass);
    }

    @NotNull
    @Override
    public ChangeStreamIterable<Document> watch(@NotNull List<? extends Bson> pipeline) {
        return db.watch(pipeline);
    }

    @NotNull
    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(@NotNull List<? extends Bson> pipeline,
                                                         @NotNull Class<TResult> tResultClass) {
        return db.watch(pipeline, tResultClass);
    }

    @NotNull
    @Override
    public ChangeStreamIterable<Document> watch(@NotNull ClientSession clientSession) {
        return db.watch(clientSession);
    }

    @NotNull
    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(@NotNull ClientSession clientSession,
                                                         @NotNull Class<TResult> tResultClass) {
        return db.watch(clientSession, tResultClass);
    }

    @NotNull
    @Override
    public ChangeStreamIterable<Document> watch(@NotNull ClientSession clientSession,
                                                @NotNull List<? extends Bson> pipeline) {
        return db.watch(clientSession, pipeline);
    }

    @NotNull
    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(@NotNull ClientSession clientSession,
                                                         @NotNull List<? extends Bson> pipeline,
                                                         @NotNull Class<TResult> tResultClass) {
        return db.watch(clientSession, pipeline, tResultClass);
    }
}

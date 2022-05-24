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
package org.apache.jackrabbit.oak.plugins.document.prefetch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;

public class CountingMongoDatabase implements MongoDatabase {

    private final MongoDatabase delegate;
    private Map<String, CountingMongoCollection> collections = new HashMap<>();

    public CountingMongoDatabase(MongoDatabase database) {
        this.delegate = database;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return delegate.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return delegate.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return delegate.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return delegate.getReadConcern();
    }

    @Override
    public MongoDatabase withCodecRegistry(CodecRegistry codecRegistry) {
        return delegate.withCodecRegistry(codecRegistry);
    }

    @Override
    public MongoDatabase withReadPreference(ReadPreference readPreference) {
        return delegate.withReadPreference(readPreference);
    }

    @Override
    public MongoDatabase withWriteConcern(WriteConcern writeConcern) {
        return delegate.withWriteConcern(writeConcern);
    }

    @Override
    public MongoDatabase withReadConcern(ReadConcern readConcern) {
        return delegate.withReadConcern(readConcern);

    }

    @Override
    public MongoCollection<Document> getCollection(String collectionName) {
        return delegate.getCollection(collectionName);

    }

    public CountingMongoCollection getCachedCountingCollection(String collectionName) {
        return collections.get(collectionName);
    }

    @Override
    public <TDocument> MongoCollection<TDocument> getCollection(String collectionName,
            Class<TDocument> documentClass) {
        CountingMongoCollection c = collections.get(collectionName);
        if (c == null) {
            c = new CountingMongoCollection(delegate.getCollection(collectionName, documentClass),
                    new AtomicReference<>(), new AtomicReference<>(), new AtomicReference<>());
            collections.put(collectionName, c);
        }
        return c;
    }

    @Override
    public Document runCommand(Bson command) {
        return delegate.runCommand(command);

    }

    @Override
    public Document runCommand(Bson command, ReadPreference readPreference) {
        return delegate.runCommand(command, readPreference);

    }

    @Override
    public <TResult> TResult runCommand(Bson command, Class<TResult> resultClass) {
        return delegate.runCommand(command, resultClass);

    }

    @Override
    public <TResult> TResult runCommand(Bson command, ReadPreference readPreference,
            Class<TResult> resultClass) {
        return delegate.runCommand(command, readPreference, resultClass);

    }

    @Override
    public Document runCommand(ClientSession clientSession, Bson command) {
        return delegate.runCommand(clientSession, command);

    }

    @Override
    public Document runCommand(ClientSession clientSession, Bson command,
            ReadPreference readPreference) {
        return delegate.runCommand(clientSession, command, readPreference);

    }

    @Override
    public <TResult> TResult runCommand(ClientSession clientSession, Bson command,
            Class<TResult> resultClass) {
        return delegate.runCommand(clientSession, command, resultClass);

    }

    @Override
    public <TResult> TResult runCommand(ClientSession clientSession, Bson command,
            ReadPreference readPreference, Class<TResult> resultClass) {
        return delegate.runCommand(clientSession, command, readPreference, resultClass);

    }

    @Override
    public void drop() {
        delegate.drop();

    }

    @Override
    public void drop(ClientSession clientSession) {
        delegate.drop(clientSession);

    }

    @Override
    public MongoIterable<String> listCollectionNames() {
        return delegate.listCollectionNames();

    }

    @Override
    public ListCollectionsIterable<Document> listCollections() {
        return delegate.listCollections();

    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(
            Class<TResult> resultClass) {
        return delegate.listCollections(resultClass);

    }

    @Override
    public MongoIterable<String> listCollectionNames(ClientSession clientSession) {
        return delegate.listCollectionNames(clientSession);

    }

    @Override
    public ListCollectionsIterable<Document> listCollections(
            ClientSession clientSession) {
        return delegate.listCollections(clientSession);

    }

    @Override
    public <TResult> ListCollectionsIterable<TResult> listCollections(
            ClientSession clientSession, Class<TResult> resultClass) {
        return delegate.listCollections(clientSession, resultClass);

    }

    @Override
    public void createCollection(String collectionName) {
        delegate.createCollection(collectionName);

    }

    @Override
    public void createCollection(String collectionName,
            CreateCollectionOptions createCollectionOptions) {
        delegate.createCollection(collectionName, createCollectionOptions);

    }

    @Override
    public void createCollection(ClientSession clientSession, String collectionName) {
        delegate.createCollection(clientSession, collectionName);

    }

    @Override
    public void createCollection(ClientSession clientSession, String collectionName,
            CreateCollectionOptions createCollectionOptions) {
        delegate.createCollection(clientSession, collectionName, createCollectionOptions);

    }

    @Override
    public void createView(String viewName, String viewOn,
            List<? extends Bson> pipeline) {
        delegate.createView(viewName, viewOn, pipeline);

    }

    @Override
    public void createView(String viewName, String viewOn, List<? extends Bson> pipeline,
            CreateViewOptions createViewOptions) {
        delegate.createView(viewName, viewOn, pipeline, createViewOptions);

    }

    @Override
    public void createView(ClientSession clientSession, String viewName, String viewOn,
            List<? extends Bson> pipeline) {
        delegate.createView(clientSession, viewName, viewOn, pipeline);

    }

    @Override
    public void createView(ClientSession clientSession, String viewName, String viewOn,
            List<? extends Bson> pipeline, CreateViewOptions createViewOptions) {
        delegate.createView(clientSession, viewName, viewOn, pipeline, createViewOptions);

    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return delegate.watch();

    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(Class<TResult> resultClass) {
        return delegate.watch(resultClass);

    }

    @Override
    public ChangeStreamIterable<Document> watch(List<? extends Bson> pipeline) {
        return delegate.watch(pipeline);

    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(List<? extends Bson> pipeline,
            Class<TResult> resultClass) {
        return delegate.watch(pipeline, resultClass);

    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession) {
        return delegate.watch(clientSession);

    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession,
            Class<TResult> resultClass) {
        return delegate.watch(clientSession, resultClass);

    }

    @Override
    public ChangeStreamIterable<Document> watch(ClientSession clientSession,
            List<? extends Bson> pipeline) {
        return delegate.watch(clientSession, pipeline);

    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession,
            List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return delegate.watch(clientSession, pipeline, resultClass);

    }

    @Override
    public AggregateIterable<Document> aggregate(List<? extends Bson> pipeline) {
        return delegate.aggregate(pipeline);

    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(List<? extends Bson> pipeline,
            Class<TResult> resultClass) {
        return delegate.aggregate(pipeline, resultClass);

    }

    @Override
    public AggregateIterable<Document> aggregate(ClientSession clientSession,
            List<? extends Bson> pipeline) {
        return delegate.aggregate(clientSession, pipeline);

    }

    @Override
    public <TResult> AggregateIterable<TResult> aggregate(ClientSession clientSession,
            List<? extends Bson> pipeline, Class<TResult> resultClass) {
        return delegate.aggregate(clientSession, pipeline, resultClass);

    }

}

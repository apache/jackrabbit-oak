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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoTestCollection;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

public class CountingMongoCollection<TDocument> extends MongoTestCollection<TDocument> {

    private final MongoCollection<TDocument> countingCollectionDelegate;
    private AtomicInteger findCounter = new AtomicInteger(0);
    private AtomicInteger nodesDeleteMany = new AtomicInteger(0);

    CountingMongoCollection(MongoCollection<TDocument> collection,
            AtomicReference<String> beforeQueryException,
            AtomicReference<String> beforeUpdateException,
            AtomicReference<String> afterUpdateException) {
        super(collection, beforeQueryException, beforeUpdateException, afterUpdateException);
        this.countingCollectionDelegate = collection;
    }

    CountingMongoCollection(
            CountingMongoCollection<TDocument> countingMongoCollection,
            @NotNull MongoCollection<TDocument> collection) {
        super(collection, new AtomicReference<String>(), new AtomicReference<String>(), new AtomicReference<String>());
        this.findCounter = countingMongoCollection.findCounter;
        this.countingCollectionDelegate = collection;
    }

    public int getFindCounter() {
        return findCounter.get();
    }

    public void resetFindCounter() {
        findCounter.set(0);
    }

    private void incFindCounter() {
        findCounter.incrementAndGet();
    }

    public int getNodesDeleteMany() {
        return nodesDeleteMany.get();
    }

    public void incNodesDeleteMany() {
        nodesDeleteMany.incrementAndGet();
    }

    public int resetNodesDeleteMany() {
        return nodesDeleteMany.getAndSet(0);
    }

    public com.mongodb.client.result.DeleteResult deleteMany(org.bson.conversions.Bson filter) {
        incNodesDeleteMany();
        return countingCollectionDelegate.deleteMany(filter);
    };

    public com.mongodb.client.result.DeleteResult deleteMany(org.bson.conversions.Bson filter, com.mongodb.client.model.DeleteOptions options) {
        incNodesDeleteMany();
        return countingCollectionDelegate.deleteMany(filter, options);
    };

    public com.mongodb.client.result.DeleteResult deleteMany(com.mongodb.client.ClientSession clientSession, org.bson.conversions.Bson filter) {
        incNodesDeleteMany();
        return countingCollectionDelegate.deleteMany(clientSession, filter);
    };

    public com.mongodb.client.result.DeleteResult deleteMany(com.mongodb.client.ClientSession clientSession, org.bson.conversions.Bson filter, com.mongodb.client.model.DeleteOptions options) {
        incNodesDeleteMany();
        return countingCollectionDelegate.deleteMany(clientSession, filter, options);
    };

    @NotNull
    @Override
    public FindIterable<TDocument> find() {
        incFindCounter();
        return countingCollectionDelegate.find();
    }

    @NotNull
    @Override
    public <TResult> FindIterable<TResult> find(@NotNull Class<TResult> tResultClass) {
        incFindCounter();
        return countingCollectionDelegate.find(tResultClass);
    }

    @NotNull
    @Override
    public FindIterable<TDocument> find(@NotNull Bson filter) {
        incFindCounter();
        return countingCollectionDelegate.find(filter);
    }

    @NotNull
    @Override
    public <TResult> FindIterable<TResult> find(@NotNull Bson filter,
                                                @NotNull Class<TResult> tResultClass) {
        incFindCounter();
        return countingCollectionDelegate.find(filter, tResultClass);
    }

    @NotNull
    @Override
    public FindIterable<TDocument> find(@NotNull ClientSession clientSession) {
        incFindCounter();
        return countingCollectionDelegate.find(clientSession);
    }

    @NotNull
    @Override
    public <TResult> FindIterable<TResult> find(@NotNull ClientSession clientSession,
                                                @NotNull Class<TResult> tResultClass) {
        incFindCounter();
        return countingCollectionDelegate.find(clientSession, tResultClass);
    }

    @NotNull
    @Override
    public FindIterable<TDocument> find(@NotNull ClientSession clientSession,
                                        @NotNull Bson filter) {
        incFindCounter();
        return countingCollectionDelegate.find(clientSession, filter);
    }

    @NotNull
    @Override
    public <TResult> FindIterable<TResult> find(@NotNull ClientSession clientSession,
                                                @NotNull Bson filter,
                                                @NotNull Class<TResult> tResultClass) {
        incFindCounter();
        return countingCollectionDelegate.find(clientSession, filter, tResultClass);
    }

    @Override
    public @NotNull MongoCollection<TDocument> withReadConcern(
            @NotNull ReadConcern readConcern) {
        return new CountingMongoCollection(this, super.withReadConcern(readConcern));
    }
    public MongoCollection<TDocument> withReadPreference(@NotNull ReadPreference readPreference) {
        return new CountingMongoCollection(this, super.withReadPreference(readPreference));
    }
}

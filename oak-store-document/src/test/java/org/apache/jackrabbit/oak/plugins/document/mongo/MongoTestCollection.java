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

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class MongoTestCollection<TDocument> implements MongoCollection<TDocument> {

    private final MongoCollection<TDocument> collection;

    private final AtomicReference<String> beforeQueryException;
    private final AtomicReference<String> beforeUpdateException;
    private final AtomicReference<String> afterUpdateException;

    MongoTestCollection(MongoCollection<TDocument> collection,
                        AtomicReference<String> beforeQueryException,
                        AtomicReference<String> beforeUpdateException,
                        AtomicReference<String> afterUpdateException) {
        this.collection = collection;
        this.beforeQueryException = beforeQueryException;
        this.beforeUpdateException = beforeUpdateException;
        this.afterUpdateException = afterUpdateException;
    }

    @NotNull
    @Override
    public MongoNamespace getNamespace() {
        return collection.getNamespace();
    }

    @NotNull
    @Override
    public Class<TDocument> getDocumentClass() {
        return collection.getDocumentClass();
    }

    @NotNull
    @Override
    public CodecRegistry getCodecRegistry() {
        return collection.getCodecRegistry();
    }

    @NotNull
    @Override
    public ReadPreference getReadPreference() {
        return collection.getReadPreference();
    }

    @NotNull
    @Override
    public WriteConcern getWriteConcern() {
        return collection.getWriteConcern();
    }

    @NotNull
    @Override
    public ReadConcern getReadConcern() {
        return collection.getReadConcern();
    }

    @NotNull
    @Override
    public <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(@NotNull Class<NewTDocument> clazz) {
        return new MongoTestCollection<>(collection.withDocumentClass(clazz), beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @NotNull
    @Override
    public MongoCollection<TDocument> withCodecRegistry(@NotNull CodecRegistry codecRegistry) {
        return new MongoTestCollection<>(collection.withCodecRegistry(codecRegistry), beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @NotNull
    @Override
    public MongoCollection<TDocument> withReadPreference(@NotNull ReadPreference readPreference) {
        return new MongoTestCollection<>(collection.withReadPreference(readPreference), beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @NotNull
    @Override
    public MongoCollection<TDocument> withWriteConcern(@NotNull WriteConcern writeConcern) {
        return new MongoTestCollection<>(collection.withWriteConcern(writeConcern), beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @NotNull
    @Override
    public MongoCollection<TDocument> withReadConcern(@NotNull ReadConcern readConcern) {
        return new MongoTestCollection<>(collection.withReadConcern(readConcern), beforeQueryException, beforeUpdateException, afterUpdateException);
    }

    @Override
    @Deprecated
    public long count() {
        return collection.count();
    }

    @Override
    @Deprecated
    public long count(@NotNull Bson filter) {
        return collection.count(filter);
    }

    @Override
    @Deprecated
    public long count(@NotNull Bson filter, @NotNull CountOptions options) {
        return collection.count(filter, options);
    }

    @Override
    @Deprecated
    public long count(@NotNull ClientSession clientSession) {
        return collection.count(clientSession);
    }

    @Override
    @Deprecated
    public long count(@NotNull ClientSession clientSession, @NotNull Bson filter) {
        return collection.count(clientSession, filter);
    }

    @Override
    @Deprecated
    public long count(@NotNull ClientSession clientSession,
                      @NotNull Bson filter,
                      @NotNull CountOptions options) {
        return collection.count(clientSession, filter, options);
    }

    @Override
    public long countDocuments() {
        return collection.countDocuments();
    }

    @Override
    public long countDocuments(@NotNull Bson filter) {
        return collection.countDocuments(filter);
    }

    @Override
    public long countDocuments(@NotNull Bson filter,
                               @NotNull CountOptions options) {
        return collection.countDocuments(filter, options);
    }

    @Override
    public long countDocuments(@NotNull ClientSession clientSession) {
        return collection.countDocuments(clientSession);
    }

    @Override
    public long countDocuments(@NotNull ClientSession clientSession,
                               @NotNull Bson filter) {
        return collection.countDocuments(clientSession, filter);
    }

    @Override
    public long countDocuments(@NotNull ClientSession clientSession,
                               @NotNull Bson filter,
                               @NotNull CountOptions options) {
        return collection.countDocuments(clientSession, filter, options);
    }

    @NotNull
    @Override
    public <TResult> DistinctIterable<TResult> distinct(@NotNull String fieldName,
                                                        @NotNull Class<TResult> tResultClass) {
        return collection.distinct(fieldName, tResultClass);
    }

    @NotNull
    @Override
    public <TResult> DistinctIterable<TResult> distinct(@NotNull String fieldName,
                                                        @NotNull Bson filter,
                                                        @NotNull Class<TResult> tResultClass) {
        return collection.distinct(fieldName, filter, tResultClass);
    }

    @NotNull
    @Override
    public <TResult> DistinctIterable<TResult> distinct(@NotNull ClientSession clientSession,
                                                        @NotNull String fieldName,
                                                        @NotNull Class<TResult> tResultClass) {
        return collection.distinct(clientSession, fieldName, tResultClass);
    }

    @NotNull
    @Override
    public <TResult> DistinctIterable<TResult> distinct(@NotNull ClientSession clientSession,
                                                        @NotNull String fieldName,
                                                        @NotNull Bson filter,
                                                        @NotNull Class<TResult> tResultClass) {
        return collection.distinct(clientSession, fieldName, filter, tResultClass);
    }

    @Override
    public long estimatedDocumentCount() {
        return collection.estimatedDocumentCount();
    }

    @Override
    public long estimatedDocumentCount(@NotNull EstimatedDocumentCountOptions options) {
        return collection.estimatedDocumentCount(options);
    }

    @NotNull
    @Override
    public FindIterable<TDocument> find() {
        maybeThrowExceptionBeforeQuery();
        return collection.find();
    }

    @NotNull
    @Override
    public <TResult> FindIterable<TResult> find(@NotNull Class<TResult> tResultClass) {
        maybeThrowExceptionBeforeQuery();
        return collection.find(tResultClass);
    }

    @NotNull
    @Override
    public FindIterable<TDocument> find(@NotNull Bson filter) {
        maybeThrowExceptionBeforeQuery();
        return collection.find(filter);
    }

    @NotNull
    @Override
    public <TResult> FindIterable<TResult> find(@NotNull Bson filter,
                                                @NotNull Class<TResult> tResultClass) {
        maybeThrowExceptionBeforeQuery();
        return collection.find(filter, tResultClass);
    }

    @NotNull
    @Override
    public FindIterable<TDocument> find(@NotNull ClientSession clientSession) {
        maybeThrowExceptionBeforeQuery();
        return collection.find(clientSession);
    }

    @NotNull
    @Override
    public <TResult> FindIterable<TResult> find(@NotNull ClientSession clientSession,
                                                @NotNull Class<TResult> tResultClass) {
        maybeThrowExceptionBeforeQuery();
        return collection.find(clientSession, tResultClass);
    }

    @NotNull
    @Override
    public FindIterable<TDocument> find(@NotNull ClientSession clientSession,
                                        @NotNull Bson filter) {
        maybeThrowExceptionBeforeQuery();
        return collection.find(clientSession, filter);
    }

    @NotNull
    @Override
    public <TResult> FindIterable<TResult> find(@NotNull ClientSession clientSession,
                                                @NotNull Bson filter,
                                                @NotNull Class<TResult> tResultClass) {
        maybeThrowExceptionBeforeQuery();
        return collection.find(clientSession, filter, tResultClass);
    }

    @NotNull
    @Override
    public AggregateIterable<TDocument> aggregate(@NotNull List<? extends Bson> pipeline) {
        return collection.aggregate(pipeline);
    }

    @NotNull
    @Override
    public <TResult> AggregateIterable<TResult> aggregate(@NotNull List<? extends Bson> pipeline,
                                                          @NotNull Class<TResult> tResultClass) {
        return collection.aggregate(pipeline, tResultClass);
    }

    @NotNull
    @Override
    public AggregateIterable<TDocument> aggregate(@NotNull ClientSession clientSession,
                                                  @NotNull List<? extends Bson> pipeline) {
        return collection.aggregate(clientSession, pipeline);
    }

    @NotNull
    @Override
    public <TResult> AggregateIterable<TResult> aggregate(@NotNull ClientSession clientSession,
                                                          @NotNull List<? extends Bson> pipeline,
                                                          @NotNull Class<TResult> tResultClass) {
        return collection.aggregate(clientSession, pipeline, tResultClass);
    }

    @NotNull
    @Override
    public ChangeStreamIterable<TDocument> watch() {
        return collection.watch();
    }

    @NotNull
    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(@NotNull Class<TResult> tResultClass) {
        return collection.watch(tResultClass);
    }

    @NotNull
    @Override
    public ChangeStreamIterable<TDocument> watch(@NotNull List<? extends Bson> pipeline) {
        return collection.watch(pipeline);
    }

    @NotNull
    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(@NotNull List<? extends Bson> pipeline,
                                                         @NotNull Class<TResult> tResultClass) {
        return collection.watch(pipeline, tResultClass);
    }

    @NotNull
    @Override
    public ChangeStreamIterable<TDocument> watch(@NotNull ClientSession clientSession) {
        return collection.watch(clientSession);
    }

    @NotNull
    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(@NotNull ClientSession clientSession,
                                                         @NotNull Class<TResult> tResultClass) {
        return collection.watch(clientSession, tResultClass);
    }

    @NotNull
    @Override
    public ChangeStreamIterable<TDocument> watch(@NotNull ClientSession clientSession,
                                                 @NotNull List<? extends Bson> pipeline) {
        return collection.watch(clientSession, pipeline);
    }

    @NotNull
    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(@NotNull ClientSession clientSession,
                                                         @NotNull List<? extends Bson> pipeline,
                                                         @NotNull Class<TResult> tResultClass) {
        return collection.watch(clientSession, pipeline, tResultClass);
    }

    @NotNull
    @Override
    public MapReduceIterable<TDocument> mapReduce(@NotNull String mapFunction,
                                                  @NotNull String reduceFunction) {
        return collection.mapReduce(mapFunction, reduceFunction);
    }

    @NotNull
    @Override
    public <TResult> MapReduceIterable<TResult> mapReduce(@NotNull String mapFunction,
                                                          @NotNull String reduceFunction,
                                                          @NotNull Class<TResult> tResultClass) {
        return collection.mapReduce(mapFunction, reduceFunction, tResultClass);
    }

    @NotNull
    @Override
    public MapReduceIterable<TDocument> mapReduce(@NotNull ClientSession clientSession,
                                                  @NotNull String mapFunction,
                                                  @NotNull String reduceFunction) {
        return collection.mapReduce(clientSession, mapFunction, reduceFunction);
    }

    @NotNull
    @Override
    public <TResult> MapReduceIterable<TResult> mapReduce(@NotNull ClientSession clientSession,
                                                          @NotNull String mapFunction,
                                                          @NotNull String reduceFunction,
                                                          @NotNull Class<TResult> tResultClass) {
        return collection.mapReduce(clientSession, mapFunction, reduceFunction, tResultClass);
    }

    @NotNull
    @Override
    public BulkWriteResult bulkWrite(@NotNull List<? extends WriteModel<? extends TDocument>> requests) {
        maybeThrowExceptionBeforeUpdate();
        BulkWriteResult result = collection.bulkWrite(requests);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public BulkWriteResult bulkWrite(@NotNull List<? extends WriteModel<? extends TDocument>> requests,
                                     @NotNull BulkWriteOptions options) {
        maybeThrowExceptionBeforeUpdate();
        BulkWriteResult result = collection.bulkWrite(requests, options);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public BulkWriteResult bulkWrite(@NotNull ClientSession clientSession,
                                     @NotNull List<? extends WriteModel<? extends TDocument>> requests) {
        maybeThrowExceptionBeforeUpdate();
        BulkWriteResult result = collection.bulkWrite(clientSession, requests);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public BulkWriteResult bulkWrite(@NotNull ClientSession clientSession,
                                     @NotNull List<? extends WriteModel<? extends TDocument>> requests,
                                     @NotNull BulkWriteOptions options) {
        maybeThrowExceptionBeforeUpdate();
        BulkWriteResult result = collection.bulkWrite(clientSession, requests, options);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @Override
    public void insertOne(@NotNull TDocument tDocument) {
        maybeThrowExceptionBeforeUpdate();
        collection.insertOne(tDocument);
        maybeThrowExceptionAfterUpdate();
    }

    @Override
    public void insertOne(@NotNull TDocument tDocument, @NotNull InsertOneOptions options) {
        maybeThrowExceptionBeforeUpdate();
        collection.insertOne(tDocument, options);
        maybeThrowExceptionAfterUpdate();
    }

    @Override
    public void insertOne(@NotNull ClientSession clientSession, @NotNull TDocument tDocument) {
        maybeThrowExceptionBeforeUpdate();
        collection.insertOne(clientSession, tDocument);
        maybeThrowExceptionAfterUpdate();
    }

    @Override
    public void insertOne(@NotNull ClientSession clientSession,
                          @NotNull TDocument tDocument,
                          @NotNull InsertOneOptions options) {
        maybeThrowExceptionBeforeUpdate();
        collection.insertOne(clientSession, tDocument, options);
        maybeThrowExceptionAfterUpdate();
    }

    @Override
    public void insertMany(@NotNull List<? extends TDocument> tDocuments) {
        maybeThrowExceptionBeforeUpdate();
        collection.insertMany(tDocuments);
        maybeThrowExceptionAfterUpdate();
    }

    @Override
    public void insertMany(@NotNull List<? extends TDocument> tDocuments,
                           @NotNull InsertManyOptions options) {
        maybeThrowExceptionBeforeUpdate();
        collection.insertMany(tDocuments, options);
        maybeThrowExceptionAfterUpdate();
    }

    @Override
    public void insertMany(@NotNull ClientSession clientSession,
                           @NotNull List<? extends TDocument> tDocuments) {
        maybeThrowExceptionBeforeUpdate();
        collection.insertMany(clientSession, tDocuments);
        maybeThrowExceptionAfterUpdate();
    }

    @Override
    public void insertMany(@NotNull ClientSession clientSession,
                           @NotNull List<? extends TDocument> tDocuments,
                           @NotNull InsertManyOptions options) {
        maybeThrowExceptionBeforeUpdate();
        collection.insertMany(clientSession, tDocuments, options);
        maybeThrowExceptionAfterUpdate();
    }

    @NotNull
    @Override
    public DeleteResult deleteOne(@NotNull Bson filter) {
        maybeThrowExceptionBeforeUpdate();
        DeleteResult result = collection.deleteOne(filter);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public DeleteResult deleteOne(@NotNull Bson filter, @NotNull DeleteOptions options) {
        maybeThrowExceptionBeforeUpdate();
        DeleteResult result = collection.deleteOne(filter, options);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public DeleteResult deleteOne(@NotNull ClientSession clientSession, @NotNull Bson filter) {
        maybeThrowExceptionBeforeUpdate();
        DeleteResult result = collection.deleteOne(clientSession, filter);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public DeleteResult deleteOne(@NotNull ClientSession clientSession,
                                  @NotNull Bson filter,
                                  @NotNull DeleteOptions options) {
        maybeThrowExceptionBeforeUpdate();
        DeleteResult result = collection.deleteOne(clientSession, filter, options);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public DeleteResult deleteMany(@NotNull Bson filter) {
        maybeThrowExceptionBeforeUpdate();
        DeleteResult result = collection.deleteMany(filter);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public DeleteResult deleteMany(@NotNull Bson filter, @NotNull DeleteOptions options) {
        maybeThrowExceptionBeforeUpdate();
        DeleteResult result = collection.deleteMany(filter, options);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public DeleteResult deleteMany(@NotNull ClientSession clientSession, @NotNull Bson filter) {
        maybeThrowExceptionBeforeUpdate();
        DeleteResult result = collection.deleteMany(clientSession, filter);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public DeleteResult deleteMany(@NotNull ClientSession clientSession,
                                   @NotNull Bson filter,
                                   @NotNull DeleteOptions options) {
        maybeThrowExceptionBeforeUpdate();
        DeleteResult result = collection.deleteMany(clientSession, filter, options);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult replaceOne(@NotNull Bson filter, @NotNull TDocument replacement) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.replaceOne(filter, replacement);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    @Deprecated
    public UpdateResult replaceOne(@NotNull Bson filter,
                                   @NotNull TDocument replacement,
                                   @NotNull UpdateOptions updateOptions) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.replaceOne(filter, replacement, updateOptions);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult replaceOne(@NotNull ClientSession clientSession,
                                   @NotNull Bson filter,
                                   @NotNull TDocument replacement) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.replaceOne(clientSession, filter, replacement);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    @Deprecated
    public UpdateResult replaceOne(@NotNull ClientSession clientSession,
                                   @NotNull Bson filter,
                                   @NotNull TDocument replacement,
                                   @NotNull UpdateOptions updateOptions) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.replaceOne(clientSession, filter, replacement, updateOptions);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult replaceOne(@NotNull Bson filter,
                                   @NotNull TDocument replacement,
                                   @NotNull ReplaceOptions replaceOptions) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.replaceOne(filter, replacement, replaceOptions);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult replaceOne(@NotNull ClientSession clientSession,
                                   @NotNull Bson filter,
                                   @NotNull TDocument replacement,
                                   @NotNull ReplaceOptions replaceOptions) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.replaceOne(clientSession, filter, replacement, replaceOptions);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult updateOne(@NotNull Bson filter, @NotNull Bson update) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.updateOne(filter, update);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult updateOne(@NotNull Bson filter,
                                  @NotNull Bson update,
                                  @NotNull UpdateOptions updateOptions) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.updateOne(filter, update, updateOptions);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult updateOne(@NotNull ClientSession clientSession,
                                  @NotNull Bson filter,
                                  @NotNull Bson update) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.updateOne(clientSession, filter, update);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult updateOne(@NotNull ClientSession clientSession,
                                  @NotNull Bson filter,
                                  @NotNull Bson update,
                                  @NotNull UpdateOptions updateOptions) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.updateOne(clientSession, filter, update, updateOptions);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult updateMany(@NotNull Bson filter, @NotNull Bson update) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.updateMany(filter, update);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult updateMany(@NotNull Bson filter,
                                   @NotNull Bson update,
                                   @NotNull UpdateOptions updateOptions) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.updateMany(filter, update, updateOptions);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult updateMany(@NotNull ClientSession clientSession,
                                   @NotNull Bson filter,
                                   @NotNull Bson update) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.updateMany(clientSession, filter, update);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @NotNull
    @Override
    public UpdateResult updateMany(@NotNull ClientSession clientSession,
                                   @NotNull Bson filter,
                                   @NotNull Bson update,
                                   @NotNull UpdateOptions updateOptions) {
        maybeThrowExceptionBeforeUpdate();
        UpdateResult result = collection.updateMany(clientSession, filter, update, updateOptions);
        maybeThrowExceptionAfterUpdate();
        return result;
    }

    @Override
    @Nullable
    public TDocument findOneAndDelete(@NotNull Bson filter) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndDelete(filter);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    @Nullable
    public TDocument findOneAndDelete(@NotNull Bson filter,
                                      @NotNull FindOneAndDeleteOptions options) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndDelete(filter, options);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    @Nullable
    public TDocument findOneAndDelete(@NotNull ClientSession clientSession,
                                      @NotNull Bson filter) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndDelete(clientSession, filter);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    @Nullable
    public TDocument findOneAndDelete(@NotNull ClientSession clientSession,
                                      @NotNull Bson filter,
                                      @NotNull FindOneAndDeleteOptions options) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndDelete(clientSession, filter, options);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(@NotNull Bson filter, @NotNull TDocument replacement) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndReplace(filter, replacement);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(@NotNull Bson filter,
                                       @NotNull TDocument replacement,
                                       @NotNull FindOneAndReplaceOptions options) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndReplace(filter, replacement, options);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(@NotNull ClientSession clientSession,
                                       @NotNull Bson filter,
                                       @NotNull TDocument replacement) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndReplace(clientSession, filter, replacement);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    @Nullable
    public TDocument findOneAndReplace(@NotNull ClientSession clientSession,
                                       @NotNull Bson filter,
                                       @NotNull TDocument replacement,
                                       @NotNull FindOneAndReplaceOptions options) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndReplace(clientSession, filter, replacement, options);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(@NotNull Bson filter, @NotNull Bson update) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndUpdate(filter, update);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(@NotNull Bson filter,
                                      @NotNull Bson update,
                                      @NotNull FindOneAndUpdateOptions options) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndUpdate(filter, update, options);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(@NotNull ClientSession clientSession,
                                      @NotNull Bson filter,
                                      @NotNull Bson update) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndUpdate(clientSession, filter, update);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    @Nullable
    public TDocument findOneAndUpdate(@NotNull ClientSession clientSession,
                                      @NotNull Bson filter,
                                      @NotNull Bson update,
                                      @NotNull FindOneAndUpdateOptions options) {
        maybeThrowExceptionBeforeUpdate();
        TDocument doc = collection.findOneAndUpdate(clientSession, filter, update, options);
        maybeThrowExceptionAfterUpdate();
        return doc;
    }

    @Override
    public void drop() {
        collection.drop();
    }

    @Override
    public void drop(@NotNull ClientSession clientSession) {
        collection.drop(clientSession);
    }

    @NotNull
    @Override
    public String createIndex(@NotNull Bson keys) {
        return collection.createIndex(keys);
    }

    @NotNull
    @Override
    public String createIndex(@NotNull Bson keys, @NotNull IndexOptions indexOptions) {
        return collection.createIndex(keys, indexOptions);
    }

    @NotNull
    @Override
    public String createIndex(@NotNull ClientSession clientSession, @NotNull Bson keys) {
        return collection.createIndex(clientSession, keys);
    }

    @NotNull
    @Override
    public String createIndex(@NotNull ClientSession clientSession,
                              @NotNull Bson keys,
                              @NotNull IndexOptions indexOptions) {
        return collection.createIndex(clientSession, keys, indexOptions);
    }

    @NotNull
    @Override
    public List<String> createIndexes(@NotNull List<IndexModel> indexes) {
        return collection.createIndexes(indexes);
    }

    @NotNull
    @Override
    public List<String> createIndexes(@NotNull List<IndexModel> indexes,
                                      @NotNull CreateIndexOptions createIndexOptions) {
        return collection.createIndexes(indexes, createIndexOptions);
    }

    @NotNull
    @Override
    public List<String> createIndexes(@NotNull ClientSession clientSession,
                                      @NotNull List<IndexModel> indexes) {
        return collection.createIndexes(clientSession, indexes);
    }

    @NotNull
    @Override
    public List<String> createIndexes(@NotNull ClientSession clientSession,
                                      @NotNull List<IndexModel> indexes,
                                      @NotNull CreateIndexOptions createIndexOptions) {
        return collection.createIndexes(clientSession, indexes, createIndexOptions);
    }

    @NotNull
    @Override
    public ListIndexesIterable<Document> listIndexes() {
        return collection.listIndexes();
    }

    @NotNull
    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(@NotNull Class<TResult> tResultClass) {
        return collection.listIndexes(tResultClass);
    }

    @NotNull
    @Override
    public ListIndexesIterable<Document> listIndexes(@NotNull ClientSession clientSession) {
        return collection.listIndexes(clientSession);
    }

    @NotNull
    @Override
    public <TResult> ListIndexesIterable<TResult> listIndexes(@NotNull ClientSession clientSession,
                                                              @NotNull Class<TResult> tResultClass) {
        return collection.listIndexes(clientSession, tResultClass);
    }

    @Override
    public void dropIndex(@NotNull String indexName) {
        collection.dropIndex(indexName);
    }

    @Override
    public void dropIndex(@NotNull String indexName, @NotNull DropIndexOptions dropIndexOptions) {
        collection.dropIndex(indexName, dropIndexOptions);
    }

    @Override
    public void dropIndex(@NotNull Bson keys) {
        collection.dropIndex(keys);
    }

    @Override
    public void dropIndex(@NotNull Bson keys, @NotNull DropIndexOptions dropIndexOptions) {
        collection.dropIndex(keys, dropIndexOptions);
    }

    @Override
    public void dropIndex(@NotNull ClientSession clientSession, @NotNull String indexName) {
        collection.dropIndex(clientSession, indexName);
    }

    @Override
    public void dropIndex(@NotNull ClientSession clientSession, @NotNull Bson keys) {
        collection.dropIndex(clientSession, keys);
    }

    @Override
    public void dropIndex(@NotNull ClientSession clientSession,
                          @NotNull String indexName,
                          @NotNull DropIndexOptions dropIndexOptions) {
        collection.dropIndex(clientSession, indexName, dropIndexOptions);
    }

    @Override
    public void dropIndex(@NotNull ClientSession clientSession,
                          @NotNull Bson keys,
                          @NotNull DropIndexOptions dropIndexOptions) {
        collection.dropIndex(clientSession, keys, dropIndexOptions);
    }

    @Override
    public void dropIndexes() {
        collection.dropIndexes();
    }

    @Override
    public void dropIndexes(@NotNull ClientSession clientSession) {
        collection.dropIndexes(clientSession);
    }

    @Override
    public void dropIndexes(@NotNull DropIndexOptions dropIndexOptions) {
        collection.dropIndexes(dropIndexOptions);
    }

    @Override
    public void dropIndexes(@NotNull ClientSession clientSession,
                            @NotNull DropIndexOptions dropIndexOptions) {
        collection.dropIndexes(clientSession, dropIndexOptions);
    }

    @Override
    public void renameCollection(@NotNull MongoNamespace newCollectionNamespace) {
        collection.renameCollection(newCollectionNamespace);
    }

    @Override
    public void renameCollection(@NotNull MongoNamespace newCollectionNamespace,
                                 @NotNull RenameCollectionOptions renameCollectionOptions) {
        collection.renameCollection(newCollectionNamespace, renameCollectionOptions);
    }

    @Override
    public void renameCollection(@NotNull ClientSession clientSession,
                                 @NotNull MongoNamespace newCollectionNamespace) {
        collection.renameCollection(clientSession, newCollectionNamespace);
    }

    @Override
    public void renameCollection(@NotNull ClientSession clientSession,
                                 @NotNull MongoNamespace newCollectionNamespace,
                                 @NotNull RenameCollectionOptions renameCollectionOptions) {
        collection.renameCollection(clientSession, newCollectionNamespace, renameCollectionOptions);
    }

    private void maybeThrowExceptionBeforeQuery() {
        String msg = beforeQueryException.get();
        if (msg != null) {
            throw new MongoException(msg);
        }
    }

    private void maybeThrowExceptionBeforeUpdate() {
        String msg = beforeUpdateException.get();
        if (msg != null) {
            throw new MongoException(msg);
        }
    }

    private void maybeThrowExceptionAfterUpdate() {
        String msg = afterUpdateException.get();
        if (msg != null) {
            throw new MongoException(msg);
        }
    }
}

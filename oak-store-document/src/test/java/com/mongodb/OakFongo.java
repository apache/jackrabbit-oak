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
package com.mongodb;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.github.fakemongo.Fongo;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerVersion;
import com.mongodb.session.ClientSession;

import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import static java.util.stream.Collectors.toList;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class OakFongo extends Fongo {

    private static final CodecRegistry CODEC_REGISTRY = fromRegistries(
            MongoClient.getDefaultCodecRegistry()
    );

    private final Map<String, FongoDB> dbMap;

    private final MongoClient client;

    public OakFongo(String name) throws Exception {
        super(name);
        this.dbMap = getDBMap();
        this.client = createClientProxy();
    }

    @Override
    public MongoClient getMongo() {
        return client;
    }

    @Override
    public FongoDB getDB(String dbname) {
        synchronized (dbMap) {
            FongoDB fongoDb = dbMap.get(dbname);
            if (fongoDb == null) {
                try {
                    fongoDb = new OakFongoDB(this, dbname);
                } catch (Exception e) {
                    throw new MongoException(e.getMessage(), e);
                }
                dbMap.put(dbname, fongoDb);
            }
            return fongoDb;
        }
    }

    @Override
    public FongoMongoDatabase getDatabase(String databaseName) {
        return new OakFongoMongoDatabase(databaseName, this);
    }

    @SuppressWarnings("unchecked")
    private Map<String, FongoDB> getDBMap() throws Exception {
        Field f = Fongo.class.getDeclaredField("dbMap");
        f.setAccessible(true);
        return (Map<String, FongoDB>) f.get(this);
    }

    private MongoClient createClientProxy() {
        MongoClient c = spy(super.getMongo());
        for (String dbName : new String[]{MongoUtils.DB, "oak"}) {
            when(c.getDatabase(dbName)).thenReturn(new OakFongoMongoDatabase(dbName, this));
        }
        try {
            Field credentialsList = Mongo.class.getDeclaredField("credentialsList");
            credentialsList.setAccessible(true);
            credentialsList.set(c, Collections.emptyList());

            ClusterDescription cd = new ClusterDescription(ClusterConnectionMode.SINGLE,
                    ClusterType.STANDALONE, Collections.emptyList());
            Cluster cl = mock(Cluster.class);
            when(cl.getDescription()).thenReturn(cd);

            Field cluster = Mongo.class.getDeclaredField("cluster");
            cluster.setAccessible(true);
            cluster.set(c, cl);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return c;
    }

    protected void beforeInsert(List<? extends DBObject> documents,
                                InsertOptions insertOptions) {}

    protected void afterInsert(WriteResult result) {}

    protected void beforeFindAndModify(DBObject query,
                                       DBObject fields,
                                       DBObject sort,
                                       boolean remove,
                                       DBObject update,
                                       boolean returnNew,
                                       boolean upsert) {}

    protected void afterFindAndModify(DBObject result) {}

    protected void beforeUpdate(DBObject q,
                                DBObject o,
                                boolean upsert,
                                boolean multi,
                                WriteConcern concern,
                                DBEncoder encoder) {}

    protected void afterUpdate(WriteResult result) {}

    protected void beforeRemove(DBObject query, WriteConcern writeConcern) {}

    protected void afterRemove(WriteResult result) {}

    protected void beforeExecuteBulkWriteOperation(boolean ordered,
                                                   Boolean bypassDocumentValidation,
                                                   List<?> writeRequests,
                                                   WriteConcern aWriteConcern) {}

    protected void beforeFind(DBObject query, DBObject projection) {}

    protected void afterFind(DBCursor cursor) {}

    protected void afterExecuteBulkWriteOperation(BulkWriteResult result) {}

    private class OakFongoDB extends FongoDB {

        private final Map<String, FongoDBCollection> collMap;

        public OakFongoDB(Fongo fongo, String name) throws Exception {
            super(fongo, name);
            this.collMap = getCollMap();
        }

        @SuppressWarnings("unchecked")
        private Map<String, FongoDBCollection> getCollMap() throws Exception {
            Field f = FongoDB.class.getDeclaredField("collMap");
            f.setAccessible(true);
            return (Map<String, FongoDBCollection>) f.get(this);
        }

        @Override
        public CommandResult command(DBObject cmd,
                                     ReadPreference readPreference,
                                     DBEncoder encoder) {
            if (cmd.containsField("serverStatus")) {
                CommandResult commandResult = okResult();
                commandResult.append("version", asString(getServerVersion()));
                return commandResult;
            } else {
                return super.command(cmd, readPreference, encoder);
            }
        }

        @Override
        public synchronized FongoDBCollection doGetCollection(String name,
                                                              boolean idIsNotUniq,
                                                              boolean validateOnInsert) {
            if (name.startsWith("system.")) {
                return super.doGetCollection(name, idIsNotUniq, validateOnInsert);
            }
            FongoDBCollection coll = collMap.get(name);
            if (coll == null) {
                coll = new OakFongoDBCollection(this, name, idIsNotUniq, validateOnInsert);
                collMap.put(name, coll);
            }
            return coll;
        }

    }

    private static String asString(ServerVersion serverVersion) {
        StringBuilder sb = new StringBuilder();
        for (int i : serverVersion.getVersionList()) {
            if (sb.length() != 0) {
                sb.append('.');
            }
            sb.append(String.valueOf(i));
        }
        return sb.toString();
    }

    private class OakFongoDBCollection extends FongoDBCollection {

        public OakFongoDBCollection(FongoDB db,
                                    String name,
                                    boolean idIsNotUniq,
                                    boolean validateOnInsert) {
            super(db, name, idIsNotUniq, validateOnInsert);
        }

        @Override
        public WriteResult insert(List<? extends DBObject> documents,
                                               InsertOptions insertOptions) {
            beforeInsert(documents, insertOptions);
            WriteResult result = super.insert(documents, insertOptions);
            afterInsert(result);
            return result;
        }

        @Override
        public WriteResult remove(DBObject query, WriteConcern writeConcern) {
            beforeRemove(query, writeConcern);
            WriteResult result = super.remove(query, writeConcern);
            afterRemove(result);
            return result;
        }

        @Override
        public WriteResult update(DBObject q,
                                  DBObject o,
                                  boolean upsert,
                                  boolean multi,
                                  WriteConcern concern,
                                  DBEncoder encoder) {
            beforeUpdate(q, o, upsert, multi, concern, encoder);
            WriteResult result = super.update(q, o, upsert, multi, concern, encoder);
            afterUpdate(result);
            return result;
        }

        @Override
        public DBObject findAndModify(DBObject query,
                                      DBObject fields,
                                      DBObject sort,
                                      boolean remove,
                                      DBObject update,
                                      boolean returnNew,
                                      boolean upsert) {
            beforeFindAndModify(query, fields, sort, remove, update, returnNew, upsert);
            DBObject result = super.findAndModify(query, fields, sort, remove, update, returnNew, upsert);
            afterFindAndModify(result);
            return result;
        }

        @Override
        BulkWriteResult executeBulkWriteOperation(boolean ordered,
                                                  Boolean bypassDocumentValidation,
                                                  List<WriteRequest> writeRequests,
                                                  WriteConcern aWriteConcern) {
            beforeExecuteBulkWriteOperation(ordered, bypassDocumentValidation, writeRequests, aWriteConcern);
            BulkWriteResult result = super.executeBulkWriteOperation(ordered, bypassDocumentValidation, writeRequests, aWriteConcern);
            afterExecuteBulkWriteOperation(result);
            return result;
        }

        @Override
        public DBCursor find(DBObject query, DBObject projection) {
            beforeFind(query, projection);
            DBCursor result = super.find(query, projection);
            afterFind(result);
            return result;
        }
    }

    private class OakFongoMongoDatabase extends FongoMongoDatabase {

        private final Fongo fongo;

        public OakFongoMongoDatabase(String databaseName, Fongo fongo) {
            super(databaseName, fongo);
            this.fongo = fongo;
        }

        @Override
        public MongoCollection<Document> getCollection(String collectionName) {
            return new OakFongoMongoCollection(this.fongo, new MongoNamespace(super.getName(), collectionName), super.getCodecRegistry(), super.getReadPreference(), super.getWriteConcern(), super.getReadConcern());
        }

        @SuppressWarnings("unchecked")
        @Override
        public <TResult> TResult runCommand(Bson command,
                                            ReadPreference readPreference,
                                            Class<TResult> tResultClass) {
            if (BasicDBObject.class.equals(tResultClass)) {
                BasicDBObject result = new BasicDBObject();
                result.append("version", asString(getServerVersion()));
                return (TResult) result;
            }
            return super.runCommand(command, readPreference, tResultClass);
        }
    }

    private class OakFongoMongoCollection extends FongoMongoCollection<Document> {

        private final Fongo fongo;

        OakFongoMongoCollection(Fongo fongo,
                                MongoNamespace namespace,
                                CodecRegistry codecRegistry,
                                ReadPreference readPreference,
                                WriteConcern writeConcern,
                                ReadConcern readConcern) {
            super(fongo, namespace, Document.class, codecRegistry, readPreference, writeConcern, readConcern);
            this.fongo = fongo;
        }

        @Override
        public void insertMany(ClientSession clientSession,
                               List<? extends Document> documents,
                               InsertManyOptions options) {
            beforeInsert(asDBObjects(documents), new InsertOptions());
            super.insertMany(clientSession, documents, options);
            WriteResult result = new WriteResult(documents.size(), false, null);
            afterInsert(result);
        }

        @Override
        public DeleteResult deleteMany(ClientSession clientSession,
                                       Bson filter,
                                       DeleteOptions options) {
            beforeRemove(asDBObject(filter), getWriteConcern());
            DeleteResult result = super.deleteMany(clientSession, filter, options);
            afterRemove(new WriteResult((int) result.getDeletedCount(), false, null));
            return result;
        }

        @Override
        public UpdateResult updateMany(ClientSession clientSession,
                                       Bson filter,
                                       Bson update,
                                       UpdateOptions updateOptions) {
            beforeUpdate(asDBObject(filter), asDBObject(update), updateOptions.isUpsert(), true, getWriteConcern(), new DefaultDBEncoder());
            UpdateResult result = super.updateMany(clientSession, filter, update, updateOptions);
            afterUpdate(new WriteResult((int) result.getModifiedCount(), true, result.getUpsertedId().asString().getValue()));
            return result;
        }

        @Override
        public Document findOneAndUpdate(ClientSession clientSession,
                                         Bson filter,
                                         Bson update,
                                         FindOneAndUpdateOptions options) {
            beforeFindAndModify(asDBObject(filter), null, null, false, asDBObject(update), options.getReturnDocument() == ReturnDocument.AFTER, options.isUpsert());
            Document result = super.findOneAndUpdate(clientSession, filter, update, options);
            afterFindAndModify(asDBObject(result));
            return result;
        }

        @Override
        public FindIterable<Document> find(ClientSession clientSession,
                                           Bson filter) {
            beforeFind(asDBObject(filter), null);
            FindIterable<Document> result = super.find(clientSession, filter);
            afterFind(new FongoDBCursor(fongo.getDB(getNamespace().getDatabaseName()).getCollection(getNamespace().getCollectionName()), asDBObject(filter), null));
            return result;
        }

        @Override
        public com.mongodb.bulk.BulkWriteResult bulkWrite(ClientSession clientSession,
                                                          List<? extends WriteModel<? extends Document>> requests,
                                                          BulkWriteOptions options) {
            beforeExecuteBulkWriteOperation(options.isOrdered(), options.getBypassDocumentValidation(), requests, getWriteConcern());
            com.mongodb.bulk.BulkWriteResult result = super.bulkWrite(clientSession, requests, options);
            afterExecuteBulkWriteOperation(new AcknowledgedBulkWriteResult(result.getInsertedCount(), result.getMatchedCount(), result.getDeletedCount(), result.getModifiedCount(), this.transform(result.getUpserts())));
            return result;
        }

        private List<BulkWriteUpsert> transform(List<com.mongodb.bulk.BulkWriteUpsert> upserts) {
            return upserts.stream().map(bulkWriteUpsert -> new BulkWriteUpsert(bulkWriteUpsert.getIndex(), bulkWriteUpsert.getId().asString().getValue())).collect(toList());
        }

        @Override
        public MongoCollection<Document> withCodecRegistry(CodecRegistry codecRegistry) {
            return new OakFongoMongoCollection(fongo, super.getNamespace(), codecRegistry, super.getReadPreference(), super.getWriteConcern(), super.getReadConcern());
        }

        @Override
        public MongoCollection<Document> withReadPreference(ReadPreference readPreference) {
            return new OakFongoMongoCollection(fongo, super.getNamespace(), super.getCodecRegistry(), readPreference, super.getWriteConcern(), super.getReadConcern());
        }

        @Override
        public MongoCollection<Document> withWriteConcern(WriteConcern writeConcern) {
            return new OakFongoMongoCollection(fongo, super.getNamespace(), super.getCodecRegistry(), super.getReadPreference(), writeConcern, super.getReadConcern());
        }

        @Override
        public MongoCollection<Document> withReadConcern(ReadConcern readConcern) {
            return new OakFongoMongoCollection(fongo, super.getNamespace(), super.getCodecRegistry(), super.getReadPreference(), super.getWriteConcern(), readConcern);
        }

        private List<DBObject> asDBObjects(List<? extends Document> docs) {
            return docs.stream().map(this::asDBObject).collect(toList());
        }

        private DBObject asDBObject(Bson bson) {
            return new BasicDBObject(bson.toBsonDocument(Document.class, CODEC_REGISTRY));
        }
    }
}

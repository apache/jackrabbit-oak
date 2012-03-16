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
package org.apache.jackrabbit.mk.blobs;

import java.io.IOException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

/**
 * A blob store that uses MongoDB.
 */
public class MongoBlobStore extends AbstractBlobStore {

    private static final String DB = "ds";
    private static final String DATASTORE_COLLECTION = "dataStore";
    private static final String DIGEST_FIELD = "digest";
    private static final String DATA_FIELD = "data";

    private Mongo con;
    private DB db;
    private DBCollection dataStore;

    public MongoBlobStore() throws IOException {
        con = new Mongo();
        db = con.getDB(DB);
        db.setWriteConcern(WriteConcern.SAFE);
        dataStore = db.getCollection(DATASTORE_COLLECTION);
        dataStore.ensureIndex(
                new BasicDBObject(DIGEST_FIELD, 1),
                new BasicDBObject("unique", true));
    }

    @Override
    protected byte[] readBlockFromBackend(BlockId id) {
        BasicDBObject key = new BasicDBObject(DIGEST_FIELD, id.digest);
        DBObject dataObject = dataStore.findOne(key);
        return (byte[]) dataObject.get(DATA_FIELD);
    }

    @Override
    protected void storeBlock(byte[] digest, int level, byte[] data) {
        BasicDBObject dataObject = new BasicDBObject(DIGEST_FIELD, digest);
        dataObject.append(DATA_FIELD, data);
        try {
            dataStore.insert(dataObject);
        } catch (MongoException.DuplicateKey ignore) {
            // ignore
        }
    }

    @Override
    public void close() {
        con.close();
    }

    @Override
    public void startMark() throws Exception {
        // TODO
        markInUse();
    }

    @Override
    protected boolean isMarkEnabled() {
        // TODO
        return false;
    }

    @Override
    protected void mark(BlockId id) throws Exception {
        // TODO
    }

    @Override
    public int sweep() throws Exception {
        // TODO
        return 0;
    }

}

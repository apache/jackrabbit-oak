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
package org.apache.jackrabbit.mongomk.impl.blob;

import org.apache.jackrabbit.mk.blobs.AbstractBlobStore;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.util.StringUtils;
import org.apache.jackrabbit.mongomk.impl.model.MongoBlob;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteResult;

/**
 * Implementation of {@link BlobStore} for the {@code MongoDB} extending from
 * {@link AbstractBlobStore}. Unlike {@link MongoGridFSBlobStore}, it saves blobs
 * into a separate collection in {@link MongoDB} instead of GridFS.
 *
 * FIXME:
 * -Do we need to create commands for retry etc.?
 * -Implement GC
 */
public class MongoBlobStore extends AbstractBlobStore {

    public static final String COLLECTION_BLOBS = "blobs";

    private final DB db;
    private long minLastModified;

    /**
     * Constructs a new {@code MongoBlobStore}
     *
     * @param db The DB.
     */
    public MongoBlobStore(DB db) {
        this.db = db;
        initBlobCollection();
    }

    @Override
    protected void storeBlock(byte[] digest, int level, byte[] data) throws Exception {
        String id = StringUtils.convertBytesToHex(digest);
        // Check if it already exists?
        MongoBlob mongoBlob = new MongoBlob();
        mongoBlob.setId(id);
        mongoBlob.setData(data);
        mongoBlob.setLevel(level);
        getBlobCollection().insert(mongoBlob);
    }

    @Override
    protected byte[] readBlockFromBackend(BlockId blockId) throws Exception {
        String id = StringUtils.convertBytesToHex(blockId.getDigest());
        MongoBlob blobMongo = getBlob(id, 0);
        byte[] data = blobMongo.getData();

        if (blockId.getPos() == 0) {
            return data;
        }

        int len = (int) (data.length - blockId.getPos());
        if (len < 0) {
            return new byte[0];
        }
        byte[] d2 = new byte[len];
        System.arraycopy(data, (int) blockId.getPos(), d2, 0, len);
        return d2;
    }

    @Override
    public void startMark() throws Exception {
        minLastModified = System.currentTimeMillis();
        markInUse();
    }

    @Override
    protected boolean isMarkEnabled() {
        return minLastModified != 0;
    }

    @Override
    protected void mark(BlockId blockId) throws Exception {
        if (minLastModified == 0) {
            return;
        }
        String id = StringUtils.convertBytesToHex(blockId.getDigest());
        DBObject query = getBlobQuery(id, minLastModified);
        DBObject update = new BasicDBObject("$set",
                new BasicDBObject(MongoBlob.KEY_LAST_MOD, System.currentTimeMillis()));
        WriteResult writeResult = getBlobCollection().update(query, update);
        if (writeResult.getError() != null) {
            // Handle
        }
    }

    @Override
    public int sweep() throws Exception {
        DBObject query = getBlobQuery(null, minLastModified);
        long beforeCount = getBlobCollection().count(query);
        WriteResult writeResult = getBlobCollection().remove(query);
        if (writeResult.getError() != null) {
            // Handle
        }

        long afterCount = getBlobCollection().count(query);
        minLastModified = 0;
        return (int)(beforeCount - afterCount);
    }

    private DBCollection getBlobCollection() {
        DBCollection collection = db.getCollection(COLLECTION_BLOBS);
        collection.setObjectClass(MongoBlob.class);
        return collection;
    }

    private void initBlobCollection() {
        if (db.collectionExists(COLLECTION_BLOBS)) {
            return;
        }
        DBCollection collection = getBlobCollection();
        DBObject index = new BasicDBObject();
        index.put(MongoBlob.KEY_ID, 1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.TRUE);
        collection.ensureIndex(index, options);
    }

    private MongoBlob getBlob(String id, long lastMod) {
        DBObject query = getBlobQuery(id, lastMod);
        return (MongoBlob)getBlobCollection().findOne(query);
    }

    private DBObject getBlobQuery(String id, long lastMod) {
        QueryBuilder queryBuilder = new QueryBuilder();
        if (id != null) {
            queryBuilder = queryBuilder.and(MongoBlob.KEY_ID).is(id);
        }
        if (lastMod > 0) {
            queryBuilder = queryBuilder.and(MongoBlob.KEY_LAST_MOD).lessThan(lastMod);
        }
        return queryBuilder.get();
    }
}
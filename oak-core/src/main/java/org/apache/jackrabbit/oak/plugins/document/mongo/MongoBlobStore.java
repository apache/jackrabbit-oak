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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.blob.CachingBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.QueryBuilder;
import com.mongodb.ReadPreference;
import com.mongodb.WriteResult;

/**
 * Implementation of blob store for the MongoDB extending from
 * {@link org.apache.jackrabbit.oak.spi.blob.AbstractBlobStore}. It saves blobs into a separate collection in
 * MongoDB (not using GridFS) and it supports basic garbage collection.
 *
 * FIXME: -Do we need to create commands for retry etc.? -Not sure if this is
 * going to work for multiple MKs talking to same MongoDB?
 */
public class MongoBlobStore extends CachingBlobStore {

    public static final String COLLECTION_BLOBS = "blobs";

    private static final Logger LOG = LoggerFactory.getLogger(MongoBlobStore.class);

    private final DB db;
    private long minLastModified;

    /**
     * Constructs a new {@code MongoBlobStore}
     *
     * @param db The DB.
     */
    public MongoBlobStore(DB db) {
        this(db, DEFAULT_CACHE_SIZE);
    }

    public MongoBlobStore(DB db, long cacheSize) {
        super(cacheSize);
        this.db = db;
        // use a block size of 2 MB - 1 KB, because MongoDB rounds up the
        // space allocated for a record to the next power of two
        // (there is an overhead per record, let's assume it is 1 KB at most)
        setBlockSize(2 * 1024 * 1024 - 1024);
        initBlobCollection();
    }

    @Override
    protected void storeBlock(byte[] digest, int level, byte[] data) throws IOException {
        String id = StringUtils.convertBytesToHex(digest);
        cache.put(id, data);
        // Check if it already exists?
        MongoBlob mongoBlob = new MongoBlob();
        mongoBlob.setId(id);
        mongoBlob.setData(data);
        mongoBlob.setLevel(level);
        mongoBlob.setLastMod(System.currentTimeMillis());
        // TODO check the return value
        // TODO verify insert is fast if the entry already exists
        try {
            getBlobCollection().insert(mongoBlob);
        } catch (DuplicateKeyException e) {
            // the same block was already stored before: ignore
        }
    }

    @Override
    protected byte[] readBlockFromBackend(BlockId blockId) throws Exception {
        String id = StringUtils.convertBytesToHex(blockId.getDigest());
        byte[] data = cache.get(id);
        if (data == null) {
            long start = System.nanoTime();
            MongoBlob blobMongo = getBlob(id, 0);
            if (blobMongo == null) {
                String message = "Did not find block " + id;
                LOG.error(message);
                throw new IOException(message);
            }
            data = blobMongo.getData();
            getStatsCollector().downloaded(id, System.nanoTime() - start, TimeUnit.NANOSECONDS, data.length);
            cache.put(id, data);
        }
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
    public void startMark() throws IOException {
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
        getBlobCollection().update(query, update);
    }

    @Override
    public int sweep() throws IOException {
        DBObject query = getBlobQuery(null, minLastModified);
        long countBefore = getBlobCollection().count(query);
        getBlobCollection().remove(query);

        long countAfter = getBlobCollection().count(query);
        minLastModified = 0;
        return (int) (countBefore - countAfter);
    }

    private DBCollection getBlobCollection() {
        DBCollection collection = db.getCollection(COLLECTION_BLOBS);
        collection.setObjectClass(MongoBlob.class);
        return collection;
    }

    private void initBlobCollection() {
        if (!db.collectionExists(COLLECTION_BLOBS)) {
            db.createCollection(COLLECTION_BLOBS, new BasicDBObject());
        }
    }

    private MongoBlob getBlob(String id, long lastMod) {
        DBObject query = getBlobQuery(id, lastMod);

        // try the secondary first
        // TODO add a configuration option for whether to try reading from secondary
        ReadPreference pref = ReadPreference.secondaryPreferred();
        DBObject fields = new BasicDBObject();
        fields.put(MongoBlob.KEY_DATA, 1);
        MongoBlob blob = (MongoBlob) getBlobCollection().findOne(query, fields, pref);
        if (blob == null) {
            // not found in the secondary: try the primary
            pref = ReadPreference.primary();
            blob = (MongoBlob) getBlobCollection().findOne(query, fields, pref);
        }
        return blob;
    }

    private static DBObject getBlobQuery(String id, long lastMod) {
        QueryBuilder queryBuilder = new QueryBuilder();
        if (id != null) {
            queryBuilder = queryBuilder.and(MongoBlob.KEY_ID).is(id);
        }
        if (lastMod > 0) {
            queryBuilder = queryBuilder.and(MongoBlob.KEY_LAST_MOD).lessThan(lastMod);
        }
        return queryBuilder.get();
    }

    @Override
    public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
        DBCollection collection = getBlobCollection();
        QueryBuilder queryBuilder = new QueryBuilder();
        if (chunkIds != null) {
            queryBuilder = queryBuilder.and(MongoBlob.KEY_ID).in(chunkIds.toArray(new String[0]));
            if (maxLastModifiedTime > 0) {
                queryBuilder = queryBuilder.and(MongoBlob.KEY_LAST_MOD)
                                    .lessThan(maxLastModifiedTime);
            }
        }

        WriteResult result = collection.remove(queryBuilder.get());
        return result.getN();
    }

    @Override
    public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
        DBCollection collection = getBlobCollection();

        DBObject fields = new BasicDBObject();
        fields.put(MongoBlob.KEY_ID, 1);

        QueryBuilder builder = new QueryBuilder();
        if (maxLastModifiedTime != 0 && maxLastModifiedTime != -1) {
            builder.and(MongoBlob.KEY_LAST_MOD).lessThanEquals(maxLastModifiedTime);
        }

        final DBCursor cur =
                collection.find(builder.get(), fields).hint(fields)
                        .addOption(Bytes.QUERYOPTION_SLAVEOK);

        //TODO The cursor needs to be closed
        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                if (cur.hasNext()) {
                    MongoBlob blob = (MongoBlob) cur.next();
                    if (blob != null) {
                        return blob.getId();
                    }
                }
                return endOfData();
            }
        };
    }
}

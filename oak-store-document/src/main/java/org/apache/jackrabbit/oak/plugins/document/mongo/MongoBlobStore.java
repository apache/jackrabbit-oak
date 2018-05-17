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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mongodb.ReadPreference;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.blob.CachingBlobStore;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import static com.mongodb.ReadPreference.primary;
import static java.util.stream.StreamSupport.stream;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * Implementation of blob store for the MongoDB extending from
 * {@link CachingBlobStore}. It saves blobs into a separate collection in
 * MongoDB (not using GridFS) and it supports basic garbage collection.
 *
 * FIXME: -Do we need to create commands for retry etc.? -Not sure if this is
 * going to work for multiple MKs talking to same MongoDB?
 */
public class MongoBlobStore extends CachingBlobStore {

    public static final String COLLECTION_BLOBS = "blobs";

    private static final Logger LOG = LoggerFactory.getLogger(MongoBlobStore.class);

    private static final int DUPLICATE_KEY_ERROR_CODE = 11000;

    private static final CodecRegistry CODEC_REGISTRY = fromRegistries(
            MongoClient.getDefaultCodecRegistry(),
            fromCodecs(new MongoBlobCodec())
    );

    private final ReadPreference defaultReadPreference;
    private final MongoCollection<MongoBlob> blobCollection;
    private long minLastModified;

    /**
     * Constructs a new {@code MongoBlobStore}
     *
     * @param db The DB.
     */
    public MongoBlobStore(MongoDatabase db) {
        this(db, DEFAULT_CACHE_SIZE);
    }

    public MongoBlobStore(MongoDatabase db, long cacheSize) {
        super(cacheSize);
        // use a block size of 2 MB - 1 KB, because MongoDB rounds up the
        // space allocated for a record to the next power of two
        // (there is an overhead per record, let's assume it is 1 KB at most)
        setBlockSize(2 * 1024 * 1024 - 1024);
        defaultReadPreference = db.getReadPreference();
        blobCollection = initBlobCollection(db);
    }

    @Override
    protected void storeBlock(byte[] digest, int level, byte[] data) throws IOException {
        String id = StringUtils.convertBytesToHex(digest);
        cache.put(id, data);

        // Create the mongo blob object
        BasicDBObject mongoBlob = new BasicDBObject(MongoBlob.KEY_ID, id);
        mongoBlob.append(MongoBlob.KEY_DATA, data);
        mongoBlob.append(MongoBlob.KEY_LEVEL, level);

        // If update only the lastMod needs to be modified
        BasicDBObject updateBlob =new BasicDBObject(MongoBlob.KEY_LAST_MOD, System.currentTimeMillis());

        BasicDBObject upsert = new BasicDBObject();
        upsert.append("$setOnInsert", mongoBlob)
            .append("$set", updateBlob);

        try {
            Bson query = getBlobQuery(id, -1);
            UpdateOptions options = new UpdateOptions().upsert(true);
            UpdateResult result = getBlobCollection().updateOne(query, upsert, options);
            if (result != null && result.getUpsertedId() == null) {
                LOG.trace("Block with id [{}] updated", id);
            } else {
                LOG.trace("Block with id [{}] created", id);
            }
        } catch (MongoException e) {
            throw new IOException(e.getMessage(), e);
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
        Bson query = getBlobQuery(id, minLastModified);
        Bson update = new BasicDBObject("$set",
                new BasicDBObject(MongoBlob.KEY_LAST_MOD, System.currentTimeMillis()));
        getBlobCollection().updateOne(query, update);
    }

    @Override
    public int sweep() throws IOException {
        Bson query = getBlobQuery(null, minLastModified);
        long num = getBlobCollection().deleteMany(query).getDeletedCount();
        minLastModified = 0;
        return (int) num;
    }

    private MongoCollection<MongoBlob> initBlobCollection(MongoDatabase db) {
        if (stream(db.listCollectionNames().spliterator(), false)
                .noneMatch(COLLECTION_BLOBS::equals)) {
            db.createCollection(COLLECTION_BLOBS);
        }
        // override the read preference configured with the MongoDB URI
        // and use the primary as default. Reading a blob will still
        // try a secondary first and then fallback to the primary.
        return db.getCollection(COLLECTION_BLOBS, MongoBlob.class)
                .withCodecRegistry(CODEC_REGISTRY)
                .withReadPreference(primary());
    }

    private MongoCollection<MongoBlob> getBlobCollection() {
        return this.blobCollection;
    }

    private MongoBlob getBlob(String id, long lastMod) {
        Bson query = getBlobQuery(id, lastMod);
        Bson fields = new BasicDBObject(MongoBlob.KEY_DATA, 1);

        // try with default read preference first, may be from secondary
        List<MongoBlob> result = new ArrayList<>(1);
        getBlobCollection().withReadPreference(defaultReadPreference).find(query)
                .projection(fields).into(result);
        if (result.isEmpty()) {
            // not found in the secondary: try the primary
            getBlobCollection().withReadPreference(primary()).find(query)
                    .projection(fields).into(result);
        }
        return result.isEmpty() ? null : result.get(0);
    }

    private static Bson getBlobQuery(String id, long lastMod) {
        List<Bson> clauses = new ArrayList<>(2);
        if (id != null) {
            clauses.add(Filters.eq(MongoBlob.KEY_ID, id));
        }
        if (lastMod > 0) {
            clauses.add(Filters.lt(MongoBlob.KEY_LAST_MOD, lastMod));
        }

        if (clauses.size() == 1) {
            return clauses.get(0);
        } else {
            return Filters.and(clauses);
        }
    }

    @Override
    public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
        Bson query = new Document();
        if (chunkIds != null) {
            query = Filters.in(MongoBlob.KEY_ID, chunkIds);
            if (maxLastModifiedTime > 0) {
                query = Filters.and(
                        query,
                        Filters.lt(MongoBlob.KEY_LAST_MOD, maxLastModifiedTime)
                );
            }
        }

        return getBlobCollection().deleteMany(query).getDeletedCount();
    }

    @Override
    public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
        Bson fields = new BasicDBObject(MongoBlob.KEY_ID, 1);

        Bson query = new Document();
        if (maxLastModifiedTime != 0 && maxLastModifiedTime != -1) {
            query = Filters.lte(MongoBlob.KEY_LAST_MOD, maxLastModifiedTime);
        }

        final MongoCursor<MongoBlob> cur = getBlobCollection().find(query)
                .projection(fields).hint(fields).iterator();

        //TODO The cursor needs to be closed
        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                if (cur.hasNext()) {
                    MongoBlob blob = cur.next();
                    if (blob != null) {
                        return blob.getId();
                    }
                }
                return endOfData();
            }
        };
    }
}

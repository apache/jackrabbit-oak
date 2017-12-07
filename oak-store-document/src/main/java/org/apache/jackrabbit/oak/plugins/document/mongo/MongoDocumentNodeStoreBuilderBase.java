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

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.mongodb.DB;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadConcernLevel;

import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Suppliers.memoize;
import static org.apache.jackrabbit.oak.plugins.document.util.MongoConnection.readConcernLevel;

/**
 * A base builder implementation for a {@link DocumentNodeStore} backed by
 * MongoDB.
 */
public abstract class MongoDocumentNodeStoreBuilderBase<T extends MongoDocumentNodeStoreBuilderBase<T>>
        extends DocumentNodeStoreBuilder<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentNodeStoreBuilder.class);

    private String mongoUri;
    private boolean socketKeepAlive;
    private MongoStatus mongoStatus;
    private long maxReplicationLagMillis = TimeUnit.HOURS.toMillis(6);

    /**
     * Uses the given information to connect to to MongoDB as backend
     * storage for the DocumentNodeStore. The write concern is either
     * taken from the URI or determined automatically based on the MongoDB
     * setup. When running on a replica set without explicit write concern
     * in the URI, the write concern will be {@code MAJORITY}, otherwise
     * {@code ACKNOWLEDGED}.
     *
     * @param uri a MongoDB URI.
     * @param name the name of the database to connect to. This overrides
     *             any database name given in the {@code uri}.
     * @param blobCacheSizeMB the blob cache size in MB.
     * @return this
     * @throws UnknownHostException if one of the hosts given in the URI
     *          is unknown.
     */
    public T setMongoDB(@Nonnull String uri,
                        @Nonnull String name,
                        int blobCacheSizeMB)
            throws UnknownHostException {
        this.mongoUri = uri;

        MongoClientOptions.Builder options = MongoConnection.getDefaultBuilder();
        options.socketKeepAlive(socketKeepAlive);
        DB db = new MongoConnection(uri, options).getDB(name);
        MongoStatus status = new MongoStatus(db);
        if (!MongoConnection.hasWriteConcern(uri)) {
            db.setWriteConcern(MongoConnection.getDefaultWriteConcern(db));
        }
        if (status.isMajorityReadConcernSupported() && status.isMajorityReadConcernEnabled() && !MongoConnection.hasReadConcern(uri)) {
            db.setReadConcern(MongoConnection.getDefaultReadConcern(db));
        }
        setMongoDB(db, status, blobCacheSizeMB);
        return thisBuilder();
    }

    /**
     * Use the given MongoDB as backend storage for the DocumentNodeStore.
     *
     * @param db the MongoDB connection
     * @return this
     */
    public T setMongoDB(@Nonnull DB db,
                        int blobCacheSizeMB) {
        return setMongoDB(db, new MongoStatus(db), blobCacheSizeMB);
    }

    /**
     * Use the given MongoDB as backend storage for the DocumentNodeStore.
     *
     * @param db the MongoDB connection
     * @return this
     */
    public T setMongoDB(@Nonnull DB db) {
        return setMongoDB(db, 16);
    }

    /**
     * Enables the socket keep-alive option for MongoDB. The default is
     * disabled.
     *
     * @param enable whether to enable it.
     * @return this
     */
    public T setSocketKeepAlive(boolean enable) {
        this.socketKeepAlive = enable;
        return thisBuilder();
    }

    public T setMaxReplicationLag(long duration, TimeUnit unit){
        maxReplicationLagMillis = unit.toMillis(duration);
        return thisBuilder();
    }

    public VersionGCSupport createVersionGCSupport() {
        DocumentStore store = getDocumentStore();
        if (store instanceof MongoDocumentStore) {
            return new MongoVersionGCSupport((MongoDocumentStore) store);
        } else {
            return super.createVersionGCSupport();
        }
    }

    public Iterable<ReferencedBlob> createReferencedBlobs(DocumentNodeStore ns) {
        final DocumentStore store = getDocumentStore();
        if (store instanceof MongoDocumentStore) {
            return () -> new MongoBlobReferenceIterator(ns, (MongoDocumentStore) store);
        } else {
            return super.createReferencedBlobs(ns);
        }
    }

    public MissingLastRevSeeker createMissingLastRevSeeker() {
        final DocumentStore store = getDocumentStore();
        if (store instanceof MongoDocumentStore) {
            return new MongoMissingLastRevSeeker((MongoDocumentStore) store, getClock());
        } else {
            return super.createMissingLastRevSeeker();
        }
    }

    /**
     * Returns the Mongo URI used in the {@link #setMongoDB(String, String, int)} method.
     *
     * @return the Mongo URI or null if the {@link #setMongoDB(String, String, int)} method hasn't
     * been called.
     */
    String getMongoUri() {
        return mongoUri;
    }

    /**
     * Returns the status of the Mongo server configured in the {@link #setMongoDB(String, String, int)} method.
     *
     * @return the status or null if the {@link #setMongoDB(String, String, int)} method hasn't
     * been called.
     */
    MongoStatus getMongoStatus() {
        return mongoStatus;
    }

    long getMaxReplicationLagMillis() {
        return maxReplicationLagMillis;
    }

    private T setMongoDB(@Nonnull DB db,
                         MongoStatus status,
                         int blobCacheSizeMB) {
        if (!MongoConnection.hasSufficientWriteConcern(db)) {
            LOG.warn("Insufficient write concern: " + db.getWriteConcern()
                    + " At least " + MongoConnection.getDefaultWriteConcern(db) + " is recommended.");
        }
        if (status.isMajorityReadConcernSupported() && !status.isMajorityReadConcernEnabled()) {
            LOG.warn("The read concern should be enabled on mongod using --enableMajorityReadConcern");
        } else if (status.isMajorityReadConcernSupported() && !MongoConnection.hasSufficientReadConcern(db)) {
            ReadConcernLevel currentLevel = readConcernLevel(db.getReadConcern());
            ReadConcernLevel recommendedLevel = readConcernLevel(MongoConnection.getDefaultReadConcern(db));
            if (currentLevel == null) {
                LOG.warn("Read concern hasn't been set. At least " + recommendedLevel + " is recommended.");
            } else {
                LOG.warn("Insufficient read concern: " + currentLevel + ". At least " + recommendedLevel + " is recommended.");
            }
        }

        this.mongoStatus = status;
        this.documentStoreSupplier = memoize(() -> new MongoDocumentStore(
                db, MongoDocumentNodeStoreBuilderBase.this));

        if (this.blobStore == null) {
            GarbageCollectableBlobStore s = new MongoBlobStore(db, blobCacheSizeMB * 1024 * 1024L);
            setGCBlobStore(s);
        }
        return thisBuilder();
    }
}

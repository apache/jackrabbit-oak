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
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadConcernLevel;
import com.mongodb.client.MongoDatabase;

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
    private boolean socketKeepAlive = true;
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

        MongoClusterListener listener = new MongoClusterListener();
        MongoClientOptions.Builder options = MongoConnection.getDefaultBuilder();
        options.addClusterListener(listener);
        options.socketKeepAlive(socketKeepAlive);
        MongoClient client = new MongoClient(new MongoClientURI(uri, options));
        MongoStatus status = new MongoStatus(client, name, listener);
        MongoDatabase db = client.getDatabase(name);
        if (!MongoConnection.hasWriteConcern(uri)) {
            db = db.withWriteConcern(MongoConnection.getDefaultWriteConcern(client));
        }
        if (status.isMajorityReadConcernSupported()
                && status.isMajorityReadConcernEnabled()
                && !MongoConnection.hasReadConcern(uri)) {
            db = db.withReadConcern(MongoConnection.getDefaultReadConcern(client, db));
        }
        setMongoDB(client, db, status, blobCacheSizeMB);
        return thisBuilder();
    }

    /**
     * Use the given MongoDB as backend storage for the DocumentNodeStore.
     *
     * @param db the MongoDB connection
     * @return this
     * @deprecated use {@link #setMongoDB(MongoClient, String, int)} instead.
     */
    public T setMongoDB(@Nonnull DB db,
                        int blobCacheSizeMB) {
        return setMongoDB(mongoClientFrom(db), db.getName(), blobCacheSizeMB);
    }

    /**
     * Use the given MongoDB as backend storage for the DocumentNodeStore.
     *
     * @param client the MongoDB connection
     * @param dbName the database name
     * @param blobCacheSizeMB the size of the blob cache in MB.
     * @return this
     */
    public T setMongoDB(@Nonnull MongoClient client,
                        @Nonnull String dbName,
                        int blobCacheSizeMB) {
        return setMongoDB(client, client.getDatabase(dbName),
                new MongoStatus(client, dbName), blobCacheSizeMB);
    }

    /**
     * Use the given MongoDB as backend storage for the DocumentNodeStore.
     *
     * @param db the MongoDB connection
     * @return this
     * @deprecated use {@link #setMongoDB(MongoClient, String)} instead.
     */
    public T setMongoDB(@Nonnull DB db) {
        return setMongoDB(mongoClientFrom(db), db.getName());
    }

    /**
     * Use the given MongoDB as backend storage for the DocumentNodeStore.
     *
     * @param client the MongoDB connection
     * @param dbName the database name
     * @return this
     */
    public T setMongoDB(@Nonnull MongoClient client,
                        @Nonnull String dbName) {
        return setMongoDB(client, dbName, 16);
    }

    /**
     * Enables or disables the socket keep-alive option for MongoDB. The default
     * is enabled.
     *
     * @param enable whether to enable or disable it.
     * @return this
     */
    public T setSocketKeepAlive(boolean enable) {
        this.socketKeepAlive = enable;
        return thisBuilder();
    }

    /**
     * @return whether socket keep-alive is enabled.
     */
    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
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

    private T setMongoDB(@Nonnull MongoClient client,
                         @Nonnull MongoDatabase db,
                         MongoStatus status,
                         int blobCacheSizeMB) {
        if (!MongoConnection.isSufficientWriteConcern(client, db.getWriteConcern())) {
            LOG.warn("Insufficient write concern: " + db.getWriteConcern()
                    + " At least " + MongoConnection.getDefaultWriteConcern(client) + " is recommended.");
        }
        if (status.isMajorityReadConcernSupported() && !status.isMajorityReadConcernEnabled()) {
            LOG.warn("The read concern should be enabled on mongod using --enableMajorityReadConcern");
        } else if (status.isMajorityReadConcernSupported() && !MongoConnection.isSufficientReadConcern(client, db.getReadConcern())) {
            ReadConcernLevel currentLevel = readConcernLevel(db.getReadConcern());
            ReadConcernLevel recommendedLevel = readConcernLevel(MongoConnection.getDefaultReadConcern(client, db));
            if (currentLevel == null) {
                LOG.warn("Read concern hasn't been set. At least " + recommendedLevel + " is recommended.");
            } else {
                LOG.warn("Insufficient read concern: " + currentLevel + ". At least " + recommendedLevel + " is recommended.");
            }
        }

        this.mongoStatus = status;
        this.documentStoreSupplier = memoize(() -> new MongoDocumentStore(
                client, db.getName(), MongoDocumentNodeStoreBuilderBase.this));

        if (this.blobStore == null) {
            GarbageCollectableBlobStore s = new MongoBlobStore(db, blobCacheSizeMB * 1024 * 1024L);
            setGCBlobStore(s);
        }
        return thisBuilder();
    }

    private static MongoClient mongoClientFrom(DB db) {
        Mongo mongo = db.getMongo();
        if (mongo instanceof MongoClient) {
            return (MongoClient) mongo;
        }
        throw new UnsupportedOperationException("DB must be constructed from MongoClient");
    }
}

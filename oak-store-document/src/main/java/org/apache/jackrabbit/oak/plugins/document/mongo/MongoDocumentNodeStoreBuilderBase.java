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

import java.util.concurrent.TimeUnit;

import com.mongodb.client.MongoClient;

import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.guava.common.base.Suppliers.memoize;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDBConnection.newMongoDBConnection;

/**
 * A base builder implementation for a {@link DocumentNodeStore} backed by
 * MongoDB.
 */
public abstract class MongoDocumentNodeStoreBuilderBase<T extends MongoDocumentNodeStoreBuilderBase<T>>
        extends DocumentNodeStoreBuilder<T> {

    private final MongoClock mongoClock = new MongoClock();
    @Deprecated
    private boolean socketKeepAlive = true;
    private MongoStatus mongoStatus;
    private long maxReplicationLagMillis = TimeUnit.HOURS.toMillis(6);
    private boolean clientSessionDisabled = false;
    private int leaseSocketTimeout = 0;
    private String uri;
    private String name;
    private String collectionCompressionType;
    private MongoClient mongoClient;

    // MongoDB connection pool settings
    private Integer maxPoolSize;
    private Integer minPoolSize;
    private Integer maxConnecting;
    private Integer maxIdleTimeMS;
    private Integer maxLifeTimeMS;
    private Integer connectTimeoutMS;
    private Integer heartbeatFrequencyMS;
    private Integer serverSelectionTimeoutMS;
    private Integer waitQueueTimeoutMS;
    private Integer readTimeoutMS;
    private Integer minHeartbeatFrequencyMS;

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
     */
    public T setMongoDB(@NotNull String uri,
                        @NotNull String name,
                        int blobCacheSizeMB) {
        this.uri = uri;
        this.name = name;
        setMongoDB(createMongoDBClient(false), blobCacheSizeMB);
        return thisBuilder();
    }

    /**
     * Use the given MongoDB as backend storage for the DocumentNodeStore.
     *
     * @param client the MongoDB connection
     * @param dbName the database name
     * @param blobCacheSizeMB the size of the blob cache in MB.
     * @return this
     */
    public T setMongoDB(@NotNull MongoClient client,
                        @NotNull String dbName,
                        int blobCacheSizeMB) {
        return setMongoDB(new MongoDBConnection(client, client.getDatabase(dbName),
                new MongoStatus(client, dbName), mongoClock), blobCacheSizeMB);
    }

    /**
     * Use the given MongoDB as backend storage for the DocumentNodeStore.
     *
     * @param client the MongoDB connection
     * @param dbName the database name
     * @return this
     */
    public T setMongoDB(@NotNull MongoClient client,
                        @NotNull String dbName) {
        return setMongoDB(client, dbName, 16);
    }

    /**
     * Enables or disables the socket keep-alive option for MongoDB. The default
     * is enabled.
     *
     * @param enable whether to enable or disable it.
     * @return this
     */
    @Deprecated
    public T setSocketKeepAlive(boolean enable) {
        this.socketKeepAlive = enable;
        return thisBuilder();
    }

    /**
     * @return whether socket keep-alive is enabled.
     */
    @Deprecated
    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    /**
     * Disables the use of a client session available with MongoDB 3.6 and
     * newer. By default the MongoDocumentStore will use a client session if
     * available. That is, when connected to MongoDB 3.6 and newer.
     *
     * @param b whether to disable the use of a client session.
     * @return this
     */
    public T setClientSessionDisabled(boolean b) {
        this.clientSessionDisabled = b;
        return thisBuilder();
    }

    /**
     * @return whether the use of a client session is disabled.
     */
    boolean isClientSessionDisabled() {
        return clientSessionDisabled;
    }

    /**
     * Sets a socket timeout for lease update operations.
     *
     * @param timeoutMillis the socket timeout in milliseconds.
     * @return this builder.
     */
    public T setLeaseSocketTimeout(int timeoutMillis) {

        this.leaseSocketTimeout = timeoutMillis;
        return thisBuilder();
    }

    public T setMongoMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return thisBuilder();
    }

    public T setMongoMinPoolSize(int minPoolSize) {
        this.minPoolSize = minPoolSize;
        return thisBuilder();
    }

    public T setMongoMaxConnecting(int maxConnecting) {
        this.maxConnecting = maxConnecting;
        return thisBuilder();
    }

    public T setMongoMaxIdleTimeMS(int maxIdleTimeMS) {
        this.maxIdleTimeMS = maxIdleTimeMS;
        return thisBuilder();
    }

    public T setMongoMaxLifeTimeMS(int maxLifeTimeMS) {
        this.maxLifeTimeMS = maxLifeTimeMS;
        return thisBuilder();
    }

    public T setMongoConnectTimeoutMS(int connectTimeoutMS) {
        this.connectTimeoutMS = connectTimeoutMS;
        return thisBuilder();
    }

    public T setMongoHeartbeatFrequencyMS(int heartbeatFrequencyMS) {
        this.heartbeatFrequencyMS = heartbeatFrequencyMS;
        return thisBuilder();
    }

    public T setMongoServerSelectionTimeoutMS(int serverSelectionTimeoutMS) {
        this.serverSelectionTimeoutMS = serverSelectionTimeoutMS;
        return thisBuilder();
    }

    public T setMongoWaitQueueTimeoutMS(int waitQueueTimeoutMS) {
        this.waitQueueTimeoutMS = waitQueueTimeoutMS;
        return thisBuilder();
    }

    public T setMongoReadTimeoutMS(int readTimeoutMS) {
        this.readTimeoutMS = readTimeoutMS;
        return thisBuilder();
    }

    public T setMongoMinHeartbeatFrequencyMS(int minHeartbeatFrequencyMS) {
        this.minHeartbeatFrequencyMS = minHeartbeatFrequencyMS;
        return thisBuilder();
    }

    /**
     * @return the lease socket timeout in milliseconds. If none is set, then
     *      zero is returned.
     */
    int getLeaseSocketTimeout() {
        return leaseSocketTimeout;
    }

    public T setMaxReplicationLag(long duration, TimeUnit unit){
        maxReplicationLagMillis = unit.toMillis(duration);
        return thisBuilder();
    }

    public T setCollectionCompressionType(String compressionType) {
        this.collectionCompressionType = compressionType;
        return thisBuilder();
    }

    @Override
    public VersionGCSupport createVersionGCSupport() {
        DocumentStore store = getDocumentStore();
        if (store instanceof MongoDocumentStore) {
            return new MongoVersionGCSupport((MongoDocumentStore) store, isFullGCAuditLoggingEnabled());
        } else {
            return super.createVersionGCSupport();
        }
    }

    @Override
    public Iterable<ReferencedBlob> createReferencedBlobs(DocumentNodeStore ns) {
        final DocumentStore store = getDocumentStore();
        if (store instanceof MongoDocumentStore) {
            return () -> new MongoBlobReferenceIterator(ns, (MongoDocumentStore) store);
        } else {
            return super.createReferencedBlobs(ns);
        }
    }

    @Override
    public MissingLastRevSeeker createMissingLastRevSeeker() {
        final DocumentStore store = getDocumentStore();
        if (store instanceof MongoDocumentStore) {
            return new MongoMissingLastRevSeeker((MongoDocumentStore) store, getClock());
        } else {
            return super.createMissingLastRevSeeker();
        }
    }


    public String getCollectionCompressionType(){
        return collectionCompressionType;
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

    /**
     * Returns the MongoDB client configured in the {@link #setMongoDB(String, String, int)} method.
     *
     * @return the client or null if the {@link #setMongoDB(String, String, int)} method hasn't been called.
     */
    public MongoClient getMongoClient() {
        return mongoClient;
    }

    long getMaxReplicationLagMillis() {
        return maxReplicationLagMillis;
    }

    MongoClock getMongoClock() {
        return mongoClock;
    }

    MongoDBConnection createMongoDBClient(boolean isLease) {
        if (uri == null || name == null) {
            throw new IllegalStateException("Cannot create MongoDB client without 'uri' or 'name'");
        }
        
        // Apply correct socket timeout based on connection type
        int socketTimeout;
        if (isLease) {
            // Cluster nodes connection: always use lease socket timeout
            socketTimeout = leaseSocketTimeout;
        } else {
            // Default connection: use OSGi read timeout if configured, otherwise 0
            socketTimeout = readTimeoutMS != null && readTimeoutMS > 0 ? readTimeoutMS : 0;
        }
        
        return newMongoDBConnection(uri, name, mongoClock, socketTimeout);
    }

    private T setMongoDB(@NotNull MongoDBConnection mongoDBConnection,
                         int blobCacheSizeMB) {
        mongoDBConnection.checkReadWriteConcern();
        this.mongoClient = mongoDBConnection.getClient();
        this.mongoStatus = mongoDBConnection.getStatus();
        this.documentStoreSupplier = memoize(() -> new MongoDocumentStore(
                mongoDBConnection.getClient(), mongoDBConnection.getDatabase(), MongoDocumentNodeStoreBuilderBase.this));

        if (this.blobStoreSupplier == null) {
            this.blobStoreSupplier = memoize(
                    () -> new MongoBlobStore(mongoDBConnection.getDatabase(), blobCacheSizeMB * 1024 * 1024L, MongoDocumentNodeStoreBuilderBase.this));
        }

        return thisBuilder();
    }
}

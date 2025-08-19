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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;

import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.commons.function.Suppliers.memoize;
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
    private Integer leaseSocketTimeout;
    private String uri;
    private String name;
    private String collectionCompressionType;
    private MongoClient mongoClient;

    // MongoDB connection pool settings
    private Integer maxPoolSize;
    private Integer minPoolSize;
    private Integer maxConnecting;
    private Integer maxIdleTimeMillis;
    private Integer maxLifeTimeMillis;
    private Integer connectTimeoutMillis;
    private Integer heartbeatFrequencyMillis;
    private Integer serverSelectionTimeoutMillis;
    private Integer waitQueueTimeoutMillis;
    private Integer readTimeoutMillis;
    private Integer minHeartbeatFrequencyMillis;

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

    public T setMongoMaxIdleTimeMillis(int maxIdleTimeMillis) {
        this.maxIdleTimeMillis = maxIdleTimeMillis;
        return thisBuilder();
    }

    public T setMongoMaxLifeTimeMillis(int maxLifeTimeMillis) {
        this.maxLifeTimeMillis = maxLifeTimeMillis;
        return thisBuilder();
    }

    public T setMongoConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return thisBuilder();
    }

    public T setMongoHeartbeatFrequencyMillis(int heartbeatFrequencyMillis) {
        this.heartbeatFrequencyMillis = heartbeatFrequencyMillis;
        return thisBuilder();
    }

    public T setMongoServerSelectionTimeoutMillis(int serverSelectionTimeoutMillis) {
        this.serverSelectionTimeoutMillis = serverSelectionTimeoutMillis;
        return thisBuilder();
    }

    public T setMongoWaitQueueTimeoutMillis(int waitQueueTimeoutMillis) {
        this.waitQueueTimeoutMillis = waitQueueTimeoutMillis;
        return thisBuilder();
    }

    public T setMongoReadTimeoutMillis(int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
        return thisBuilder();
    }

    public T setMongoMinHeartbeatFrequencyMillis(int minHeartbeatFrequencyMillis) {
        this.minHeartbeatFrequencyMillis = minHeartbeatFrequencyMillis;
        return thisBuilder();
    }

    /**
     * @return the lease socket timeout in milliseconds. If none is set, then
     *      zero is returned.
     */
    int getLeaseSocketTimeout() {
        return leaseSocketTimeout != null ? leaseSocketTimeout : 0;
    }

    /**
     * @return true if lease socket timeout was explicitly set via setLeaseSocketTimeout()
     */
    boolean hasLeaseSocketTimeout() {
        return leaseSocketTimeout != null;
    }

    /**
     * Builds a configured MongoClientSettings with all settings applied.
     *
     * @param isLease true for cluster nodes connection, false for default connection pool
     * @return fully configured MongoClientSettings
     */
    MongoClientSettings buildMongoClientSettings(boolean isLease) {
        MongoClientSettings.Builder options = MongoConnection.getDefaultBuilder();
        options.applyConnectionString(new ConnectionString(uri));
        
        // Apply socket timeout based on connection type
        int socketTimeout;
        if (isLease) {
            // Cluster nodes connection: use lease socket timeout, or default if not explicitly set
            socketTimeout = leaseSocketTimeout != null ? leaseSocketTimeout : DocumentNodeStoreService.DEFAULT_MONGO_LEASE_SO_TIMEOUT_MILLIS;
        } else {
            // Default connection: use OSGi read timeout if configured, otherwise 0
            socketTimeout = readTimeoutMillis != null && readTimeoutMillis > 0 ? readTimeoutMillis : 0;
        }
        
        // Apply connection pool settings
        options.applyToConnectionPoolSettings(poolBuilder -> {
            if (maxPoolSize != null) poolBuilder.maxSize(maxPoolSize);
            if (minPoolSize != null) poolBuilder.minSize(minPoolSize);
            if (maxConnecting != null) poolBuilder.maxConnecting(maxConnecting);
            if (maxIdleTimeMillis != null) {
                poolBuilder.maxConnectionIdleTime(maxIdleTimeMillis, TimeUnit.MILLISECONDS);
            }
            if (maxLifeTimeMillis != null) {
                poolBuilder.maxConnectionLifeTime(maxLifeTimeMillis, TimeUnit.MILLISECONDS);
            }
            if (waitQueueTimeoutMillis != null) {
                poolBuilder.maxWaitTime(waitQueueTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        });
        
        // Apply socket settings
        options.applyToSocketSettings(socketBuilder -> {
            if (socketTimeout > 0) {
                socketBuilder.readTimeout(socketTimeout, TimeUnit.MILLISECONDS);
            }
            if (connectTimeoutMillis != null) {
                socketBuilder.connectTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        });
        
        // Apply server settings
        options.applyToServerSettings(serverBuilder -> {
            if (heartbeatFrequencyMillis != null && heartbeatFrequencyMillis > 0) {
                serverBuilder.heartbeatFrequency(heartbeatFrequencyMillis, TimeUnit.MILLISECONDS);
            }
            if (minHeartbeatFrequencyMillis != null && minHeartbeatFrequencyMillis > 0) {
                serverBuilder.minHeartbeatFrequency(minHeartbeatFrequencyMillis, TimeUnit.MILLISECONDS);
            }
        });
        
        // Apply cluster settings
        options.applyToClusterSettings(clusterBuilder -> {
            if (serverSelectionTimeoutMillis != null) {
                clusterBuilder.serverSelectionTimeout(serverSelectionTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        });
        
        return options.build();
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
        
        MongoClientSettings settings = buildMongoClientSettings(isLease);
        return newMongoDBConnection(uri, name, mongoClock, settings);
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

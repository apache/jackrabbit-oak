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

import com.mongodb.MongoClient;

import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.jetbrains.annotations.NotNull;

import static com.google.common.base.Suppliers.memoize;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDBConnection.newMongoDBConnection;

/**
 * A base builder implementation for a {@link DocumentNodeStore} backed by
 * MongoDB.
 */
public abstract class MongoDocumentNodeStoreBuilderBase<T extends MongoDocumentNodeStoreBuilderBase<T>>
        extends DocumentNodeStoreBuilder<T> {

    private final MongoClock mongoClock = new MongoClock();
    private boolean socketKeepAlive = true;
    private MongoStatus mongoStatus;
    private long maxReplicationLagMillis = TimeUnit.HOURS.toMillis(6);
    private boolean clientSessionDisabled = false;
    private int leaseSocketTimeout = 0;
    private String uri;
    private String name;
    private String collectionCompressionType;

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
        setMongoDB(createMongoDBClient(0), blobCacheSizeMB);
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
            return new MongoVersionGCSupport((MongoDocumentStore) store);
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

    long getMaxReplicationLagMillis() {
        return maxReplicationLagMillis;
    }

    MongoClock getMongoClock() {
        return mongoClock;
    }

    MongoDBConnection createMongoDBClient(int socketTimeout) {
        if (uri == null || name == null) {
            throw new IllegalStateException("Cannot create MongoDB client without 'uri' or 'name'");
        }
        return newMongoDBConnection(uri, name, mongoClock, socketTimeout, socketKeepAlive);
    }

    private T setMongoDB(@NotNull MongoDBConnection client,
                         int blobCacheSizeMB) {
        client.checkReadWriteConcern();
        this.mongoStatus = client.getStatus();
        this.documentStoreSupplier = memoize(() -> new MongoDocumentStore(
                client.getClient(), client.getDatabase(), MongoDocumentNodeStoreBuilderBase.this));

        if (this.blobStoreSupplier == null) {
            this.blobStoreSupplier = memoize(
                    () -> new MongoBlobStore(client.getDatabase(), blobCacheSizeMB * 1024 * 1024L, MongoDocumentNodeStoreBuilderBase.this));
        }

        return thisBuilder();
    }
}

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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoClusterListener;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code MongoConnection} abstracts connection to the {@code MongoDB}.
 */
public class MongoConnection {

    public static final String MONGODB_PREFIX = "mongodb://";

    private static final Logger LOG = LoggerFactory.getLogger(MongoConnection.class);
    private static final int DEFAULT_MAX_WAIT_TIME = (int) TimeUnit.MINUTES.toMillis(1);
    private static final int DEFAULT_HEARTBEAT_FREQUENCY_MS = (int) TimeUnit.SECONDS.toMillis(5);
    private static final WriteConcern WC_UNKNOWN = new WriteConcern("unknown");
    private static final Set<ReadConcernLevel> REPLICA_RC = ImmutableSet.of(ReadConcernLevel.MAJORITY, ReadConcernLevel.LINEARIZABLE);
    private final ConnectionString mongoURI;
    private final MongoClient mongo;

    /**
     * Constructs a new connection using the specified MongoDB connection string.
     * See also http://docs.mongodb.org/manual/reference/connection-string/
     *
     * @param uri the MongoDB URI
     * @throws MongoException if there are failures
     */
    public MongoConnection(String uri) throws MongoException {
        this(uri, MongoConnection.getDefaultBuilder());
    }

    /**
     * Constructs a new connection using the specified MongoDB connection
     * String. The default client options are taken from the provided builder.
     *
     * @param uri the connection URI.
     * @param builder the client option defaults.
     * @throws MongoException if there are failures
     */
    public MongoConnection(String uri, MongoClientSettings.Builder builder)
            throws MongoException {
        mongoURI = new ConnectionString(uri);
        mongo = MongoClients.create(builder.applyConnectionString(mongoURI).build());
    }

    /**
     * Constructs a new {@code MongoConnection}.
     *
     * @param host The host address.
     * @param port The port.
     * @param database The database name.
     * @throws MongoException if there are failures
     */
    public MongoConnection(String host, int port, String database)
            throws MongoException {
        this("mongodb://" + host + ":" + port + "/" + database);
    }

    /**
     * Constructs a new {@code MongoConnection}.
     *
     * @param uri the connection URI.
     * @param client the already connected client.
     */
    public MongoConnection(String uri, MongoClient client) {
        mongoURI = new ConnectionString(uri);
        mongo = client;
    }

    /**
     * @return the {@link MongoClient} for this connection.
     */
    public MongoClient getMongoClient() {
        return mongo;
    }

    /**
     * Returns the {@link MongoDatabase} as passed in the URI of the
     * constructor.
     *
     * @return the {@link MongoDatabase}.
     */
    public MongoDatabase getDatabase() {
        return mongo.getDatabase(mongoURI.getDatabase());
    }

    /**
     * Returns the {@link MongoDatabase} with the given name.
     *
     * @return The {@link MongoDatabase}.
     */
    public MongoDatabase getDatabase(@NotNull String name) {
        return mongo.getDatabase(name);
    }

    /**
     * @return the database name specified in the URI.
     */
    public String getDBName() {
        return mongoURI.getDatabase();
    }

    /**
     * Closes the underlying Mongo instance
     */
    public void close() {
        mongo.close();
    }

    //--------------------------------------< Utility Methods >

    /**
     * Constructs a builder with default options set. These can be overridden later
     *
     * @return builder with default options set
     */
    public static MongoClientSettings.Builder getDefaultBuilder() {
        return MongoClientSettings.builder()
                .applicationName("Oak DocumentMK")
                .applyToConnectionPoolSettings(builder ->
                    builder.maxWaitTime(DEFAULT_MAX_WAIT_TIME, MILLISECONDS)
                )
                .applyToServerSettings(builder ->
                    builder.heartbeatFrequency(DEFAULT_HEARTBEAT_FREQUENCY_MS, MILLISECONDS)
                )
                .applyToClusterSettings(builder ->
                    builder.addClusterListener(new MongoClusterListener())
                );
    }

    public static String toString(MongoClientSettings opts) {
        return Objects.toStringHelper(opts)
                .add("connectionsPerHost", opts.getConnectionPoolSettings().getMaxSize())
                .add("connectTimeout", opts.getSocketSettings().getConnectTimeout(MILLISECONDS))
                .add("socketTimeout", opts.getSocketSettings().getReadTimeout(MILLISECONDS))
                .add("maxWaitTime", opts.getConnectionPoolSettings().getMaxWaitTime(MILLISECONDS))
                .add("heartbeatFrequency", opts.getServerSettings().getHeartbeatFrequency(MILLISECONDS))
                .add("readPreference", opts.getReadPreference().getName())
                .add("writeConcern", opts.getWriteConcern())
                .toString();
    }

    public static String toString(ConnectionString connectionString) {
        return Objects.toStringHelper(connectionString)
                .add("connectionsPerHost", connectionString.getMaxConnecting())
                .add("connectTimeout", connectionString.getConnectTimeout())
                .add("socketTimeout", connectionString.getSocketTimeout())
                .add("maxWaitTime", connectionString.getMaxWaitTime())
                .add("heartbeatFrequency", connectionString.getHeartbeatFrequency())
                .add("readPreference", connectionString.getReadPreference())
                .add("writeConcern", connectionString.getWriteConcern())
                .toString();
    }

    /**
     * Returns {@code true} if the given {@code uri} has a write concern set.
     * @param uri the URI to check.
     * @return {@code true} if the URI has a write concern set, {@code false}
     *      otherwise.
     */
    public static boolean hasWriteConcern(@NotNull String uri) {
        WriteConcern wc = new ConnectionString(checkNotNull(uri)).getWriteConcern();
        return wc != null && !WC_UNKNOWN.equals(wc);
    }

    /**
     * Returns {@code true} if the given {@code uri} has a read concern set.
     * @param uri the URI to check.
     * @return {@code true} if the URI has a read concern set, {@code false}
     *      otherwise.
     */
    public static boolean hasReadConcern(@NotNull String uri) {
        ReadConcern rc = new ConnectionString(checkNotNull(uri)).getReadConcern();
        return rc != null && readConcernLevel(rc) != null;
    }

    /**
     * Returns the default write concern depending on MongoDB deployment.
     * <ul>
     *     <li>{@link WriteConcern#MAJORITY}: for a MongoDB replica set</li>
     *     <li>{@link WriteConcern#ACKNOWLEDGED}: for single MongoDB instance</li>
     * </ul>
     *
     * @param client the connection to MongoDB.
     * @return the default write concern to use for Oak.
     */
    public static WriteConcern getDefaultWriteConcern(@NotNull MongoClient client) {
        WriteConcern w;

        if (isReplicaSet(client)) {
            w = WriteConcern.MAJORITY;
        } else {
            w = WriteConcern.ACKNOWLEDGED;
        }
        return w;
    }

    /**
     * Returns the default read concern depending on MongoDB deployment.
     * <ul>
     *     <li>{@link ReadConcern#MAJORITY}: for a MongoDB replica set with w=majority</li>
     *     <li>{@link ReadConcern#LOCAL}: for other cases</li>
     * </ul>
     *
     * @param db the connection to MongoDB.
     * @return the default write concern to use for Oak.
     */
    public static ReadConcern getDefaultReadConcern(@NotNull MongoClient client,
                                                    @NotNull MongoDatabase db) {
        ReadConcern r;
        if (isReplicaSet(client) && isMajorityWriteConcern(db)) {
            r = ReadConcern.MAJORITY;
        } else {
            r = ReadConcern.LOCAL;
        }
        return r;
    }

    /**
     * Returns true if the majority write concern is used for the given DB.
     *
     * @param db the connection to MongoDB.
     * @return true if the majority write concern has been configured; false otherwise
     */
    public static boolean isMajorityWriteConcern(@NotNull MongoDatabase db) {
        return WriteConcern.MAJORITY.getWString().equals(db.getWriteConcern().getWObject());
    }

    /**
     * Returns {@code true} if the given write concern is sufficient for Oak. On
     * a replica set Oak expects at least w=2. For a single MongoDB node
     * deployment w=1 is sufficient.
     *
     * @param client the client.
     * @param wc the write concern.
     * @return whether the write concern is sufficient.
     */
    public static boolean isSufficientWriteConcern(@NotNull MongoClient client,
                                                   @NotNull WriteConcern wc) {
        Object wObj = checkNotNull(wc).getWObject();
        int w;
        if (wObj instanceof Number) {
            w = ((Number) wObj).intValue();
        } else if (wObj == null) {
            // default acknowledged
            w = 1;
        } else if (WriteConcern.MAJORITY.getWString().equals(wObj)) {
            // majority in a replica set means at least w=2
            w = 2;
        } else {
            throw new IllegalArgumentException(
                    "Unknown write concern: " + wc);
        }
        if (isReplicaSet(client)) {
            return w >= 2;
        } else {
            return w >= 1;
        }
    }

    /**
     * Returns {@code true} if the given read concern is sufficient for Oak. On
     * a replica set Oak expects majority or linear. For a single MongoDB node
     * deployment local is sufficient.
     *
     * @param client the client.
     * @param rc the read concern.
     * @return whether the read concern is sufficient.
     */
    public static boolean isSufficientReadConcern(@NotNull MongoClient client,
                                                  @NotNull ReadConcern rc) {
        ReadConcernLevel r = readConcernLevel(checkNotNull(rc));

        if (!isReplicaSet(client)) {
            return true;
        } else {
            return REPLICA_RC.contains(r);
        }
    }

    public static ReadConcernLevel readConcernLevel(ReadConcern readConcern) {
        if (readConcern.isServerDefault()) {
            return null;
        } else {
            return ReadConcernLevel.fromString(readConcern.asDocument().getString("level").getValue());
        }
    }

    /**
     * Returns {@code true} if the MongoDB server is part of a Replica Set,
     * {@code false} otherwise. The Replica Set Status is achieved by the
     * ReplicaSetStatusListener, which is triggered whenever the cluster
     * description changes.
     *
     * @param client the mongo client.
     * @return {@code true} if part of Replica Set, {@code false} otherwise.
     */
    public static boolean isReplicaSet(@NotNull MongoClient client) {
        return getOakClusterListener(client).map(MongoClusterListener::isReplicaSet)
                .orElseGet(() -> {
                    LOG.warn("Method isReplicaSet called for a MongoClient without any OakClusterListener");
                    return false;
                });
    }

    /**
     * Returns the {@ServerAddress} of the MongoDB cluster.
     *
     * @param client the mongo client.
     * @return {@ServerAddress} of the cluster. {@null} if not connected or no listener was available.
     */
    @Nullable
    public static ServerAddress getAddress(@NotNull MongoClient client) {
        return getOakClusterListener(client).map(MongoClusterListener::getServerAddress)
                .orElseGet(() -> {
                    LOG.warn("Method getAddress called for a MongoClient without any OakClusterListener");
                    return null;
                });
    }

    /**
     * Returns the {@ServerAddress} of the MongoDB cluster primary node.
     *
     * @param client the mongo client.
     * @return {@ServerAddress} of the primary. {@null} if not connected or no listener was available.
     */
    @Nullable
    public static ServerAddress getPrimaryAddress(@NotNull MongoClient client) {
        return getOakClusterListener(client).map(MongoClusterListener::getPrimaryAddress)
                .orElseGet(() -> {
                    LOG.warn("Method getPrimaryAddress called for a MongoClient without any OakClusterListener");
                    return null;
                });
    }

    private static Optional<MongoClusterListener> getOakClusterListener(@NotNull MongoClient client) {
        return client.getClusterDescription().getClusterSettings()
                .getClusterListeners().stream()
                .filter(clusterListener -> clusterListener instanceof MongoClusterListener)
                .map(clusterListener -> (MongoClusterListener) clusterListener)
                .findFirst();
    }
}

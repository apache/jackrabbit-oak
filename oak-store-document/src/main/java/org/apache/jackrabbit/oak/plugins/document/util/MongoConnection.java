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

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.base.MoreObjects;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;

import static java.util.Objects.requireNonNull;

import org.jetbrains.annotations.NotNull;

/**
 * The {@code MongoConnection} abstracts connection to the {@code MongoDB}.
 */
public class MongoConnection {

    private static final int DEFAULT_MAX_WAIT_TIME = (int) TimeUnit.MINUTES.toMillis(1);
    private static final int DEFAULT_HEARTBEAT_FREQUENCY_MS = (int) TimeUnit.SECONDS.toMillis(5);
    private static final WriteConcern WC_UNKNOWN = new WriteConcern("unknown");
    private static final Set<ReadConcernLevel> REPLICA_RC = Set.of(ReadConcernLevel.MAJORITY, ReadConcernLevel.LINEARIZABLE);
    private final MongoClientURI mongoURI;
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
    public MongoConnection(String uri, MongoClientOptions.Builder builder)
            throws MongoException {
        mongoURI = new MongoClientURI(uri, builder);
        mongo = new MongoClient(mongoURI);
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
        mongoURI = new MongoClientURI(uri, MongoConnection.getDefaultBuilder());
        mongo = client;
    }

    /**
     *
     * @return the {@link MongoClientURI} for this connection
     */
    public MongoClientURI getMongoURI() {
        return mongoURI;
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
    public static MongoClientOptions.Builder getDefaultBuilder() {
        return new MongoClientOptions.Builder()
                .description("MongoConnection for Oak DocumentMK")
                .maxWaitTime(DEFAULT_MAX_WAIT_TIME)
                .heartbeatFrequency(DEFAULT_HEARTBEAT_FREQUENCY_MS)
                .threadsAllowedToBlockForConnectionMultiplier(100);
    }

    public static String toString(MongoClientOptions opts) {
        return MoreObjects.toStringHelper(opts)
                .add("connectionsPerHost", opts.getConnectionsPerHost())
                .add("connectTimeout", opts.getConnectTimeout())
                .add("socketTimeout", opts.getSocketTimeout())
                .add("socketKeepAlive", opts.isSocketKeepAlive())
                .add("maxWaitTime", opts.getMaxWaitTime())
                .add("heartbeatFrequency", opts.getHeartbeatFrequency())
                .add("threadsAllowedToBlockForConnectionMultiplier",
                        opts.getThreadsAllowedToBlockForConnectionMultiplier())
                .add("readPreference", opts.getReadPreference().getName())
                .add("writeConcern", opts.getWriteConcern())
                .toString();
    }

    /**
     * Returns {@code true} if the given {@code uri} has a write concern set.
     * @param uri the URI to check.
     * @return {@code true} if the URI has a write concern set, {@code false}
     *      otherwise.
     */
    public static boolean hasWriteConcern(@NotNull String uri) {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        builder.writeConcern(WC_UNKNOWN);
        WriteConcern wc = new MongoClientURI(requireNonNull(uri), builder)
                .getOptions().getWriteConcern();
        return !WC_UNKNOWN.equals(wc);
    }

    /**
     * Returns {@code true} if the given {@code uri} has a read concern set.
     * @param uri the URI to check.
     * @return {@code true} if the URI has a read concern set, {@code false}
     *      otherwise.
     */
    public static boolean hasReadConcern(@NotNull String uri) {
        ReadConcern rc = new MongoClientURI(requireNonNull(uri))
                .getOptions().getReadConcern();
        return readConcernLevel(rc) != null;
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
        if (client.getReplicaSetStatus() != null) {
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
        if (requireNonNull(client).getReplicaSetStatus() != null && isMajorityWriteConcern(db)) {
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
        Object wObj = requireNonNull(wc).getWObject();
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
        if (client.getReplicaSetStatus() != null) {
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
        ReadConcernLevel r = readConcernLevel(requireNonNull(rc));
        if (client.getReplicaSetStatus() == null) {
            return true;
        } else {
            return Objects.nonNull(r) && REPLICA_RC.contains(r);
        }
    }

    public static ReadConcernLevel readConcernLevel(ReadConcern readConcern) {
        if (readConcern.isServerDefault()) {
            return null;
        } else {
            return ReadConcernLevel.fromString(readConcern.asDocument().getString("level").getValue());
        }
    }
}

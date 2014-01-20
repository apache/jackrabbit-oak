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
package org.apache.jackrabbit.oak.plugins.mongomk.util;

import com.google.common.base.Objects;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

/**
 * The {@code MongoConnection} abstracts connection to the {@code MongoDB}.
 */
public class MongoConnection {

    private final DB db;
    private final MongoClient mongo;

    /**
     * Constructs a new {@code MongoConnection}.
     *
     * @param host The host address.
     * @param port The port.
     * @param database The database name.
     * @throws Exception If an error occurred while trying to connect.
     */
    public MongoConnection(String host, int port, String database) throws Exception {
        MongoClientOptions options = getDefaultBuilder().build();
        ServerAddress serverAddress = new ServerAddress(host, port);
        mongo = new MongoClient(serverAddress, options);
        db = mongo.getDB(database);
    }

    /**
     * Returns the {@link DB}.
     *
     * @return The {@link DB}.
     */
    public DB getDB() {
        return db;
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
                .description("MongoConnection for Oak MongoMK")
                .threadsAllowedToBlockForConnectionMultiplier(100);
    }

    public static String toString(MongoClientOptions opts) {
        return Objects.toStringHelper(opts)
                .add("connectionsPerHost", opts.getConnectionsPerHost())
                .add("connectTimeout", opts.getConnectTimeout())
                .add("socketTimeout", opts.getSocketTimeout())
                .add("socketKeepAlive", opts.isSocketKeepAlive())
                .add("autoConnectRetry", opts.isAutoConnectRetry())
                .add("maxAutoConnectRetryTime", opts.getMaxAutoConnectRetryTime())
                .add("maxWaitTime", opts.getMaxWaitTime())
                .add("threadsAllowedToBlockForConnectionMultiplier",
                        opts.getThreadsAllowedToBlockForConnectionMultiplier())
                .add("readPreference", opts.getReadPreference().getName())
                .add("writeConcern", opts.getWriteConcern())
                .toString();
    }
}
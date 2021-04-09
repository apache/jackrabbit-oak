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

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadConcernLevel;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.document.util.MongoConnection.readConcernLevel;

/**
 * Simple struct that contains {@code MongoClient}, {@code MongoDatabase} and
 * {@code MongoStatus}.
 */
final class MongoDBConnection {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBConnection.class);

    private final MongoClient client;
    private final MongoDatabase db;
    private final MongoStatus status;
    private final MongoClock clock;
    private final MongoSessionFactory sessionFactory;

    MongoDBConnection(@NotNull MongoClient client,
                      @NotNull MongoDatabase database,
                      @NotNull MongoStatus status,
                      @NotNull MongoClock clock) {
        this.client = checkNotNull(client);
        this.db = checkNotNull(database);
        this.status = checkNotNull(status);
        this.clock = checkNotNull(clock);
        this.sessionFactory = new MongoSessionFactory(client, clock);
    }

    static MongoDBConnection newMongoDBConnection(@NotNull String uri,
                                                  @NotNull String name,
                                                  @NotNull MongoClock clock,
                                                  int socketTimeout,
                                                  boolean socketKeepAlive) {
        CompositeServerMonitorListener serverMonitorListener = new CompositeServerMonitorListener();
        MongoClientOptions.Builder options = MongoConnection.getDefaultBuilder();
        options.addServerMonitorListener(serverMonitorListener);
        options.socketKeepAlive(socketKeepAlive);
        if (socketTimeout > 0) {
            options.socketTimeout(socketTimeout);
        }
        MongoClient client = new MongoClient(new MongoClientURI(uri, options));
        MongoStatus status = new MongoStatus(client, name);
        serverMonitorListener.addListener(status);
        MongoDatabase db = client.getDatabase(name);
        if (!MongoConnection.hasWriteConcern(uri)) {
            db = db.withWriteConcern(MongoConnection.getDefaultWriteConcern(client));
        }
        if (status.isMajorityReadConcernSupported()
                && status.isMajorityReadConcernEnabled()
                && !MongoConnection.hasReadConcern(uri)) {
            db = db.withReadConcern(MongoConnection.getDefaultReadConcern(client, db));
        }
        return new MongoDBConnection(client, db, status, clock);
    }

    @NotNull
    MongoClient getClient() {
        return client;
    }

    @NotNull
    MongoDatabase getDatabase() {
        return db;
    }

    @NotNull
    MongoStatus getStatus() {
        return status;
    }

    @NotNull
    MongoClock getClock() {
        return clock;
    }

    @NotNull
    MongoCollection<BasicDBObject> getCollection(@NotNull String name) {
        return db.getCollection(name, BasicDBObject.class);
    }

    @NotNull
    ClientSession createClientSession() {
        return sessionFactory.createClientSession();
    }

    void close() {
        client.close();
    }

    /**
     * Checks read and write concern on the {@code MongoDatabase} and logs warn
     * messages when they differ from the recommended values.
     */
    void checkReadWriteConcern() {
        if (!MongoConnection.isSufficientWriteConcern(client, db.getWriteConcern())) {
            LOG.warn("Insufficient write concern: {} At least {} is recommended.",
                    db.getWriteConcern(), MongoConnection.getDefaultWriteConcern(client));
        }
        if (status.isMajorityReadConcernSupported() && !status.isMajorityReadConcernEnabled()) {
            LOG.warn("The read concern should be enabled on mongod using --enableMajorityReadConcern");
        } else if (status.isMajorityReadConcernSupported() && !MongoConnection.isSufficientReadConcern(client, db.getReadConcern())) {
            ReadConcernLevel currentLevel = readConcernLevel(db.getReadConcern());
            ReadConcernLevel recommendedLevel = readConcernLevel(MongoConnection.getDefaultReadConcern(client, db));
            if (currentLevel == null) {
                LOG.warn("Read concern hasn't been set. At least {} is recommended.",
                        recommendedLevel);
            } else {
                LOG.warn("Insufficient read concern: {}}. At least {} is recommended.",
                        currentLevel, recommendedLevel);
            }
        }
    }
}

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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDockerRule;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

/**
 * A utility class to get a {@link MongoConnection} to a local mongo instance
 * and clean a test database.
 */
public class MongoUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MongoUtils.class);

    protected static Map<String, Exception> exceptions = new ConcurrentHashMap<>();

    protected static final String HOST =
            System.getProperty("mongo.host", "127.0.0.1");

    protected static final int PORT =
            Integer.getInteger("mongo.port", 27017);

    public static final String DB =
            System.getProperty("mongo.db", "MongoMKDB");

    private static final String OPTIONS = "connectTimeoutMS=3000&serverSelectionTimeoutMS=3000";

    public static final String URL = createMongoURL();

    private static String createMongoURL() {
        // first try configured URL
        String mongoUrl = System.getProperty("mongo.url");
        if (mongoUrl == null || mongoUrl.isEmpty()) {
            mongoUrl = "mongodb://" + HOST + ":" + PORT + "/" + DB + "?" + OPTIONS;
        }
        if (!DocumentStoreFixture.MongoFixture.SKIP_MONGO) {
            // check if we can connect
            MongoConnection c = getConnectionByURL(mongoUrl);
            if (c != null) {
                c.close();
                return mongoUrl;
            }
            // fallback to docker based MongoDB if available
            MongoDockerRule dockerRule = new MongoDockerRule();
            if (MongoDockerRule.isDockerAvailable()) {
                AtomicReference<String> host = new AtomicReference<>();
                AtomicInteger port = new AtomicInteger();
                try {
                    dockerRule.apply(new Statement() {
                        @Override
                        public void evaluate() {
                            host.set(dockerRule.getHost());
                            port.set(dockerRule.getPort());
                        }
                    }, Description.EMPTY).evaluate();
                    mongoUrl = "mongodb://" + host + ":" + port.get() + "/" + DB + "?" + OPTIONS;
                } catch (Throwable t) {
                    LOG.warn("Unable to get MongoDB port from Docker", t);
                }
            }
        }
        return mongoUrl;
    }

    /**
     * Get a connection if available. If not available, null is returned.
     *
     * @return the connection or null
     */
    public static MongoConnection getConnection() {
        return getConnectionByURL(URL);
    }

    /**
     * Get a connection if available. If not available, null is returned.
     *
     * @param dbName the database name
     * @return the connection or null
     */
    public static MongoConnection getConnection(String dbName) {
        MongoClientURI clientURI;
        try {
            clientURI = new MongoClientURI(URL);
        } catch (IllegalArgumentException e) {
            // configured URL is invalid
            return null;
        }
        StringBuilder uri = new StringBuilder("mongodb://");
        String separator = "";
        for (String host : clientURI.getHosts()) {
            uri.append(separator);
            separator = ",";
            uri.append(host);
        }
        uri.append("/").append(dbName).append("?").append(OPTIONS);
        return getConnectionByURL(uri.toString());
    }

    /**
     * Drop all user defined collections in the given database. System
     * collections are not dropped. This method returns silently if MongoDB is
     * not available.
     *
     * @param dbName the database name.
     */
    public static void dropCollections(String dbName) {
        MongoConnection c = getConnection(dbName);
        if (c == null) {
            return;
        }
        try {
            dropCollections(c.getDatabase());
        } finally {
            c.close();
        }
    }

    /**
     * Drop all user defined collections. System collections are not dropped.
     *
     * @param db the connection
     * @deprecated use {@link #dropCollections(MongoDatabase)} instead.
     */
    public static void dropCollections(DB db) {
        for (String name : db.getCollectionNames()) {
            if (!name.startsWith("system.")) {
                db.getCollection(name).drop();
            }
        }
    }

    /**
     * Drop all user defined collections. System collections are not dropped.
     *
     * @param db the connection
     */
    public static void dropCollections(MongoDatabase db) {
        for (String name : db.listCollectionNames()) {
            if (!name.startsWith("system.")) {
                db.getCollection(name).drop();
            }
        }
    }

    /**
     * Drops the database with the given name. This method returns silently if
     * MongoDB is not available.
     *
     * @param dbName the name of the database to drop.
     */
    public static void dropDatabase(String dbName) {
        MongoConnection c = getConnection(dbName);
        if (c == null) {
            return;
        }
        try {
            c.getDatabase().drop();
        } finally {
            c.close();
        }
    }

    /**
     * @return true if MongoDB is available, false otherwise.
     */
    public static boolean isAvailable() {
        MongoConnection c = getConnection();
        try {
            return c != null;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    //----------------------------< internal >----------------------------------

    /**
     * Get a connection if available. If not available, null is returned.
     *
     * @param url the mongodb url
     * @return the connection or null
     */
    private static MongoConnection getConnectionByURL(String url) {
        if (DocumentStoreFixture.MongoFixture.SKIP_MONGO || exceptions.get(url) != null) {
            return null;
        }
        MongoConnection mongoConnection;
        try {
            mongoConnection = new MongoConnection(url);
            mongoConnection.getDatabase().runCommand(new BasicDBObject("ping", 1));
            // dropCollections(mongoConnection.getDB());
        } catch (Exception e) {
            exceptions.put(url, e);
            mongoConnection = null;
        }
        return mongoConnection;
    }
}

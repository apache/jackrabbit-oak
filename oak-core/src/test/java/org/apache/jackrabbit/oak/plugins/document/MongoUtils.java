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

import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;

/**
 * A utility class to get a {@link MongoConnection} to a local mongo instance
 * and clean a test database.
 */
public class MongoUtils {

    protected static final String HOST =
            System.getProperty("mongo.host", "127.0.0.1");

    protected static final int PORT =
            Integer.getInteger("mongo.port", 27017);

    public static final String DB =
            System.getProperty("mongo.db", "MongoMKDB");

    public static final String URL =
            System.getProperty("mongo.url", "mongodb://" + HOST + ":" + PORT + "/" + DB +
                    "?connectTimeoutMS=3000&serverSelectionTimeoutMS=3000");

    protected static Exception exception;

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
        return getConnectionByURL("mongodb://" + HOST + ":" + PORT + "/" + dbName);
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
            dropCollections(c.getDB());
        } finally {
            c.close();
        }
    }

    /**
     * Drop all user defined collections. System collections are not dropped.
     *
     * @param db the connection
     */
    public static void dropCollections(DB db) {
        for (String name : db.getCollectionNames()) {
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
            c.getDB().dropDatabase();
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
        if (exception != null) {
            return null;
        }
        MongoConnection mongoConnection;
        try {
            mongoConnection = new MongoConnection(url);
            mongoConnection.getDB().command(new BasicDBObject("ping", 1));
            // dropCollections(mongoConnection.getDB());
        } catch (Exception e) {
            exception = e;
            mongoConnection = null;
        }
        return mongoConnection;
    }
}

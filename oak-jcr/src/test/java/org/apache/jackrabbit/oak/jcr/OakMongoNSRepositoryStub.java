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
package org.apache.jackrabbit.oak.jcr;

import java.lang.ref.WeakReference;
import java.util.Properties;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.mongodb.BasicDBObject;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;

/**
 * A repository stub using the DocumentNodeStore.
 */
public class OakMongoNSRepositoryStub extends OakRepositoryStub {

    protected static final String HOST =
            System.getProperty("mongo.host", "127.0.0.1");

    protected static final int PORT =
            Integer.getInteger("mongo.port", 27017);

    protected static final String DB =
            System.getProperty("mongo.db", "MongoMKDB");

    private final MongoConnection connection;
    private final Repository repository;

    /**
     * Constructor as required by the JCR TCK.
     *
     * @param settings repository settings
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    public OakMongoNSRepositoryStub(Properties settings) throws RepositoryException {
        super(settings);
        Session session = null;
        try {
            this.connection = new MongoConnection(HOST, PORT, DB);
            this.repository = createRepository(connection);

            session = getRepository().login(superuser);
            TestContentLoader loader = new TestContentLoader();
            loader.loadTestContent(session);
        } catch (Exception e) {
            throw new RepositoryException(e);
        } finally {
            if (session != null) {
                session.logout();
            }
        }
        Runtime.getRuntime().addShutdownHook(
                new Thread(new ShutdownHook(connection)));
    }

    private static Repository createRepository(MongoConnection connection) {
        DocumentNodeStore store = new DocumentMK.Builder().
                memoryCacheSize(64 * 1024 * 1024).
                setPersistentCache("target/persistentCache,time").
                setMongoDB(connection.getDB()).
                getNodeStore();
        QueryEngineSettings qs = new QueryEngineSettings();
        qs.setFullTextComparisonWithoutIndex(true);
        return new Jcr(store).with(qs).createRepository();
    }

    /**
     * A shutdown hook that closed the MongoDB connection if needed.
     */
    private static class ShutdownHook implements Runnable {

        private final WeakReference<MongoConnection> reference;

        public ShutdownHook(MongoConnection connection) {
            this.reference = new WeakReference<MongoConnection>(connection);
        }

        @Override
        public void run() {
            MongoConnection connection = reference.get();
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static boolean isMongoDBAvailable() {
        MongoConnection connection = null;
        try {
            connection = createConnection(DB);
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    static MongoConnection createConnection(String db) throws Exception {
        boolean success = false;
        MongoConnection con = new MongoConnection(HOST, PORT, db);
        try {
            con.getDB().command(new BasicDBObject("ping", 1));
            success = true;
        } finally {
            if (!success) {
                con.close();
            }
        }
        return con;
    }

    /**
     * Returns the configured repository instance.
     *
     * @return the configured repository instance.
     */
    @Override
    public synchronized Repository getRepository() {
        return repository;
    }

}

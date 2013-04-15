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
package org.apache.jackrabbit.mongomk;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mongomk.util.MongoConnection;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;

/**
 * Base class for test cases that need a {@link MongoConnection}
 * to a clean test database. Tests in subclasses are automatically
 * skipped if the configured MongoDB connection can not be created.
 */
public abstract class AbstractMongoConnectionTest extends MongoMKTestBase {

    protected static final String HOST =
            System.getProperty("mongo.host", "127.0.0.1");

    protected static final int PORT =
            Integer.getInteger("mongo.port", 27017);

    protected static final String DB =
            System.getProperty("mongo.db", "MongoMKDB");

    protected static Boolean mongoAvailable;

    private static Exception mongoException;
    
    protected MongoConnection mongoConnection;

    protected MongoMK mk;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        if (mongoAvailable == null) {
            MongoConnection mongoConnection = new MongoConnection(HOST, PORT, DB);
            try {
                mongoConnection.getDB().command(new BasicDBObject("ping", 1));
                mongoAvailable = Boolean.TRUE;
            } catch (Exception e) {
                mongoAvailable = Boolean.FALSE;
                mongoException = e;
            } finally {
                mongoConnection.close();
            }
        }
        Assume.assumeNoException(mongoException);
    }

    @Before
    public void setUpConnection() throws Exception {
        mongoConnection = new MongoConnection(HOST, PORT, DB);
        dropCollections(mongoConnection.getDB());
        mk = new MongoMK.Builder().setMongoDB(mongoConnection.getDB()).open();
    }

    @After
    public void tearDownConnection() throws Exception {
        mk.dispose();
        // the db might already be closed
        mongoConnection.close();
        mongoConnection = new MongoConnection(HOST, PORT, DB);
        dropCollections(mongoConnection.getDB());
        mongoConnection.close();
    }

    @Override
    protected MicroKernel getMicroKernel() {
        return mk;
    }

    protected void dropCollections(DB db) throws Exception {
        for (String name : db.getCollectionNames()) {
            if (!name.startsWith("system.")) {
                db.getCollection(name).drop();
            }
        }
    }

}

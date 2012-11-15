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
package org.apache.jackrabbit.mongomk.multitenancy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.util.Properties;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.blob.MongoBlobStore;
import org.apache.jackrabbit.mongomk.impl.blob.MongoGridFSBlobStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.DB;

/**
 * Tests for multi-tenancy.
 */
public class MultiTenancyTest {

    private static MicroKernel mk1;
    private static MicroKernel mk2;
    private static MicroKernel mk3;
    private static MongoConnection mongoConnection1;
    private static MongoConnection mongoConnection2;
    private static MongoConnection mongoConnection3;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        createMongoConnections();
    }

    @Before
    public void setUp() throws Exception {
        dropCollections();
        setupMicroKernels();
    }

    @After
    public void tearDown() throws Exception {
        dropCollections();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        dropDatabases();
    }

    /**
     * Scenario: 3 MKs total, 2 MKs point to DB1, 1 points to DB2.
     */
    @Test
    public void basicMultiTenancy() {
        mk1.commit("/", "+\"a\" : {}", null, null);
        assertEquals(1, mk1.getChildNodeCount("/", null));
        assertEquals(0, mk2.getChildNodeCount("/", null));
        assertEquals(1, mk3.getChildNodeCount("/", null));

        mk2.commit("/", "+\"b\" : {}", null, null);
        mk2.commit("/", "+\"c\" : {}", null, null);
        assertEquals(1, mk1.getChildNodeCount("/", null));
        assertEquals(2, mk2.getChildNodeCount("/", null));
        assertEquals(1, mk3.getChildNodeCount("/", null));

        assertTrue(mk1.nodeExists("/a", null));
        assertFalse(mk1.nodeExists("/b", null));
        assertFalse(mk1.nodeExists("/c", null));

        assertFalse(mk2.nodeExists("/a", null));
        assertTrue(mk2.nodeExists("/b", null));
        assertTrue(mk2.nodeExists("/c", null));

        assertTrue(mk3.nodeExists("/a", null));
        assertFalse(mk3.nodeExists("/b", null));
        assertFalse(mk3.nodeExists("/c", null));
    }

    private static void createMongoConnections() throws Exception {
        InputStream is = MultiTenancyTest.class.getResourceAsStream("/config.cfg");
        Properties properties = new Properties();
        properties.load(is);

        String host = properties.getProperty("host");
        int port = Integer.parseInt(properties.getProperty("port"));
        String db1 = properties.getProperty("db");
        String db2 = properties.getProperty("db2");

        mongoConnection1 = new MongoConnection(host, port, db1);
        mongoConnection2 = new MongoConnection(host, port, db2);
        mongoConnection3 = new MongoConnection(host, port, db1);
    }

    private static void dropDatabases() {
        mongoConnection1.getDB().dropDatabase();
        mongoConnection2.getDB().dropDatabase();
        mongoConnection3.getDB().dropDatabase();
    }

    private void dropCollections() {
        doDropCollections(mongoConnection1.getDB());
        doDropCollections(mongoConnection2.getDB());
        doDropCollections(mongoConnection3.getDB());
    }

    private void doDropCollections(DB db) {
        db.getCollection(MongoBlobStore.COLLECTION_BLOBS).drop();
        db.getCollection(MongoNodeStore.COLLECTION_COMMITS).drop();
        db.getCollection(MongoNodeStore.COLLECTION_NODES).drop();
        db.getCollection(MongoNodeStore.COLLECTION_SYNC).drop();
    }

    private void setupMicroKernels() {
        DB db = mongoConnection1.getDB();
        mk1 = new MongoMicroKernel(mongoConnection1, new MongoNodeStore(db),
                new MongoGridFSBlobStore(db));

        DB db2 = mongoConnection2.getDB();
        mk2 = new MongoMicroKernel(mongoConnection2, new MongoNodeStore(db2),
                new MongoGridFSBlobStore(db2));

        DB db3 = mongoConnection1.getDB();
        mk3 = new MongoMicroKernel(mongoConnection3, new MongoNodeStore(db3),
                new MongoGridFSBlobStore(db3));
    }
}
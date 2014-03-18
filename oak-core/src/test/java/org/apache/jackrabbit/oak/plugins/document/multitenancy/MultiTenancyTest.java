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
package org.apache.jackrabbit.oak.plugins.document.multitenancy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.DB;

/**
 * Tests for multi-tenancy.
 */
public class MultiTenancyTest extends AbstractMongoConnectionTest {

    private static final String DB2 =
            System.getProperty("mongo.db2", "MongoMKDB2");

    private static MongoConnection mongoConnection2;
    private static MongoConnection mongoConnection3;

    private static MicroKernel mk1;
    private static MicroKernel mk2;
    private static MicroKernel mk3;

    @BeforeClass
    public static void createMongoConnections() throws Exception {
        mongoConnection2 = MongoUtils.getConnection(DB2);
        mongoConnection3 = MongoUtils.getConnection();
    }


    @Before
    public void setupMicroKernels() throws Exception {
        mongoConnection2.getDB().dropDatabase();
        // DB1 handled by the AbstractMongoConnectionTest

        DB db = mongoConnection.getDB();
        mk1 = new DocumentMK.Builder().setMongoDB(db).open();

        DB db2 = mongoConnection2.getDB();
        mk2 = new DocumentMK.Builder().setMongoDB(db2).open();

        DB db3 = mongoConnection3.getDB();
        mk3 = new DocumentMK.Builder().setMongoDB(db3).open();
    }

    @After
    public void dropCollections() throws Exception {
        MongoUtils.dropCollections(mongoConnection2.getDB());
        // DB1 handled by the AbstractMongoConnectionTest
    }

    /**
     * Scenario: 3 MKs total, 2 MKs point to DB1, 1 points to DB2.
     */
    @Test
    @Ignore
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

}
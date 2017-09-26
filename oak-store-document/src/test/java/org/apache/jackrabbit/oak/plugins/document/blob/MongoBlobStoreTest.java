/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.blob;

import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStoreTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Tests the {@link MongoBlobStore} implementation.
 */
public class MongoBlobStoreTest extends AbstractBlobStoreTest {

    private MongoConnection mongoConnection;

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    @Override
    protected boolean supportsStatsCollection() {
        return true;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        mongoConnection = MongoUtils.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());

        MongoBlobStore blobStore = new MongoBlobStore(mongoConnection.getDB());
        blobStore.setBlockSize(128);
        blobStore.setBlockSizeMin(48);
        this.store = blobStore;
    }

    @After
    @Override
    public void tearDown() throws Exception {
        MongoUtils.dropCollections(mongoConnection.getDB());
        mongoConnection.close();
        super.tearDown();
    }

}
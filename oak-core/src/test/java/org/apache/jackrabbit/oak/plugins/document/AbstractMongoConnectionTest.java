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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Base class for test cases that need a {@link MongoConnection}
 * to a clean test database. Tests in subclasses are automatically
 * skipped if the configured MongoDB connection can not be created.
 */
public abstract class AbstractMongoConnectionTest extends DocumentMKTestBase {

    protected MongoConnection mongoConnection;
    protected DocumentMK mk;

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeNotNull(MongoUtils.getConnection());
    }

    @Before
    public void setUpConnection() throws Exception {
        mongoConnection = MongoUtils.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        mk = new DocumentMK.Builder().clock(getTestClock()).setMongoDB(mongoConnection.getDB()).open();
    }

    protected Clock getTestClock() throws InterruptedException {
        return Clock.SIMPLE;
    }

    @After
    public void tearDownConnection() throws Exception {
        mk.dispose();
        // the db might already be closed
        mongoConnection.close();
        mongoConnection = MongoUtils.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        mongoConnection.close();
    }

    @Override
    protected MicroKernel getMicroKernel() {
        return mk;
    }

}

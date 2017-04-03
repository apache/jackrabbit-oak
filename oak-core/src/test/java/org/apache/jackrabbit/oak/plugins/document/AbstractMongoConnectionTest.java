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

import com.mongodb.DB;

import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

/**
 * Base class for test cases that need a {@link MongoConnection}
 * to a clean test database. Tests in subclasses are automatically
 * skipped if the configured MongoDB connection can not be created.
 */
public abstract class AbstractMongoConnectionTest extends DocumentMKTestBase {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    protected MongoConnection mongoConnection;
    protected DocumentMK mk;

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    @Before
    public void setUpConnection() throws Exception {
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        setRevisionClock(getTestClock());
        setClusterNodeInfoClock(getTestClock());
        mk = newBuilder(mongoConnection.getDB()).open();
    }

    protected void setRevisionClock(Clock c) {
        Revision.setClock(c);
    }

    protected void setClusterNodeInfoClock(Clock c) {
        ClusterNodeInfo.setClock(c);
    }

    protected DocumentMK.Builder newBuilder(DB db) throws Exception {
        return addToBuilder(new DocumentMK.Builder()).clock(getTestClock()).setMongoDB(db);
    }

    protected DocumentMK.Builder addToBuilder(DocumentMK.Builder mk) {
        return mk;
    }

    protected Clock getTestClock() throws InterruptedException {
        return Clock.SIMPLE;
    }

    @After
    public void tearDownConnection() throws Exception {
        mk.dispose();
        DB db = connectionFactory.getConnection().getDB();
        MongoUtils.dropCollections(db);
        db.getMongo().close();
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    @Override
    protected DocumentMK getDocumentMK() {
        return mk;
    }

    protected static byte[] readFully(DocumentMK mk, String blobId) {
        int remaining = (int) mk.getLength(blobId);
        byte[] bytes = new byte[remaining];

        int offset = 0;
        while (remaining > 0) {
            int count = mk.read(blobId, offset, bytes, offset, remaining);
            if (count < 0) {
                break;
            }
            offset += count;
            remaining -= count;
        }
        return bytes;
    }
}

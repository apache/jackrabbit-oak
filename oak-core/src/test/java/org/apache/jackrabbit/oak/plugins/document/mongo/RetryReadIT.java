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

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.MongoException;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests retry logic in MongoDocumentStore (OAK-1641).
 */
public class RetryReadIT extends AbstractMongoConnectionTest {

    private TestStore store;

    @Override
    public void setUpConnection() throws Exception {
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        DocumentMK.Builder builder = new DocumentMK.Builder();
        builder.clock(getTestClock());
        store = new TestStore(mongoConnection.getDB(), builder);
        mk = builder.setDocumentStore(store).open();
    }

    @Test
    public void retry() {
        // must survive two consecutive failures. -> 2 retries
        store.failRead = 2;
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNull(doc);
        // previous result is cached and will not fail
        store.failRead = 3;
        doc = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNull(doc);
        // must fail with three consecutive failures on unknown path
        try {
            store.find(NODES, Utils.getIdFromPath("/bar"));
            fail("must fail with DocumentStoreException");
        } catch (DocumentStoreException e) {
            // expected
        }
    }

    private static class TestStore extends MongoDocumentStore {

        private int failRead = 0;

        public TestStore(DB db, DocumentMK.Builder builder) {
            super(db, builder);
        }

        @Override
        protected <T extends Document> T findUncached(Collection<T> collection,
                                                      String key,
                                                      DocumentReadPreference docReadPref) {
            if (failRead > 0) {
                failRead--;
                throw new MongoException("read failed");
            }
            return super.findUncached(collection, key, docReadPref);
        }
    }

}

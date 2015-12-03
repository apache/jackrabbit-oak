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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import com.mongodb.DB;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * <code>MongoDocumentStoreTest</code>...
 */
public class MongoDocumentStoreTest extends AbstractMongoConnectionTest {

    private TestStore store;

    @Override
    public void setUpConnection() throws Exception {
        mongoConnection = connectionFactory.getConnection();
        MongoUtils.dropCollections(mongoConnection.getDB());
        DocumentMK.Builder builder = new DocumentMK.Builder();
        store = new TestStore(mongoConnection.getDB(), builder);
        builder.setDocumentStore(store);
        mk = builder.setMongoDB(mongoConnection.getDB()).open();
    }

    @Test
    public void timeoutQuery() {
        String fromId = Utils.getKeyLowerLimit("/");
        String toId = Utils.getKeyUpperLimit("/");
        store.setMaxLockedQueryTimeMS(1);
        long index = 0;
        for (int i = 0; i < 100; i++) {
            // keep adding nodes until the query runs into the timeout
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < 1000; j++) {
                sb.append("+\"node-").append(index++).append("\":{}");
            }
            mk.commit("/", sb.toString(), null, null);
            store.queriesWithoutLock.set(0);
            store.resetLockAcquisitionCount();
            List<NodeDocument> docs = store.query(Collection.NODES, fromId, toId,
                    "foo", System.currentTimeMillis(), Integer.MAX_VALUE);
            assertTrue(docs.isEmpty());
            if (store.queriesWithoutLock.get() > 0) {
                assertEquals(1, store.getLockAcquisitionCount());
                return;
            }
        }
        fail("No query timeout triggered even after adding " + index + " nodes");
    }

    static final class TestStore extends MongoDocumentStore {

        AtomicInteger queriesWithoutLock = new AtomicInteger();

        TestStore(DB db, DocumentMK.Builder builder) {
            super(db, builder);
        }

        @Nonnull
        @Override
        <T extends Document> List<T> queryInternal(Collection<T> collection,
                                                   String fromKey,
                                                   String toKey,
                                                   String indexedProperty,
                                                   long startValue,
                                                   int limit,
                                                   long maxQueryTime,
                                                   boolean withLock) {
            if (collection == Collection.NODES && !withLock) {
                queriesWithoutLock.incrementAndGet();
            }
            return super.queryInternal(collection, fromKey, toKey,
                    indexedProperty, startValue, limit, maxQueryTime, withLock);
        }
    }
}

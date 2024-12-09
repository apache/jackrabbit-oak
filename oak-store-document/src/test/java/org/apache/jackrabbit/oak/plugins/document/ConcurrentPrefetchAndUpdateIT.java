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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.guava.common.collect.Lists;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConcurrentPrefetchAndUpdateIT extends AbstractMongoConnectionTest {

    protected static final int NUM_NODES = 50;

    private static final Random RAND = new Random();

    private TestStore store;

    private final AtomicLong counter = new AtomicLong();

    private final List<String> ids = new ArrayList<>();

    @Before
    @Override
    public void setUpConnection() throws Exception {
        System.setProperty(DocumentNodeStore.SYS_PROP_PREFETCH, String.valueOf(true));
        mongoConnection = connectionFactory.getConnection();
        assertNotNull(mongoConnection);
        MongoDatabase db = mongoConnection.getDatabase();
        MongoUtils.dropCollections(db);
        DocumentMK.Builder builder = new DocumentMK.Builder()
                .clock(getTestClock()).setAsyncDelay(0);
        store = new TestStore(mongoConnection.getMongoClient(), db, builder);
        mk = builder.setDocumentStore(store).open();
    }

    @After
    public void clearSystemProperty() {
        System.clearProperty(DocumentNodeStore.SYS_PROP_PREFETCH);
    }

    @Test
    public void cacheConsistency() throws Exception {
        Revision r = newRevision();
        List<UpdateOp> ops = new ArrayList<>();
        for (int i = 0; i < NUM_NODES; i++) {
            String id = Utils.getIdFromPath("/node-" + i);
            ids.add(id);
            UpdateOp op = new UpdateOp(id, true);
            NodeDocument.setLastRev(op, r);
            ops.add(op);
        }
        store.create(NODES, ops);

        for (int i = 0; i < 100; i++) {
            Thread q = new Thread(this::prefetchDocuments);
            Thread u = new Thread(this::updateDocuments);
            Thread iv = new Thread(this::invalidate);
            q.start();
            u.start();
            iv.start();
            q.join();
            u.join();
            iv.join();
            for (String id : ids) {
                NodeDocument doc = store.find(NODES, id);
                assertNotNull(doc);
                assertEquals("Unexpected revision timestamp for " + doc.getId(),
                        counter.get(), doc.getLastRev().get(1).getTimestamp());
            }
        }
    }

    private Revision newRevision() {
        return new Revision(counter.incrementAndGet(), 0, 1);
    }

    private void prefetchDocuments() {
        randomWait();
        store.prefetch(NODES, ids);
    }

    private void updateDocuments() {
        randomWait();
        UpdateOp op = new UpdateOp("foo", false);
        NodeDocument.setLastRev(op, newRevision());
        List<UpdateOp> ops = new ArrayList<>();
        for (String id : ids) {
            ops.add(op.shallowCopy(id));
        }
        store.createOrUpdate(NODES, ops);
    }

    private void invalidate() {
        randomWait();
        List<String> invalidate = new ArrayList<>(ids);
        Collections.shuffle(invalidate);
        // evict random 80% of the docs from the cache
        for (String id : invalidate.subList(0, (int) (NUM_NODES * 0.8))) {
            store.invalidateCache(NODES, id);
        }
    }

    private static void randomWait() {
        try {
            Thread.sleep(0, RAND.nextInt(100_000));
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private static final class TestStore extends MongoDocumentStore {

        TestStore(MongoClient client, MongoDatabase db, DocumentMK.Builder builder) {
            super(client, db, builder);
        }

        @Override
        protected <T extends Document> T convertFromDBObject(
                @NotNull Collection<T> collection, @Nullable DBObject n) {
            randomWait();
            return super.convertFromDBObject(collection, n);
        }
    }
}


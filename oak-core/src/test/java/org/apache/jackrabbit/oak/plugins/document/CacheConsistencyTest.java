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

import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.mongodb.DB;
import com.mongodb.DBObject;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertTrue;

/**
 * Test for OAK-1897
 */
public class CacheConsistencyTest extends AbstractMongoConnectionTest {

    private TestStore store;

    @Before
    @Override
    public void setUpConnection() throws Exception {
        mongoConnection = connectionFactory.getConnection();
        DB db = mongoConnection.getDB();
        MongoUtils.dropCollections(db);
        DocumentMK.Builder builder = new DocumentMK.Builder()
                .clock(getTestClock()).setAsyncDelay(0);
        store = new TestStore(db, builder);
        mk = builder.setDocumentStore(store).open();
    }

    @Test
    public void cacheConsistency() throws Exception {
        mk.commit("/", "+\"node\":{}", null, null);
        // add a child node. this will require an update
        // of _lastRev on /node
        mk.commit("/node", "+\"child\":{}", null, null);

        // make sure the document is not cached
        store.invalidateCache(NODES, Utils.getIdFromPath("/node"));

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                store.query(NODES,
                        Utils.getKeyLowerLimit("/"),
                        Utils.getKeyUpperLimit("/"), 10);
            }
        });
        // block thread when it tries to convert db objects
        store.semaphores.put(t, new Semaphore(0));
        t.start();

        while (!store.semaphores.get(t).hasQueuedThreads()) {
            Thread.sleep(10);
        }

        final Semaphore done = new Semaphore(0);
        new Thread(new Runnable() {
            @Override
            public void run() {
                // trigger write back of _lastRevs
                mk.runBackgroundOperations();
                done.release();
            }
        }).start();

        // wait at most one second for background thread
        done.tryAcquire(1, TimeUnit.SECONDS);

        // release thread
        store.semaphores.get(t).release();
        t.join();

        NodeState root = mk.getNodeStore().getRoot();
        assertTrue(root.getChildNode("node").getChildNode("child").exists());
    }

    private static final class TestStore extends MongoDocumentStore {

        final Map<Thread, Semaphore> semaphores = Maps.newConcurrentMap();

        TestStore(DB db, DocumentMK.Builder builder) {
            super(db, builder);
        }

        @Override
        protected <T extends Document> T convertFromDBObject(
                @Nonnull Collection<T> collection, @Nullable DBObject n) {
            Semaphore s = semaphores.get(Thread.currentThread());
            if (s != null) {
                s.acquireUninterruptibly();
            }
            try {
                return super.convertFromDBObject(collection, n);
            } finally {
                if (s != null) {
                    s.release();
                }
            }
        }
    }

}

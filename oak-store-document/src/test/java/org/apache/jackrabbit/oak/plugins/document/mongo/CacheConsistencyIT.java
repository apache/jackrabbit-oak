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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import com.mongodb.DB;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for OAK-3103
 */
public class CacheConsistencyIT extends AbstractMongoConnectionTest {

    private MongoDocumentStore store;

    @Before
    @Override
    public void setUpConnection() throws Exception {
        mongoConnection = connectionFactory.getConnection();
        DB db = mongoConnection.getDB();
        MongoUtils.dropCollections(db);
        DocumentMK.Builder builder = new DocumentMK.Builder()
                .clock(getTestClock()).setAsyncDelay(0);
        store = new MongoDocumentStore(db, builder);
        mk = builder.setDocumentStore(store).open();
    }

    @Test
    public void evictWhileUpdateLoop() throws Throwable {
        for (int i = 0; i < 10; i++) {
            runTest();
            tearDownConnection();
            setUpConnection();
        }
    }

    private void runTest() throws Throwable {
        addNodes(null, "/test", "/test/foo");

        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                String id = Utils.getIdFromPath("/test/foo");
                List<String> ids = Lists.newArrayList();
                ids.add(id);
                long v = 0;
                while (exceptions.isEmpty()) {
                    try {
                        UpdateOp op = new UpdateOp(ids.get(0), false);
                        op.set("p", ++v);
                        store.update(NODES, ids, op);
                        NodeDocument doc = store.find(NODES, id);
                        Object p = doc.get("p");
                        assertEquals(v, ((Long) p).longValue());
                    } catch (Throwable e) {
                        exceptions.add(e);
                    }
                }
            }
        }, "update");
        t1.start();

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                String id = Utils.getIdFromPath("/test/foo");
                long v = 0;
                while (exceptions.isEmpty()) {
                    try {
                        UpdateOp op = new UpdateOp(id, false);
                        op.set("q", ++v);
                        NodeDocument old = store.findAndUpdate(NODES, op);
                        Object q = old.get("q");
                        if (q != null) {
                            assertEquals(v - 1, ((Long) q).longValue());
                        }
                    } catch (Throwable e) {
                        exceptions.add(e);
                    }
                }
            }
        }, "findAndUpdate");
        t2.start();

        Thread t3 = new Thread(new Runnable() {
            @Override
            public void run() {
                String id = Utils.getIdFromPath("/test/foo");
                long p = 0;
                long q = 0;
                while (exceptions.isEmpty()) {
                    try {
                        NodeDocument doc = store.find(NODES, id);
                        if (doc != null) {
                            Object value = doc.get("p");
                            if (value != null) {
                                assertTrue((Long) value >= p);
                                p = (Long) value;
                            }
                            value = doc.get("q");
                            if (value != null) {
                                assertTrue("previous: " + q + ", now: " + value, (Long) value >= q);
                                q = (Long) value;
                            }
                        }
                    } catch (Throwable e) {
                        exceptions.add(e);
                    }
                }
            }
        }, "reader");
        t3.start();

        NodeDocumentCache cache = store.getNodeDocumentCache();

        // run for at most five seconds
        long end = System.currentTimeMillis() + 1000;
        String id = Utils.getIdFromPath("/test/foo");
        while (t1.isAlive() && t2.isAlive() && t3.isAlive()
                && System.currentTimeMillis() < end) {
            if (cache.getIfPresent(id) != null) {
                Thread.sleep(0, (int) (Math.random() * 100));
                // simulate eviction
                cache.invalidate(id);
            }
        }
        for (Throwable e : exceptions) {
            throw e;
        }
        exceptions.add(new Exception("end"));
        t1.join();
        t2.join();
        t3.join();
    }

}

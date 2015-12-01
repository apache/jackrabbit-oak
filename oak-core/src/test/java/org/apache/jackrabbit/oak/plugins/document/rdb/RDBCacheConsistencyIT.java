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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.AbstractRDBConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Before;
import org.junit.Test;

import com.google.common.cache.Cache;
import com.google.common.collect.Lists;

/**
 * Test for OAK-3659
 */
public class RDBCacheConsistencyIT extends AbstractRDBConnectionTest {

    private RDBDocumentStore store;

    @Before
    @Override
    public void setUpConnection() throws Exception {
        dataSource = RDBDataSourceFactory.forJdbcUrl(URL, USERNAME, PASSWD); // /*"jdbc:derby:foo;create=true"*/
        DocumentMK.Builder builder = new DocumentMK.Builder().clock(getTestClock()).setAsyncDelay(0);
        RDBOptions opt = new RDBOptions().tablePrefix("T" + UUID.randomUUID().toString().replace("-", "")).dropTablesOnClose(true);
        store = new RDBDocumentStore(dataSource, builder, opt);
        mk = builder.setDocumentStore(store).setLeaseCheck(false).open();
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
                        op.set("mb", "update");
                        store.update(NODES, ids, op);
                        NodeDocument doc = store.find(NODES, id);
                        Object p = doc.get("p");
                        assertEquals("Unexpected result after update-then-find sequence, last modification of document by '"
                                + doc.get("mb") + "' thread @" + doc.getModCount(), v, ((Long) p).longValue());
                        // System.out.println("u @" + doc.getModCount() + " p=" + v + "; q=" + doc.get("q"));
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
                long lastWrittenV = 0;
                while (exceptions.isEmpty()) {
                    try {
                        UpdateOp op = new UpdateOp(id, false);
                        op.set("q", ++v);
                        op.set("mb", "findAndUpdate");
                        NodeDocument old = store.findAndUpdate(NODES, op);
                        Object q = old.get("q");
                        if (q != null) {
                            assertEquals("Unexpected result after findAndUpdate, last modification of document by '" + old.get("mb")
                                    + "' thread @" + old.getModCount(), lastWrittenV, ((Long) q).longValue());
                        }
                        lastWrittenV = v;
                        // System.out.println("f @" + old.getModCount() + " p=" + old.get("p") + "; q=" + q);
                    } catch (DocumentStoreException e) {
                        // System.err.println("f update of v to " + v + " failed: " + e.getMessage());
                        // keep going, RDBDocumentStore might have given up due
                        // to race conditions
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
                long mc = 0;
                while (exceptions.isEmpty()) {
                    try {
                        NodeDocument doc = store.find(NODES, id);
                        if (doc != null) {
                            Object value = doc.get("p");
                            if (value != null) {
                                assertTrue(
                                        "reader thread at @" + doc.getModCount()
                                                + ": observed property value for 'p' (incremented by 'update' thread) decreased, last change by '"
                                                + doc.get("mb") + "' thread; previous: " + p + " (at @" + mc + "), now: " + value,
                                        (Long) value >= p);
                                p = (Long) value;
                            }
                            value = doc.get("q");
                            if (value != null) {
                                assertTrue(
                                        "reader thread at @" + doc.getModCount()
                                                + ": observed property value for 'q' (incremented by 'findAndUpdate' thread) decreased, last change by '"
                                                + doc.get("mb") + "' thread; previous: " + q + " (at @" + mc + "), now: " + value,
                                        (Long) value >= q);
                                q = (Long) value;
                            }
                        }
                        mc = doc.getModCount().longValue();
                    } catch (Throwable e) {
                        exceptions.add(e);
                    }
                }
            }
        }, "reader");
        t3.start();

        Cache<CacheValue, NodeDocument> cache = store.getNodeDocumentCache();

        // run for at most five seconds
        long end = System.currentTimeMillis() + 1000;
        String id = Utils.getIdFromPath("/test/foo");
        CacheValue key = new StringValue(id);
        while (t1.isAlive() && t2.isAlive() && t3.isAlive()
                && System.currentTimeMillis() < end) {
            if (cache.getIfPresent(key) != null) {
                Thread.sleep(0, (int) (Math.random() * 100));
                // simulate eviction
                // System.out.println("EVICT");
                cache.invalidate(key);
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

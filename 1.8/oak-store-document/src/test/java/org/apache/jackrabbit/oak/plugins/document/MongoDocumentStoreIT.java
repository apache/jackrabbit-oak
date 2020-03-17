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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

import com.google.common.collect.Maps;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;

/**
 * Tests {@code MongoDocumentStore} with concurrent updates.
 */
public class MongoDocumentStoreIT extends AbstractMongoConnectionTest {

    private static final int NUM_THREADS = 3;
    private static final int UPDATES_PER_THREAD = 10;

    @Test
    public void concurrent() throws Exception {
        final long time = System.currentTimeMillis();
        mk.commit("/", "+\"test\":{}", null, null);
        final String id = Utils.getIdFromPath("/test");
        final DocumentStore docStore = mk.getDocumentStore();
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < NUM_THREADS; i++) {
            final int tId = i;
            threads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    Revision r = new Revision(time, tId, 0);
                    for (int i = 0; i < UPDATES_PER_THREAD; i++) {
                        UpdateOp update = new UpdateOp(id, false);
                        update.setMapEntry("prop", r, String.valueOf(i));
                        docStore.createOrUpdate(NODES, update);
                    }
                }
            }));
        }
        final List<Exception> exceptions = new ArrayList<Exception>();
        final AtomicBoolean running = new AtomicBoolean(true);
        Thread reader = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Map<Revision, Integer> previous = Maps.newHashMap();
                    while (running.get()) {
                        NodeDocument doc = docStore.find(NODES, id);
                        if (doc == null) {
                            throw new Exception("document is null");
                        }
                        Map<Revision, String> values = doc.getValueMap("prop");
                        for (Map.Entry<Revision, String> entry : values.entrySet()) {
                            Revision r = entry.getKey();
                            Integer previousValue = previous.get(r);
                            Integer currentValue = Integer.parseInt(entry.getValue());
                            if (previousValue != null &&
                                    previousValue > currentValue) {
                                throw new Exception("inconsistent read for " +
                                        r + ". previous value: " + previousValue +
                                        ", now: " + entry.getValue());
                            }
                            // remember for next round
                            previous.put(r, currentValue);
                        }
                        Thread.yield();
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        });
        reader.start();
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        running.set(false);
        reader.join();
        for (Exception e : exceptions) {
            throw e;
        }
        NodeDocument doc = docStore.find(NODES, id);
        assertNotNull(doc);
        Map<Revision, String> values = doc.getLocalMap("prop");
        assertNotNull(values);
        for (Map.Entry<Revision, String> entry : values.entrySet()) {
            assertEquals(String.valueOf(UPDATES_PER_THREAD - 1), entry.getValue());
        }
    }

    @Test
    public void concurrentLoop() throws Exception {
        // run for 5 seconds
        long end = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (System.currentTimeMillis() < end) {
            concurrent();
            tearDownConnection();
            setUpConnection();
        }
    }

    @Test
    public void negativeCache() throws Exception {
        String id = Utils.getIdFromPath("/test");
        DocumentStore docStore = mk.getDocumentStore();
        assertNull(docStore.find(NODES, id));
        mk.commit("/", "+\"test\":{}", null, null);
        assertNotNull(docStore.find(NODES, id));
    }

    @Test
    public void modCount() throws Exception {
        DocumentStore docStore = mk.getDocumentStore();
        String head = mk.commit("/", "+\"test\":{}", null, null);
        mk.commit("/test", "^\"prop\":\"v1\"", head, null);
        // make sure _lastRev is persisted and _modCount updated accordingly
        mk.runBackgroundOperations();

        NodeDocument doc = docStore.find(NODES, Utils.getIdFromPath("/test"));
        assertNotNull(doc);
        Long mc1 = doc.getModCount();
        assertNotNull(mc1);
        try {
            mk.commit("/test", "^\"prop\":\"v2\"", head, null);
            fail();
        } catch (DocumentStoreException e) {
            // expected
        }
        doc = docStore.find(NODES, Utils.getIdFromPath("/test"));
        assertNotNull(doc);
        Long mc2 = doc.getModCount();
        assertNotNull(mc2);
        assertTrue(mc2 > mc1);
    }

    // OAK-3556
    @Test
    public void create() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        String id = Utils.getIdFromPath("/test");
        UpdateOp updateOp = new UpdateOp(id, true);
        Revision r1 = Revision.newRevision(1);
        updateOp.setMapEntry("p", r1, "a");
        Revision r2 = Revision.newRevision(1);
        updateOp.setMapEntry("p", r2, "b");
        Revision r3 = Revision.newRevision(1);
        updateOp.setMapEntry("p", r3, "c");
        assertTrue(store.create(NODES, Collections.singletonList(updateOp)));

        // maxCacheAge=0 forces loading from storage
        NodeDocument doc = store.find(NODES, id, 0);
        assertNotNull(doc);
        Map<Revision, String> valueMap = doc.getValueMap("p");
        assertEquals(3, valueMap.size());
    }

    // OAK-3582
    @Test
    public void createWithNull() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        String id = Utils.getIdFromPath("/test");
        UpdateOp updateOp = new UpdateOp(id, true);
        Revision r1 = Revision.newRevision(1);
        updateOp.setMapEntry("p", r1, "a");
        Revision r2 = Revision.newRevision(1);
        updateOp.setMapEntry("p", r2, null);
        Revision r3 = Revision.newRevision(1);
        updateOp.setMapEntry("p", r3, "c");
        assertTrue(store.create(NODES, Collections.singletonList(updateOp)));

        // maxCacheAge=0 forces loading from storage
        NodeDocument doc = store.find(NODES, id, 0);
        assertNotNull(doc);
        Map<Revision, String> valueMap = doc.getValueMap("p");
        assertEquals(3, valueMap.size());
    }
}

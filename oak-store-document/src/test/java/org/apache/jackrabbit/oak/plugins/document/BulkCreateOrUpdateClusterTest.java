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

import static java.util.Collections.shuffle;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.collect.Lists;

import ch.qos.logback.classic.Level;

public class BulkCreateOrUpdateClusterTest extends AbstractMultiDocumentStoreTest {

    final Logger logger = LoggerFactory.getLogger(getClass());

    public BulkCreateOrUpdateClusterTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    /**
     * Run multiple batch updates concurrently. Each thread modifies only its own documents.
     */
    @Test
    public void testConcurrentNoConflict() throws InterruptedException {
        int amountPerThread = 100;
        int threadCount = 10;
        int amount = amountPerThread * threadCount;

        List<UpdateOp> updates = new ArrayList<UpdateOp>(amount);
        // create even items
        for (int i = 0; i < amount; i += 2) {
            String id = this.getClass().getName() + ".testConcurrentNoConflict" + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set("prop", 100);
            updates.add(up);
        }
        ds1.create(Collection.NODES, updates);

        final Set<Exception> exceptions = new HashSet<Exception>();
        List<Thread> threads = new ArrayList<Thread>();
        final Map<String, NodeDocument> oldDocs = new ConcurrentHashMap<String, NodeDocument>();
        for (int i = 0; i < threadCount; i++) {
            final DocumentStore selectedDs = i % 2 == 0 ? this.ds1 : this.ds2;
            final List<UpdateOp> threadUpdates = new ArrayList<UpdateOp>(amountPerThread);
            for (int j = 0; j < amountPerThread; j++) {
                String id = this.getClass().getName() + ".testConcurrentNoConflict" + (j + i * amountPerThread);
                UpdateOp up = new UpdateOp(id, true);
                up.set("prop", 200 + i + j);
                threadUpdates.add(up);
                removeMe.add(id);
            }
            shuffle(threadUpdates);
            threads.add(new Thread() {
                public void run() {
                    try {
                        for (NodeDocument d : selectedDs.createOrUpdate(Collection.NODES, threadUpdates)) {
                            if (d == null) {
                                continue;
                            }
                            oldDocs.put(d.getId(), d);
                        }
                    }
                    catch (Exception ex) {
                        exceptions.add(ex);
                    }
                }
            });
        }

        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
            if (t.isAlive()) {
                fail("Thread hasn't finished in 10s");
            }
        }

        if (!exceptions.isEmpty()) {
            String msg = exceptions.size() + " out of " + threadCount +  " failed with exceptions, the first being: " + exceptions.iterator().next();
            fail(msg);
        }

        for (int i = 0; i < amount; i++) {
            String id = this.getClass().getName() + ".testConcurrentNoConflict" + i;

            NodeDocument oldDoc = oldDocs.get(id);
            NodeDocument newDoc; //avoid cache issues
            if (i % 2 == 1) {
                assertNull("The returned value should be null for created doc", oldDoc);
                newDoc = ds1.find(Collection.NODES, id);
            } else {
                assertNotNull("The returned doc shouldn't be null for updated doc", oldDoc);
                assertEquals("The old value is not correct", 100l, oldDoc.get("prop"));
                newDoc = ds2.find(Collection.NODES, id);
            }
            assertNotEquals("The document hasn't been updated", 100l, newDoc.get("prop"));
        }
    }

    /**
     * Run multiple batch updates concurrently. Each thread modifies the same set of documents.
     */
    @Test
    public void testConcurrentWithConflict() throws InterruptedException {
        assumeTrue(this.dsf != DocumentStoreFixture.RDB_DERBY);

        int threadCount = 10;
        int amount = 500;

        List<UpdateOp> updates = new ArrayList<UpdateOp>(amount);
        // create even items
        for (int i = 0; i < amount; i += 2) {
            String id = this.getClass().getName() + ".testConcurrentNoConflict" + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set("prop", 100);
            updates.add(up);
            removeMe.add(id);
        }
        ds1.create(Collection.NODES, updates);

        final Set<Exception> exceptions = new HashSet<Exception>();
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < threadCount; i++) {
            final DocumentStore selectedDs = i % 2 == 0 ? this.ds1 : this.ds2;
            final List<UpdateOp> threadUpdates = new ArrayList<UpdateOp>(amount);
            for (int j = 0; j < amount; j++) {
                String id = this.getClass().getName() + ".testConcurrentWithConflict" + j;
                UpdateOp up = new UpdateOp(id, true);
                up.set("prop", 200 + i * amount + j);
                threadUpdates.add(up);
                removeMe.add(id);
            }
            shuffle(threadUpdates);
            threads.add(new Thread() {
                public void run() {
                    try {
                        selectedDs.createOrUpdate(Collection.NODES, threadUpdates);
                    }
                    catch (Exception ex) {
                        exceptions.add(ex);
                    }
                }
            });
        }

        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            long time = System.currentTimeMillis();
            t.join(75000);
            time = System.currentTimeMillis() - time;
            logger.info("join took " + time + " ms");
            if (t.isAlive()) {
                fail("Thread hasn't finished in 10s");
            }
        }

        if (!exceptions.isEmpty()) {
            String msg = exceptions.size() + " out of " + threadCount +  " failed with exceptions, the first being: " + exceptions.iterator().next();
            fail(msg);
        }

        for (int i = 0; i < amount; i++) {
            String id = this.getClass().getName() + ".testConcurrentWithConflict" + i;

            NodeDocument newDoc = ds1.find(Collection.NODES, id);
            assertNotNull("The document hasn't been inserted", newDoc);
            assertNotEquals("The document hasn't been updated", 100l, newDoc.get("prop"));

            newDoc = ds2.find(Collection.NODES, id);
            assertNotNull("The document hasn't been inserted", newDoc);
            assertNotEquals("The document hasn't been updated", 100l, newDoc.get("prop"));
        }
    }

    @Test
    public void testSimpleConflictHandling() {
        LogCustomizer logCustomizer = LogCustomizer.forLogger(RDBDocumentStore.class.getName()).enable(Level.DEBUG)
                .contains("invalidating cache and retrying").create();
        logCustomizer.starting();

        try {
            String id1 = this.getClass().getName() + ".testSimpleConflictHandling1";
            String id2 = this.getClass().getName() + ".testSimpleConflictHandling2";
            String id3 = this.getClass().getName() + ".testSimpleConflictHandling3";

            removeMe.add(id1);
            removeMe.add(id2);
            removeMe.add(id3);

            {
                UpdateOp op1a = new UpdateOp(id1, true);
                op1a.set("foo", 1);
                UpdateOp op2a = new UpdateOp(id2, true);
                op2a.set("foo", 1);
                UpdateOp op3a = new UpdateOp(id3, true);
                op3a.set("foo", 1);

                List<NodeDocument> resulta = ds1.createOrUpdate(Collection.NODES, List.of(op1a, op2a, op3a));
                assertEquals(3, resulta.size());
            }

            {
                UpdateOp op2b = new UpdateOp(id2, false);
                op2b.increment("foo", 1);
                NodeDocument prev2 = ds2.createOrUpdate(Collection.NODES, op2b);
                assertNotNull(prev2);
                assertEquals(1L, ((Long)prev2.get("foo")).longValue());
            }

            {
                UpdateOp op1c = new UpdateOp(id1, true);
                op1c.increment("foo", 1);
                UpdateOp op2c = new UpdateOp(id2, true);
                op2c.increment("foo", 1);
                UpdateOp op3c = new UpdateOp(id3, true);
                op3c.increment("foo", 1);

                List<NodeDocument> resultc = ds1.createOrUpdate(Collection.NODES, List.of(op1c, op2c, op3c));
                assertEquals(3, resultc.size());
                for (NodeDocument d : resultc) {
                    Long fooval = (Long) d.get("foo");
                    assertEquals((d.getId().equals(id2)) ? 2L : 1L, fooval.longValue());
                }
            }

            if (ds1 instanceof RDBDocumentStore) {
                // for RDB, verify that the cache invalidation was reached
                assertEquals(1, logCustomizer.getLogs().size());
            }
        }
        finally {
            logCustomizer.finished();
        }
    }
}

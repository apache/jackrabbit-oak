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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BulkCreateOrUpdateTest extends AbstractDocumentStoreTest {

    public BulkCreateOrUpdateTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @Before
    public void before() {
        DataSource dataSource = dsf.getRDBDataSource();
        if (dataSource instanceof RDBDataSourceWrapper) {
            // test drivers that do not return precise batch results
            ((RDBDataSourceWrapper)dataSource).setBatchResultPrecise(false);
        }
    }

    @After
    public void after() {
        DataSource dataSource = dsf.getRDBDataSource();
        if (dataSource instanceof RDBDataSourceWrapper) {
            ((RDBDataSourceWrapper)dataSource).setBatchResultPrecise(true);
        }
    }

    /**
     * This tests create multiple items using createOrUpdate() method. The
     * return value should be a list of null values.
     */
    @Test
    public void testCreateMultiple() {
        final int amount = 100;

        List<UpdateOp> updates = new ArrayList<UpdateOp>(amount);

        for (int i = 0; i < amount; i++) {
            String id = this.getClass().getName() + ".testCreateMultiple" + i;
            UpdateOp up = new UpdateOp(id, true);
            updates.add(up);
            removeMe.add(id);
        }

        List<NodeDocument> docs = ds.createOrUpdate(Collection.NODES, updates);
        assertEquals(amount, docs.size());
        for (int i = 0; i < amount; i++) {
            assertNull("There shouldn't be a value for created doc", docs.get(i));
            assertNotNull("The node hasn't been created", ds.find(Collection.NODES, updates.get(i).getId()));
        }
    }

    /**
     * This method updates multiple items using createOrUpdate() method. The
     * return value should be a list of items before the update.
     */
    @Test
    public void testUpdateMultiple() {
        final int amount = 100;
        List<UpdateOp> updates = new ArrayList<UpdateOp>(amount);

        for (int i = 0; i < amount; i++) {
            String id = this.getClass().getName() + ".testUpdateMultiple" + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set("prop", 100);
            updates.add(up);
            removeMe.add(id);
        }

        ds.create(Collection.NODES, updates);

        for (int i = 0; i < amount; i++) {
            UpdateOp up = updates.get(i).copy();
            up.set("prop", 200);
            updates.set(i, up);
        }

        List<NodeDocument> docs = ds.createOrUpdate(Collection.NODES, updates);
        assertEquals(amount, docs.size());
        for (int i = 0; i < amount; i++) {
            NodeDocument oldDoc = docs.get(i);
            String id = oldDoc.getId();
            NodeDocument newDoc = ds.find(Collection.NODES, id);
            assertEquals("The result list order is incorrect", updates.get(i).getId(), id);
            assertEquals("The old value is not correct", 100l, oldDoc.get("prop"));
            assertEquals("The document hasn't been updated", 200l, newDoc.get("prop"));
        }
    }

    /**
     * This method creates or updates multiple items using createOrUpdate()
     * method. New items have odd indexes and updates items have even indexes.
     * The return value should be a list of old documents (for the updates) or
     * nulls (for the inserts).
     */
    @Test
    public void testCreateOrUpdateMultiple() {
        int amount = 100;
        List<UpdateOp> updates = new ArrayList<UpdateOp>(amount);

        // create even items
        for (int i = 0; i < amount; i += 2) {
            String id = this.getClass().getName() + ".testCreateOrUpdateMultiple" + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set("prop", 100);
            updates.add(up);
            removeMe.add(id);
        }
        ds.create(Collection.NODES, updates);
        updates.clear();

        // createOrUpdate all items
        for (int i = 0; i < amount; i++) {
            String id = this.getClass().getName() + ".testCreateOrUpdateMultiple" + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set("prop", 200);
            updates.add(up);
            removeMe.add(id);
        }
        List<NodeDocument> docs = ds.createOrUpdate(Collection.NODES, updates);

        assertEquals(amount, docs.size());
        for (int i = 0; i < amount; i++) {
            String id = this.getClass().getName() + ".testCreateOrUpdateMultiple" + i;

            NodeDocument oldDoc = docs.get(i);
            NodeDocument newDoc = ds.find(Collection.NODES, id);
            if (i % 2 == 1) {
                assertNull("The returned value should be null for created doc", oldDoc);
            } else {
                assertNotNull("The returned doc shouldn't be null for updated doc", oldDoc);
                assertEquals("The old value is not correct", 100l, oldDoc.get("prop"));
                assertEquals("The result list order is incorrect", updates.get(i).getId(), oldDoc.getId());
            }
            assertEquals("The document hasn't been updated", 200l, newDoc.get("prop"));
        }
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
        ds.create(Collection.NODES, updates);

        List<Thread> threads = new ArrayList<Thread>();
        final Map<String, NodeDocument> oldDocs = new ConcurrentHashMap<String, NodeDocument>();
        for (int i = 0; i < threadCount; i++) {
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
                    for (NodeDocument d : ds.createOrUpdate(Collection.NODES, threadUpdates)) {
                        if (d == null) {
                            continue;
                        }
                        oldDocs.put(d.getId(), d);
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

        for (int i = 0; i < amount; i++) {
            String id = this.getClass().getName() + ".testConcurrentNoConflict" + i;

            NodeDocument oldDoc = oldDocs.get(id);
            NodeDocument newDoc = ds.find(Collection.NODES, id);
            if (i % 2 == 1) {
                assertNull("The returned value should be null for created doc", oldDoc);
            } else {
                assertNotNull("The returned doc shouldn't be null for updated doc", oldDoc);
                assertEquals("The old value is not correct", 100l, oldDoc.get("prop"));
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
        ds.create(Collection.NODES, updates);

        final Set<Exception> exceptions = new HashSet<Exception>();
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < threadCount; i++) {
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
                        ds.createOrUpdate(Collection.NODES, threadUpdates);
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
            t.join(10000);
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

            NodeDocument newDoc = ds.find(Collection.NODES, id);
            assertNotNull("The document hasn't been inserted", newDoc);
            assertNotEquals("The document hasn't been updated", 100l, newDoc.get("prop"));
        }
    }

    /**
     * This method adds a few updateOperations modifying the same document.
     */
    @Test
    public void testUpdateSameDocument() {
        final int amount = 5;
        List<UpdateOp> updates = new ArrayList<UpdateOp>(amount);
        String id = this.getClass().getName() + ".testUpdateSameDocument";
        removeMe.add(id);

        for (int i = 0; i < amount; i++) {
            UpdateOp up = new UpdateOp(id, true);
            up.set("update_id", i);
            up.set("prop_" + i, 100);
            updates.add(up);
        }

        List<NodeDocument> docs = ds.createOrUpdate(Collection.NODES, updates);
        assertEquals(amount, docs.size());
        assertNull("The old value should be null for the first update", docs.get(0));
        Long prevModCount = null;
        for (int i = 1; i < amount; i++) {
            Long modCount = docs.get(i).getModCount();
            if (prevModCount != null) {
                assertNotNull(modCount);
                assertTrue("_modCount, when present, must be increasing, but changed from " + prevModCount + " to " + modCount,
                        prevModCount.longValue() < modCount.longValue());
            }
            prevModCount = modCount;
            assertEquals("The old value is not correct", Long.valueOf(i - 1), docs.get(i).get("update_id"));
        }

        NodeDocument newDoc = ds.find(Collection.NODES, id);
        if (newDoc.getModCount() != null) {
            assertEquals("The final mod count is not correct", Long.valueOf(5), newDoc.getModCount());
        }
        for (int i = 0; i < amount; i++) {
            assertEquals("The value is not correct", 100l, newDoc.get("prop_" + i));
        }
    }
}

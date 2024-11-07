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

import static java.util.Collections.synchronizedList;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStoreJDBC;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;
import org.slf4j.event.Level;

import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.Maps;

public class MultiDocumentStoreTest extends AbstractMultiDocumentStoreTest {

    public MultiDocumentStoreTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @Test
    public void testInterleavedBatchUpdate() {
        int amount = 10;
        int halfAmount = amount / 2;
        String baseId = this.getClass().getName() + ".testInterleavedBatchUpdate";

        // remove if present
        for (int i = 0; i < amount; i++) {
            String id = baseId + "-" + i;
            NodeDocument nd = super.ds1.find(Collection.NODES, id);
            if (nd != null) {
                super.ds1.remove(Collection.NODES, id);
            }
            removeMe.add(id);
        }

        {
            // create half of the entries in ds1
            List<UpdateOp> ops = new ArrayList<UpdateOp>();
            for (int i = 0; i < halfAmount; i++) {
                String id = baseId + "-" + i;
                UpdateOp up = new UpdateOp(id, true);
                up.set("_createdby", "ds1");
                ops.add(up);
            }
            List<NodeDocument> result = super.ds1.createOrUpdate(Collection.NODES, ops);
            assertEquals(halfAmount, result.size());
            for (NodeDocument doc : result) {
                assertNull(doc);
            }
        }

        {
            // create all of the entries in ds2
            List<UpdateOp> ops = new ArrayList<UpdateOp>();
            for (int i = 0; i < amount; i++) {
                String id = baseId + "-" + i;
                UpdateOp up = new UpdateOp(id, true);
                up.set("_createdby", "ds2");
                ops.add(up);
            }
            List<NodeDocument> result = super.ds2.createOrUpdate(Collection.NODES, ops);
            assertEquals(amount, result.size());
            for (NodeDocument doc : result) {
                // documents are either new or have been created by ds1
                if (doc != null) {
                    assertEquals("ds1", doc.get("_createdby"));
                }
            }
        }

        // final check: does DS1 see all documents including the changes made by DS2?
        for (int i = 0; i < amount; i++) {
            String id = baseId + "-" + i;
            NodeDocument doc = super.ds1.find(Collection.NODES, id, 0);
            assertNotNull(doc);
            assertEquals("ds2", doc.get("_createdby"));
        }
    }

    @Test
    public void concurrentBatchUpdate() throws Exception {
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch go = new CountDownLatch(1);
        final List<String> ids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ids.add(Utils.getIdFromPath("/node-" + i));
        }
        removeMe.addAll(ids);

        // make sure not present before test run
        ds1.remove(Collection.NODES, ids);

        final List<Exception> exceptions = synchronizedList(new ArrayList<Exception>());
        final Map<String, NodeDocument> result1 = Maps.newHashMap();
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    List<UpdateOp> ops = new ArrayList<>();
                    for (String id : ids) {
                        UpdateOp op = new UpdateOp(id, true);
                        op.set("_t1", "value");
                        ops.add(op);
                    }
                    Collections.shuffle(ops);
                    ready.countDown();
                    go.await();
                    List<NodeDocument> docs = ds1.createOrUpdate(Collection.NODES, ops);
                    for (int i = 0; i < ops.size(); i++) {
                        UpdateOp op = ops.get(i);
                        result1.put(op.getId(), docs.get(i));
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        }, "t1");
        final Map<String, NodeDocument> result2 = Maps.newHashMap();
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    List<UpdateOp> ops = new ArrayList<>();
                    for (String id : ids) {
                        UpdateOp op = new UpdateOp(id, true);
                        op.set("_t2", "value");
                        ops.add(op);
                    }
                    Collections.shuffle(ops);
                    ready.countDown();
                    go.await();
                    List<NodeDocument> docs = ds2.createOrUpdate(Collection.NODES, ops);
                    for (int i = 0; i < ops.size(); i++) {
                        UpdateOp op = ops.get(i);
                        result2.put(op.getId(), docs.get(i));
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        }, "t2");
        t1.start();
        t2.start();
        ready.await();
        go.countDown();
        t1.join();
        t2.join();
        for (Exception e : exceptions) {
            fail(e.toString());
        }
        for (String id : ids) {
            NodeDocument d1 = result1.get(id);
            NodeDocument d2 = result2.get(id);
            if (d1 != null) {
                if (d2 != null) {
                    fail("found " + id + " in both result sets, modcounts are: " + d1.getModCount() + "/" + d2.getModCount());
                }
            } else {
                assertNotNull("id " + id + " is in neither result set", d2);
            }
        }
    }

    @Test
    public void batchUpdateCachedDocument() throws Exception {
        String id = Utils.getIdFromPath("/foo");
        removeMe.add(id);

        UpdateOp op = new UpdateOp(id, true);
        op.set("_ds1", 1);
        assertNull(ds1.createOrUpdate(Collection.NODES, op));

        // force ds2 to populate the cache with doc
        assertNotNull(ds2.find(Collection.NODES, id));

        // modify doc via ds1
        op = new UpdateOp(id, false);
        op.set("_ds1", 2);
        assertNotNull(ds1.createOrUpdate(Collection.NODES, op));

        // modify doc via ds2 with batch createOrUpdate
        op = new UpdateOp(id, false);
        op.set("_ds2", 1);
        List<UpdateOp> ops = new ArrayList<>();
        ops.add(op);
        for (int i = 0; i < 10; i++) {
            // add more ops to make sure a batch
            // update call is triggered
            String docId = Utils.getIdFromPath("/node-" + i);
            UpdateOp update = new UpdateOp(docId, true);
            update.set("_ds2", 1);
            removeMe.add(docId);
            ops.add(update);
        }
        List<NodeDocument> old = ds2.createOrUpdate(Collection.NODES, ops);
        assertEquals(11, old.size());
        assertNotNull(old.get(0));
        assertEquals(2L, old.get(0).get("_ds1"));

        NodeDocument foo = ds2.find(Collection.NODES, id);
        assertNotNull(foo);
        assertEquals(2L, foo.get("_ds1"));
        assertEquals(1L, foo.get("_ds2"));
    }

    @Test
    public void testUpdateRemovedNode() {
        String id = Utils.getIdFromPath("testUpdateRemovedNode");
        removeMe.add(id);

        UpdateOp op = new UpdateOp(id, true);
        assertNull(ds1.createOrUpdate(Collection.NODES, op));
        // get it into the cache
        NodeDocument n = ds1.find(Collection.NODES, id);
        assertNotNull(n);

        // delete it through the other instance
        ds2.remove(Collection.NODES, Collections.singletonList(id));

        // assume still in the cache?
        NodeDocument n2 = ds1.find(Collection.NODES, id);
        assertNotNull(n2);

        // create-or-update should at least work after one retry
        // see OAK-7953 - note that the retry shouldn't be needed; see OAK-7745
        // for more information
        try {
            UpdateOp op2 = new UpdateOp(id, true);
            assertNull(ds1.createOrUpdate(Collection.NODES, op2));
        } catch (DocumentStoreException ex) {
            UpdateOp op2 = new UpdateOp(id, true);
            assertNull(ds1.createOrUpdate(Collection.NODES, op2));
        }
    }

    @Test
    public void testUpdateOrCreateDeletedDocument() {

        String id = Utils.getIdFromPath("/foo");
        removeMe.add(id);

        UpdateOp op1 = new UpdateOp(id, true);
        assertNull(ds1.createOrUpdate(Collection.NODES, op1));

        NodeDocument d = ds1.find(Collection.NODES, id);
        assertNotNull(d);
        Long mc = d.getModCount();
 
        ds2.remove(Collection.NODES, id);

        UpdateOp op2 = new UpdateOp(id, true);
        NodeDocument prev = ds1.createOrUpdate(Collection.NODES, op2);

        assertNull(prev);

        // make sure created
        NodeDocument created = ds1.find(Collection.NODES, id);
        assertNotNull(created);
        assertEquals(mc, created.getModCount());
    }

    @Test
    public void testUpdateNoCreateDeletedDocument() {
        String id = Utils.getIdFromPath("/foo");
        removeMe.add(id);

        UpdateOp op1 = new UpdateOp(id, true);
        assertNull(ds1.createOrUpdate(Collection.NODES, op1));

        NodeDocument d = ds1.find(Collection.NODES, id);
        assertNotNull(d);

        ds2.remove(Collection.NODES, id);

        UpdateOp op2 = new UpdateOp(id, false);
        NodeDocument prev = ds1.createOrUpdate(Collection.NODES, op2);

        assertNull(prev);

        // make sure not created
        assertNull(ds1.find(Collection.NODES, id));
    }

    @Test
    public void batchUpdateNoCreateDeletedDocument() {
        batchUpdateNoCreateDeletedDocument(2);
    }

    @Test
    public void batchUpdateNoCreateDeletedDocumentMany() {
        batchUpdateNoCreateDeletedDocument(10);
    }

    private void batchUpdateNoCreateDeletedDocument(int numUpdates) {
        String id1 = this.getClass().getName() + ".batchUpdateNoCreateDeletedDocument";
        List<String> ids = new ArrayList<>();
        for (int i = 1; i < numUpdates; i++) {
            ids.add(id1 + "b" + i);
        }

        removeMe.add(id1);
        removeMe.addAll(ids);

        List<String> allIds = new ArrayList<>(ids);
        allIds.add(id1);
        // create docs
        for (String id : allIds) {
            UpdateOp up = new UpdateOp(id, true);
            assertNull(ds1.createOrUpdate(Collection.NODES, up));
            NodeDocument doc = ds1.find(Collection.NODES, id);
            assertNotNull(doc);
        }

        // remove id1 on ds2
        ds2.remove(Collection.NODES, id1);

        // bulk update
        List<UpdateOp> ops = new ArrayList<>();
        ops.add(new UpdateOp(id1, false));
        for (String id : ids) {
            ops.add(new UpdateOp(id, false));
        }

        List<NodeDocument> result = ds1.createOrUpdate(Collection.NODES, ops);
        assertEquals(numUpdates, result.size());

        // id1 result should be reported as null and not be created
        assertNull(result.get(0));
        assertNull(ds1.find(Collection.NODES, id1));

        // for other ids result should be reported with previous doc
        for (int i = 1; i < numUpdates; i++) {
            NodeDocument prev = result.get(i);
            assertNotNull(prev);
            String id = ids.get(i - 1);
            assertEquals(id, prev.getId());

            NodeDocument updated = ds1.find(Collection.NODES, id);
            assertNotNull(updated);
            if (prev.getModCount() != null) {
                assertNotEquals(updated.getModCount(), prev.getModCount());
            }
        }
    }

    @Test
    public void testTraceLoggingForBulkUpdates() {
        if (ds instanceof RDBDocumentStore) {
            int count = 10;

            List<UpdateOp> ops = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                UpdateOp op = new UpdateOp(getIdFromPath("/bulktracelog-" + i), true);
                ops.add(op);
                removeMe.add(op.getId());
            }
            ds.createOrUpdate(NODES, ops);

            LogCustomizer logCustomizerJDBC = LogCustomizer.forLogger(RDBDocumentStoreJDBC.class.getName()).enable(Level.TRACE)
                    .matchesRegex("update: batch result.*").create();
            logCustomizerJDBC.starting();
            LogCustomizer logCustomizer = LogCustomizer.forLogger(RDBDocumentStore.class.getName()).enable(Level.TRACE)
                    .matchesRegex("bulkUpdate: success.*").create();
            logCustomizer.starting();

            try {
                ops.clear();

                // modify first entry through secondary store
                String modifiedRow = getIdFromPath("/bulktracelog-" + 0);
                UpdateOp op2 = new UpdateOp(modifiedRow, false);
                op2.set("foo", "bar");
                ds2.createOrUpdate(NODES, op2);

                // delete second entry through secondary store
                String deletedRow = getIdFromPath("/bulktracelog-" + 1);
                ds2.remove(NODES, deletedRow);

                for (int i = 0; i < count; i++) {
                    UpdateOp op = new UpdateOp(getIdFromPath("/bulktracelog-" + i), false);
                    op.set("foo", "qux");
                    ops.add(op);
                    removeMe.add(op.getId());
                }
                ds.createOrUpdate(NODES, ops);

                assertTrue(logCustomizer.getLogs().size() == 1);
                assertTrue(logCustomizer.getLogs().get(0).contains("failure for [" + modifiedRow + ", " + deletedRow + "]"));
                // System.out.println(logCustomizer.getLogs());
                assertTrue(logCustomizerJDBC.getLogs().size() == 1);
                assertTrue(logCustomizerJDBC.getLogs().get(0).contains("0 (for " + modifiedRow + " (1)"));
                assertTrue(logCustomizerJDBC.getLogs().get(0).contains("0 (for " + deletedRow + " (1)"));
                // System.out.println(logCustomizerJDBC.getLogs());
            } finally {
                logCustomizer.finished();
                logCustomizerJDBC.finished();
            }
        }
    }
}

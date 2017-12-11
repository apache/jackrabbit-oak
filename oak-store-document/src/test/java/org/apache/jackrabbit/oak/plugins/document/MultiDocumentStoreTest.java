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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

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
        final List<String> ids = Lists.newArrayList();
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
                    List<UpdateOp> ops = Lists.newArrayList();
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
                    List<UpdateOp> ops = Lists.newArrayList();
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
        List<UpdateOp> ops = Lists.newArrayList();
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

    private static long letTimeElapse() {
        long ts = System.currentTimeMillis();
        while (System.currentTimeMillis() == ts) {
            // busy wait
        }
        return System.currentTimeMillis();
    }
}

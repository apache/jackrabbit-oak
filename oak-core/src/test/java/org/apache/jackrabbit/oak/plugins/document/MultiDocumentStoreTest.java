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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiDocumentStoreTest extends AbstractMultiDocumentStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(MultiDocumentStoreTest.class);

    public MultiDocumentStoreTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @Test
    public void testInterleavedUpdate() {
        String id = this.getClass().getName() + ".testInterleavedUpdate";

        // remove if present
        NodeDocument nd = super.ds1.find(Collection.NODES, id);
        if (nd != null) {
            super.ds1.remove(Collection.NODES, id);
        }

        UpdateOp up = new UpdateOp(id, true);
        up.set("_foo", 0);
        assertTrue(super.ds1.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);

        long increments = 10;

        for (int i = 0; i < increments; i++) {
            up = new UpdateOp(id, true);
            up.increment("_foo", 1);
            if (i % 2 == 0) {
                super.ds1.update(Collection.NODES, Collections.singletonList(id), up);
            }
            else {
                super.ds2.update(Collection.NODES, Collections.singletonList(id), up);
            }
        }

        // read uncached
        nd = super.ds1.find(Collection.NODES, id, 0);
        assertEquals("_foo should have been incremented 10 times", increments, nd.get("_foo"));
    }

    @Test
    public void testInvalidateCache() {
        // use a "proper" ID because otherwise Mongo's cache invalidation will fail
        // see OAK-2588
        String id = "1:/" + this.getClass().getName() + ".testInvalidateCache";

        // remove if present
        NodeDocument nd = super.ds1.find(Collection.NODES, id);
        if (nd != null) {
            super.ds1.remove(Collection.NODES, id);
        }

        UpdateOp up = new UpdateOp(id, true);
        up.set("_foo", "bar");
        assertTrue(super.ds1.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);

        // fill both caches
        NodeDocument nd1 = super.ds1.find(Collection.NODES, id);
        NodeDocument nd2 = super.ds2.find(Collection.NODES, id);
        assertNotNull(nd1);
        assertNotNull(nd2);
        long firstVersion = nd1.getModCount();
        assertEquals(firstVersion, nd2.getModCount().longValue());

        // letTimeElapse();

        // update through ds1
        UpdateOp upds1 = new UpdateOp(id, true);
        upds1.set("foo", "qux");
        super.ds1.update(Collection.NODES, Collections.singletonList(id), upds1);
        nd1 = super.ds1.find(Collection.NODES, id);
        assertEquals("modcount should have changed in ds1", firstVersion + 1, nd1.getModCount().longValue());

        // check cached version in ds2
        nd2 = super.ds2.find(Collection.NODES, id);
        assertEquals("ds2 should still be on first version", firstVersion, nd2.getModCount().longValue());

        // check uncached version in ds2
        nd2 = super.ds2.find(Collection.NODES, id, 0);
        assertEquals("ds2 should now see the second version", firstVersion + 1, nd2.getModCount().longValue());

        // check cached version in ds2 (was the cache refreshed?)
        NodeDocument nd2b = super.ds2.find(Collection.NODES, id);
        assertEquals("ds2 should now see the second version", firstVersion + 1, nd2b.getModCount().longValue());

        // update through ds2
        UpdateOp upds2 = new UpdateOp(id, true);
        upds2.set("foo", "blub");
        super.ds2.update(Collection.NODES, Collections.singletonList(id), upds1);
        nd2 = super.ds2.find(Collection.NODES, id);
        assertEquals("modcount should have incremented again", firstVersion + 2, nd2.getModCount().longValue());

        long ds1checktime = nd1.getLastCheckTime();
        letTimeElapse();

        // try the invalidation
        ds1.invalidateCache();

        // ds1 should see the same version even when doing a cached read
        nd1 = super.ds1.find(Collection.NODES, id);
        assertEquals("modcount should have incremented again", firstVersion + 2, nd1.getModCount().longValue());
        assertTrue(nd1.getLastCheckTime() > ds1checktime);
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

    @Test
    public void testChangeVisibility() {
        String id = this.getClass().getName() + ".testChangeVisibility";

        super.ds1.remove(Collection.NODES, id);

        UpdateOp up = new UpdateOp(id, true);
        up.set("_foo", 0);
        up.set("_bar", 0);
        assertTrue(super.ds1.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);
        NodeDocument orig = super.ds1.find(Collection.NODES, id);

        // only run test if DS supports modcount
        if (orig.getModCount() != null) {
            long origMc = orig.getModCount();

            UpdateOp up2 = new UpdateOp(id, false);
            up2.increment("_foo", 1L);
            super.ds2.update(Collection.NODES, Collections.singletonList(id), up2);
            NodeDocument ds2doc = super.ds2.find(Collection.NODES, id);
            long ds2Mc = ds2doc.getModCount();
            assertTrue("_modCount needs to be > " + origMc + " but was " + ds2Mc, ds2Mc > origMc);

            UpdateOp up1 = new UpdateOp(id, false);
            up1.increment("_bar", 1L);
            super.ds1.update(Collection.NODES, Collections.singletonList(id), up1);

            NodeDocument ds1doc = super.ds1.find(Collection.NODES, id);
            long ds1Mc = ds1doc.getModCount();
            assertTrue("_modCount needs to be > " + ds2Mc + " but was " + ds1Mc, ds1Mc > ds2Mc);
        }
    }

    @Test
    public void concurrentUpdate() throws Exception {
        String id = Utils.getIdFromPath("/foo");
        ds1.remove(Collection.NODES, id);
        ds2.invalidateCache();
        removeMe.add(id);
        UpdateOp op = new UpdateOp(id, true);
        ds1.create(Collection.NODES, Collections.singletonList(op));

        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());
        List<Thread> threads = Lists.newArrayList();
        threads.add(new Thread(new Updater(ds1, id, exceptions)));
        threads.add(new Thread(new Updater(ds2, id, exceptions)));
        Reader r = new Reader(id, exceptions, ds1, ds2);
        Thread reader = new Thread(r);
        for (Thread t : threads) {
            t.start();
        }
        reader.start();
        for (Thread t : threads) {
            t.join();
        }
        r.terminate();
        reader.join();
        for (Exception e : exceptions) {
            throw e;
        }
    }

    private static final class Reader implements Runnable {

        private final String id;
        private final List<Exception> exceptions;
        private final List<DocumentStore> stores;
        private volatile boolean terminate = false;
        private final Map<Long, NodeDocument> docs = Maps.newHashMap();

        public Reader(String id, List<Exception> exceptions, DocumentStore... stores) {
            this.id = id;
            this.exceptions = exceptions;
            this.stores = Lists.newArrayList(stores);
        }

        void terminate() {
            terminate = true;
        }

        @Override
        public void run() {
            Random random = new Random();
            while (!terminate) {
                try {
                    DocumentStore ds = stores.get(random.nextInt(stores.size()));
                    NodeDocument d = ds.find(Collection.NODES, id);
                    long modCount = d.getModCount();
                    NodeDocument seen = docs.get(modCount);
                    if (seen == null) {
                        docs.put(modCount, d);
                    } else {
                        Map<String, Object> expected = getPropertyValues(seen);
                        Map<String, Object> actual = getPropertyValues(d);
                        assertEquals(expected, actual);
                    }
                    Thread.sleep(random.nextInt(1));
                } catch (AssertionError e) {
                    exceptions.add(new Exception(e.getMessage()));
                    break;
                } catch (Exception e) {
                    exceptions.add(e);
                    break;
                }
            }
        }

        static Map<String, Object> getPropertyValues(NodeDocument doc) {
            Map<String, Object> props = Maps.newHashMap();
            for (String k : doc.keySet()) {
                if (Utils.isPropertyName(k)) {
                    props.put(k, doc.get(k));
                }
            }
            return props;
        }
    }

    private static final class Updater implements Runnable {

        private final DocumentStore ds;
        private final String id;
        private final List<Exception> exceptions;
        private long counter = 0;

        public Updater(DocumentStore ds, String id, List<Exception> exceptions) {
            this.ds = ds;
            this.id = id;
            this.exceptions = exceptions;
        }

        @Override
        public void run() {
            String p = Thread.currentThread().getName();
            for (int i = 0; i < 1000; i++) {
                UpdateOp op = new UpdateOp(id, false);
                op.set(p, counter++);
                try {
                    ds.update(Collection.NODES, Collections.singletonList(id), op);
                } catch (Exception e) {
                    if (ds instanceof RDBDocumentStore
                            && e.getMessage().contains("race?")) {
                        LOG.warn(e.toString());
                    } else {
                        exceptions.add(e);
                    }
                }
            }
        }
    }

    private static long letTimeElapse() {
        long ts = System.currentTimeMillis();
        while (System.currentTimeMillis() == ts) {
            // busy wait
        }
        return System.currentTimeMillis();
    }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class MultiDocumentStoreTest extends AbstractMultiDocumentStoreTest {

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
        up.set("_id", id);
        up.set("_foo", 0l);
        assertTrue(super.ds1.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);

        long increments = 10;

        for (int i = 0; i < increments; i++) {
            up = new UpdateOp(id, true);
            up.set("_id", id);
            up.increment("_foo", 1l);
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
    public void testInterleavedUpdate2() {
        String id = this.getClass().getName() + ".testInterleavedUpdate2";

        // remove if present
        NodeDocument nd1 = super.ds1.find(Collection.NODES, id);
        if (nd1 != null) {
            super.ds1.remove(Collection.NODES, id);
        }

        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        up.set("_modified", 1L);
        assertTrue(super.ds1.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);

        nd1 = super.ds1.find(Collection.NODES, id, 0);
        Number n = nd1.getModCount();
        if (n != null) {
            // Document store uses modCount
            int n1 = n.intValue();

            // get the document into ds2's cache
            NodeDocument nd2 = super.ds2.find(Collection.NODES, id, 0);
            int n2 = nd2.getModCount().intValue();
            assertEquals(n1, n2);

            UpdateOp upds1 = new UpdateOp(id, true);
            upds1.set("_id", id);
            upds1.set("foo", "bar");
            upds1.set("_modified", 2L);
            super.ds1.update(Collection.NODES, Collections.singletonList(id), upds1);
            nd1 = super.ds1.find(Collection.NODES, id);
            int oldn1 = n1;
            n1 = nd1.getModCount().intValue();
            assertEquals(oldn1 + 1, n1);
            assertEquals("bar", nd1.get("foo"));

            // modify in DS2
            UpdateOp upds2 = new UpdateOp(id, true);
            upds2.set("_id", id);
            upds2.set("foo", "qux");
            upds2.set("_modified", 3L);
            super.ds2.update(Collection.NODES, Collections.singletonList(id), upds2);
            nd2 = super.ds2.find(Collection.NODES, id);
            n2 = nd2.getModCount().intValue();
            assertEquals(oldn1 + 1, n2);
            assertEquals("qux", nd2.get("foo"));

            // both stores are now at the same modCount with different contents
            upds1 = new UpdateOp(id, true);
            upds1.set("_id", id);
            upds1.set("foo", "barbar");
            upds1.max("_modified", 0L);
            NodeDocument prev = super.ds1.findAndUpdate(Collection.NODES, upds1);
            // prev document should contain mod from DS2
            assertEquals("qux", prev.get("foo"));
            assertEquals(oldn1 + 2, prev.getModCount().intValue());
            assertEquals(3L, prev.getModified().intValue());

            // the new document must not have a _modified time smaller than
            // before the update
            nd1 = super.ds1.find(Collection.NODES, id, 0);
            assertEquals(super.dsname + ": _modified value must never ever get smaller", 3L, nd1.getModified().intValue());

            // verify that _modified can indeed be *set* to a smaller value, see
            // https://issues.apache.org/jira/browse/OAK-2940
            upds1 = new UpdateOp(id, true);
            upds1.set("_id", id);
            upds1.set("_modified", 0L);
            super.ds1.findAndUpdate(Collection.NODES, upds1);
            nd1 = super.ds1.find(Collection.NODES, id, 0);
            assertEquals(super.dsname + ": _modified value must be set to 0", 0L, nd1.getModified().intValue());
        }
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
        up.set("_id", id);
        up.set("_foo", "bar");
        assertTrue(super.ds1.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);

        // fill both caches
        NodeDocument nd1 = super.ds1.find(Collection.NODES, id);
        NodeDocument nd2 = super.ds2.find(Collection.NODES, id);
        assertNotNull(nd1);
        assertNotNull(nd2);
        long firstVersion = nd1.getModCount().longValue();
        assertEquals(firstVersion, nd2.getModCount().longValue());

        // letTimeElapse();

        // update through ds1
        UpdateOp upds1 = new UpdateOp(id, true);
        upds1.set("_id", id);
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
        upds2.set("_id", id);
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
                up.set(Document.ID, id);
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
                up.set(Document.ID, id);
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

    @Ignore("OAK-3634")
    @Test
    public void testChangeVisibility() {
        String id = this.getClass().getName() + ".testChangeVisibility";

        super.ds1.remove(Collection.NODES, id);

        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        up.set("_foo", 0);
        up.set("_bar", 0);
        assertTrue(super.ds1.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);
        NodeDocument orig = super.ds1.find(Collection.NODES, id);

        // only run test if DS supports modcount
        if (orig.getModCount() != null) {
            long origMc = orig.getModCount().longValue();

            UpdateOp up2 = new UpdateOp(id, false);
            up2.set("_id", id);
            up2.increment("_foo", 1L);
            super.ds2.update(Collection.NODES, Collections.singletonList(id), up2);
            NodeDocument ds2doc = super.ds2.find(Collection.NODES, id);
            long ds2Mc = ds2doc.getModCount().longValue();
            assertTrue("_modCount needs to be > " + origMc + " but was " + ds2Mc, ds2Mc > origMc);

            UpdateOp up1 = new UpdateOp(id, false);
            up1.set("_id", id);
            up1.increment("_bar", 1L);
            super.ds1.update(Collection.NODES, Collections.singletonList(id), up1);

            NodeDocument ds1doc = super.ds1.find(Collection.NODES, id);
            long ds1Mc = ds1doc.getModCount().longValue();
            assertTrue("_modCount needs to be > " + ds2Mc + " but was " + ds1Mc, ds1Mc > ds2Mc);
        }
    }

    @Ignore("OAK-3634")
    @Test
    public void concurrentUpdate() throws Exception {
        String id = Utils.getIdFromPath("/foo");
        ds1.remove(Collection.NODES, id);
        ds2.invalidateCache();
        removeMe.add(id);
        UpdateOp op = new UpdateOp(id, true);
        op.set(Document.ID, id);
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
                    long modCount = d.getModCount().longValue();
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
            try {
                for (int i = 0; i < 1000; i++) {
                    UpdateOp op = new UpdateOp(id, false);
                    op.set(p, counter++);
                    ds.update(Collection.NODES, Collections.singletonList(id), op);
                }
            } catch (Exception e) {
                exceptions.add(e);
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

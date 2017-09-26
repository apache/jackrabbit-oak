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
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests measuring the performance of various {@link DocumentStore} operations.
 * <p>
 * These tests are disabled by default due to their long running time. On the command line
 * specify {@code -DDocumentStorePerformanceTest=true} to enable them.
 */
public class DocumentStorePerformanceTest extends AbstractMultiDocumentStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentStorePerformanceTest.class);
    private static final boolean ENABLED = Boolean.getBoolean(DocumentStorePerformanceTest.class.getSimpleName());

    public DocumentStorePerformanceTest(DocumentStoreFixture dsf) {
        super(dsf);
        assumeTrue(ENABLED);
    }

    @Test
    public void testCreatePerfSmall() {
        createPerf(16, 1);
    }

    @Test
    public void testCreatePerfSmallBatch() {
        createPerf(16, 64);
    }

    @Test
    public void testCreatePerfSmallBatch2() {
        createPerf(16, 256);
    }

    @Test
    public void testCreatePerfBig() {
        createPerf(32 * 1024, 1);
    }

    private void createPerf(int size, int amount) {
        String pval = generateString(size, true);
        long duration = 1000;
        long end = System.currentTimeMillis() + duration;
        long cnt = 0;
        List<String> ids = new ArrayList<String>();

        while (System.currentTimeMillis() < end) {
            ids.clear();
            List<UpdateOp> ups = new ArrayList<UpdateOp>();
            for (int i = 0; i < amount; i++) {
                String id = this.getClass().getName() + ".testCreatePerf-" + size + "-" + cnt + "-" + i;
                UpdateOp up = new UpdateOp(id, true);
                up.set("foo", pval);
                ups.add(up);
                ids.add(id);
            }
            boolean success = super.ds.create(Collection.NODES, ups);
            removeMe.addAll(ids);
            assertTrue("documents with " + ids + " not created", success);
            cnt += 1;
        }

        LOG.info("document creation with property of size " + size + " and batch size " + amount + " for " + super.dsname + " was "
                + cnt + " in " + duration + "ms (" + (cnt / (duration / 1000f)) + "/s)");
    }

    @Test
    public void testPerfCollectionPaging() {
        testPerfCollectionPaging(this.getClass().getName() + ".testPerfCollectionPaging", false);
    }

    @Test
    public void testPerfCollectionPagingUnCached() {
        testPerfCollectionPaging(this.getClass().getName() + ".testPerfCollectionPagingUnCached", true);
    }

    private void testPerfCollectionPaging(String name, boolean invalidateCache) {
        String cid = name;
        int nodecount = 20000;
        int initialFetchCount = 100;
        int maxFetchCount = 1600;
        int fetchcount = initialFetchCount;
        long duration = 2000;
        int cnt = 0;
        List<UpdateOp> ups = new ArrayList<UpdateOp>();

        UpdateOp container = new UpdateOp(cid, true);
        ups.add(container);
        removeMe.add(cid);
        for (int i = 0; i < nodecount; i++) {
            String id = String.format("%s/%08d", cid, i);
            removeMe.add(id);
            UpdateOp u = new UpdateOp(id, true);
            ups.add(u);
        }

        boolean success = super.ds.create(Collection.NODES, ups);
        assertTrue(success);
        super.ds.invalidateCache();

        long end = System.currentTimeMillis() + duration;
        String sid = cid;
        int found = 0;
        while (System.currentTimeMillis() < end) {
            long now = System.currentTimeMillis();
            List<NodeDocument> result = super.ds.query(Collection.NODES, sid, cid + "X", fetchcount);
            if (super.ds.getCacheStats() != null && result.size() > 0) {
                // check freshness of returned documents
                long created = result.get(0).getLastCheckTime();
                assertTrue(
                        "'getLastCheckTime' timestamp of NodeDocument too old (" + created + " vs " + now + ") (on " + super.dsname + ")",
                        created >= now);
            }
            found += result.size();
            if (result.size() < fetchcount) {
                if (sid.equals(cid)) {
                    fail("first page must not be empty");
                }
                sid = cid;
                assertEquals(nodecount, found);
                found = 0;
                fetchcount = initialFetchCount;
            }
            else {
                sid = result.get(result.size() -1).getId();
                if (fetchcount < maxFetchCount) {
                    fetchcount *= 2;
                }
            }
            cnt += 1;
            if (invalidateCache) {
                super.ds.invalidateCache();
            }
        }

        LOG.info("collection lookups " + (invalidateCache ? "(uncached) " : "") + super.dsname + " was " + cnt + " in " + duration
                + "ms (" + (cnt / (duration / 1000f)) + "/s)");
    }

    @Test
    public void testPerfLastRevBatch() {
        String bid = this.getClass().getName() + ".testPerfLastRevBatch";
        int nodecount = 100;
        long duration = 5000;
        int cnt = 0;
        List<String> ids = new ArrayList<String>();
        Revision cr = Revision.fromString("r0-0-1");

        // create test nodes
        for (int i = 0; i < nodecount; i++) {
            String id = bid + "-" + i;
            super.ds.remove(Collection.NODES, id);
            removeMe.add(id);
            UpdateOp up = new UpdateOp(id, true);
            up.set("testprop", generateString(100 * i, true));
            up.setMapEntry("_lastRev", cr, "setup");
            up.set("_modified", NodeDocument.getModifiedInSecs(System.currentTimeMillis()));
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            assertTrue("creation failed for " + id + " in " + super.dsname, success);
            ids.add(id);
        }

        long end = System.currentTimeMillis() + duration;
        while (System.currentTimeMillis() < end) {
            UpdateOp up = new UpdateOp(bid, true);
            up.setMapEntry("_lastRev", cr, "iteration-" + cnt);
            up.max("_modified", NodeDocument.getModifiedInSecs(System.currentTimeMillis()));
            super.ds.update(Collection.NODES, ids, up);
            cnt += 1;
        }

        // check postcondition
        super.ds.invalidateCache();
        for (int i = 0; i < nodecount; i++) {
            NodeDocument d = super.ds.find(Collection.NODES, bid + "-" + i);
            assertNotNull(d);
            Map<Revision, String> m = (Map<Revision, String>)d.get("_lastRev");
            assertEquals("iteration-" + (cnt - 1), m.get(cr));
        }

        LOG.info("batch update for _lastRev for " + super.dsname + " was "
                + cnt + " in " + duration + "ms (" + (cnt / (duration / 1000f)) + "/s)");
    }

    @Test
    public void testPerfReadBigDocCached() {
        perfReadBigDoc(true, this.getClass().getName() + ".testReadBigDocCached");
    }

    @Test
    public void testPerfReadBigDocAfterInvalidate() {
        perfReadBigDoc(false, this.getClass().getName() + ".testReadBigDocAfterInvalidate");
    }

    private void perfReadBigDoc(boolean cached, String name) {
        String id = name;
        long duration = 1000;
        int cnt = 0;

        super.ds.remove(Collection.NODES, Collections.singletonList(id));
        UpdateOp up = new UpdateOp(id, true);
        for (int i = 0; i < 100; i++) {
            up.set("foo" + i, generateString(1024, true));
        }
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);

        long end = System.currentTimeMillis() + duration;
        while (System.currentTimeMillis() < end) {
            if (!cached) {
                super.ds.invalidateCache(Collection.NODES, id);
            }
            NodeDocument d = super.ds.find(Collection.NODES, id, 10);
            assertNotNull(d);
            cnt += 1;
        }

        LOG.info("big doc read " + (cached ? "" : "(after invalidate) ") + "from " + super.dsname + " was " + cnt + " in "
                + duration + "ms (" + (cnt / (duration / 1000f)) + "/s)");
    }

    @Test
    public void testUpdatePerfSmall() {
        updatePerf(16, false);
    }

    @Test
    public void testUpdatePerfSmallGrowing() {
        updatePerf(16, true);
    }

    @Test
    public void testUpdatePerfBig() {
        updatePerf(32 * 1024, false);
    }

    private void updatePerf(int size, boolean growing) {
        String pval = generateString(size, true);
        long duration = 1000;
        long end = System.currentTimeMillis() + duration;
        long cnt = 0;
        Set<Revision> expectedRevs = new HashSet<Revision>();

        String id = this.getClass().getName() + ".testUpdatePerf" + (growing ? "Growing" : "") + "-" + size;
        removeMe.add(id);

        while (System.currentTimeMillis() < end) {
            UpdateOp up = new UpdateOp(id, true);
            if (growing) {
                Revision r = new Revision(System.currentTimeMillis(), (int) cnt, 1);
                up.setMapEntry("foo", r, pval);
                up.setMapEntry("_commitRoot", r, "1");
                up.increment("c", 1);
                up.max("max", System.currentTimeMillis());
                expectedRevs.add(r);
            } else {
                up.set("foo", pval);
            }
            NodeDocument old = super.ds.createOrUpdate(Collection.NODES, up);
            if (cnt == 0) {
                assertNull("expect null on create", old);
            } else {
                assertNotNull("fail on update " + cnt, old);
            }
            cnt += 1;
        }

        if (growing) {
            NodeDocument result = super.ds.find(Collection.NODES, id, 0);
            Map<Revision, Object> m = (Map<Revision, Object>)result.get("foo");
            assertEquals("number of revisions", expectedRevs.size(), m.size());
            assertTrue(m.keySet().equals(expectedRevs));
        }

        LOG.info("document updates with property of size " + size + (growing ? " (growing)" : "") + " for " + super.dsname + " was " + cnt + " in " + duration + "ms (" + (cnt / (duration / 1000f)) + "/s)");
    }

    @Test
    public void testConcurrentUpdatePerf1DS() throws InterruptedException {
        String id = this.getClass().getName() + ".testConcurrentUpdatePerf1DS";
        concurrentUpdatePerf(id, 1);
    }

    @Test
    public void testConcurrentUpdatePerf2DS() throws InterruptedException {
        String id = this.getClass().getName() + ".testConcurrentUpdatePerf1DS";
        concurrentUpdatePerf(id, 2);
    }

    private void concurrentUpdatePerf(String testName, int stores) throws InterruptedException {
        final String id = testName;
        final long duration = 1000;

        ds1.remove(Collection.NODES, id);
        UpdateOp up = new UpdateOp(id, true);
        up.set(Document.MOD_COUNT, 1L);
        up.set("c", 0L);
        up.set("u", 0L);
        super.ds1.create(Collection.NODES, Collections.singletonList(up));
        removeMe.add(id);

        final DocumentStore ts1 = ds1;
        final DocumentStore ts2 = stores == 2 ? ds2 : ds1;

        final AtomicBoolean threadTwoIsActive = new AtomicBoolean(false);
        final AtomicBoolean threadOneIsDone = new AtomicBoolean(false);

        Thread one = new Thread(new Runnable() {
            @Override
            public void run() {
                int failures = 0;
                while (!threadTwoIsActive.get()) {
                }
                // operation that requires fetching the previous state
                UpdateOp up = new UpdateOp(id, false);
                up.increment("c", 1);
                up.notEquals("qux", "qux");
                long end = System.currentTimeMillis() + duration;
                while (System.currentTimeMillis() < end) {
                    try {
                        ts1.update(Collection.NODES, Collections.singletonList(id), up);
                    } catch (RuntimeException ex) {
                        failures += 1;
                    }
                }
                try {
                    UpdateOp up2 = new UpdateOp(id, false);
                    up2.set("cfailures", failures);
                    ts1.update(Collection.NODES, Collections.singletonList(id), up2);
                } catch (RuntimeException ex) {
                }
                threadOneIsDone.set(true);
            }
        }, "cond");

        Thread two = new Thread(new Runnable() {
            @Override
            public void run() {
                int failures = 0;
                // operation that does not require fetching the previous state
                UpdateOp up = new UpdateOp(id, false);
                up.increment("u", 1);
                while (!threadOneIsDone.get()) {
                    try {
                        ts2.update(Collection.NODES, Collections.singletonList(id), up);
                        threadTwoIsActive.set(true);
                    } catch (RuntimeException ex) {
                        failures += 1;
                    }
                }
                threadTwoIsActive.set(true);
                try {
                    UpdateOp up2 = new UpdateOp(id, false);
                    up2.set("ufailures", failures);
                    ts1.update(Collection.NODES, Collections.singletonList(id), up2);
                } catch (RuntimeException ex) {
                }
            }
        }, "uncond");

        two.start();
        one.start();

        two.join();
        one.join();

        // reading uncached because for some reason MongoDS doesn't see the
        // changes made in ds2
        NodeDocument nd = ds1.find(Collection.NODES, id, 0);
        assertNotNull(nd);
        int cc = nd.get("c") == null ? 0 : Integer.valueOf(nd.get("c").toString());
        int uc = nd.get("u") == null ? 0 : Integer.valueOf(nd.get("u").toString());
        long mc = nd.getModCount();
        String msg = String.format(
                "Concurrent updates %s on %s cond. updates: %d (failures: %s), uncond. updates: %d (failures: %s), _modCount: %d, ops/sec: %d, %% of cond. updates: %d",
                stores == 1 ? "(one ds)" : "(two ds)", super.dsname, cc, nd.get("cfailures"), uc, nd.get("ufailures"), mc,
                mc * 1000 / duration, cc * 100 / mc);
        LOG.info(msg);
    }
}

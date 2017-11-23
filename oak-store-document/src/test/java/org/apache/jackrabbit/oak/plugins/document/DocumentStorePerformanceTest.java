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
            List<UpdateOp> ops = new ArrayList<UpdateOp>();
            for (String id : ids) {
                ops.add(up.shallowCopy(id));
            }
            super.ds.createOrUpdate(Collection.NODES, ops);
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
}

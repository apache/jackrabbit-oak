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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicDocumentStoreTest extends AbstractDocumentStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(BasicDocumentStoreTest.class);

    public BasicDocumentStoreTest(DocumentStoreFixture dsf) {
        super(dsf);
    }

    @Test
    public void testAddAndRemove() {
        String id = this.getClass().getName() + ".testAddAndRemove";

        // remove if present
        NodeDocument nd = super.ds.find(Collection.NODES, id);
        if (nd != null) {
            super.ds.remove(Collection.NODES, id);
        }

        // add
        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);
    }

    @Test
    public void testMaxId() {
        // TODO see OAK-1589
        Assume.assumeTrue(!(super.ds instanceof MongoDocumentStore));
        int min = 0;
        int max = 32768;
        int test = 0;

        while (max - min >= 2) {
            test = (max + min) / 2;
            String id = generateString(test);
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            if (success) {
                // check that we really can read it
                NodeDocument findme = super.ds.find(Collection.NODES, id, 0);
                assertNotNull("failed to retrieve previously stored document", findme);
                super.ds.remove(Collection.NODES, id);
                min = test;
            } else {
                max = test;
            }
        }

        LOG.info("max id length for " + super.dsname + " was " + test);
    }

    @Test
    public void testMaxProperty() {
        int min = 0;
        int max = 1024 * 1024 * 4; // 32M
        int test = 0;

        while (max - min >= 256) {
            test = (max + min) / 2;
            String id = this.getClass().getName() + ".testMaxProperty-" + test;
            String pval = generateString(test);
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            up.set("foo", pval);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            if (success) {
                // check that we really can read it
                NodeDocument findme = super.ds.find(Collection.NODES, id, 0);
                assertNotNull("failed to retrieve previously stored document", findme);
                super.ds.remove(Collection.NODES, id);
                min = test;
            } else {
                max = test;
            }
        }

        LOG.info("max prop length for " + super.dsname + " was " + test);
    }

    @Test
    public void testDeleteNonExisting() {
        String id = this.getClass().getName() + ".testDeleteNonExisting-" + UUID.randomUUID();
        // delete is best effort
        ds.remove(Collection.NODES, id);
    }

    @Test
    public void testDeleteNonExistingMultiple() {
        String id = this.getClass().getName() + ".testDeleteNonExistingMultiple-" + UUID.randomUUID();
        // create a test node
        UpdateOp up = new UpdateOp(id + "-2", true);
        up.set("_id", id + "-2");
        boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
        assertTrue(success);
        List<String> todelete = new ArrayList<String>();
        todelete.add(id + "-2");
        todelete.add(id);
        ds.remove(Collection.NODES, todelete);
        // id-2 should be removed
        Document d = ds.find(Collection.NODES, id + "-2");
        assertTrue(d == null);
    }

    @Test
    public void testUpdateMultiple() {
        String id = this.getClass().getName() + ".testUpdateMultiple";
        // create a test node
        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
        assertTrue(success);
        removeMe.add(id);

        // update a non-existing one and this one
        List<String> toupdate = new ArrayList<String>();
        toupdate.add(id + "-" + UUID.randomUUID());
        toupdate.add(id);

        UpdateOp up2 = new UpdateOp(id, false);
        up2.set("foo", "bar");
        ds.update(Collection.NODES, toupdate, up2);

        // id should be updated
        ds.invalidateCache();
        Document d = ds.find(Collection.NODES, id);
        assertNotNull(d);
        assertEquals(d.get("foo").toString(), "bar");
    }

    @Test
    public void testQuery() {
        // create ten documents
        String base = this.getClass().getName() + ".testQuery-";
        for (int i = 0; i < 10; i++) {
            String id = base + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            assertTrue("document with " + id + " not created", success);
            removeMe.add(id);
        }

        List<String> result = getKeys(ds.query(Collection.NODES, base, base + "A", 5));
        assertEquals(5, result.size());
        assertTrue(result.contains(base + "4"));
        assertFalse(result.contains(base + "5"));

        result = getKeys(ds.query(Collection.NODES, base, base + "A", 20));
        assertEquals(10, result.size());
        assertTrue(result.contains(base + "0"));
        assertTrue(result.contains(base + "9"));
    }

    @Test
    public void testQueryBinary() {
        // create ten documents
        String base = this.getClass().getName() + ".testQueryBinary-";
        for (int i = 0; i < 10; i++) {
            String id = base + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            up.set(NodeDocument.HAS_BINARY_FLAG, i % 2L);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            assertTrue("document with " + id + " not created", success);
            removeMe.add(id);
        }

        List<String> result = getKeys(ds.query(Collection.NODES, base, base + "Z", NodeDocument.HAS_BINARY_FLAG,
                NodeDocument.HAS_BINARY_VAL, 1000));
        assertEquals(5, result.size());
        assertTrue(result.contains(base + "1"));
        assertFalse(result.contains(base + "0"));
    }

    @Test
    public void testQueryCollation() {
        // create ten documents
        String base = this.getClass().getName() + ".testQueryCollation";
        List<UpdateOp> creates = new ArrayList<UpdateOp>();

        List<String>expected = new ArrayList<String>();
        // test US-ASCII except control characters
        for (char c : "!\"#$%&'()*+,-./0123456789:;<=>?@AZ[\\]^_`az{|}~".toCharArray()) {
            String id = base + c;
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            creates.add(up);
            removeMe.add(id);
            id = base + "/" + c;
            up = new UpdateOp(id, true);
            up.set("_id", id);
            creates.add(up);
            expected.add(id);
            removeMe.add(id);
        }
        boolean success = super.ds.create(Collection.NODES, creates);
        assertTrue("documents not created", success);

        List<String> result = getKeys(ds.query(Collection.NODES, base + "/", base + "0", 1000));

        List<String> diff = new ArrayList<String>();
        diff.addAll(result);
        diff.removeAll(expected);
        if (! diff.isEmpty()) {
            fail("unexpected query results (broken collation handling in persistence?): " + diff);
        }

        diff = new ArrayList<String>();
        diff.addAll(expected);
        diff.removeAll(result);
        if (! diff.isEmpty()) {
            fail("missing query results (broken collation handling in persistence?): " + diff);
        }
        assertEquals("incorrect result ordering in query result (broken collation handling in persistence?)", expected, result);
    }

    private List<String> getKeys(List<NodeDocument> docs) {
        List<String> result = new ArrayList<String>();
        for (NodeDocument doc : docs) {
            result.add(doc.getId());
        }
        return result;
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
    public void testCreatePerfBig() {
        createPerf(32 * 1024, 1);
    }

    private void createPerf(int size, int amount) {
        String pval = generateString(size);
        long duration = 1000;
        long end = System.currentTimeMillis() + duration;
        long cnt = 0;
        List<String> ids = new ArrayList<String>();

        while (System.currentTimeMillis() < end) {
            List<UpdateOp> ups = new ArrayList<UpdateOp>();
            for (int i = 0; i < amount; i++) {
                String id = this.getClass().getName() + ".testCreatePerf-" + size + "-" + cnt + "-" + i;
                UpdateOp up = new UpdateOp(id, true);
                up.set("_id", id);
                up.set("foo", pval);
                ups.add(up);
                ids.add(id);
            }
            boolean success = super.ds.create(Collection.NODES, ups);
            removeMe.addAll(ids);
            assertTrue("documents with " + ids + " not created", success);
            cnt += 1;
        }

        LOG.info("document creation with property of size " + size + " and batch size " + amount + " for " + super.dsname + " was " + cnt + " in " + duration + "ms ("
                + (cnt / (duration / 1000f)) + "/s)");
    }

    @Test
    public void testUpdatePerfSmall() {
        updatePerf(16);
    }

    @Test
    public void testUpdatePerfBig() {
        updatePerf(32 * 1024);
    }

    private void updatePerf(int size) {
        String pval = generateString(size);
        long duration = 1000;
        long end = System.currentTimeMillis() + duration;
        long cnt = 0;

        String id = this.getClass().getName() + ".testUpdatePerf-" + size;
        removeMe.add(id);

        while (System.currentTimeMillis() < end) {
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            up.set("foo", pval);
            super.ds.createOrUpdate(Collection.NODES, up);
            cnt += 1;
        }

        LOG.info("document updates with property of size " + size + " for " + super.dsname + " was " + cnt + " in " + duration + "ms ("
                + (cnt / (duration / 1000f)) + "/s)");
    }

    private static String generateString(int length) {
        StringBuffer buf = new StringBuffer(length);
        while (length-- > 0) {
            buf.append('A' + ((int) (26 * Math.random())));
        }
        return buf.toString();
    }
}

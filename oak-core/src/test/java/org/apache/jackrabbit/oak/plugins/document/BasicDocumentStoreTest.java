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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.document.cache.CachingDocumentStore;
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
            String id = generateString(test, true);
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
            String pval = generateString(test, true);
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
    public void testInterestingPropLengths() {
        int lengths[] = { 1, 10, 100, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000,
                15000, 16000, 20000 };

        for (int test : lengths) {
            String id = this.getClass().getName() + ".testInterestingPropLengths-" + test;
            String pval = generateString(test, true);
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            up.set("foo", pval);
            super.ds.remove(Collection.NODES, id);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            assertTrue("failed to insert a document with property of length " + test + "(ASCII) in " + super.dsname, success);
            super.ds.remove(Collection.NODES, id);
        }

        for (int test : lengths) {
            String id = this.getClass().getName() + ".testInterestingPropLengths-" + test;
            String pval = generateString(test, false);
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            up.set("foo", pval);
            super.ds.remove(Collection.NODES, id);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            try {
                assertTrue("failed to insert a document with property of length " + test
                        + "(potentially non-ASCII, actual octet length in UTF-8: " + pval.getBytes("UTF-8").length + ") in "
                        + super.dsname, success);
            } catch (UnsupportedEncodingException e) {
                // outch
            }
            super.ds.remove(Collection.NODES, id);
        }
    }

    @Test
    public void testModifiedMaxUpdate() {
        String id = this.getClass().getName() + ".testModifiedMaxUpdate";
        // create a test node
        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        up.set("_modified", 1000L);
        boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
        assertTrue(success);
        removeMe.add(id);

        // update with smaller _modified
        UpdateOp up2 = new UpdateOp(id, true);
        up2.max("_modified", 100L);
        up2.set("_id", id);
        super.ds.findAndUpdate(Collection.NODES, up2);

        super.ds.invalidateCache();

        // this should find the document; will fail if the MAX operation wasn't applied to the indexed property
        List<NodeDocument> results = super.ds.query(Collection.NODES, this.getClass().getName() + ".testModifiedMaxUpdatd", this.getClass().getName() + ".testModifiedMaxUpdatf", "_modified", 1000, 1);
        assertEquals("document not found, maybe indexed _modified property not properly updated", 1, results.size());
    }

    @Test
    public void testInterestingStrings() {
        // TODO see OAK-1913
        Assume.assumeTrue(!(super.dsname.equals("RDB-MySQL")));

        String[] tests = new String[] {
            "simple:foo", "cr:a\n\b", "dquote:a\"b", "bs:a\\b", "euro:a\u201c", "gclef:\uD834\uDD1E", "tab:a\tb", "nul:a\u0000b"
        };

        for (String t : tests) {
            int pos = t.indexOf(":");
            String testname = t.substring(0, pos);
            String test = t.substring(pos + 1);
            String id = this.getClass().getName() + ".testInterestingStrings-" + testname;
            UpdateOp up = new UpdateOp(id, true);
            up.set("_id", id);
            up.set("foo", test);
            super.ds.remove(Collection.NODES, id);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            assertTrue("failed to insert a document with property value of " + test + " in " + super.dsname, success);
            // re-read from persistence
            super.ds.invalidateCache();
            NodeDocument nd = super.ds.find(Collection.NODES, id);
            assertEquals("failure to round-trip " + testname + " through " + super.dsname, test, nd.get("foo"));
            super.ds.remove(Collection.NODES, id);
        }
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
        super.ds.remove(Collection.NODES, id);
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
        assertEquals(id, d.getId());
        assertEquals("bar", d.get("foo").toString());
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
        String base = "2:/" + this.getClass().getName() + ".testQueryCollation";
        List<UpdateOp> creates = new ArrayList<UpdateOp>();

        List<String> expected = new ArrayList<String>();
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
        if (!diff.isEmpty()) {
            fail("unexpected query results (broken collation handling in persistence?): " + diff);
        }

        diff = new ArrayList<String>();
        diff.addAll(expected);
        diff.removeAll(result);
        if (!diff.isEmpty()) {
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
        String pval = generateString(size, true);
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
        container.set("_id", cid);
        ups.add(container);
        removeMe.add(cid);
        for (int i = 0; i < nodecount; i++) {
            String id = String.format("%s/%08d", cid, i);
            removeMe.add(id);
            UpdateOp u = new UpdateOp(id, true);
            u.set("_id", id);
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
            if ((super.ds instanceof CachingDocumentStore) && result.size() > 0) {
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
            up.set("_id", id);
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
    public void testPerfReadBigDoc() {
        String id = this.getClass().getName() + ".testReadBigDoc";
        long duration = 1000;
        int cnt = 0;

        super.ds.remove(Collection.NODES, Collections.singletonList(id));
        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        for (int i = 0; i < 100; i++) {
            up.set("foo" + i, generateString(1024, true));
        }
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);

        long end = System.currentTimeMillis() + duration;
        while (System.currentTimeMillis() < end) {
            NodeDocument d = super.ds.find(Collection.NODES, id, 10); // allow 10ms old entries
            cnt += 1;
        }

        LOG.info("big doc read from " + super.dsname + " was "
                + cnt + " in " + duration + "ms (" + (cnt / (duration / 1000f)) + "/s)");
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
            up.set("_id", id);
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

        LOG.info("document updates with property of size " + size + (growing ? " (growing)" : "") + " for " + super.dsname
                + " was " + cnt + " in " + duration + "ms (" + (cnt / (duration / 1000f)) + "/s)");
    }

    private static String generateString(int length, boolean ascii) {
        char[] s = new char[length];
        for (int i = 0; i < length; i++) {
            if (ascii) {
                s[i] = (char) (32 + (int) (95 * Math.random()));
            } else {
                s[i] = (char) (32 + (int) ((0xd7ff - 32) * Math.random()));
            }
        }
        return new String(s);
    }

    @Test
    public void testPerfUpdateLimit() throws SQLException, UnsupportedEncodingException {
        internalTestPerfUpdateLimit("testPerfUpdateLimit", "raw row update (set long)", 0);
    }

    @Test
    public void testPerfUpdateLimitString() throws SQLException, UnsupportedEncodingException {
        internalTestPerfUpdateLimit("testPerfUpdateLimitString", "raw row update (set long/string)", 1);
    }

    @Test
    public void testPerfUpdateLimitStringBlob() throws SQLException, UnsupportedEncodingException {
        internalTestPerfUpdateLimit("testPerfUpdateLimitStringBlob", "raw row update (set long/string/blob)", 2);
    }

    @Test
    public void testPerfUpdateAppendString() throws SQLException, UnsupportedEncodingException {
        internalTestPerfUpdateLimit("testPerfUpdateAppendString", "raw row update (append string)", 3);
    }

    @Test
    public void testPerfUpdateGrowingDoc() throws SQLException, UnsupportedEncodingException {
        internalTestPerfUpdateLimit("testPerfUpdateGrowingDoc", "raw row update (string + blob)", 4);
    }

    private void internalTestPerfUpdateLimit(String name, String desc, int mode) throws SQLException, UnsupportedEncodingException {
        if (super.rdbDataSource != null) {
            String key = name;
            Connection connection = null;
            String table = DocumentStoreFixture.TABLEPREFIX + "NODES";

            // create test node
            try {
                connection = super.rdbDataSource.getConnection();
                connection.setAutoCommit(false);
                PreparedStatement stmt = connection.prepareStatement("insert into " + table
                        + " (ID, MODCOUNT, DATA) values (?, ?, ?)");
                try {
                    stmt.setString(1, key);
                    stmt.setLong(2, 0);
                    stmt.setString(3, "X");
                    stmt.executeUpdate();
                    connection.commit();
                } finally {
                    stmt.close();
                }
            } catch (SQLException ex) {
                // ignored
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        // ignored
                    }
                }
            }

            removeMe.add(key);
            StringBuffer expect = new StringBuffer("X");

            String appendString = generateString(512, true);

            long duration = 1000;
            long end = System.currentTimeMillis() + duration;
            long cnt = 0;
            byte bdata[] = new byte[65536];
            String sdata = appendString;
            boolean needsConcat = super.dsname.contains("MySQL");
            int dataInChars = (super.dsname.contains("Oracle") ? 4000 : 16384);
            int dataInBytes = dataInChars / 3;

            while (System.currentTimeMillis() < end) {

                try {
                    connection = super.rdbDataSource.getConnection();
                    connection.setAutoCommit(false);

                    if (mode == 0) {
                        PreparedStatement stmt = connection.prepareStatement("update " + table + " set MODCOUNT = ? where ID = ?");
                        try {
                            stmt.setLong(1, cnt);
                            stmt.setString(2, key);
                            assertEquals(1, stmt.executeUpdate());
                            connection.commit();
                        } finally {
                            stmt.close();
                        }
                    } else if (mode == 1) {
                        PreparedStatement stmt = connection.prepareStatement("update " + table
                                + " set MODCOUNT = ?, DATA = ? where ID = ?");
                        try {
                            stmt.setLong(1, cnt);
                            stmt.setString(2, "JSON data " + UUID.randomUUID());
                            stmt.setString(3, key);
                            assertEquals(1, stmt.executeUpdate());
                            connection.commit();
                        } finally {
                            stmt.close();
                        }
                    } else if (mode == 2) {
                        PreparedStatement stmt = connection.prepareStatement("update " + table
                                + " set MODCOUNT = ?, DATA = ?, BDATA = ? where ID = ?");
                        try {
                            stmt.setLong(1, cnt);
                            stmt.setString(2, "JSON data " + UUID.randomUUID());
                            bdata[(int) cnt % bdata.length] = (byte) (cnt & 0xff);
                            stmt.setString(2, "JSON data " + UUID.randomUUID());
                            stmt.setBytes(3, bdata);
                            stmt.setString(4, key);
                            assertEquals(1, stmt.executeUpdate());
                            connection.commit();
                        } finally {
                            stmt.close();
                        }
                    } else if (mode == 3) {
                        PreparedStatement stmt = connection.prepareStatement("update "
                                + table
                                + " set "
                                + (needsConcat ? "DATA = CONCAT(DATA, ?)" : "DATA = DATA || CAST(? as varchar(" + dataInChars
                                        + "))") + " where ID = ?");
                        try {
                            stmt.setString(1, appendString);
                            stmt.setString(2, key);
                            assertEquals(1, stmt.executeUpdate());
                            connection.commit();
                            expect.append(appendString);
                        } catch (SQLException ex) {
                            String state = ex.getSQLState();
                            if ("22001".equals(state) /* everybody */ || ("72000".equals(state) && 1489 == ex.getErrorCode()) /* Oracle */) {
                                // overflow
                                connection.rollback();
                                stmt = connection.prepareStatement("update " + table
                                        + " set MODCOUNT = MODCOUNT + 1, DATA = ? where ID = ?");
                                stmt.setString(1, "X");
                                stmt.setString(2, key);
                                assertEquals(1, stmt.executeUpdate());
                                connection.commit();
                                expect = new StringBuffer("X");
                            } else {
                                throw (ex);
                            }
                        } finally {
                            stmt.close();
                        }
                    } else if (mode == 4) {
                        PreparedStatement stmt = connection.prepareStatement("update " + table
                                + " set MODIFIED = ?, HASBINARY = ?, MODCOUNT = ?, CMODCOUNT = ?, DSIZE = ?, DATA = ?, BDATA = ? where ID = ?");
                        try {
                            int si = 1;
                            stmt.setObject(si++, System.currentTimeMillis() / 5, Types.BIGINT);
                            stmt.setObject(si++, 0, Types.SMALLINT);
                            stmt.setObject(si++, cnt, Types.BIGINT);
                            stmt.setObject(si++, null, Types.BIGINT);
                            stmt.setObject(si++, sdata.length(), Types.BIGINT);

                            if (sdata.length() < dataInBytes) {
                                stmt.setString(si++, sdata);
                                stmt.setBinaryStream(si++, null, 0);
                            }
                            else {
                                stmt.setString(si++, "null");
                                stmt.setBytes(si++, sdata.getBytes("UTF-8"));
                            }
                            stmt.setString(si++, key);
                            assertEquals(1, stmt.executeUpdate());
                            connection.commit();
                            sdata += appendString;
                        } finally {
                            stmt.close();
                        }

                    }
                } catch (SQLException ex) {
                    LOG.error(ex.getMessage() + " " + ex.getSQLState() + " " + ex.getErrorCode(), ex);
                } finally {
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            // ignored
                        }
                    }
                }

                cnt += 1;
            }

            // check persisted values
            if (mode == 3) {
                try {
                    connection = super.rdbDataSource.getConnection();
                    connection.setAutoCommit(false);
                    PreparedStatement stmt = connection.prepareStatement("select DATA, MODCOUNT from " + table + " where ID = ?");
                    try {
                        stmt.setString(1, key);
                        ResultSet rs = stmt.executeQuery();
                        assertTrue(rs.next());
                        String got = rs.getString(1);
                        long modc = rs.getLong(2);
                        LOG.info("column reset " + modc + " times");
                        assertEquals(expect.toString(), got);
                    } finally {
                        stmt.close();
                    }
                } finally {
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            // ignored
                        }
                    }
                }
            }

            LOG.info(desc + " for " + super.dsname + " was " + cnt + " in " + duration + "ms (" + (cnt / (duration / 1000f))
                    + "/s)");
        }
    }

    // make sure _collisionsModCount property is maintained properly when it exists
    @Test
    public void testCollisionsModCount() {
        String id = this.getClass().getName() + ".testCollisionsModCount";

        // remove if present
        NodeDocument nd = super.ds.find(Collection.NODES, id);
        if (nd != null) {
            super.ds.remove(Collection.NODES, id);
        }

        // add
        Revision revision = Revision.fromString("r0-0-1");
        UpdateOp up = new UpdateOp(id, true);
        up.set("_id", id);
        up.setMapEntry("_collisions", revision, "foo");
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(up)));
        removeMe.add(id);

        // get it
        nd = super.ds.find(Collection.NODES, id);
        assertNotNull(nd);
        Number cmc = (Number)nd.get("_collisionsModCount");
        if (cmc == null) {
            // not supported
        }
        else {
            // update 
            Revision revision2 = Revision.fromString("r0-0-2");
            UpdateOp up2 = new UpdateOp(id, false);
            up2.set("_id", id);
            up2.setMapEntry("_collisions", revision2, "foobar");
            NodeDocument old = super.ds.findAndUpdate(Collection.NODES, up2);
            assertNotNull(old);

            nd = super.ds.find(Collection.NODES, id, 0);
            assertNotNull(nd);
            Number cmc2 = (Number)nd.get("_collisionsModCount");
            assertNotNull(cmc2);
            assertTrue(cmc2.longValue() > cmc.longValue());

            // update 
            UpdateOp up3 = new UpdateOp(id, false);
            up3.set("_id", id);
            up3.set("foo", "bar");
            old = super.ds.findAndUpdate(Collection.NODES, up3);
            assertNotNull(old);

            nd = super.ds.find(Collection.NODES, id, 0);
            assertNotNull(nd);
            Number cmc3 = (Number)nd.get("_collisionsModCount");
            assertNotNull(cmc3);
            assertTrue(cmc2.longValue() == cmc3.longValue());
        }
    }
}

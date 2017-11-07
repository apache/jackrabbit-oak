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
import static org.junit.Assume.assumeNotNull;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;

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
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(up)));
        super.ds.invalidateCache();
        assertNotNull(super.ds.find(Collection.NODES, id));
        removeMe.add(id);
    }

    @Test
    public void testAddAndRemoveWithoutIdInUpdateOp() {
        String id = this.getClass().getName() + ".testAddAndRemoveWithoutIdInUpdateOp";

        // remove if present
        NodeDocument nd = super.ds.find(Collection.NODES, id);
        if (nd != null) {
            super.ds.remove(Collection.NODES, id);
        }

        // add
        UpdateOp up = new UpdateOp(id, true);
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(up)));
        super.ds.invalidateCache();
        assertNotNull(super.ds.find(Collection.NODES, id));
        removeMe.add(id);
    }

    @Test
    public void testValuesForSystemProps() {
        String id = this.getClass().getName() + ".testValuesForSystemProps";

        // remove if present
        NodeDocument nd = super.ds.find(Collection.NODES, id);
        if (nd != null) {
            super.ds.remove(Collection.NODES, id);
        }
        removeMe.add(id);

        // add
        UpdateOp up = new UpdateOp(id, true);
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(up)));

        super.ds.invalidateCache();
        nd = super.ds.find(Collection.NODES, id, 0);
        assertNull(nd.get(NodeDocument.DELETED_ONCE));
        assertNull(nd.get(NodeDocument.HAS_BINARY_FLAG));
        assertFalse(nd.wasDeletedOnce());
        assertFalse(nd.hasBinary());
        
        up = new UpdateOp(id, false);
        up.set(NodeDocument.DELETED_ONCE, true);
        super.ds.findAndUpdate(Collection.NODES, up);
        
        super.ds.invalidateCache();
        nd = super.ds.find(Collection.NODES, id, 0);
        assertEquals(true, nd.get(NodeDocument.DELETED_ONCE));
        assertNull(nd.get(NodeDocument.HAS_BINARY_FLAG));
        assertTrue(nd.wasDeletedOnce());
        assertFalse(nd.hasBinary());

        up = new UpdateOp(id, false);
        up.set(NodeDocument.DELETED_ONCE, false);
        up.set(NodeDocument.HAS_BINARY_FLAG, NodeDocument.HAS_BINARY_VAL);
        super.ds.findAndUpdate(Collection.NODES, up);
        
        super.ds.invalidateCache();
        nd = super.ds.find(Collection.NODES, id, 0);
        assertEquals(false, nd.get(NodeDocument.DELETED_ONCE));
        assertEquals(NodeDocument.HAS_BINARY_VAL, nd.get(NodeDocument.HAS_BINARY_FLAG));
        assertFalse(nd.wasDeletedOnce());
        assertTrue(nd.hasBinary());
        
        // remove
        up = new UpdateOp(id, false);
        up.remove(NodeDocument.DELETED_ONCE);
        up.remove(NodeDocument.HAS_BINARY_FLAG);
        super.ds.findAndUpdate(Collection.NODES, up);
        
        super.ds.invalidateCache();
        nd = super.ds.find(Collection.NODES, id, 0);
        assertNull(nd.get(NodeDocument.DELETED_ONCE));
        assertNull(nd.get(NodeDocument.HAS_BINARY_FLAG));
    }

    @Test
    public void testSetId() {
        String id = this.getClass().getName() + ".testSetId";

        UpdateOp up = new UpdateOp(id, true);
        try {
            up.set("_id", id);
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testCreateOrUpdate() {
        String id = this.getClass().getName() + ".testCreateOrUpdate";

        // remove if present
        NodeDocument nd = super.ds.find(Collection.NODES, id);
        if (nd != null) {
            super.ds.remove(Collection.NODES, id);
        }

        // create
        UpdateOp up = new UpdateOp(id, true);
        assertNull(super.ds.createOrUpdate(Collection.NODES, up));

        // update
        up = new UpdateOp(id, true);
        assertNotNull(super.ds.createOrUpdate(Collection.NODES, up));
        removeMe.add(id);
    }

    @Test
    public void testCreateOrUpdateWithoutIdInUpdateOp() {
        String id = this.getClass().getName() + ".testCreateOrUpdateWithoutIdInUpdateOp";

        // remove if present
        NodeDocument nd = super.ds.find(Collection.NODES, id);
        if (nd != null) {
            super.ds.remove(Collection.NODES, id);
        }

        // create
        UpdateOp up = new UpdateOp(id, true);
        assertNull(super.ds.createOrUpdate(Collection.NODES, up));

        // update
        up = new UpdateOp(id, true);
        assertNotNull(super.ds.createOrUpdate(Collection.NODES, up));
        removeMe.add(id);
    }

    @Test
    public void testAddAndRemoveJournalEntry() {
        // OAK-4021
        String id = this.getClass().getName() + ".testAddAndRemoveJournalEntry";

        // remove if present
        Document d = super.ds.find(Collection.JOURNAL, id);
        if (d != null) {
            super.ds.remove(Collection.JOURNAL, id);
        }

        // add
        UpdateOp up = new UpdateOp(id, true);
        assertTrue(super.ds.create(Collection.JOURNAL, Collections.singletonList(up)));
    }

    @Test
    public void testConditionalUpdate() {
        String id = this.getClass().getName() + ".testConditionalUpdate";

        // remove if present
        NodeDocument nd = super.ds.find(Collection.NODES, id);
        if (nd != null) {
            super.ds.remove(Collection.NODES, id);
        }

        String existingProp = "_recoverylock";
        String existingRevisionProp = "recoverylock";
        String nonExistingProp = "_qux";
        String nonExistingRevisionProp = "qux";
        Revision r = new Revision(1, 1, 1);

        // add
        UpdateOp up = new UpdateOp(id, true);
        up.set(existingProp, "lock");
        up.setMapEntry(existingRevisionProp, r, "lock");
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(up)));

        // updates
        up = new UpdateOp(id, false);
        up.notEquals(nonExistingProp, "none");
        NodeDocument result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNotNull(result);

        up = new UpdateOp(id, false);
        up.equals(nonExistingProp, null);
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNotNull(result);

        up = new UpdateOp(id, false);
        up.notEquals(nonExistingRevisionProp, r, "none");
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNotNull(result);

        up = new UpdateOp(id, false);
        up.equals(nonExistingRevisionProp, r, null);
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNotNull(result);

        up = new UpdateOp(id, false);
        up.equals(existingProp, "none");
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNull(result);

        up = new UpdateOp(id, false);
        up.equals(existingProp, null);
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNull(result);

        up = new UpdateOp(id, false);
        up.equals(existingRevisionProp, r, "none");
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNull(result);

        up = new UpdateOp(id, false);
        up.equals(existingRevisionProp, r, null);
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNull(result);

        up = new UpdateOp(id, false);
        up.notEquals(existingProp, "lock");
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNull(result);

        up = new UpdateOp(id, false);
        up.equals(existingProp, null);
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNull(result);

        up = new UpdateOp(id, false);
        up.notEquals(existingRevisionProp, r, "lock");
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNull(result);

        up = new UpdateOp(id, false);
        up.equals(existingRevisionProp, r, null);
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNull(result);

        up = new UpdateOp(id, false);
        up.equals(existingProp, "lock");
        up.set(existingProp, "none");
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNotNull(result);

        up = new UpdateOp(id, false);
        up.equals(existingRevisionProp, r, "lock");
        up.setMapEntry(existingRevisionProp, r, "none");
        result = super.ds.findAndUpdate(Collection.NODES, up);
        assertNotNull(result);

        removeMe.add(id);
    }

    @Test
    public void testConditionalUpdateForbidden() {
        String id = this.getClass().getName() + ".testConditionalupdateForbidden";

        // remove if present
        NodeDocument nd = super.ds.find(Collection.NODES, id);
        if (nd != null) {
            super.ds.remove(Collection.NODES, id);
        }

        try {
            UpdateOp up = new UpdateOp(id, true);
            up.equals("foo", "bar");
            super.ds.create(Collection.NODES, Collections.singletonList(up));
            fail("conditional create should fail");
        }
        catch (IllegalStateException expected) {
            // reported by UpdateOp
        }

        UpdateOp cup = new UpdateOp(id, true);
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(cup)));
        removeMe.add(id);

        try {
            UpdateOp up = new UpdateOp(id, false);
            up.equals("foo", "bar");
            super.ds.createOrUpdate(Collection.NODES, up);
            fail("conditional createOrUpdate should fail");
        }
        catch (IllegalArgumentException expected) {
            // reported by DocumentStore
        }

        try {
            UpdateOp up = new UpdateOp(id, false);
            up.equals("foo", "bar");
            super.ds.createOrUpdate(Collection.NODES, Collections.singletonList(up));
            fail("conditional createOrUpdate should fail");
        }
        catch (IllegalArgumentException expected) {
            // reported by DocumentStore
        }
    }

    @Test
    public void testMaxIdAscii() {
        int result = testMaxId(true);
        assertTrue("needs to support keys of 512 bytes length, but only supports " + result, result >= 512);
    }

    @Test
    public void testMaxIdNonAscii() {
        testMaxId(false);
    }

    @Test
    public void testLongId() {
        String id = "0:/" + generateId(2048, true);
        assertNull("find() with ultra-long id needs to return 'null'", super.ds.find(Collection.NODES, id));

        if (! super.dsname.contains("Memory")) {
            UpdateOp up = new UpdateOp(id,  true);
            assertFalse("create() with ultra-long id needs to fail", super.ds.create(Collection.NODES, Collections.singletonList(up)));
        }
    }

    //OAK-3001
    @Test
    public void testRangeRemove() {
        String idPrefix = this.getClass().getName() + ".testRangeRemove";

        com.google.common.collect.Range<Long> modTimes = Range.closed(1L, 30L);
        for (Long modTime : ContiguousSet.create(modTimes, DiscreteDomain.longs())) {
            String id = idPrefix + modTime;
            // remove if present
            Document d = super.ds.find(Collection.JOURNAL, id);
            if (d != null) {
                super.ds.remove(Collection.JOURNAL, id);
            }

            // add
            UpdateOp up = new UpdateOp(id, true);
            up.set("_modified", modTime);
            super.ds.create(Collection.JOURNAL, Collections.singletonList(up));
            removeMeJournal.add(id);
        }

        assertEquals("Number of entries removed didn't match", 3,
                ds.remove(Collection.JOURNAL, "_modified", 20, 24));

        assertEquals("Number of entries removed didn't match", 0,
                ds.remove(Collection.JOURNAL, "_modified", 20, 24));

        assertEquals("Number of entries removed didn't match", 4,
                ds.remove(Collection.JOURNAL, "_modified", -1, 5));

        assertEquals("Number of entries removed didn't match", 5,
                ds.remove(Collection.JOURNAL, "_modified", 0, 10));

        // interesting cases
        assertEquals("Number of entries removed didn't match", 0,
                ds.remove(Collection.JOURNAL, "_modified", 20, 19));

        assertEquals("Number of entries removed didn't match", 0,
                ds.remove(Collection.JOURNAL, "_modified", 31, 40));

        assertEquals("Number of entries removed didn't match", 2,
                ds.remove(Collection.JOURNAL, "_modified", 28, 40));
    }

    private int testMaxId(boolean ascii) {
        int min = 0;
        int max = 32768;
        int test = 0;
        int last = 0;

        while (max - min >= 2) {
            test = (max + min) / 2;
            String id = generateId(test, ascii);
            // make sure it's gone before trying to create it
            try {
                super.ds.remove(Collection.NODES, id);
            } catch (DocumentStoreException ignored) {
            }
            UpdateOp up = new UpdateOp(id, true);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            if (success) {
                // check that we really can read it
                NodeDocument findme = super.ds.find(Collection.NODES, id, 0);
                assertNotNull("failed to retrieve previously stored document", findme);
                assertEquals(id, findme.getId());
                super.ds.remove(Collection.NODES, id);
                min = test;
                last = test;
            } else {
                max = test;
            }
        }

        LOG.info("max " + (ascii ? "ASCII ('0')" : "non-ASCII (U+1F4A9)") + " id length for " + super.dsname + " was " + last);
        return last;
    }

    @Test
    public void testMaxProperty() {
        int min = 0;
        int max = 1024 * 1024 * 8;
        int test = 0;
        int last = 0;

        while (max - min >= 256) {
            if (test == 0) {
                test = max; // try largest first
            } else {
                test = (max + min) / 2;
            }
            String id = this.getClass().getName() + ".testMaxProperty-" + test;
            String pval = generateString(test, true);
            UpdateOp up = new UpdateOp(id, true);
            up.set("foo", pval);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            if (success) {
                // check that we really can read it
                NodeDocument findme = super.ds.find(Collection.NODES, id, 0);
                assertNotNull("failed to retrieve previously stored document", findme);
                super.ds.remove(Collection.NODES, id);
                min = test;
                last = test;
            } else {
                max = test;
            }
        }

        LOG.info("max prop length for " + super.dsname + " was " + last);
    }

    @Test
    public void testInterestingPropLengths() throws UnsupportedEncodingException {
        int lengths[] = { 1, 10, 100, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000, 14000,
                15000, 16000, 20000 };

        for (int test : lengths) {
            String id = this.getClass().getName() + ".testInterestingPropLengths-" + test;
            String pval = generateString(test, true);
            UpdateOp up = new UpdateOp(id, true);
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
            up.set("foo", pval);
            super.ds.remove(Collection.NODES, id);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            assertTrue("failed to insert a document with property of length " + test
                    + " (potentially non-ASCII, actual octet length with UTF-8 encoding: " + pval.getBytes("UTF-8").length + ") in "
                    + super.dsname, success);
            // check that update works as well
            if (success) {
                try {
                    super.ds.findAndUpdate(Collection.NODES, up);
                } catch (Exception ex) {
                    ex.printStackTrace(System.err);
                    fail("failed to update a document with property of length " + test
                            + " (potentially non-ASCII, actual octet length with UTF-8 encoding: " + pval.getBytes("UTF-8").length + ") in "
                            + super.dsname);
                }
            }
            super.ds.remove(Collection.NODES, id);
        }
    }

    @Test
    public void testModifiedMaxUpdateQuery() {
        String id = this.getClass().getName() + ".testModifiedMaxUpdate";
        // create a test node
        UpdateOp up = new UpdateOp(id, true);
        up.set("_modified", 1000L);
        boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
        assertTrue(success);
        removeMe.add(id);

        // update with smaller _modified
        UpdateOp up2 = new UpdateOp(id, true);
        up2.max("_modified", 100L);
        super.ds.findAndUpdate(Collection.NODES, up2);

        super.ds.invalidateCache();

        // this should find the document; will fail if the MAX operation wasn't applied to the indexed property
        String startId = this.getClass().getName() + ".testModifiedMaxUpdatd";
        String endId = this.getClass().getName() + ".testModifiedMaxUpdatf";
        List<NodeDocument> results = super.ds.query(Collection.NODES, startId, endId, "_modified", 1000, 1);
        assertEquals("document not found, maybe indexed _modified property not properly updated", 1, results.size());
    }

    @Test
    public void testModifiedMaxUpdateQuery2() {
        // test for https://issues.apache.org/jira/browse/OAK-4388
        String id = this.getClass().getName() + ".testModifiedMaxUpdate2";
        // create a test node
        UpdateOp up = new UpdateOp(id, true);
        up.set("_modified", 1000L);
        boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
        assertTrue(success);
        removeMe.add(id);

        for (int i = 0; i < 25; i++) {
            // update with smaller _modified
            UpdateOp up2 = new UpdateOp(id, true);
            up2.max("_modified", 100L);
            super.ds.findAndUpdate(Collection.NODES, up2);
            super.ds.invalidateCache();
            NodeDocument doc = super.ds.find(Collection.NODES, id, 0);
            assertEquals("modified should not have been set back (test iteration " + i + ")", 1000, (long)doc.getModified());
        }
    }

    @Test
    public void testModifyDeletedOnce() {
        // https://issues.apache.org/jira/browse/OAK-3852
        String id = this.getClass().getName() + ".testModifyDeletedOnce";
        // create a test node
        UpdateOp up = new UpdateOp(id, true);
        up.set(NodeDocument.DELETED_ONCE, Boolean.FALSE);
        boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
        assertTrue(success);
        removeMe.add(id);
        NodeDocument nd = super.ds.find(Collection.NODES, id, 0);
        assertNotNull(nd);
        Boolean dovalue = (Boolean)nd.get(NodeDocument.DELETED_ONCE);
        if (dovalue != null) {
            // RDB persistence does not distinguish null and false
            assertEquals(dovalue.booleanValue(), Boolean.FALSE);
        }
    }

    @Test
    public void testInterestingStrings() {
        // https://jira.mongodb.org/browse/JAVA-1305
        boolean repoUsesBadUnicodeAPI = dsf instanceof DocumentStoreFixture.MongoFixture;

        String[] tests = new String[] { "simple:foo", "cr:a\n\b", "dquote:a\"b", "bs:a\\b", "euro:a\u201c", "gclef:\uD834\uDD1E",
                "tab:a\tb", "nul:a\u0000b", "brokensurrogate:\ud800" };

        for (String t : tests) {
            boolean roundTrips = roundtripsThroughJavaUTF8(t);
            if (!roundTrips && repoUsesBadUnicodeAPI) {
                // skip the test because it will fail, see OAK-3683
                break;
            }

            int pos = t.indexOf(":");
            String testname = t.substring(0, pos);
            String test = t.substring(pos + 1);
            String id = this.getClass().getName() + ".testInterestingStrings-" + testname;
            super.ds.remove(Collection.NODES, id);
            UpdateOp up = new UpdateOp(id, true);
            up.set("foo", test);
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            assertTrue("failed to insert a document with property value of " + test + " (" + testname + ") in " + super.dsname
                    + " (JDK roundtripping: " + roundTrips + ")", success);
            // re-read from persistence
            super.ds.invalidateCache();
            NodeDocument nd = super.ds.find(Collection.NODES, id);
            assertEquals(
                    "failure to round-trip " + testname + " through " + super.dsname + " (JDK roundtripping: " + roundTrips + ")",
                    test, nd.get("foo"));
            super.ds.remove(Collection.NODES, id);
        }
    }

    private static boolean roundtripsThroughJavaUTF8(String test) {
        Charset utf8 = Charset.forName("UTF-8");
        byte bytes[] = test.getBytes(utf8);
        return test.equals(new String(bytes, utf8));
    }

    @Test
    public void testCreatePartialFailure() {
        String bid = this.getClass().getName() + ".testCreatePartialFailure-";
        int cnt = 10;
        assertTrue(cnt > 8);

        // clear repo
        for (int i = 0; i < cnt; i++) {
            super.ds.remove(Collection.NODES, bid + i);
            removeMe.add(bid + i);
        }

        // create one of the test nodes
        int pre = cnt / 2;
        UpdateOp up = new UpdateOp(bid + pre, true);
        up.set("foo", "bar");
        assertTrue(super.ds.create(Collection.NODES, Collections.singletonList(up)));

        // batch create
        Set<String> toCreate = new HashSet<String>();
        Set<String> toCreateFailEarly = new HashSet<String>();
        List<UpdateOp> ups = new ArrayList<UpdateOp>();
        for (int i = 0; i < cnt; i++) {
            UpdateOp op = new UpdateOp(bid + i, true);
            op.set("foo", "qux");
            ups.add(op);
            if (i != pre) {
                toCreate.add(bid + i);
            }
            if (i < pre) {
                toCreateFailEarly.add(bid + i);
            }
        }
        assertFalse(super.ds.create(Collection.NODES, ups));

        // check how many nodes are there
        Set<String> created = new HashSet<String>();
        for (int i = 0; i < cnt; i++) {
            boolean present = null != super.ds.find(Collection.NODES, bid + i, 0);
            if (i == pre && !present) {
                fail(super.dsname + ": batch update removed previously existing node " + (bid + i));
            } else if (present && i != pre) {
                created.add(bid + i);
            }
        }

        // diagnostics
        toCreate.removeAll(created);
        if (created.isEmpty()) {
            LOG.info(super.dsname + ": create() apparently is atomic");
        } else if (created.size() == toCreate.size()) {
            LOG.info(super.dsname + ": create() apparently is best-effort");
        } else if (created.equals(toCreateFailEarly)) {
            LOG.info(super.dsname + ": create() stops at first failure");
        } else {
            LOG.info(super.dsname + ": create() created: " + created + ", missing: " + toCreate);
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
    public void testUpdateModified() {
        String id = this.getClass().getName() + ".testUpdateModified";
        // create a test node
        super.ds.remove(Collection.SETTINGS, id);
        UpdateOp up = new UpdateOp(id, true);
        boolean success = super.ds.create(Collection.SETTINGS, Collections.singletonList(up));
        assertTrue(success);
        removeMeSettings.add(id);

        Document d = super.ds.find(Collection.SETTINGS, id);
        Object m = d.get("_modified");
        assertNull("_modified should be null until set", m);

        up = new UpdateOp(id, true);
        up.set("_modified", 123L);
        super.ds.findAndUpdate(Collection.SETTINGS, up); 

        d = super.ds.find(Collection.SETTINGS, id);
        m = d.get("_modified");
        assertNotNull("_modified should now be != null", m);
        assertEquals("123", m.toString());

        up = new UpdateOp(id, true);
        up.max("_modified", 122L);
        super.ds.findAndUpdate(Collection.SETTINGS, up); 

        d = super.ds.find(Collection.SETTINGS, id);
        m = d.get("_modified");
        assertNotNull("_modified should now be != null", m);
        assertEquals("123", m.toString());

        up = new UpdateOp(id, true);
        up.max("_modified", 124L);
        super.ds.findAndUpdate(Collection.SETTINGS, up); 

        ds.invalidateCache();
        d = super.ds.find(Collection.SETTINGS, id);
        m = d.get("_modified");
        assertNotNull("_modified should now be != null", m);
        assertEquals("124", m.toString());
    }

    @Test
    public void testQuery() {
        // create ten documents
        String base = this.getClass().getName() + ".testQuery-";
        for (int i = 0; i < 10; i++) {
            String id = base + i;
            UpdateOp up = new UpdateOp(id, true);
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
    public void testQueryDeletedOnce() {
        // create ten documents
        String base = this.getClass().getName() + ".testQueryDeletedOnce-";
        for (int i = 0; i < 10; i++) {
            String id = base + i;
            UpdateOp up = new UpdateOp(id, true);
            up.set(NodeDocument.DELETED_ONCE, Boolean.valueOf(i % 2 == 0));
            boolean success = super.ds.create(Collection.NODES, Collections.singletonList(up));
            assertTrue("document with " + id + " not created", success);
            removeMe.add(id);
        }

        List<String> result = getKeys(ds.query(Collection.NODES, base, base + "Z", NodeDocument.DELETED_ONCE,
                1L, 1000));
        assertEquals(5, result.size());
        assertTrue(result.contains(base + "0"));
        assertFalse(result.contains(base + "1"));
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
            creates.add(up);
            removeMe.add(id);
            id = base + "/" + c;
            up = new UpdateOp(id, true);
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

    @Test
    public void modCountCondition() {
        String id = this.getClass().getName() + ".modCountCondition";
        removeMe.add(id);
        UpdateOp op = new UpdateOp(id, true);
        op.set("p", "a");
        assertTrue(ds.create(Collection.NODES, Collections.singletonList(op)));
        NodeDocument doc = ds.find(Collection.NODES, id);
        assertNotNull(doc);
        Long modCount = doc.getModCount();
        // can only proceed if store maintains modCount
        assumeNotNull(modCount);
        // check equals (non-matching)
        op = new UpdateOp(id, false);
        op.set("p", "b");
        op.equals(Document.MOD_COUNT, modCount + 1);
        assertNull(ds.findAndUpdate(Collection.NODES, op));
        // check equals (matching)
        op = new UpdateOp(id, false);
        op.set("p", "b");
        op.equals(Document.MOD_COUNT, modCount);
        assertNotNull(ds.findAndUpdate(Collection.NODES, op));
        // validate
        doc = ds.find(Collection.NODES, id);
        assertNotNull(doc);
        assertEquals("b", doc.get("p"));
        modCount = doc.getModCount();
        assertNotNull(modCount);
        // check not equals (non-matching)
        op = new UpdateOp(id, false);
        op.set("p", "c");
        op.notEquals(Document.MOD_COUNT, modCount);
        assertNull(ds.findAndUpdate(Collection.NODES, op));
        // check not equals (matching)
        op = new UpdateOp(id, false);
        op.set("p", "c");
        op.notEquals(Document.MOD_COUNT, modCount + 1);
        assertNotNull(ds.findAndUpdate(Collection.NODES, op));
        // validate
        doc = ds.find(Collection.NODES, id);
        assertNotNull(doc);
        assertEquals("c", doc.get("p"));
    }

    private List<String> getKeys(List<NodeDocument> docs) {
        List<String> result = new ArrayList<String>();
        for (NodeDocument doc : docs) {
            result.add(doc.getId());
        }
        return result;
    }

    private static String generateId(int length, boolean ascii) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            if (ascii) {
                sb.append("0");
            }
            else {
                sb.append(Character.toChars(0x1F4A9));
            }
        }
        return sb.toString();
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

    @Test
    public void description() throws Exception{
        Map<String, String> desc = ds.getMetadata();
        assertNotNull(desc.get("type"));
    }

    @Test
    public void testServerTimeDiff() throws Exception {
        UpdateOp up = new UpdateOp("0:/", true);
        super.ds.create(Collection.NODES, Collections.singletonList(up));
        removeMe.add("0:/");
        long td = super.ds.determineServerTimeDifferenceMillis();
        LOG.info("Server time difference on " + super.dsname + ": " + td + "ms");
    }

    @Test
    public void removeWithCondition() throws Exception {

        Set<String> existingDocs = new HashSet<String>();
        for (NodeDocument doc : Utils.getAllDocuments(ds)) {
            existingDocs.add(doc.getPath());
        }

        List<UpdateOp> docs = Lists.newArrayList();
        docs.add(newDocument("/foo", 100));
        removeMe.add(Utils.getIdFromPath("/foo"));
        docs.add(newDocument("/bar", 200));
        removeMe.add(Utils.getIdFromPath("/bar"));
        docs.add(newDocument("/baz", 300));
        removeMe.add(Utils.getIdFromPath("/baz"));
        ds.create(Collection.NODES, docs);

        for (UpdateOp op : docs) {
            assertNotNull(ds.find(Collection.NODES, op.getId()));
        }

        Map<String, Long> toRemove = Maps.newHashMap();
        toRemove.put(Utils.getIdFromPath("/foo"), 100L); // matches
        toRemove.put(Utils.getIdFromPath("/bar"), 300L); // modified differs
        toRemove.put(Utils.getIdFromPath("/qux"), 100L); // does not exist
        toRemove.put(Utils.getIdFromPath("/baz"), 300L); // matches

        int removed = ds.remove(Collection.NODES, toRemove);

        assertEquals(2, removed);
        assertNotNull(ds.find(Collection.NODES, Utils.getIdFromPath("/bar")));
        for (NodeDocument doc : Utils.getAllDocuments(ds)) {
            if (!doc.getPath().equals("/bar") && !existingDocs.contains(doc.getPath())) {
                fail("document must not exist: " + doc.getId());
            }
        }
    }

    @Test
    public void removeInvalidatesCache() throws Exception {
        String id = Utils.getIdFromPath("/foo");
        long modified = 1;
        removeMe.add(id);
        ds.create(Collection.NODES, Collections.singletonList(newDocument(id, modified)));
        ds.remove(Collection.NODES, Collections.singletonMap(id, modified));
        assertNull(ds.getIfCached(Collection.NODES, id));
    }

    // OAK-3932
    @Test
    public void getIfCachedNonExistingDocument() throws Exception {
        String id = Utils.getIdFromPath("/foo");
        assertNull(ds.find(Collection.NODES, id));
        assertNull(ds.getIfCached(Collection.NODES, id));
    }

    private UpdateOp newDocument(String path, long modified) {
        String id = Utils.getIdFromPath(path);
        UpdateOp op = new UpdateOp(id, true);
        op.set(NodeDocument.MODIFIED_IN_SECS, modified);
        return op;
    }
}

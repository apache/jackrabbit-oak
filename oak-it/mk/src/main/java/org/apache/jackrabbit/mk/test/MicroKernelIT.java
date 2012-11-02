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
package org.apache.jackrabbit.mk.test;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.test.util.TestInputStream;
import org.apache.jackrabbit.mk.util.MicroKernelInputStream;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for verifying that a {@code MicroKernel} implementation
 * obeys the contract of the {@code MicroKernel} API.
 */
@RunWith(Parameterized.class)
public class MicroKernelIT extends AbstractMicroKernelIT {

    public MicroKernelIT(MicroKernelFixture fixture) {
        super(fixture, 1);
    }

    @Override
    protected void addInitialTestContent() {
        mk.commit("/", "+\"test\" : {" +
                "\"stringProp\":\"stringVal\"," +
                "\"intProp\":42," +
                "\"floatProp\":42.2," +
                "\"booleanProp\": true," +
                "\"multiIntProp\":[1,2,3]}", null, "");
    }

    @Test
    public void revisionOps() {
        String head = mk.getHeadRevision();
        assertNotNull(head);

        try {
            Thread.sleep(100);
        } catch (InterruptedException ignore) {
        }

        long now = System.currentTimeMillis();

        // get history since 'now'
        JSONArray array = parseJSONArray(mk.getRevisionHistory(now, -1, null));
        // history should be empty since there was no commit since 'now'
        assertEquals(0, array.size());

        // get oldest available revision
        array = parseJSONArray(mk.getRevisionHistory(0, 1, null));
        // there should be exactly 1 revision
        assertEquals(1, array.size());

        long ts0 = System.currentTimeMillis();

        final int NUM_COMMITS = 100;

        // perform NUM_COMMITS commits
        for (int i = 0; i < NUM_COMMITS; i++) {
            mk.commit("/test", "+\"child" + i + "\":{}", null, "commit#" + i);
        }

        // get oldest available revision
        array = parseJSONArray(mk.getRevisionHistory(ts0, -1, null));
        // there should be exactly NUM_COMMITS revisions
        assertEquals(NUM_COMMITS, array.size());
        long previousTS = ts0;
        for (int i = 0; i < NUM_COMMITS; i++) {
            JSONObject rev = getObjectArrayEntry(array, i);
            assertPropertyExists(rev, "id", String.class);
            assertPropertyExists(rev, "ts", Long.class);
            // verify commit msg
            assertPropertyValue(rev, "msg", "commit#" + i);
            // verify chronological order
            long ts = (Long) resolveValue(rev, "ts");
            assertTrue(previousTS <= ts);
            previousTS = ts;
        }

        // last revision should be the current head revision
        assertPropertyValue(getObjectArrayEntry(array, array.size() - 1), "id", mk.getHeadRevision());

        String fromRev = (String) resolveValue(getObjectArrayEntry(array, 0), "id");
        String toRev = (String) resolveValue(getObjectArrayEntry(array, array.size() - 1), "id");

        // verify journal
        array = parseJSONArray(mk.getJournal(fromRev, toRev, ""));
        // there should be exactly NUM_COMMITS entries
        assertEquals(NUM_COMMITS, array.size());
        // verify that 1st and last rev match fromRev and toRev
        assertPropertyValue(getObjectArrayEntry(array, 0), "id", fromRev);
        assertPropertyValue(getObjectArrayEntry(array, array.size() - 1), "id", toRev);

        previousTS = ts0;
        for (int i = 0; i < NUM_COMMITS; i++) {
            JSONObject rev = getObjectArrayEntry(array, i);
            assertPropertyExists(rev, "id", String.class);
            assertPropertyExists(rev, "ts", Long.class);
            assertPropertyExists(rev, "changes", String.class);
            // TODO verify json diff
            // verify commit msg
            assertPropertyValue(rev, "msg", "commit#" + i);
            // verify chronological order
            long ts = (Long) resolveValue(rev, "ts");
            assertTrue(previousTS <= ts);
            previousTS = ts;
        }

        // test with 'negative' range (from and to swapped)
        array = parseJSONArray(mk.getJournal(toRev, fromRev, ""));
        // there should be exactly 0 entries
        assertEquals(0, array.size());
    }

    @Test
    public void revisionOpsFiltered() {
        // test getRevisionHistory/getJournal path filter
        mk.commit("/test", "+\"foo\":{} +\"bar\":{}", null, "");

        try {
            Thread.sleep(100);
        } catch (InterruptedException ignore) {
        }

        long ts = System.currentTimeMillis();

        String revFoo = mk.commit("/test/foo", "^\"p1\":123", null, "");
        String revBar = mk.commit("/test/bar", "^\"p2\":456", null, "");
        String rev0 = revFoo;

        // get history since ts (no filter)
        JSONArray array = parseJSONArray(mk.getRevisionHistory(ts, -1, null));
        // history should contain 2 commits: revFoo and revBar
        assertEquals(2, array.size());
        assertPropertyValue(getObjectArrayEntry(array, 0), "id", revFoo);
        assertPropertyValue(getObjectArrayEntry(array, 1), "id", revBar);

        // get history since ts (non-matching filter)
        array = parseJSONArray(mk.getRevisionHistory(ts, -1, "/blah"));
        // history should contain 0 commits since filter doesn't match
        assertEquals(0, array.size());

        // get history since ts (filter on /test/bar)
        array = parseJSONArray(mk.getRevisionHistory(ts, -1, "/test/bar"));
        // history should contain 1 commit: revBar
        assertEquals(1, array.size());
        assertPropertyValue(getObjectArrayEntry(array, 0), "id", revBar);

        // get journal (no filter)
        array = parseJSONArray(mk.getJournal(rev0, null, ""));
        // journal should contain 2 commits: revFoo and revBar
        assertEquals(2, array.size());
        assertPropertyValue(getObjectArrayEntry(array, 0), "id", revFoo);
        assertPropertyValue(getObjectArrayEntry(array, 1), "id", revBar);

        // get journal (non-matching filter)
        array = parseJSONArray(mk.getJournal(rev0, null, "/blah"));
        // journal should contain 0 commits since filter doesn't match
        assertEquals(0, array.size());

        // get journal (filter on /test/bar)
        array = parseJSONArray(mk.getJournal(rev0, null, "/test/bar"));
        // journal should contain 1 commit: revBar
        assertEquals(1, array.size());
        assertPropertyValue(getObjectArrayEntry(array, 0), "id", revBar);
        String diff = (String) resolveValue(getObjectArrayEntry(array, 0), "changes");
        assertNotNull(diff);
        assertTrue(diff.contains("456"));
        assertFalse(diff.contains("123"));
    }

    @Test
    public void diff() {
        String rev0 = mk.getHeadRevision();

        String rev1 = mk.commit("/test", "+\"target\":{}", null, null);
        assertTrue(mk.nodeExists("/test/target", null));

        // get reverse diff
        String reverseDiff = mk.diff(rev1, rev0, null, -1);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // commit reverse diff
        String rev2 = mk.commit("", reverseDiff, null, null);
        assertFalse(mk.nodeExists("/test/target", null));

        // diff of rev0->rev2 should be empty
        assertEquals("", mk.diff(rev0, rev2, null, -1));
    }

    @Test
    public void diffFiltered() {
        String rev0 = mk.commit("/test", "+\"foo\":{} +\"bar\":{}", null, "");

        String rev1 = mk.commit("/test/foo", "^\"p1\":123", null, "");
        String rev2 = mk.commit("/test/bar", "^\"p2\":456", null, "");

        // test with path filter
        String diff = mk.diff(rev0, rev2, "/test", -1);
        assertNotNull(diff);
        assertFalse(diff.isEmpty());
        assertTrue(diff.contains("foo"));
        assertTrue(diff.contains("bar"));
        assertTrue(diff.contains("123"));
        assertTrue(diff.contains("456"));

        diff = mk.diff(rev0, rev2, "/test/foo", -1);
        assertNotNull(diff);
        assertFalse(diff.isEmpty());
        assertTrue(diff.contains("foo"));
        assertFalse(diff.contains("bar"));
        assertTrue(diff.contains("123"));
        assertFalse(diff.contains("456"));

        // non-matching filter
        diff = mk.diff(rev0, rev2, "/blah", -1);
        assertNotNull(diff);
        assertTrue(diff.isEmpty());
    }

    @Test
    public void diffDepthLimited() {
        // initial content (/test/foo)
        String rev0 = mk.commit("/test", "+\"foo\":{}", null, "");

        // add /test/foo/bar
        String rev1 = mk.commit("/test/foo", "+\"bar\":{\"p1\":123}", null, "");

        // modify property /test/foo/bar/p1
        String rev2 = mk.commit("/test/foo/bar", "^\"p1\":456", null, "");

        // diff with depth -1
        assertEquals(mk.diff(rev0, rev2, "/", Integer.MAX_VALUE), mk.diff(rev0, rev2, "/", -1));

        // diff with depth 5
        String diff = mk.diff(rev0, rev2, "/", 5);
        // returned +"/test/foo/bar":{"p1":456}
        assertNotNull(diff);
        assertFalse(diff.isEmpty());
        assertTrue(diff.contains("/test/foo/bar"));
        assertTrue(diff.contains("456"));

        // diff with depth 0
        diff = mk.diff(rev0, rev2, "/", 0);
        // returned ^"/test", indicating that there are changes below /test
        assertNotNull(diff);
        assertFalse(diff.isEmpty());
        assertFalse(diff.contains("/test/foo"));
        assertFalse(diff.contains("456"));
        assertTrue(diff.contains("/test"));
        assertTrue(diff.startsWith("^"));

        // diff with depth 1
        diff = mk.diff(rev0, rev2, "/", 1);
        // returned ^"/test/foo", indicating that there are changes below /test/foo
        assertNotNull(diff);
        assertFalse(diff.isEmpty());
        assertFalse(diff.contains("/test/foo/bar"));
        assertFalse(diff.contains("456"));
        assertTrue(diff.contains("/test/foo"));
        assertTrue(diff.startsWith("^"));
    }

    @Test
    public void snapshotIsolation() {
        final int NUM_COMMITS = 100;

        String[] revs = new String[NUM_COMMITS];

        // perform NUM_COMMITS commits
        for (int i = 0; i < NUM_COMMITS; i++) {
            revs[i] = mk.commit("/test", "^\"cnt\":" + i, null, null);
        }
        // verify that each revision contains the expected distinct property value
        for (int i = 0; i < NUM_COMMITS; i++) {
            JSONObject obj = parseJSONObject(mk.getNodes("/test", revs[i], 1, 0, -1, null));
            assertPropertyValue(obj, "cnt", (long) i);
        }
    }

    @Test
    public void waitForCommit() throws InterruptedException {
        final long SHORT_TIMEOUT = 1000;
        final long LONG_TIMEOUT = 1000;

        // concurrent commit
        String oldHead = mk.getHeadRevision();

        Thread t = new Thread("") {
            @Override
            public void run() {
                String newHead = mk.commit("/", "+\"foo\":{}", null, "");
                setName(newHead);
            }
        };
        t.start();
        String newHead = mk.waitForCommit(oldHead, LONG_TIMEOUT);
        t.join();

        assertFalse(oldHead.equals(newHead));
        assertEquals(newHead, t.getName());
        assertEquals(newHead, mk.getHeadRevision());

        // the current head is already more recent than oldRevision;
        // the method should return immediately (TIMEOUT not applied)
        String currentHead = mk.getHeadRevision();
        long t0 = System.currentTimeMillis();
        newHead = mk.waitForCommit(oldHead, LONG_TIMEOUT);
        long t1 = System.currentTimeMillis();
        assertTrue((t1 - t0) < LONG_TIMEOUT);
        assertEquals(currentHead, newHead);

        // there's no more recent head available;
        // the method should wait for the given short timeout
        currentHead = mk.getHeadRevision();
        long t2 = System.currentTimeMillis();
        newHead = mk.waitForCommit(currentHead, SHORT_TIMEOUT);
        long t3 = System.currentTimeMillis();
        assertTrue((t3 - t2) >= SHORT_TIMEOUT);
        assertEquals(currentHead, newHead);
    }

    @Test
    public void addAndMove() {
        String head = mk.getHeadRevision();
        head = mk.commit("",
                "+\"/root\":{}\n" +
                        "+\"/root/a\":{}\n",
                head, "");

        head = mk.commit("",
                "+\"/root/a/b\":{}\n" +
                        ">\"/root/a\":\"/root/c\"\n",
                head, "");

        assertFalse(mk.nodeExists("/root/a", head));
        assertTrue(mk.nodeExists("/root/c/b", head));
    }

    @Test
    public void copy() {
        mk.commit("/", "*\"test\":\"testCopy\"", null, "");

        assertTrue(mk.nodeExists("/testCopy", null));
        assertTrue(mk.nodeExists("/test", null));

        JSONObject obj = parseJSONObject(mk.getNodes("/test", null, 99, 0, -1, null));
        JSONObject obj1 = parseJSONObject(mk.getNodes("/testCopy", null, 99, 0, -1, null));
        assertEquals(obj, obj1);
    }

    @Test
    public void addAndCopy() {
        mk.commit("/",
                "+\"x\":{}\n" +
                        "+\"y\":{}\n",
                null, "");

        mk.commit("/",
                "+\"x/a\":{}\n" +
                        "*\"x\":\"y/x1\"\n",
                null, "");

        assertTrue(mk.nodeExists("/x/a", null));
        assertTrue(mk.nodeExists("/y/x1/a", null));
    }

    @Test
    public void copyToDescendant() {
        mk.commit("/",
                "+\"test/child\":{}\n" +
                        "*\"test\":\"test/copy\"\n",
                null, "");

        assertTrue(mk.nodeExists("/test/child", null));
        assertTrue(mk.nodeExists("/test/copy/child", null));
        JSONObject obj = parseJSONObject(mk.getNodes("/test", null, 99, 0, -1, null));
        assertPropertyValue(obj, ":childNodeCount", 2l);
        assertPropertyValue(obj, "copy/:childNodeCount", 1l);
        assertPropertyValue(obj, "copy/child/:childNodeCount", 0l);

        mk.commit("", "+\"/root\":{} +\"/root/N4\":{} *\"/root/N4\":\"/root/N4/N5\"", null, null);
        assertTrue(mk.nodeExists("/root", null));
        assertTrue(mk.nodeExists("/root/N4", null));
        assertTrue(mk.nodeExists("/root/N4/N5", null));
        obj = parseJSONObject(mk.getNodes("/root", null, 99, 0, -1, null));
        assertPropertyValue(obj, ":childNodeCount", 1l);
        assertPropertyValue(obj, "N4/:childNodeCount", 1l);
        assertPropertyValue(obj, "N4/N5/:childNodeCount", 0l);
    }

    @Test
    public void getNodes() {
        String head = mk.getHeadRevision();

        // verify initial content
        JSONObject obj = parseJSONObject(mk.getNodes("/", head, 1, 0, -1, null));
        assertPropertyValue(obj, "test/stringProp", "stringVal");
        assertPropertyValue(obj, "test/intProp", 42L);
        assertPropertyValue(obj, "test/floatProp", 42.2);
        assertPropertyValue(obj, "test/booleanProp", true);
        assertPropertyValue(obj, "test/multiIntProp", new Object[]{1, 2, 3});
    }

    @Test
    public void getNodesHash() {
        // :hash must be explicitly specified in the filter
        JSONObject obj = parseJSONObject(mk.getNodes("/", null, 1, 0, -1, null));
        assertPropertyNotExists(obj, ":hash");
        obj = parseJSONObject(mk.getNodes("/", null, 1, 0, -1, "{\"properties\":[\"*\"]}"));
        assertPropertyNotExists(obj, ":hash");

        // verify initial content with :hash property
        obj = parseJSONObject(mk.getNodes("/", null, 1, 0, -1, "{\"properties\":[\"*\",\":hash\"]}"));
        assertPropertyValue(obj, "test/booleanProp", true);

        if (obj.get(":hash") == null) {
            // :hash is optional, an implementation might not support it
            return;
        }

        assertPropertyExists(obj, ":hash", String.class);
        String hash0 = (String) resolveValue(obj, ":hash");

        // modify a property and verify that the hash of the root node changed
        mk.commit("/test", "^\"booleanProp\":false", null, null);
        obj = parseJSONObject(mk.getNodes("/", null, 1, 0, -1, "{\"properties\":[\"*\",\":hash\"]}"));
        assertPropertyValue(obj, "test/booleanProp", false);

        assertPropertyExists(obj, ":hash", String.class);
        String hash1 = (String) resolveValue(obj, ":hash");

        assertFalse(hash0.equals(hash1));

        // undo property modification and verify that the hash
        // of the root node is now the same as before the modification
        mk.commit("/test", "^\"booleanProp\":true", null, null);
        obj = parseJSONObject(mk.getNodes("/", null, 1, 0, -1, "{\"properties\":[\"*\",\":hash\"]}"));
        assertPropertyValue(obj, "test/booleanProp", true);

        assertPropertyExists(obj, ":hash", String.class);
        String hash2 = (String) resolveValue(obj, ":hash");

        assertFalse(hash1.equals(hash2));
        assertTrue(hash0.equals(hash2));
    }

    @Test
    public void getNodesChildNodeCount() {
        // :childNodeCount is included per default
        JSONObject obj = parseJSONObject(mk.getNodes("/", null, 1, 0, -1, null));
        assertPropertyExists(obj, ":childNodeCount", Long.class);
        assertPropertyExists(obj, "test/:childNodeCount", Long.class);
        long count = (Long) resolveValue(obj, ":childNodeCount") ;
        assertEquals(count, mk.getChildNodeCount("/", null));

        obj = parseJSONObject(mk.getNodes("/", null, 1, 0, -1, "{\"properties\":[\"*\"]}"));
        assertPropertyExists(obj, ":childNodeCount", Long.class);
        assertPropertyExists(obj, "test/:childNodeCount", Long.class);
        count = (Long) resolveValue(obj, ":childNodeCount") ;
        assertEquals(count, mk.getChildNodeCount("/", null));

        // explicitly exclude :childNodeCount
        obj = parseJSONObject(mk.getNodes("/", null, 1, 0, -1, "{\"properties\":[\"*\",\"-:childNodeCount\"]}"));
        assertPropertyNotExists(obj, ":childNodeCount");
        assertPropertyNotExists(obj, "test/:childNodeCount");
    }

    @Test
    public void getNodesFiltered() {
        String head = mk.getHeadRevision();

        // verify initial content using filter
        String filter = "{ \"properties\" : [ \"*ntProp\", \"-mult*\" ] } ";
        JSONObject obj = parseJSONObject(mk.getNodes("/", head, 1, 0, -1, filter));
        assertPropertyExists(obj, "test/intProp");
        assertPropertyNotExists(obj, "test/multiIntProp");
        assertPropertyNotExists(obj, "test/stringProp");
        assertPropertyNotExists(obj, "test/floatProp");
        assertPropertyNotExists(obj, "test/booleanProp");
    }

    @Test
    public void getNodesNonExistingPath() {
        String head = mk.getHeadRevision();

        String nonExistingPath = "/test/" + System.currentTimeMillis();
        assertFalse(mk.nodeExists(nonExistingPath, head));

        assertNull(mk.getNodes(nonExistingPath, head, 0, 0, -1, null));
    }

    @Test
    public void getNodesNonExistingRevision() {
        String nonExistingRev = "12345678";

        try {
            mk.nodeExists("/test", nonExistingRev);
            fail("Success with non-existing revision: " + nonExistingRev);
        } catch (MicroKernelException e) {
            // expected
        }

        try {
            mk.getNodes("/test", nonExistingRev, 0, 0, -1, null);
            fail("Success with non-existing revision: " + nonExistingRev);
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void getNodesDepth() {
        mk.commit("", "+\"/testRoot\":{\"depth\":0}", null, "");
        mk.commit("/testRoot", "+\"a\":{\"depth\":1}\n" +
                "+\"a/b\":{\"depth\":2}\n" +
                "+\"a/b/c\":{\"depth\":3}\n" +
                "+\"a/b/c/d\":{\"depth\":4}\n" +
                "+\"a/b/c/d/e\":{\"depth\":5}\n",
                null, "");

        // depth = 0: properties, including :childNodeCount and empty child node objects
        JSONObject obj = parseJSONObject(mk.getNodes("/testRoot", null, 0, 0, -1, null));
        assertPropertyValue(obj, "depth", 0l);
        assertPropertyValue(obj, ":childNodeCount", 1l);
        JSONObject child = resolveObjectValue(obj, "a");
        assertNotNull(child);
        assertEquals(0, child.size());

        // depth = 1: properties, child nodes, their properties (including :childNodeCount)
        // and their empty child node objects
        obj = parseJSONObject(mk.getNodes("/testRoot", null, 1, 0, -1, null));
        assertPropertyValue(obj, "depth", 0l);
        assertPropertyValue(obj, ":childNodeCount", 1l);
        assertPropertyValue(obj, "a/depth", 1l);
        assertPropertyValue(obj, "a/:childNodeCount", 1l);
        child = resolveObjectValue(obj, "a/b");
        assertNotNull(child);
        assertEquals(0, child.size());

        // depth = 2: [and so on...]
        obj = parseJSONObject(mk.getNodes("/testRoot", null, 2, 0, -1, null));
        assertPropertyValue(obj, "depth", 0l);
        assertPropertyValue(obj, ":childNodeCount", 1l);
        assertPropertyValue(obj, "a/depth", 1l);
        assertPropertyValue(obj, "a/:childNodeCount", 1l);
        assertPropertyValue(obj, "a/b/depth", 2l);
        assertPropertyValue(obj, "a/b/:childNodeCount", 1l);
        child = resolveObjectValue(obj, "a/b/c");
        assertNotNull(child);
        assertEquals(0, child.size());

        // depth = 3: [and so on...]
        obj = parseJSONObject(mk.getNodes("/testRoot", null, 3, 0, -1, null));
        assertPropertyValue(obj, "depth", 0l);
        assertPropertyValue(obj, ":childNodeCount", 1l);
        assertPropertyValue(obj, "a/depth", 1l);
        assertPropertyValue(obj, "a/:childNodeCount", 1l);
        assertPropertyValue(obj, "a/b/depth", 2l);
        assertPropertyValue(obj, "a/b/:childNodeCount", 1l);
        assertPropertyValue(obj, "a/b/c/depth", 3l);
        assertPropertyValue(obj, "a/b/c/:childNodeCount", 1l);
        child = resolveObjectValue(obj, "a/b/c/d");
        assertNotNull(child);
        assertEquals(0, child.size());

        // getNodes(path, revId) must return same result as getNodes(path, revId, 1, 0, -1, null)
        obj = parseJSONObject(mk.getNodes("/testRoot", null, 1, 0, -1, null));
        JSONObject obj1 = parseJSONObject(mk.getNodes("/testRoot", null, 1, 0, -1, null));
        assertEquals(obj, obj1);
    }

    @Test
    public void getNodesOffset() {
        // number of siblings (multiple of 10)
        final int NUM_SIBLINGS = 1000;
        // set of all sibling names
        final Set<String> siblingNames = new HashSet<String>(NUM_SIBLINGS);

        // populate siblings
        Random rand = new Random();
        StringBuffer sb = new StringBuffer("+\"/testRoot\":{");
        for (int i = 0; i < NUM_SIBLINGS; i++) {
            String name = "n" + rand.nextLong();
            siblingNames.add(name);
            sb.append("\n\"");
            sb.append(name);
            sb.append("\":{}");
            if (i < NUM_SIBLINGS - 1) {
                sb.append(',');
            }
        }
        sb.append("\n}");
        String head = mk.commit("", sb.toString(), null, "");

        // get all siblings in one call
        JSONObject obj = parseJSONObject(mk.getNodes("/testRoot", head, 0, 0, -1, null));
        assertPropertyValue(obj, ":childNodeCount", (long) NUM_SIBLINGS);
        assertEquals((long) NUM_SIBLINGS, mk.getChildNodeCount("/testRoot", head));
        assertEquals(siblingNames, getNodeNames(obj));

        // list of sibling names in iteration order
        final List<String> orderedSiblingNames = new ArrayList<String>(NUM_SIBLINGS);

        // get siblings one by one
        for (int i = 0; i < NUM_SIBLINGS; i++) {
            obj = parseJSONObject(mk.getNodes("/testRoot", head, 0, i, 1, null));
            assertPropertyValue(obj, ":childNodeCount", (long) NUM_SIBLINGS);
            Set<String> set = getNodeNames(obj);
            assertEquals(1, set.size());
            orderedSiblingNames.add(set.iterator().next());
        }

        // check completeness
        Set<String> names = new HashSet<String>(siblingNames);
        names.removeAll(orderedSiblingNames);
        assertTrue(names.isEmpty());

        // we've now established the expected iteration order

        // get siblings in 10 chunks
        for (int i = 0; i < 10; i++) {
            obj = parseJSONObject(mk.getNodes("/testRoot", head, 0, i * 10, NUM_SIBLINGS / 10, null));
            assertPropertyValue(obj, ":childNodeCount", (long) NUM_SIBLINGS);
            names = getNodeNames(obj);
            assertEquals(NUM_SIBLINGS / 10, names.size());
            List<String> subList = orderedSiblingNames.subList(i * 10, (i * 10) + (NUM_SIBLINGS / 10));
            names.removeAll(subList);
            assertTrue(names.isEmpty());
        }

        // test offset with filter
        try {
            parseJSONObject(mk.getNodes("/testRoot", head, 0, 10, NUM_SIBLINGS / 10, "{\"nodes\":[\"n0*\"]}"));
            fail();
        } catch (Throwable e) {
            // expected
        }
    }

    @Test
    public void getNodesMaxNodeCount() {
        // number of siblings
        final int NUM_SIBLINGS = 100;

        // populate siblings
        StringBuffer sb = new StringBuffer("+\"/testRoot\":{");
        for (int i = 0; i < NUM_SIBLINGS; i++) {
            String name = "n" + i;
            sb.append("\n\"");
            sb.append(name);
            sb.append("\":{");

            for (int n = 0; n < NUM_SIBLINGS; n++) {
                String childName = "n" + n;
                sb.append("\n\"");
                sb.append(childName);
                sb.append("\":{}");
                if (n < NUM_SIBLINGS - 1) {
                    sb.append(',');
                }
            }
            sb.append("}");
            if (i < NUM_SIBLINGS - 1) {
                sb.append(',');
            }
        }
        sb.append("\n}");
        String head = mk.commit("", sb.toString(), null, "");

        // get all siblings
        JSONObject obj = parseJSONObject(mk.getNodes("/testRoot", head, 1, 0, -1, null));
        assertPropertyValue(obj, ":childNodeCount", (long) NUM_SIBLINGS);
        assertEquals((long) NUM_SIBLINGS, mk.getChildNodeCount("/testRoot", head));
        Set<String> names = getNodeNames(obj);
        assertTrue(names.size() == NUM_SIBLINGS);
        String childName = names.iterator().next();
        JSONObject childObj = resolveObjectValue(obj, childName);
        assertPropertyValue(childObj, ":childNodeCount", (long) NUM_SIBLINGS);
        assertTrue(getNodeNames(childObj).size() == NUM_SIBLINGS);

        // get max 10 siblings
        int maxSiblings = 10;
        obj = parseJSONObject(mk.getNodes("/testRoot", head, 1, 0, maxSiblings, null));
        assertPropertyValue(obj, ":childNodeCount", (long) NUM_SIBLINGS);
        assertEquals((long) NUM_SIBLINGS, mk.getChildNodeCount("/testRoot", head));
        names = getNodeNames(obj);
        assertEquals(maxSiblings, names.size());
        childName = names.iterator().next();
        childObj = resolveObjectValue(obj, childName);
        assertPropertyValue(childObj, ":childNodeCount", (long) NUM_SIBLINGS);
        assertTrue(getNodeNames(childObj).size() == maxSiblings);

        // get max 5 siblings using filter
        maxSiblings = 5;
        obj = parseJSONObject(mk.getNodes("/testRoot", head, 1, 0, maxSiblings, "{\"nodes\":[\"n1*\"]}"));
        assertPropertyValue(obj, ":childNodeCount", (long) NUM_SIBLINGS);
        assertEquals((long) NUM_SIBLINGS, mk.getChildNodeCount("/testRoot", head));
        names = getNodeNames(obj);
        assertTrue(names.size() == maxSiblings);
        childName = names.iterator().next();
        childObj = resolveObjectValue(obj, childName);
        assertPropertyValue(childObj, ":childNodeCount", (long) NUM_SIBLINGS);
        assertTrue(getNodeNames(childObj).size() == maxSiblings);
    }

    @Test
    public void getNodesRevision() {
        // 1st pass
        long tst = System.currentTimeMillis();
        String head = mk.commit("/test", "^\"tst\":" + tst, null, null);
        assertEquals(head, mk.getHeadRevision());
        // test getNodes with 'null' revision
        assertPropertyValue(parseJSONObject(mk.getNodes("/test", null, 1, 0, -1, null)), "tst", tst);
        // test getNodes with specific revision
        assertPropertyValue(parseJSONObject(mk.getNodes("/test", head, 1, 0, -1, null)), "tst", tst);

        // 2nd pass
        ++tst;
        String oldHead = head;
        head = mk.commit("/test", "^\"tst\":" + tst, null, null);
        assertFalse(head.equals(oldHead));
        assertEquals(head, mk.getHeadRevision());
        // test getNodes with 'null' revision
        assertPropertyValue(parseJSONObject(mk.getNodes("/test", null, 1, 0, -1, null)), "tst", tst);
        // test getNodes with specific revision
        assertPropertyValue(parseJSONObject(mk.getNodes("/test", head, 1, 0, -1, null)), "tst", tst);
    }

    @Test
    public void missingName() {
        String head = mk.getHeadRevision();

        assertTrue(mk.nodeExists("/test", head));
        try {
            String path = "/test/";
            mk.getNodes(path, head, 1, 0, -1, null);
            fail("Success with invalid path: " + path);
        } catch (AssertionError e) {
            // expected
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void addNodeWithRelativePath() {
        String head = mk.getHeadRevision();

        head = mk.commit("/", "+\"foo\" : {} \n+\"foo/bar\" : {}", head, "");
        assertTrue(mk.nodeExists("/foo", head));
        assertTrue(mk.nodeExists("/foo/bar", head));
    }

    @Test
    public void commitWithEmptyPath() {
        String head = mk.getHeadRevision();

        head = mk.commit("", "+\"/ene\" : {}\n+\"/ene/mene\" : {}\n+\"/ene/mene/muh\" : {}", head, "");
        assertTrue(mk.nodeExists("/ene/mene/muh", head));
    }

    @Test
    public void addPropertyWithRelativePath() {
        String head = mk.getHeadRevision();

        head = mk.commit("/",
                "+\"fuu\" : {} \n" +
                        "^\"fuu/bar\" : 42", head, "");
        JSONObject obj = parseJSONObject(mk.getNodes("/fuu", head, 1, 0, -1, null));
        assertPropertyValue(obj, "bar", 42L);
    }

    @Test
    public void addMultipleNodes() {
        String head = mk.getHeadRevision();

        long millis = System.currentTimeMillis();
        String node1 = "n1_" + millis;
        String node2 = "n2_" + millis;
        head = mk.commit("/", "+\"" + node1 + "\" : {} \n+\"" + node2 + "\" : {}\n", head, "");
        assertTrue(mk.nodeExists('/' + node1, head));
        assertTrue(mk.nodeExists('/' + node2, head));
    }

    @Test
    public void addDeepNodes() {
        String head = mk.getHeadRevision();

        head = mk.commit("/",
                "+\"a\" : {} \n" +
                        "+\"a/b\" : {} \n" +
                        "+\"a/b/c\" : {} \n" +
                        "+\"a/b/c/d\" : {} \n",
                head, "");

        assertTrue(mk.nodeExists("/a", head));
        assertTrue(mk.nodeExists("/a/b", head));
        assertTrue(mk.nodeExists("/a/b/c", head));
        assertTrue(mk.nodeExists("/a/b/c/d", head));
    }

    @Test
    public void addItemsIncrementally() {
        String head = mk.getHeadRevision();

        String node = "n_" + System.currentTimeMillis();

        head = mk.commit("/",
                "+\"" + node + "\" : {} \n" +
                        "+\"" + node + "/child1\" : {} \n" +
                        "+\"" + node + "/child2\" : {} \n" +
                        "+\"" + node + "/child1/grandchild11\" : {} \n" +
                        "^\"" + node + "/prop1\" : 41\n" +
                        "^\"" + node + "/child1/prop2\" : 42\n" +
                        "^\"" + node + "/child1/grandchild11/prop3\" : 43",
                head, "");

        JSONObject obj = parseJSONObject(mk.getNodes('/' + node, head, 3, 0, -1, null));
        assertPropertyValue(obj, "prop1", 41L);
        assertPropertyValue(obj, ":childNodeCount", 2L);
        assertPropertyValue(obj, "child1/prop2", 42L);
        assertPropertyValue(obj, "child1/:childNodeCount", 1L);
        assertPropertyValue(obj, "child1/grandchild11/prop3", 43L);
        assertPropertyValue(obj, "child1/grandchild11/:childNodeCount", 0L);
        assertPropertyValue(obj, "child2/:childNodeCount", 0L);
    }

    @Test
    public void removeNode() {
        String head = mk.getHeadRevision();
        String node = "removeNode_" + System.currentTimeMillis();

        head = mk.commit("/", "+\"" + node + "\" : {\"child\":{}}", head, "");

        head = mk.commit('/' + node, "-\"child\"", head, "");
        JSONObject obj = parseJSONObject(mk.getNodes('/' + node, head, 1, 0, -1, null));
        assertPropertyValue(obj, ":childNodeCount", 0L);
    }

    @Test
    public void moveNode() {
        String rev0 = mk.getHeadRevision();
        JSONObject obj = parseJSONObject(mk.getNodes("/test", null, 99, 0, -1, null));
        mk.commit("/", ">\"test\" : \"moved\"", null, "");
        assertTrue(mk.nodeExists("/test", rev0));
        assertFalse(mk.nodeExists("/test", null));
        assertTrue(mk.nodeExists("/moved", null));
        JSONObject obj1 = parseJSONObject(mk.getNodes("/moved", null, 99, 0, -1, null));
        assertEquals(obj, obj1);
    }

    @Test
    public void illegalMove() {
        mk.commit("", "+\"/test/sub\":{}", null, "");
        try {
            // try to move /test to /test/sub/test
            mk.commit("/", "> \"test\": \"/test/sub/test\"", null, "");
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void overwritingMove() {
        String head = mk.getHeadRevision();

        head = mk.commit("/", "+\"a\" : {} \n+\"b\" : {} \n", head, "");
        try {
            mk.commit("/", ">\"a\" : \"b\"  ", head, "");
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void conflictingMove() {
        String head = mk.getHeadRevision();

        head = mk.commit("/", "+\"a\" : {} \n+\"b\" : {}\n", head, "");

        String r1 = mk.commit("/", ">\"a\" : \"b/a\"", head, "");
        assertFalse(mk.nodeExists("/a", r1));
        assertTrue(mk.nodeExists("/b", r1));
        assertTrue(mk.nodeExists("/b/a", r1));

        try {
            mk.commit("/", ">\"b\" : \"a/b\"", head, "");
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void conflictingAddDelete() {
        String head = mk.getHeadRevision();

        head = mk.commit("/", "+\"a\" : {} \n+\"b\" : {}\n", head, "");

        String r1 = mk.commit("/", "-\"b\" \n +\"a/x\" : {}", head, "");
        assertFalse(mk.nodeExists("/b", r1));
        assertTrue(mk.nodeExists("/a", r1));
        assertTrue(mk.nodeExists("/a/x", r1));

        try {
            mk.commit("/", "-\"a\" \n +\"b/x\" : {}", head, "");
            fail();
        } catch (MicroKernelException e) {
            // expected
        }
    }

    @Test
    public void removeProperty() {
        String head = mk.getHeadRevision();
        long t = System.currentTimeMillis();
        String node = "removeProperty_" + t;

        head = mk.commit("/", "+\"" + node + "\" : {\"prop\":\"value\"}", head, "");

        head = mk.commit("/", "^\"" + node + "/prop\" : null", head, "");
        JSONObject obj = parseJSONObject(mk.getNodes('/' + node, head, 1, 0, -1, null));
        assertPropertyValue(obj, ":childNodeCount", 0L);
    }

    @Test
    public void branchAndMerge() {
        // make sure /branch doesn't exist in head
        assertFalse(mk.nodeExists("/branch", null));

        // create a branch on head
        String branchRev = mk.branch(null);
        String branchRootRev = branchRev;

        // add a node /branch in branchRev
        branchRev = mk.commit("", "+\"/branch\":{}", branchRev, "");
        // make sure /branch doesn't exist in head
        assertFalse(mk.nodeExists("/branch", null));
        // make sure /branch does exist in branchRev
        assertTrue(mk.nodeExists("/branch", branchRev));

        // add a node /branch/foo in branchRev
        branchRev = mk.commit("", "+\"/branch/foo\":{}", branchRev, "");

        // make sure branchRev doesn't show up in revision history
        String hist = mk.getRevisionHistory(0, -1, null);
        JSONArray array = parseJSONArray(hist);
        for (Object entry : array) {
            assertTrue(entry instanceof JSONObject);
            JSONObject rev = (JSONObject) entry;
            assertFalse(branchRev.equals(rev.get("id")));
        }

        // add a node /test123 in head
        mk.commit("", "+\"/test123\":{}", null, "");
        // make sure /test123 doesn't exist in branchRev
        assertFalse(mk.nodeExists("/test123", branchRev));

        // merge branchRev with head
        String newHead = mk.merge(branchRev, "");
        // make sure /test123 still exists in head
        assertTrue(mk.nodeExists("/test123", null));
        // make sure /branch/foo does now exist in head
        assertTrue(mk.nodeExists("/branch/foo", null));

        try {
            mk.getJournal(branchRootRev, null, "/");
            fail("getJournal should throw for branch revisions");
        } catch (MicroKernelException e) {
            // expected
        }
        try {
            mk.getJournal(branchRootRev, branchRev, "/");
            fail("getJournal should throw for branch revisions");
        } catch (MicroKernelException e) {
            // expected
        }

        String jrnl = mk.getJournal(newHead, newHead, "/");
        array = parseJSONArray(jrnl);
        assertEquals(1, array.size());
        JSONObject rev = getObjectArrayEntry(array, 0);
        assertPropertyValue(rev, "id", newHead);
        String diff = (String) resolveValue(rev, "changes");
        // TODO properly verify json diff format
        // make sure json diff contains +"/branch":{...}
        assertTrue(diff.matches("\\s*\\+\\s*\"/branch\"\\s*:\\s*\\{\\s*\"foo\"\\s*:\\s*\\{\\s*\\}\\s*\\}\\s*"));
    }

    @Test
    public void oneBranchAddedChildren1() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/branch1", "/branch1/child1");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev, "/branch1", "/branch1/child1");
        assertNodesNotExist(null, "/branch1", "/branch1/child1");

        addNodes(null, "/trunk/child2");
        assertNodesExist(null, "/trunk/child2");
        assertNodesNotExist(branchRev, "/trunk/child2");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child2", "/branch1", "/branch1/child1");
    }

    @Test
    public void oneBranchAddedChildren2() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/trunk/child1/child2");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev, "/trunk/child1/child2");
        assertNodesNotExist(null, "/trunk/child1/child2");

        addNodes(null, "/trunk/child3");
        assertNodesExist(null, "/trunk/child3");
        assertNodesNotExist(branchRev, "/trunk/child3");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child3");
    }

    @Test
    public void oneBranchAddedChildren3() {
        addNodes(null, "/root", "/root/child1");
        assertNodesExist(null, "/root", "/root/child1");

        String branchRev = mk.branch(null);

        addNodes(null, "/root/child2");
        assertNodesExist(null, "/root", "/root/child1", "/root/child2");
        assertNodesExist(branchRev, "/root", "/root/child1");
        assertNodesNotExist(branchRev, "/root/child2");

        branchRev = addNodes(branchRev, "/root/child1/child3", "/root/child4");
        assertNodesExist(branchRev, "/root", "/root/child1", "/root/child1/child3", "/root/child4");
        assertNodesNotExist(branchRev, "/root/child2");
        assertNodesExist(null, "/root", "/root/child1", "/root/child2");
        assertNodesNotExist(null, "/root/child1/child3", "/root/child4");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/root", "/root/child1", "/root/child2",
                "/root/child1/child3", "/root/child4");
    }

    @Test
    public void oneBranchRemovedChildren() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev = mk.branch(null);

        branchRev = removeNodes(branchRev, "/trunk/child1");
        assertNodesExist(branchRev, "/trunk");
        assertNodesNotExist(branchRev, "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");
    }

    @Test
    public void oneBranchChangedProperties() {
        addNodes(null, "/trunk", "/trunk/child1");
        setProp(null, "/trunk/child1/prop1", "value1");
        setProp(null, "/trunk/child1/prop2", "value2");
        assertNodesExist(null, "/trunk", "/trunk/child1");
        assertPropExists(null, "/trunk/child1", "prop1");
        assertPropExists(null, "/trunk/child1", "prop2");

        String branchRev = mk.branch(null);

        branchRev = setProp(branchRev, "/trunk/child1/prop1", "value1a");
        branchRev = setProp(branchRev, "/trunk/child1/prop2", null);
        branchRev = setProp(branchRev, "/trunk/child1/prop3", "value3");
        assertPropValue(branchRev, "/trunk/child1", "prop1", "value1a");
        assertPropNotExists(branchRev, "/trunk/child1", "prop2");
        assertPropValue(branchRev, "/trunk/child1", "prop3", "value3");
        assertPropValue(null, "/trunk/child1", "prop1", "value1");
        assertPropExists(null, "/trunk/child1", "prop2");
        assertPropNotExists(null, "/trunk/child1", "prop3");

        mk.merge(branchRev, "");
        assertPropValue(null, "/trunk/child1", "prop1", "value1a");
        assertPropNotExists(null, "/trunk/child1", "prop2");
        assertPropValue(null, "/trunk/child1", "prop3", "value3");
    }

    @Test
    public void oneBranchAddedSubChildren() {
        addNodes(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");
        assertNodesExist(branchRev, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");
        assertNodesNotExist(null, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");

        addNodes(null, "/trunk/child1/child2/child3/child4", "/trunk/child5");
        assertNodesExist(null, "/trunk/child1/child2/child3/child4", "/trunk/child5");
        assertNodesNotExist(branchRev, "/trunk/child1/child2/child3/child4", "/trunk/child5");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3", "/trunk/child1/child2/child3/child4");
        assertNodesExist(null, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");
    }

    @Test
    public void oneBranchAddedChildrenAndAddedProperties() {
        addNodes(null, "/trunk", "/trunk/child1");
        setProp(null, "/trunk/child1/prop1", "value1");
        setProp(null, "/trunk/child1/prop2", "value2");
        assertNodesExist(null, "/trunk", "/trunk/child1");
        assertPropExists(null, "/trunk/child1", "prop1");
        assertPropExists(null, "/trunk/child1", "prop2");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/branch1", "/branch1/child1");
        branchRev = setProp(branchRev, "/branch1/child1/prop1", "value1");
        branchRev = setProp(branchRev, "/branch1/child1/prop2", "value2");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1");
        assertPropExists(branchRev, "/trunk/child1", "prop1");
        assertPropExists(branchRev, "/trunk/child1", "prop2");
        assertNodesExist(branchRev, "/branch1", "/branch1/child1");
        assertPropExists(branchRev, "/branch1/child1", "prop1");
        assertPropExists(branchRev, "/branch1/child1", "prop2");
        assertNodesNotExist(null, "/branch1", "/branch1/child1");
        assertPropNotExists(null, "/branch1/child1", "prop1");
        assertPropNotExists(null, "/branch1/child1", "prop2");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1");
        assertPropExists(null, "/trunk/child1", "prop1");
        assertPropExists(null, "/trunk/child1", "prop2");
        assertNodesExist(null, "/branch1", "/branch1/child1");
        assertPropExists(null, "/branch1/child1", "prop1");
        assertPropExists(null, "/branch1/child1", "prop2");
    }

    @Test
    public void twoBranchesAddedChildren1() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev1 = mk.branch(null);
        String branchRev2 = mk.branch(null);

        branchRev1 = addNodes(branchRev1, "/branch1", "/branch1/child1");
        branchRev2 = addNodes(branchRev2, "/branch2", "/branch2/child2");
        assertNodesExist(branchRev1, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev2, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev1, "/branch1/child1");
        assertNodesNotExist(branchRev1, "/branch2/child2");
        assertNodesExist(branchRev2, "/branch2/child2");
        assertNodesNotExist(branchRev2, "/branch1/child1");
        assertNodesNotExist(null, "/branch1/child1", "/branch2/child2");

        addNodes(null, "/trunk/child2");
        assertNodesExist(null, "/trunk/child2");
        assertNodesNotExist(branchRev1, "/trunk/child2");
        assertNodesNotExist(branchRev2, "/trunk/child2");

        mk.merge(branchRev1, "");
        assertNodesExist(null, "/trunk", "/branch1", "/branch1/child1");
        assertNodesNotExist(null, "/branch2", "/branch2/child2");

        mk.merge(branchRev2, "");
        assertNodesExist(null, "/trunk", "/branch1", "/branch1/child1", "/branch2", "/branch2/child2");
    }

    @Test
    public void emptyMergeCausesNoChange() {
        String rev1 = mk.commit("", "+\"/child1\":{}", null, "");

        String branchRev = mk.branch(null);
        branchRev = mk.commit("", "+\"/child2\":{}", branchRev, "");
        branchRev = mk.commit("", "-\"/child2\"", branchRev, "");

        String rev2 = mk.merge(branchRev, "");

        assertTrue(mk.nodeExists("/child1", null));
        assertFalse(mk.nodeExists("/child2", null));
        assertEquals(rev1, rev2);
    }

    @Test
    public void trunkMergeNotAllowed() {
        String rev = mk.commit("", "+\"/child1\":{}", null, "");
        try {
            mk.merge(rev, "");
            fail("Exception expected");
        } catch (Exception expected) {}
    }

    @Test
    public void testSmallBlob() {
        testBlobs(1234, 8 * 1024);
    }

    @Test
    public void testMediumBlob() {
        testBlobs(1234567, 8 * 1024);
    }

    @Test
    public void testLargeBlob() {
        testBlobs(32 * 1024 * 1024, 1024 * 1024);
    }

    private void testBlobs(int size, int bufferSize) {
        // write data
        TestInputStream in = new TestInputStream(size);
        String id = mk.write(in);
        assertNotNull(id);
        assertTrue(in.isClosed());

        // write identical data
        in = new TestInputStream(size);
        String id1 = mk.write(in);
        assertNotNull(id1);
        assertTrue(in.isClosed());
        // both id's must be identical since they refer to identical data
        assertEquals(id, id1);

        // verify length
        assertEquals(mk.getLength(id), size);

        // verify data
        InputStream in1 = new TestInputStream(size);
        InputStream in2 = new BufferedInputStream(
                new MicroKernelInputStream(mk, id), bufferSize);
        try {
            while (true) {
                int x = in1.read();
                int y = in2.read();
                if (x == -1 || y == -1) {
                    if (x == y) {
                        break;
                    }
                }
                assertEquals("data does not match", x, y);
            }
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }
}

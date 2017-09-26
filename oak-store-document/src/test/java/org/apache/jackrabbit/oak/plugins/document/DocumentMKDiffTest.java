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

import java.util.concurrent.TimeUnit;

import org.json.simple.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for NodeStore#diff
 */
public class DocumentMKDiffTest extends AbstractMongoConnectionTest {

    @Test
    public void oak596() {
        String rev1 = mk.commit("/", "+\"node1\":{\"node2\":{\"prop1\":\"val1\",\"prop2\":\"val2\"}}", null, null);
        String rev2 = mk.commit("/", "^\"node1/node2/prop1\":\"val1 new\" ^\"node1/node2/prop2\":null", null, null);

        String diff = mk.diff(rev1, rev2, "/node1/node2", 0);
        assertTrue(diff.contains("^\"/node1/node2/prop2\":null"));
        assertTrue(diff.contains("^\"/node1/node2/prop1\":\"val1 new\""));
    }

    @Test
    public void addPathOneLevel() {
        String rev0 = mk.getHeadRevision();

        String rev1 = mk.commit("/", "+\"level1\":{}", null, null);
        assertTrue(mk.nodeExists("/level1", null));

        String reverseDiff = mk.diff(rev1, rev0, null, 0);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        mk.commit("", reverseDiff, null, null);
        assertFalse(mk.nodeExists("/level1", null));
    }

    @Test
    public void addPathTwoLevels() {
        String rev0 = mk.getHeadRevision();

        String rev1 = mk.commit("/", "+\"level1\":{}", null, null);
        rev1 = mk.commit("/", "+\"level1/level2\":{}", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));

        String reverseDiff = mk.diff(rev1, rev0, null, 0);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        mk.commit("", reverseDiff, null, null);
        assertFalse(mk.nodeExists("/level1", null));
        assertFalse(mk.nodeExists("/level1/level2", null));
    }

    @Test
    public void addPathTwoSameLevels() {
        String rev0 = mk.getHeadRevision();

        String rev1 = mk.commit("/", "+\"level1a\":{}", null, null);
        rev1 = mk.commit("/", "+\"level1b\":{}", null, null);
        assertTrue(mk.nodeExists("/level1a", null));
        assertTrue(mk.nodeExists("/level1b", null));

        String reverseDiff = mk.diff(rev1, rev0, null, 0);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        mk.commit("", reverseDiff, null, null);
        assertFalse(mk.nodeExists("/level1a", null));
        assertFalse(mk.nodeExists("/level1b", null));
    }

    @Test
    @Ignore("New DocumentMK only supports depth 0")
    public void removePath() {
        // Add level1 & level1/level2
        String rev0 = mk.commit("/",
                "+\"level1\":{}" +
                "+\"level1/level2\":{}", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));

        // Remove level1/level2
        String rev1 = mk.commit("/", "-\"level1/level2\"", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertFalse(mk.nodeExists("/level1/level2", null));

        // Generate reverseDiff from rev1 to rev0
        String reverseDiff = mk.diff(rev1, rev0, "/level1", 0);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit the reverseDiff and check rev0 state is restored
        mk.commit("", reverseDiff, null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));

        // Remove level1
        String rev2 = mk.commit("/", "-\"level1\"", null, null);
        assertFalse(mk.nodeExists("/level1", null));
        assertFalse(mk.nodeExists("/level1/level2", null));

        // Generate reverseDiff from rev2 to rev0
        reverseDiff = mk.diff(rev2, rev0, null, 1);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit the reverseDiff and check rev0 state is restored
        mk.commit("", reverseDiff, null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));
    }

    @Test
    @Ignore("New DocumentMK only supports depth 0")
    public void movePath() {
        String rev1 = mk.commit("/", "+\"level1\":{}", null, null);
        rev1 = mk.commit("/", "+\"level1/level2\":{}", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));

        String rev2 = mk.commit("/", ">\"level1\" : \"level1new\"", null, null);
        assertFalse(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1new", null));
        assertTrue(mk.nodeExists("/level1new/level2", null));

        String reverseDiff = mk.diff(rev2, rev1, null, 1);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        mk.commit("", reverseDiff, null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));
        assertFalse(mk.nodeExists("/level1new", null));
        assertFalse(mk.nodeExists("/level1new/level2", null));
    }

    @Test
    public void copyPath() {
        String rev1 = mk.commit("/", "+\"level1\":{}", null, null);
        rev1 = mk.commit("/", "+\"level1/level2\":{}", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));

        String rev2 = mk.commit("/", "*\"level1\" : \"level1new\"", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1new", null));
        assertTrue(mk.nodeExists("/level1new/level2", null));

        String reverseDiff = mk.diff(rev2, rev1, null, 0);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        mk.commit("", reverseDiff, null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));
        assertFalse(mk.nodeExists("/level1new", null));
        assertFalse(mk.nodeExists("/level1new/level2", null));
    }

    @Test
    public void setProperty() {
        String rev0 = mk.commit("/", "+\"level1\":{}", null, null);
        assertTrue(mk.nodeExists("/level1", null));

        // Add property.
        String rev1 = mk.commit("/", "^\"level1/prop1\": \"value1\"", null, null);
        JSONObject obj = parseJSONObject(mk.getNodes("/level1", null, 0, 0, -1, null));
        assertPropertyExists(obj, "prop1");

        // Generate reverseDiff from rev1 to rev0
        String reverseDiff = mk.diff(rev1, rev0, "/level1", 0);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit the reverseDiff and check property is gone.
        mk.commit("", reverseDiff, null, null);
        assertTrue(mk.nodeExists("/level1", null));
        obj = parseJSONObject(mk.getNodes("/level1", null, 0, 0, -1, null));
        assertPropertyNotExists(obj, "prop1");
    }

    @Test
    public void removeProperty() {
        String rev0 = mk.commit("/", "+\"level1\":{ \"prop1\" : \"value\"}", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        JSONObject obj = parseJSONObject(mk.getNodes("/level1", null, 0, 0, -1, null));
        assertPropertyExists(obj, "prop1");

        // Remove property
        String rev1 = mk.commit("/", "^\"level1/prop1\" : null", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        obj = parseJSONObject(mk.getNodes("/level1", null, 0, 0, -1, null));
        assertPropertyNotExists(obj, "prop1");

        // Generate reverseDiff from rev1 to rev0
        String reverseDiff = mk.diff(rev1, rev0, "/level1", 0);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit the reverseDiff and check property is added back.
        mk.commit("", reverseDiff, null, null);
        obj = parseJSONObject(mk.getNodes("/level1", null, 0, 0, -1, null));
        assertPropertyExists(obj, "prop1");
    }

    @Test
    public void changeProperty() {
        String rev0 = mk.commit("/", "+\"level1\":{ \"prop1\" : \"value1\"}", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        JSONObject obj = parseJSONObject(mk.getNodes("/level1", null, 0, 0, -1, null));
        assertPropertyValue(obj, "prop1", "value1");

        // Change property
        String rev1 = mk.commit("/", "^\"level1/prop1\" : \"value2\"", null, null);
        obj = parseJSONObject(mk.getNodes("/level1", null, 0, 0, -1, null));
        assertPropertyValue(obj, "prop1", "value2");

        // Generate reverseDiff from rev1 to rev0
        String reverseDiff = mk.diff(rev1, rev0, "/level1", 0);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit the reverseDiff and check property is set back.
        mk.commit("", reverseDiff, null, null);
        obj = parseJSONObject(mk.getNodes("/level1", null, 0, 0, -1, null));
        assertPropertyValue(obj, "prop1", "value1");
    }

    @Test
    public void diffForChangeBelowManyChildren() throws InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD * 2; i++) {
            sb.append("+\"node-").append(i).append("\":{}");
        }
        String branchRev = mk.branch(null);
        mk.commit("/", sb.toString(), null, null);
        branchRev = mk.commit("/", "+\"branch\":{}", branchRev, null);
        branchRev = mk.commit("/branch", sb.toString(), branchRev, null);
        // wait a while, _modified has 5 seconds resolution
        Thread.sleep(TimeUnit.SECONDS.toMillis(6));
        // create a base commits for the diffs
        String base = mk.commit("/", "+\"foo\":{}", null, null);
        branchRev = mk.commit("/branch", "+\"foo\":{}", branchRev, null);
        String branchBase = branchRev;
        // these are the commits we want to get the diffs for
        String rev = mk.commit("/node-0", "+\"foo\":{}", null, null);
        branchRev = mk.commit("/branch/node-0", "+\"foo\":{}", branchRev, null);
        // perform diffs
        String diff = mk.diff(base, rev, "/", 0);
        assertTrue(diff, diff.contains("^\"/node-0\""));
        diff = mk.diff(branchBase, branchRev, "/branch", 0);
        assertTrue(diff, diff.contains("^\"/branch/node-0\""));
    }

    @Test
    public void diffManyChildren() {
        diffManyChildren(false);
    }

    @Test
    public void diffManyChildrenOnBranch() {
        diffManyChildren(true);
    }

    private void diffManyChildren(boolean onBranch) {
        String baseRev = mk.getHeadRevision();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD * 2; i++) {
            sb.append("+\"node-").append(i).append("\":{}");
        }
        String rev = mk.commit("/", sb.toString(), null, null);
        String jsop = mk.diff(baseRev, rev, "/", 0);
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD * 2; i++) {
            assertTrue(jsop, jsop.contains("+\"/node-" + i + "\""));
        }
        jsop = mk.diff(rev, baseRev, "/", 0);
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD * 2; i++) {
            assertTrue(jsop, jsop.contains("-\"/node-" + i + "\""));
        }

        if (onBranch) {
            rev = mk.branch(rev);
        }
        String rev2 = mk.commit("/", "+\"node-new\":{}", rev, null);
        jsop = mk.diff(rev, rev2, "/", 0);
        assertTrue(jsop, jsop.contains("+\"/node-new\""));

        String rev3 = mk.commit("/", "^\"node-new/prop\":\"value\"", rev2, null);
        jsop = mk.diff(rev2, rev3, "/", 0);
        assertTrue(jsop, jsop.contains("^\"/node-new\""));

        String rev4 = mk.commit("/", "+\"node-new/foo\":{}", rev3, null);
        jsop = mk.diff(rev3, rev4, "/", 0);
        assertTrue(jsop, jsop.contains("^\"/node-new\""));

        String rev5 = mk.commit("/", "^\"node-new/foo/prop\":\"value\"", rev4, null);
        jsop = mk.diff(rev4, rev5, "/", 0);
        assertTrue(jsop, jsop.contains("^\"/node-new\""));

        String rev6 = mk.commit("/", "-\"node-new/foo\"", rev5, null);
        jsop = mk.diff(rev5, rev6, "/", 0);
        assertTrue(jsop, jsop.contains("^\"/node-new\""));
    }

    @Test
    public void diffMergedRevision() {
        mk.commit("/", "+\"test\":{}", null, null);
        String before = mk.getHeadRevision();

        String b = mk.branch(null);
        b = mk.commit("/test", "^\"p\":1", b, null);
        String after = mk.merge(b, null);

        String jsop = mk.diff(before, after, "/", 0).trim();
        assertEquals("^\"/test\":{}", jsop);
    }
}
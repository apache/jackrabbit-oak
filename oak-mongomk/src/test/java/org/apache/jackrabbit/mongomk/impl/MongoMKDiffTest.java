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
package org.apache.jackrabbit.mongomk.impl;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.json.simple.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for MicroKernel#diff
 */
public class MongoMKDiffTest extends BaseMongoMicroKernelTest {

    @Test
    @Ignore("OAK-596")
    public void oak_596() {
        String rev0 = mk.getHeadRevision();
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

        String reverseDiff = mk.diff(rev1, rev0, null, -1);
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

        String reverseDiff = mk.diff(rev1, rev0, null, -1);
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

        String reverseDiff = mk.diff(rev1, rev0, null, -1);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        mk.commit("", reverseDiff, null, null);
        assertFalse(mk.nodeExists("/level1a", null));
        assertFalse(mk.nodeExists("/level1b", null));
    }

    @Test
    public void removePath() {
        // Add level1 & level1/level2
        String rev0 = mk.commit("/","+\"level1\":{}" +
                "+\"level1/level2\":{}", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));

        // Remove level1/level2
        String rev1 = mk.commit("/", "-\"level1/level2\"", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertFalse(mk.nodeExists("/level1/level2", null));

        // Generate reverseDiff from rev1 to rev0
        String reverseDiff = mk.diff(rev1, rev0, null, -1);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit the reverseDiff and check rev0 state is restored
        mk.commit("", reverseDiff, null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));

        // Remove level1 at rev0
        String rev2 = mk.commit("/", "-\"level1\"", rev0, null);
        assertFalse(mk.nodeExists("/level1", null));
        assertFalse(mk.nodeExists("/level1/level2", null));

        // Generate reverseDiff from rev2 to rev0
        reverseDiff = mk.diff(rev2, rev0, null, -1);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit the reverseDiff and check rev0 state is restored
        mk.commit("", reverseDiff, null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));
    }

    @Test
    public void movePath() {
        String rev1 = mk.commit("/", "+\"level1\":{}", null, null);
        rev1 = mk.commit("/", "+\"level1/level2\":{}", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1/level2", null));

        String rev2 = mk.commit("/", ">\"level1\" : \"level1new\"", null, null);
        assertFalse(mk.nodeExists("/level1", null));
        assertTrue(mk.nodeExists("/level1new", null));
        assertTrue(mk.nodeExists("/level1new/level2", null));

        String reverseDiff = mk.diff(rev2, rev1, null, -1);
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

        String reverseDiff = mk.diff(rev2, rev1, null, -1);
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
        JSONObject obj = parseJSONObject(mk.getNodes("/level1", null, 1, 0, -1, null));
        assertPropertyExists(obj, "prop1");

        // Generate reverseDiff from rev1 to rev0
        String reverseDiff = mk.diff(rev1, rev0, null, -1);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit the reverseDiff and check property is gone.
        mk.commit("", reverseDiff, null, null);
        assertTrue(mk.nodeExists("/level1", null));
        obj = parseJSONObject(mk.getNodes("/level1", null, 1, 0, -1, null));
        assertPropertyNotExists(obj, "prop1");
    }

    @Test
    public void removeProperty() {
        String rev0 = mk.commit("/", "+\"level1\":{ \"prop1\" : \"value\"}", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        JSONObject obj = parseJSONObject(mk.getNodes("/level1", null, 1, 0, -1, null));
        assertPropertyExists(obj, "prop1");

        // Remove property
        String rev1 = mk.commit("/", "^\"level1/prop1\" : null", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        obj = parseJSONObject(mk.getNodes("/level1", null, 1, 0, -1, null));
        assertPropertyNotExists(obj, "prop1");

        // Generate reverseDiff from rev1 to rev0
        String reverseDiff = mk.diff(rev1, rev0, null, -1);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit the reverseDiff and check property is added back.
        mk.commit("", reverseDiff, null, null);
        obj = parseJSONObject(mk.getNodes("/level1", null, 1, 0, -1, null));
        assertPropertyExists(obj, "prop1");
    }

    @Test
    public void changeProperty() {
        String rev0 = mk.commit("/", "+\"level1\":{ \"prop1\" : \"value1\"}", null, null);
        assertTrue(mk.nodeExists("/level1", null));
        JSONObject obj = parseJSONObject(mk.getNodes("/level1", null, 1, 0, -1, null));
        assertPropertyValue(obj, "prop1", "value1");

        // Change property
        String rev1 = mk.commit("/", "^\"level1/prop1\" : \"value2\"", null, null);
        obj = parseJSONObject(mk.getNodes("/level1", null, 1, 0, -1, null));
        assertPropertyValue(obj, "prop1", "value2");

        // Generate reverseDiff from rev1 to rev0
        String reverseDiff = mk.diff(rev1, rev0, null, -1);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit the reverseDiff and check property is set back.
        mk.commit("", reverseDiff, null, null);
        obj = parseJSONObject(mk.getNodes("/level1", null, 1, 0, -1, null));
        assertPropertyValue(obj, "prop1", "value1");
    }
}
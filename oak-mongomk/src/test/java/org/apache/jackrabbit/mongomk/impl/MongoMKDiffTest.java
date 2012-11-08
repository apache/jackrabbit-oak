package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.json.simple.JSONObject;
import org.junit.Test;

/**
 * Tests for MicroKernel#diff
 */
public class MongoMKDiffTest extends BaseMongoMicroKernelTest {

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
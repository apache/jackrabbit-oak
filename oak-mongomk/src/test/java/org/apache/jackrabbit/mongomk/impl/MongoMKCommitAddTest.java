package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.json.simple.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for {@link MongoMicroKernel#commit(String, String, String, String)}
 * with emphasis on add node and property operations.
 */
public class MongoMKCommitAddTest extends BaseMongoMicroKernelTest {

    @Test
    public void addSingleNode() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);

        long childCount = mk.getChildNodeCount("/", null);
        assertEquals(1, childCount);

        String nodes = mk.getNodes("/", null, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "a/:childNodeCount", 0L);
    }

    @Test
    public void addDuplicateNode() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);
        try {
            mk.commit("/", "+\"a\" : {}", null, null);
            fail("Exception expected");
        } catch (Exception expected) {}
    }

    @Test
    public void addSingleProperty() throws Exception {
        mk.commit("/", "+\"a\" : {} +\"a/key1\" : \"value1\"", null, null);

        long childCount = mk.getChildNodeCount("/", null);
        assertEquals(1, childCount);

        String nodes = mk.getNodes("/", null, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "a/:childNodeCount", 0L);
        assertPropertyValue(obj, "a/key1", "value1");
    }

    @Test
    public void addPropertyWithoutAddingNode() throws Exception {
        try {
            mk.commit("/", "+\"a/key1\" : \"value1\"", null, null);
            fail("Exception expected");
        } catch (Exception expected) {}
    }

    @Test
    public void setPropertyWithoutAddingNode() throws Exception {
        try {
            mk.commit("/", "^\"a/key1\" : \"value1\"", null, null);
            fail("Exception expected");
        } catch (Exception expected) {}
    }

    /**
     * This test works even in MicroKernelImpl but we should consider whether
     * throwing a MicroKernelException is a better alternative.
     */
    @Test
    @Ignore
    public void addDuplicateProperty() throws Exception {
        mk.commit("/", "+\"a\" : {} +\"a/key1\" : \"value1\" +\"a/key1\" : \"value1\"", null, null);

        long childCount = mk.getChildNodeCount("/", null);
        assertEquals(1, childCount);

        String nodes = mk.getNodes("/", null, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "a/:childNodeCount", 0L);
        assertPropertyValue(obj, "a/key1", "value1");
    }
}
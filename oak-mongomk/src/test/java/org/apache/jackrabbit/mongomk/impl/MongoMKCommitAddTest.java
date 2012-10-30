package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.json.simple.JSONObject;
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
        assertPropertyValue(obj, ":childNodeCount", 1L);
    }

    @Test
    public void addNodeWithChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\": {}, \"c\": {}, \"d\" : {} }", null, null);

        assertTrue(mk.nodeExists("/a",null));
        assertTrue(mk.nodeExists("/a/b",null));
        assertTrue(mk.nodeExists("/a/c",null));
        assertTrue(mk.nodeExists("/a/d",null));
    }

    @Test
    public void addNodeWithNestedChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\": { \"c\" : { \"d\" : {} } } }", null, null);

        assertTrue(mk.nodeExists("/a",null));
        assertTrue(mk.nodeExists("/a/b",null));
        assertTrue(mk.nodeExists("/a/b/c",null));
        assertTrue(mk.nodeExists("/a/b/c/d",null));
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
        mk.commit("/", "+\"a\" : {} ^\"a/key1\" : \"value1\"", null, null);

        long childCount = mk.getChildNodeCount("/", null);
        assertEquals(1, childCount);

        String nodes = mk.getNodes("/", null, 1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, ":childNodeCount", 1L);
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
}
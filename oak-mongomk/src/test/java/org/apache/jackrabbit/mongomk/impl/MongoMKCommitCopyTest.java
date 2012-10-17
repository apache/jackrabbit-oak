package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.json.simple.JSONObject;
import org.junit.Test;

/**
 * Tests for {@link MongoMicroKernel#commit(String, String, String, String)}
 * with emphasis on copy operations.
 */
public class MongoMKCommitCopyTest extends BaseMongoMicroKernelTest {

    @Test
    public void copySingleNode() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);

        long childCount = mk.getChildNodeCount("/", null);
        assertEquals(1, childCount);

        mk.commit("/", "*\"a\" : \"b\"", null, null);

        childCount = mk.getChildNodeCount("/", null);
        assertEquals(2, childCount);
    }

    @Test
    public void copySingleNodeWithChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : {} }", null, null);

        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));

        mk.commit("/", "*\"a\" : \"c\"", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/c", null));
        assertTrue(mk.nodeExists("/c/b", null));
    }

    @Test
    public void addNodeAndCopy() {
        mk.commit("/", "+\"a\":{}", null, null);

        mk.commit("/", "+\"a/b\":{}\n" +
                        "*\"a/b\":\"c/b\"", null, null);

        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/c/b", null));
    }

    @Test
    public void addNodeAndCopyParent() {
        mk.commit("/", "+\"a\":{}", null, null);

        mk.commit("/", "+\"a/b\":{}\n" +
                        "*\"a\":\"c\"", null, null);

        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/c/b", null));
    }

    @Test
    public void removeNodeAndCopy() {
        mk.commit("/", "+\"a\":{ \"b\" : {} }", null, null);

        try {
            mk.commit("/", "-\"a/b\"\n" +
                    "*\"a/b\":\"c\"", null, null);
            fail("Expected expected");
        } catch (Exception expected) {}
    }

    @Test
    public void removeNodeAndCopyParent() {
        mk.commit("/", "+\"a\":{ \"b\" : {} }", null, null);

        mk.commit("/", "-\"a/b\"\n" +
                        "*\"a\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a/b", null));
        assertFalse(mk.nodeExists("/c/b", null));
    }

    // FIXME - Add tests where property added/removed and the parent node is also modified.

    @Test
    public void addPropertyAndCopy() {
        mk.commit("/", "+\"a\":{}", null, null);

        mk.commit("/", "+\"a/key1\": \"value1\"\n" +
                        "*\"a\":\"c\"", null, null);

        String nodes = mk.getNodes("/", null, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "a/key1", "value1");
        assertPropertyValue(obj, "c/key1", "value1");
    }

    @Test
    public void removePropertyAndCopy() {
        mk.commit("/", "+\"a\":{ \"key1\" : \"value1\"}", null, null);

        mk.commit("/", "^\"a/key1\" : null\n" +
                        "*\"a\":\"c\"", null, null);

        String nodes = mk.getNodes("/", null, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyNotExists(obj, "a/key1");
        assertPropertyNotExists(obj, "c/key1");
    }

    @Test
    public void copySingleNodeWithProperties() throws Exception {
        mk.commit("/", "+\"a\" : { \"key1\" : \"value1\" }", null, null);

        assertTrue(mk.nodeExists("/a", null));
        String nodes = mk.getNodes("/", null, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "a/key1", "value1");

        mk.commit("/", "*\"a\" : \"c\"", null, null);
        assertTrue(mk.nodeExists("/c", null));
        nodes = mk.getNodes("/", null, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "c/key1", "value1");
    }

    @Test
    public void copyFromNonExistentNode() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);

        long childCount = mk.getChildNodeCount("/", null);
        assertEquals(1, childCount);

        try {
            mk.commit("/", "*\"b\" : \"c\"", null, null);
            fail("Exception expected");
        } catch (Exception expected) {}
    }

    @Test
    public void copyToAnExistentNode() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : {} }", null, null);
        mk.commit("/", "+\"c\" : {}", null, null);

        try {
            mk.commit("/", "*\"c\" : \"a/b\"", null, null);
            fail("Exception expected");
        } catch (Exception expected) {}
    }
}
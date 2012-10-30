package org.apache.jackrabbit.mongomk.impl;

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
    public void copyNode() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);
        assertTrue(mk.nodeExists("/a", null));

        mk.commit("/", "*\"a\" : \"b\"", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/b", null));
    }

    @Test
    public void copyNodeWithChild() throws Exception {
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
    public void copyNodeWithChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : {},  \"c\" : {}, \"d\" : {}}", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/c", null));
        assertTrue(mk.nodeExists("/a/d", null));

        mk.commit("/", "*\"a\" : \"e\"", null, null);
        assertTrue(mk.nodeExists("/e", null));
        assertTrue(mk.nodeExists("/e/b", null));
        assertTrue(mk.nodeExists("/e/c", null));
        assertTrue(mk.nodeExists("/e/d", null));
    }

    @Test
    public void copyNodeWithNestedChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : { \"c\" : { \"d\" : {} } } }", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/b/c", null));
        assertTrue(mk.nodeExists("/a/b/c/d", null));

        mk.commit("/", "*\"a\" : \"e\"", null, null);
        assertTrue(mk.nodeExists("/e", null));
        assertTrue(mk.nodeExists("/e/b", null));
        assertTrue(mk.nodeExists("/e/b/c", null));
        assertTrue(mk.nodeExists("/e/b/c/d", null));

        mk.commit("/", "*\"e/b\" : \"f\"", null, null);
        assertTrue(mk.nodeExists("/f", null));
        assertTrue(mk.nodeExists("/f/c", null));
        assertTrue(mk.nodeExists("/f/c/d", null));
    }

    @Test
    public void copyNodeWithProperties() throws Exception {
        mk.commit("/", "+\"a\" : { \"key1\" : \"value1\" }", null, null);
        assertTrue(mk.nodeExists("/a", null));
        String nodes = mk.getNodes("/", null, 1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "a/key1", "value1");

        mk.commit("/", "*\"a\" : \"c\"", null, null);
        assertTrue(mk.nodeExists("/c", null));
        nodes = mk.getNodes("/", null, 1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "c/key1", "value1");
    }

    @Test
    public void copyFromNonExistentNode() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);
        assertTrue(mk.nodeExists("/a", null));

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

    @Test
    public void addNodeAndCopy() {
        mk.commit("/", "+\"a\":{}", null, null);
        mk.commit("/", "+\"a/b\":{}\n" +
                        "*\"a/b\":\"c\"", null, null);

        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/c", null));
    }

    @Test
    public void addNodeWithChildrenAndCopy() {
        mk.commit("/", "+\"a\":{}", null, null);
        mk.commit("/", "+\"a/b\":{ \"c\" : {}, \"d\" : {} }\n" +
                        "*\"a/b\":\"e\"", null, null);

        assertTrue(mk.nodeExists("/a/b/c", null));
        assertTrue(mk.nodeExists("/a/b/d", null));
        assertTrue(mk.nodeExists("/e/c", null));
        assertTrue(mk.nodeExists("/e/d", null));
    }

    @Test
    public void addNodeWithNestedChildrenAndCopy() {
        mk.commit("/", "+\"a\":{ \"b\" : { \"c\" : { } } }", null, null);
        mk.commit("/", "+\"a/b/c/d\":{}\n"
                     + "*\"a\":\"e\"", null, null);

        assertTrue(mk.nodeExists("/a/b/c/d", null));
        assertTrue(mk.nodeExists("/e/b/c/d", null));
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
    public void removeNodeWithNestedChildrenAndCopy() {
        mk.commit("/", "+\"a\":{ \"b\" : { \"c\" : { \"d\" : {} } } }", null, null);
        mk.commit("/", "-\"a/b/c/d\"\n"
                     + "*\"a\" : \"e\"", null, null);

        assertFalse(mk.nodeExists("/a/b/c/d", null));
        assertTrue(mk.nodeExists("/e/b/c", null));
        assertFalse(mk.nodeExists("/e/b/c/d", null));
    }

    @Test
    public void removeNodeAndCopyParent() {
        mk.commit("/", "+\"a\":{ \"b\" : {} }", null, null);
        mk.commit("/", "-\"a/b\"\n" +
                        "*\"a\":\"c\"", null, null);

        assertFalse(mk.nodeExists("/a/b", null));
        assertFalse(mk.nodeExists("/c/b", null));
    }

    @Test
    public void setPropertyAndCopy() {
        mk.commit("/", "+\"a\":{}", null, null);
        mk.commit("/", "^\"a/key1\": \"value1\"\n" +
                        "*\"a\":\"c\"", null, null);

        String nodes = mk.getNodes("/", null, 1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "a/key1", "value1");
        assertPropertyValue(obj, "c/key1", "value1");
    }

    @Test
    public void setNestedPropertyAndCopy() {
        mk.commit("/", "+\"a\":{ \"b\" : {} }", null, null);
        mk.commit("/", "^\"a/b/key1\": \"value1\"\n" +
                        "*\"a\":\"c\"", null, null);

        String nodes = mk.getNodes("/", null, 2 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "a/b/key1", "value1");
        assertPropertyValue(obj, "c/b/key1", "value1");
    }

    @Test
    public void modifyParentAddPropertyAndCopy() {
        mk.commit("/", "+\"a\":{}", null, null);
        mk.commit("/", "+\"b\" : {}\n"
                     + "^\"a/key1\": \"value1\"\n"
                     + "*\"a\":\"c\"", null, null);

        String nodes = mk.getNodes("/", null, 1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, "a/key1", "value1");
        assertPropertyValue(obj, "c/key1", "value1");
    }

    @Test
    public void removePropertyAndCopy() {
        mk.commit("/", "+\"a\":{ \"b\" : { \"key1\" : \"value1\" } }", null, null);
        mk.commit("/", "^\"a/b/key1\": null\n" +
                        "*\"a\":\"c\"", null, null);

        String nodes = mk.getNodes("/", null, 1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyNotExists(obj, "a/b/key1");
        assertPropertyNotExists(obj, "c/b/key1");
    }

    @Test
    public void removeNestedPropertyAndCopy() {
        mk.commit("/", "+\"a\":{ \"key1\" : \"value1\"}", null, null);
        mk.commit("/", "^\"a/key1\" : null\n" +
                        "*\"a\":\"c\"", null, null);

        String nodes = mk.getNodes("/", null, 1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyNotExists(obj, "a/key1");
        assertPropertyNotExists(obj, "c/key1");
    }

    @Test
    public void modifyParentRemovePropertyAndCopy() {
        mk.commit("/", "+\"a\":{ \"key1\" : \"value1\"}", null, null);
        mk.commit("/", "+\"b\" : {}\n"
                     + "^\"a/key1\" : null\n"
                     + "*\"a\":\"c\"", null, null);

        String nodes = mk.getNodes("/", null, 1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyNotExists(obj, "a/key1");
        assertPropertyNotExists(obj, "c/key1");
    }
}
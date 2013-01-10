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
 * with emphasis on remove node and property operations.
 */
public class MongoMKCommitRemoveTest extends BaseMongoMicroKernelTest {

    @Test
    public void removeSingleNode() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);

        long childCount = mk.getChildNodeCount("/", null);
        assertEquals(1, childCount);

        mk.commit("/", "-\"a\"", null, null);
        childCount = mk.getChildNodeCount("/", null);
        assertEquals(0, childCount);
    }

    @Test
    public void removeNonExistentNode() throws Exception {
        try {
            mk.commit("/", "-\"a\"", null, null);
            fail("Exception expected");
        } catch (Exception expected) {}
    }

    @Test
    public void removeNodeTwice() throws Exception {
        String base = mk.commit("", "+\"/a\":{}", null, null);
        mk.commit("", "-\"/a\"", base, null);
        assertTrue(mk.nodeExists("/a", base));
        mk.commit("", "-\"/a\"", base, null);
    }

    @Test
    public void removeAndAddNode() throws Exception {
        String base = mk.commit("", "+\"/a\":{}", null, null);
        String rev = mk.commit("", "-\"/a\"", base, null);
        assertTrue(mk.nodeExists("/a", base));
        assertFalse(mk.nodeExists("/a", rev));
        mk.commit("", "+\"/a\":{}", rev, null);
    }

    @Test
    public void removeNodeWithChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : {},  \"c\" : {}, \"d\" : {}}", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/c", null));
        assertTrue(mk.nodeExists("/a/d", null));

        mk.commit("/", "-\"a/b\"", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertFalse(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/c", null));
        assertTrue(mk.nodeExists("/a/d", null));
    }

    @Test
    public void removeNodeWithNestedChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\" : { \"c\" : { \"d\" : {} } } }", null, null);
        assertTrue(mk.nodeExists("/a", null));
        assertTrue(mk.nodeExists("/a/b", null));
        assertTrue(mk.nodeExists("/a/b/c", null));
        assertTrue(mk.nodeExists("/a/b/c/d", null));

        mk.commit("/", "-\"a\"", null, null);
        assertFalse(mk.nodeExists("/a", null));
        assertFalse(mk.nodeExists("/a/b", null));
        assertFalse(mk.nodeExists("/a/b/c", null));
        assertFalse(mk.nodeExists("/a/b/c/d", null));
    }
}
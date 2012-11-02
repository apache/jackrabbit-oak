package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.junit.Test;

/**
 * Tests for {@link MongoMicroKernel#getChildNodeCount(String, String)}
 */
public class MongoMKGetChildCountTest extends BaseMongoMicroKernelTest {

    @Test
    public void noChild() throws Exception {
        long childCount = mk.getChildNodeCount("/", null);
        assertEquals(0, childCount);
    }

    @Test
    public void singleChild() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);

        long childCount = mk.getChildNodeCount("/", null);
        assertEquals(1, childCount);
    }

    @Test
    public void multipleChilden() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\": {}, \"c\": {}, \"d\" : {} }", null, null);

        long childCount = mk.getChildNodeCount("/", null);
        assertEquals(1, childCount);

        childCount = mk.getChildNodeCount("/a", null);
        assertEquals(3, childCount);

        childCount = mk.getChildNodeCount("/a/b", null);
        assertEquals(0, childCount);

        childCount = mk.getChildNodeCount("/a/c", null);
        assertEquals(0, childCount);

        childCount = mk.getChildNodeCount("/a/d", null);
        assertEquals(0, childCount);
    }

    @Test
    public void multipleNestedChildren() throws Exception {
        mk.commit("/", "+\"a\" : { \"b\": { \"c\" : { \"d\" : {} } } }", null, null);

        long childCount = mk.getChildNodeCount("/", null);
        assertEquals(1, childCount);

        childCount = mk.getChildNodeCount("/a", null);
        assertEquals(1, childCount);

        childCount = mk.getChildNodeCount("/a/b", null);
        assertEquals(1, childCount);

        childCount = mk.getChildNodeCount("/a/b/c", null);
        assertEquals(1, childCount);

        childCount = mk.getChildNodeCount("/a/b/c/d", null);
        assertEquals(0, childCount);
    }

    @Test
    public void nonExistingPath() throws Exception {
        mk.commit("/", "+\"a\" : {}", null, null);

        try {
            mk.getChildNodeCount("/nonexisting", null);
            fail("Expected: non-existing path exception");
        } catch (Exception expected){}
    }
}
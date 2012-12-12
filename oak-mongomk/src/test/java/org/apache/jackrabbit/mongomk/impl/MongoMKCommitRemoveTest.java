package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.junit.Ignore;
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
    @Ignore("OAK-507") // FIXME
    public void removeNodeTwice() throws Exception {
        String base = mk.commit("", "+\"/a\":{}", null, null);
        mk.commit("", "-\"/a\"", base, null);
        assertTrue(mk.nodeExists("/a", base));
        mk.commit("", "-\"/a\"", base, null);
    }

    @Test
    public void removeAndAddNode() throws Exception {
        String base = mk.commit("", "+\"/a\":{}", null, null);
        mk.commit("", "-\"/a\"", base, null);
        assertTrue(mk.nodeExists("/a", base));
        mk.commit("", "+\"/a\":{}", base, null);
    }
}
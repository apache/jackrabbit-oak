package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.junit.Test;

public class MongoMicroKernelDiffTest extends BaseMongoMicroKernelTest {

    @Test
    public void diffOneLevel() {
        String rev0 = mk.getHeadRevision();

        String rev1 = mk.commit("/", "+\"target\":{}", null, null);
        assertTrue(mk.nodeExists("/target", null));

        // Get reverse diff
        String reverseDiff = mk.diff(rev1, rev0, null, -1);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit reverse diff
        mk.commit("", reverseDiff, null, null);
        assertFalse(mk.nodeExists("/target", null));
    }
}

package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.junit.Test;

/**
 * Tests for MicroKernel#diff
 */
public class MongoMicroKernelDiffTest extends BaseMongoMicroKernelTest {

    @Test
    public void addPathOneLevel() {
        String rev0 = mk.getHeadRevision();

        String rev1 = mk.commit("/", "+\"level1\":{}", null, null);
        assertTrue(mk.nodeExists("/level1", null));

        String reverseDiff = mk.diff(rev1, rev0, null, -1);
        assertNotNull(reverseDiff);
        assertTrue(reverseDiff.length() > 0);

        // Commit reverse diff
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

        // Commit reverse diff
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

        // Commit reverse diff
        mk.commit("", reverseDiff, null, null);
        assertFalse(mk.nodeExists("/level1a", null));
        assertFalse(mk.nodeExists("/level1b", null));
    }
}

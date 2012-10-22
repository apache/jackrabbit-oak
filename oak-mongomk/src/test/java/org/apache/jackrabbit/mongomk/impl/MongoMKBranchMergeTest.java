package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.junit.Test;

/**
 * Tests for {@code MicroKernel#branch}
 */
public class MongoMKBranchMergeTest extends BaseMongoMicroKernelTest {

    @Test
    public void singleBranch() {
        String branchRev1 = mk.branch(null);

        branchRev1 = mk.commit("", "+\"/branch1\":{}", branchRev1, "");
        branchRev1 = mk.commit("", "+\"/branch1/child1\":{}", branchRev1, "");

        assertFalse(mk.nodeExists("/branch1/child1", null));
        assertFalse(mk.nodeExists("/branch1/child1", null));

        mk.commit("", "+\"/trunk\":{}", null, "");

        assertTrue(mk.nodeExists("/trunk", null));
        assertFalse(mk.nodeExists("/trunk", branchRev1));
    }

    @Test
    public void twoBranches() {
        String branchRev1 = mk.branch(null);
        String branchRev2 = mk.branch(null);

        branchRev1 = mk.commit("", "+\"/branch1\":{}", branchRev1, "");
        branchRev2 = mk.commit("", "+\"/branch2\":{}", branchRev2, "");
        branchRev1 = mk.commit("", "+\"/branch1/child1\":{}", branchRev1, "");
        branchRev2 = mk.commit("", "+\"/branch2/child2\":{}", branchRev2, "");

        assertFalse(mk.nodeExists("/branch1/child1", null));
        assertFalse(mk.nodeExists("/branch2/child2", null));

        assertTrue(mk.nodeExists("/branch1/child1", branchRev1));
        assertFalse(mk.nodeExists("/branch2/child2", branchRev1));

        assertFalse(mk.nodeExists("/branch1/child1", branchRev2));
        assertTrue(mk.nodeExists("/branch2/child2", branchRev2));

        mk.commit("", "+\"/trunk\":{}", null, "");

        assertTrue(mk.nodeExists("/trunk", null));
        assertFalse(mk.nodeExists("/trunk", branchRev1));
        assertFalse(mk.nodeExists("/trunk", branchRev2));
    }

    @Test
    public void twoBranchesAndMerge() {
        String branchRev1 = mk.branch(null);
        String branchRev2 = mk.branch(null);

        branchRev1 = mk.commit("", "+\"/branch1\":{}", branchRev1, "");
        branchRev2 = mk.commit("", "+\"/branch2\":{}", branchRev2, "");
        branchRev1 = mk.commit("", "+\"/branch1/child1\":{}", branchRev1, "");
        branchRev2 = mk.commit("", "+\"/branch2/child2\":{}", branchRev2, "");

        assertFalse(mk.nodeExists("/branch1/child1", null));
        assertFalse(mk.nodeExists("/branch2/child2", null));

        assertTrue(mk.nodeExists("/branch1/child1", branchRev1));
        assertFalse(mk.nodeExists("/branch2/child2", branchRev1));

        assertFalse(mk.nodeExists("/branch1/child1", branchRev2));
        assertTrue(mk.nodeExists("/branch2/child2", branchRev2));

        mk.commit("", "+\"/trunk\":{}", null, "");

        assertTrue(mk.nodeExists("/trunk", null));
        assertFalse(mk.nodeExists("/trunk", branchRev1));
        assertFalse(mk.nodeExists("/trunk", branchRev2));

        mk.merge(branchRev1, "");
        assertTrue(mk.nodeExists("/trunk", null));
        assertTrue(mk.nodeExists("/branch1", null));
        assertTrue(mk.nodeExists("/branch1/child1", null));
        assertFalse(mk.nodeExists("/branch2", null));
        assertFalse(mk.nodeExists("/branch2/child2", null));

        mk.merge(branchRev2, "");
        assertTrue(mk.nodeExists("/trunk", null));
        assertTrue(mk.nodeExists("/branch1", null));
        assertTrue(mk.nodeExists("/branch1/child1", null));
        assertTrue(mk.nodeExists("/branch2", null));
        assertTrue(mk.nodeExists("/branch2/child2", null));
    }

    @Test
    public void emptyMergeCausesNoChange() {
        String rev1 = mk.commit("", "+\"/child1\":{}", null, "");

        String branchRev = mk.branch(null);
        branchRev = mk.commit("", "+\"/child2\":{}", branchRev, "");
        branchRev = mk.commit("", "-\"/child2\"", branchRev, "");

        String rev2 = mk.merge(branchRev, "");

        assertTrue(mk.nodeExists("/child1", null));
        assertFalse(mk.nodeExists("/child2", null));
        assertEquals(rev1, rev2);
    }

    @Test
    public void trunkMergeNotAllowed() {
        String rev = mk.commit("", "+\"/child1\":{}", null, "");
        try {
            mk.merge(rev, "");
            fail("Exception expected");
        } catch (Exception expected) {}
    }

    // FIXME - Add more complicated branch/merge tests with properties, conflicts etc.
}
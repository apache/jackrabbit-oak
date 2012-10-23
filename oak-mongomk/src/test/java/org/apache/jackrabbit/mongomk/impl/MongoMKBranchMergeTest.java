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
    public void oneBranchOneChild() {
        // Commit to initial trunk.
        commitNodes(null, "/trunk", "/trunk/child1");

        // Check initial trunk children exist on trunk.
        assertNodesExist(null, "/trunk", "/trunk/child1");

        // Branch.
        String branchRev = mk.branch(null);

        // Commit to branch.
        branchRev = commitNodes(branchRev, "/branch1", "/branch1/child1");

        // Check initial trunk children still exist in branch.
        assertNodesExist(branchRev, "/trunk", "/trunk/child1");

        // Check that branch children also exist in branch.
        assertNodesExist(branchRev, "/branch1", "/branch1/child1");

        // But branch children does not exist in trunk.
        assertNodesNotExist(null, "/branch1", "/branch1/child1");

        // Add another child on trunk.
        commitNodes(null, "/trunk/child2");

        // Check that the new child exists in trunk but not on branch still.
        assertNodesExist(null, "/trunk/child2");
        assertNodesNotExist(branchRev, "/trunk/child2");
    }

    @Test
    public void twoBranchesOneChild() {
        // Commit to initial trunk.
        commitNodes(null, "/trunk", "/trunk/child1");

        // Check initial trunk children exist on trunk.
        assertNodesExist(null, "/trunk", "/trunk/child1");

        // Branches.
        String branchRev1 = mk.branch(null);
        String branchRev2 = mk.branch(null);

        // Commit to branches.
        branchRev1 = commitNodes(branchRev1, "/branch1", "/branch1/child1");
        branchRev2 = commitNodes(branchRev2, "/branch2", "/branch2/child2");

        // Check initial trunk children still exist in branches.
        assertNodesExist(branchRev1, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev2, "/trunk", "/trunk/child1");

        // Check that branch children also exist in branches.
        assertTrue(mk.nodeExists("/branch1/child1", branchRev1));
        assertFalse(mk.nodeExists("/branch2/child2", branchRev1));
        assertTrue(mk.nodeExists("/branch2/child2", branchRev2));
        assertFalse(mk.nodeExists("/branch1/child1", branchRev2));

        // But branch children does not exist in trunk.
        assertFalse(mk.nodeExists("/branch1/child1", null));
        assertFalse(mk.nodeExists("/branch2/child2", null));

        // Add another child on trunk.
        commitNodes(null, "/trunk/child2");

        // Check that the new child exists in trunk but not on branches still.
        assertNodesExist(null, "/trunk/child2");
        assertNodesNotExist(branchRev1, "/trunk/child2");
        assertNodesNotExist(branchRev2, "/trunk/child2");
    }

    @Test
    public void twoBranchesAndMerge() {
        // Commit to initial trunk.
        commitNodes(null, "/trunk", "/trunk/child1");

        // Check initial trunk children exist on trunk.
        assertNodesExist(null, "/trunk", "/trunk/child1");

        // Branches.
        String branchRev1 = mk.branch(null);
        String branchRev2 = mk.branch(null);

        // Commit to branches.
        branchRev1 = commitNodes(branchRev1, "/branch1", "/branch1/child1");
        branchRev2 = commitNodes(branchRev2, "/branch2", "/branch2/child2");

        // Check initial trunk children still exist in branches.
        assertNodesExist(branchRev1, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev2, "/trunk", "/trunk/child1");

        // Check that branch children also exist in branches.
        assertTrue(mk.nodeExists("/branch1/child1", branchRev1));
        assertFalse(mk.nodeExists("/branch2/child2", branchRev1));
        assertTrue(mk.nodeExists("/branch2/child2", branchRev2));
        assertFalse(mk.nodeExists("/branch1/child1", branchRev2));

        // But branch children does not exist in trunk.
        assertFalse(mk.nodeExists("/branch1/child1", null));
        assertFalse(mk.nodeExists("/branch2/child2", null));

        // Add another child on trunk.
        commitNodes(null, "/trunk/child2");

        // Check that the new child exists in trunk but not on branches still.
        assertNodesExist(null, "/trunk/child2");
        assertNodesNotExist(branchRev1, "/trunk/child2");
        assertNodesNotExist(branchRev2, "/trunk/child2");

        // Merge branch1 and do the checks.
        mk.merge(branchRev1, "");
        assertTrue(mk.nodeExists("/trunk", null));
        assertTrue(mk.nodeExists("/branch1", null));
        assertTrue(mk.nodeExists("/branch1/child1", null));
        assertFalse(mk.nodeExists("/branch2", null));
        assertFalse(mk.nodeExists("/branch2/child2", null));

        // Merge branch2 and do the checks.
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

    private String commitNodes(String rev, String...nodes) {
        String newRev = rev;
        for (String node : nodes) {
            newRev = mk.commit("", "+\"" + node + "\":{}", rev, "");
        }
        return newRev;
    }

    private void assertNodesExist(String revision, String...paths) {
        doAssertNodes(true, revision, paths);
    }

    private void assertNodesNotExist(String revision, String...paths) {
        doAssertNodes(false, revision, paths);
    }

    private void doAssertNodes(boolean checkExists, String revision, String...paths) {
        for (String path : paths) {
            boolean exists = mk.nodeExists(path, revision);
            if (checkExists) {
                assertTrue(exists);
            } else {
                assertFalse(exists);
            }
        }
    }
}
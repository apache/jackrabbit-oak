package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.json.simple.JSONObject;
import org.junit.Test;

/**
 * FIXME - Add more complicated branch/merge tests with properties, conflicts etc.
 * Also test getNodes work with branch.
 *
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
    public void twoBranchesOneChildAndMerge() {
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
    public void oneBranchSubChildren() {
        // Commit to initial trunk.
        commitNodes(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");

        // Check initial trunk children exist on trunk.
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");

        // Branch.
        String branchRev = mk.branch(null);

        // Commit to branch.
        branchRev = commitNodes(branchRev, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");

        // Check initial trunk children still exist in branch.
        assertNodesExist(branchRev, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");

        // Check that branch children also exist in branch.
        assertNodesExist(branchRev, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");

        // But branch children does not exist in trunk.
        assertNodesNotExist(null, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");

        // Add more children on trunk.
        commitNodes(null, "/trunk/child1/child2/child3/child4", "/trunk/child5");

        // Check that the new children exists in trunk but not on branch still.
        assertNodesExist(null, "/trunk/child1/child2/child3/child4", "/trunk/child5");
        assertNodesNotExist(branchRev, "/trunk/child1/child2/child3/child4", "/trunk/child5");
    }

    @Test
    public void oneBranchOneChildWithProperties() {
        // Commit to initial trunk.
        commitNodes(null, "/trunk", "/trunk/child1");

        // Add some props on node.
        commitProp(null, "/trunk/child1/prop1", "value1");
        commitProp(null, "/trunk/child1/prop2", "value2");

        // Check initial trunk children exist on trunk.
        assertNodesExist(null, "/trunk", "/trunk/child1");

        // Check initial properties exist on trunk.
        assertPropExists(null, "/trunk/child1", "prop1");
        assertPropExists(null, "/trunk/child1", "prop2");

        // Branch.
        String branchRev = mk.branch(null);

        // Commit to branch.
        branchRev = commitNodes(branchRev, "/branch1", "/branch1/child1");

        // Add some props on node.
        branchRev = commitProp(branchRev, "/branch1/child1/prop1", "value1");
        branchRev = commitProp(branchRev, "/branch1/child1/prop2", "value2");

        // Check initial trunk children still exist in branch.
        assertNodesExist(branchRev, "/trunk", "/trunk/child1");

        // Check initial properties still exist in branch.
        assertPropExists(branchRev, "/trunk/child1", "prop1");
        assertPropExists(branchRev, "/trunk/child1", "prop2");

        // Check that branch children also exist in branch.
        assertNodesExist(branchRev, "/branch1", "/branch1/child1");

        // Check properties exist in branch.
        assertPropExists(branchRev, "/branch1/child1", "prop1");
        assertPropExists(branchRev, "/branch1/child1", "prop2");

        // But branch children does not exist in trunk.
        assertNodesNotExist(null, "/branch1", "/branch1/child1");

        // And branch properties does not exist in trunk.
        assertPropNotExists(null, "/branch1/child1", "prop1");
        assertPropNotExists(null, "/branch1/child1", "prop2");

        // Merge branch1 and do the checks.
        mk.merge(branchRev, "");
        assertTrue(mk.nodeExists("/trunk", null));
        assertTrue(mk.nodeExists("/branch1", null));
        assertTrue(mk.nodeExists("/branch1/child1", null));
        assertFalse(mk.nodeExists("/branch2", null));
        assertFalse(mk.nodeExists("/branch2/child2", null));
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

    private void assertPropExists(String rev, String path, String property) {
        String nodes = mk.getNodes(path, rev, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyExists(obj, property);
    }

    private void assertPropNotExists(String rev, String path, String property) {
        String nodes = mk.getNodes(path, rev, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        if (nodes == null) {
            return;
        }
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyNotExists(obj, property);
    }

    private String commitNodes(String rev, String...nodes) {
        String newRev = rev;
        for (String node : nodes) {
            newRev = mk.commit("", "+\"" + node + "\":{}", rev, "");
        }
        return newRev;
    }

    private String commitProp(String rev, String prop, String value) {
        return mk.commit("", "^\"" + prop + "\" : \"" + value + "\"", rev, "");
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
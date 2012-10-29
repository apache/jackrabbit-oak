package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.json.simple.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

/**
 * FIXME - Add more complicated branch/merge tests such as:
 * -Branch with changed child nodes.
 * -Merges with conflicts.
 *
 * Tests for {@code MicroKernel#branch}
 */
public class MongoMKBranchMergeTest extends BaseMongoMicroKernelTest {

    @Test
    public void oneBranchAddedChildren1() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/branch1", "/branch1/child1");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev, "/branch1", "/branch1/child1");
        assertNodesNotExist(null, "/branch1", "/branch1/child1");

        addNodes(null, "/trunk/child2");
        assertNodesExist(null, "/trunk/child2");
        assertNodesNotExist(branchRev, "/trunk/child2");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child2", "/branch1", "/branch1/child1");
    }

    @Test
    public void oneBranchAddedChildren2() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/trunk/child1/child2");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev, "/trunk/child1/child2");
        assertNodesNotExist(null, "/trunk/child1/child2");

        addNodes(null, "/trunk/child3");
        assertNodesExist(null, "/trunk/child3");
        assertNodesNotExist(branchRev, "/trunk/child3");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child3");
    }

    @Test
    public void addExistingRootInBranch() {
        addNodes(null, "/root");
        assertNodesExist(null, "/root");

        String branchRev = mk.branch(null);
        try {
            branchRev = addNodes(branchRev, "/root");
            fail("Should not be able to add the same root node twice");
        } catch (Exception expected) {}
    }

    @Test
    public void addExistingChildInBranch() {
        addNodes(null, "/root", "/root/child1");
        assertNodesExist(null, "/root", "/root/child1");

        String branchRev = mk.branch(null);
        branchRev = addNodes(branchRev, "/root/child2");
        assertNodesExist(branchRev, "/root/child1", "/root/child2");

        try {
            branchRev = addNodes(branchRev, "/root/child1");
            fail("Should not be able to add the same root node twice");
        } catch (Exception expected) {}
    }

    // FIXME - The last merge does not work correctly.
    @Test
    @Ignore
    public void oneBranchAddedChildren3() {
        addNodes(null, "/root", "/root/child1");
        assertNodesExist(null, "/root", "/root/child1");

        String branchRev = mk.branch(null);

        addNodes(null, "/root/child2");
        assertNodesExist(null, "/root", "/root/child1", "/root/child2");
        assertNodesExist(branchRev, "/root", "/root/child1");
        assertNodesNotExist(branchRev, "/root/child2");

        branchRev = addNodes(branchRev, "/root/child1/child3", "/root/child4");
        assertNodesExist(branchRev, "/root", "/root/child1", "/root/child1/child3", "/root/child4");
        assertNodesNotExist(branchRev, "/root/child2");
        assertNodesExist(null, "/root", "/root/child1", "/root/child2");
        assertNodesNotExist(null, "/root/child1/child3", "/root/child4");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/root", "/root/child1", "/root/child2",
                "/root/child1/child3", "/root/child4");
    }

    @Test
    public void oneBranchRemovedChildren() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev = mk.branch(null);

        branchRev = removeNodes(branchRev, "/trunk/child1");
        assertNodesExist(branchRev, "/trunk");
        assertNodesNotExist(branchRev, "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk");
        assertNodesNotExist(null, "/trunk/child1");
    }

    @Test
    public void oneBranchChangedProperties() {
        addNodes(null, "/trunk", "/trunk/child1");
        setProp(null, "/trunk/child1/prop1", "value1");
        setProp(null, "/trunk/child1/prop2", "value2");
        assertNodesExist(null, "/trunk", "/trunk/child1");
        assertPropExists(null, "/trunk/child1", "prop1");
        assertPropExists(null, "/trunk/child1", "prop2");

        String branchRev = mk.branch(null);

        branchRev = setProp(branchRev, "/trunk/child1/prop1", "value1a");
        branchRev = setProp(branchRev, "/trunk/child1/prop2", null);
        branchRev = setProp(branchRev, "/trunk/child1/prop3", "value3");
        assertPropValue(branchRev, "/trunk/child1", "prop1", "value1a");
        assertPropNotExists(branchRev, "/trunk/child1", "prop2");
        assertPropValue(branchRev, "/trunk/child1", "prop3", "value3");
        assertPropValue(null, "/trunk/child1", "prop1", "value1");
        assertPropExists(null, "/trunk/child1", "prop2");
        assertPropNotExists(null, "/trunk/child1", "prop3");

        mk.merge(branchRev, "");
        assertPropValue(null, "/trunk/child1", "prop1", "value1a");
        assertPropNotExists(null, "/trunk/child1", "prop2");
        assertPropValue(null, "/trunk/child1", "prop3", "value3");
    }

    @Test
    public void oneBranchAddedSubChildren() {
        addNodes(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3");
        assertNodesExist(branchRev, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");
        assertNodesNotExist(null, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");

        addNodes(null, "/trunk/child1/child2/child3/child4", "/trunk/child5");
        assertNodesExist(null, "/trunk/child1/child2/child3/child4", "/trunk/child5");
        assertNodesNotExist(branchRev, "/trunk/child1/child2/child3/child4", "/trunk/child5");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1", "/trunk/child1/child2", "/trunk/child1/child2/child3", "/trunk/child1/child2/child3/child4");
        assertNodesExist(null, "/branch1", "/branch1/child1", "/branch1/child1/child2", "/branch1/child1/child2/child3");
    }

    @Test
    public void oneBranchAddedChildrenAndAddedProperties() {
        addNodes(null, "/trunk", "/trunk/child1");
        setProp(null, "/trunk/child1/prop1", "value1");
        setProp(null, "/trunk/child1/prop2", "value2");
        assertNodesExist(null, "/trunk", "/trunk/child1");
        assertPropExists(null, "/trunk/child1", "prop1");
        assertPropExists(null, "/trunk/child1", "prop2");

        String branchRev = mk.branch(null);

        branchRev = addNodes(branchRev, "/branch1", "/branch1/child1");
        branchRev = setProp(branchRev, "/branch1/child1/prop1", "value1");
        branchRev = setProp(branchRev, "/branch1/child1/prop2", "value2");
        assertNodesExist(branchRev, "/trunk", "/trunk/child1");
        assertPropExists(branchRev, "/trunk/child1", "prop1");
        assertPropExists(branchRev, "/trunk/child1", "prop2");
        assertNodesExist(branchRev, "/branch1", "/branch1/child1");
        assertPropExists(branchRev, "/branch1/child1", "prop1");
        assertPropExists(branchRev, "/branch1/child1", "prop2");
        assertNodesNotExist(null, "/branch1", "/branch1/child1");
        assertPropNotExists(null, "/branch1/child1", "prop1");
        assertPropNotExists(null, "/branch1/child1", "prop2");

        mk.merge(branchRev, "");
        assertNodesExist(null, "/trunk", "/trunk/child1");
        assertPropExists(null, "/trunk/child1", "prop1");
        assertPropExists(null, "/trunk/child1", "prop2");
        assertNodesExist(null, "/branch1", "/branch1/child1");
        assertPropExists(null, "/branch1/child1", "prop1");
        assertPropExists(null, "/branch1/child1", "prop2");
    }

    @Test
    public void twoBranchesAddedChildren1() {
        addNodes(null, "/trunk", "/trunk/child1");
        assertNodesExist(null, "/trunk", "/trunk/child1");

        String branchRev1 = mk.branch(null);
        String branchRev2 = mk.branch(null);

        branchRev1 = addNodes(branchRev1, "/branch1", "/branch1/child1");
        branchRev2 = addNodes(branchRev2, "/branch2", "/branch2/child2");
        assertNodesExist(branchRev1, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev2, "/trunk", "/trunk/child1");
        assertNodesExist(branchRev1, "/branch1/child1");
        assertNodesNotExist(branchRev1, "/branch2/child2");
        assertNodesExist(branchRev2, "/branch2/child2");
        assertNodesNotExist(branchRev2, "/branch1/child1");
        assertNodesNotExist(null, "/branch1/child1", "/branch2/child2");

        addNodes(null, "/trunk/child2");
        assertNodesExist(null, "/trunk/child2");
        assertNodesNotExist(branchRev1, "/trunk/child2");
        assertNodesNotExist(branchRev2, "/trunk/child2");

        mk.merge(branchRev1, "");
        assertNodesExist(null, "/trunk", "/branch1", "/branch1/child1");
        assertNodesNotExist(null, "/branch2", "/branch2/child2");

        mk.merge(branchRev2, "");
        assertNodesExist(null, "/trunk", "/branch1", "/branch1/child1", "/branch2", "/branch2/child2");
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

    private void assertPropValue(String rev, String path, String property, String value) {
        String nodes = mk.getNodes(path, rev, -1 /*depth*/, 0 /*offset*/, -1 /*maxChildNodes*/, null /*filter*/);
        JSONObject obj = parseJSONObject(nodes);
        assertPropertyValue(obj, property, value);
    }

    private String addNodes(String rev, String...nodes) {
        String newRev = rev;
        for (String node : nodes) {
            newRev = mk.commit("", "+\"" + node + "\":{}", rev, "");
        }
        return newRev;
    }

    private String removeNodes(String rev, String...nodes) {
        String newRev = rev;
        for (String node : nodes) {
            newRev = mk.commit("", "-\"" + node + "\"", rev, "");
        }
        return newRev;
    }

    private String setProp(String rev, String prop, Object value) {
        value = value == null? null : "\"" + value + "\"";
        return mk.commit("", "^\"" + prop + "\" : " + value, rev, "");
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
                assertTrue(path + " does not exist", exists);
            } else {
                assertFalse(path + " should not exist", exists);
            }
        }
    }
}
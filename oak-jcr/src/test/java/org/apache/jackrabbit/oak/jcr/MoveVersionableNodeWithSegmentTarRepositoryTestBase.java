package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Session;

import static org.junit.Assert.assertEquals;

/**
 * Base test for moving versionable nodes over deleted versionable nodes.
 * This test uses SEGMENT_TAR fixture for repository.
 * This test is used to reproduce the bug for OAK-8848.
 *
 * Extend and change the fixture to test with different node stores (i.e.: DOCUMENT_NS - Mongo, MEMORY_NS, etc.).
 */
public class MoveVersionableNodeWithSegmentTarRepositoryTestBase extends AbstractRepositoryTest {

    private static final String mixVersionable = "mix:versionable";

    public MoveVersionableNodeWithSegmentTarRepositoryTestBase(NodeStoreFixture fixture) {
        super(fixture);
    }

    /**
     * Creates a versionable node with the specified name under the given parent node, then
     * saves the session.
     * @param parent
     * @param nodeName
     * @return
     * @throws Exception
     */
    private Node createVersionableNode(Node parent, String nodeName) throws Exception {
        Node newNode = (parent.hasNode(nodeName)) ? parent.getNode(nodeName) : parent.addNode(nodeName);
        if (newNode.canAddMixin(mixVersionable)) {
            newNode.addMixin(mixVersionable);
        } else {
            throw new NotExecutableException();
        }
        newNode.getSession().save();
        return newNode;
    }

    /**
     * Checks out the node, sets the property then saves the session and checks the node back in.
     * To be used in tests where version history needs to be populated.
     */
    private void setNodePropertyAndCheckIn(Node node, String propertyName, String propertyValue) throws Exception {
        node.checkout();
        node.setProperty(propertyName, propertyValue);
        node.getSession().save();
        node.checkin();
    }

    /*
        * 1. Create a versionable unstructured node at nodeName1/nodeName2/sourceNode
        * 2. Create a versionable unstructured node at nodeName1/nodeName3/sourceNode
        * 3. create version histories for both nodes
        * 4. remove nodeName1/nodeName3/nodeName1 (that's because move(src,dest) throws an exception if dest already exists)
        * 5. move nodeName1/nodeName2/sourceNode to nodeName1/nodeName3/sourceNode and call session.save()
        * 6. should work according to JCR specification - reproduces bug for OAK-8848: will throw ConstraintViolationException
        *  - "Property is protected: jcr:versionHistory
     */
    @Test
    public void testMoveNodeWithVersionHistoryOverDeletedNodeWithVersionHistory() throws Exception {

        Session session = getAdminSession();
        Node testRootNode = session.getRootNode().addNode("node1");

        String newNodeName = "sourceNode";
        Node sourceParent = testRootNode.addNode("node2"); // nodeName1/nodeName2
        Node sourceNode = createVersionableNode(sourceParent, newNodeName); // nodeName1/nodeName2/sourceNode
        Node destParent = testRootNode.addNode("node3"); // nodeName1/nodeName3
        Node destNode = createVersionableNode(destParent, newNodeName); // nodeName1/nodeName3/sourceNode

        // add version histories for sourceNode and destNode
        setNodePropertyAndCheckIn(sourceNode, "sourceNode_testProp", "sourceNode_testValue_1");
        setNodePropertyAndCheckIn(destNode, "destNode_testProp", "destNode_testValue_1");

        session.save();

        setNodePropertyAndCheckIn(sourceNode, "sourceNode_testProp", "sourceNode_testValue_2");
        setNodePropertyAndCheckIn(destNode, "destNode_testProp", "destNode_testValue_2");

        session.save();

        String destPath = destNode.getPath();
        session.removeItem(destNode.getPath());

        session.move(sourceNode.getPath(), destPath);
        session.save();

        assertEquals(sourceNode.getPath(), destPath);
    }

    /**
     * Creates two versionable nodes (with mix:versionable), deletes one and moves the other over the deleted one.
     * The move operation should succeed.
     * Reproduces bug for OAK-8848: will throw ConstraintViolationException - "Property is protected: jcr:versionHistory".
     */
    @Test
    public void testMoveVersionableNodeOverDeletedVersionableNode_SimpleHierarchy() throws Exception {

        Session session = getAdminSession();
        Node testRootNode = session.getRootNode().addNode("node1");

        Node sourceParent = testRootNode.addNode("node2"); // nodeName1/nodeName2
        Node sourceNode = createVersionableNode(sourceParent, "source"); // nodeName1/nodeName2/sourceNode
        Node destParent = testRootNode.addNode("node3"); // nodeName1/nodeName3
        Node destNode = createVersionableNode(destParent, "dest"); // nodeName1/nodeName3/sourceNode // nodeName1/nodeName3/sourceNode

        String destPath = destNode.getPath();
        session.removeItem(destNode.getPath());

        session.move(sourceNode.getPath(), destPath);
        session.save();

        assertEquals(sourceNode.getPath(), destPath);
    }

    @Test
    public void testMoveVersionableNodeOverDeletedVersionableNode_ComplexHierarchy_1() throws Exception {

        Session session = getAdminSession();
        Node testRootNode = session.getRootNode().addNode("node1");

        // node1 / node_1_1 / node_1_2 / node_1_3 / [source]
        Node sourceParent = testRootNode.addNode("node_1_1").addNode("node_1_2").addNode("node_1_3");
        Node sourceNode = createVersionableNode(sourceParent, "source");

        // node1 / node_2_1 / node_2_2 / [dest]
        Node destParent = testRootNode.addNode("node_2_1").addNode("node_2_2");
        Node destNode = createVersionableNode(destParent, "dest");

        String destPath = destNode.getPath();
        session.removeItem(destNode.getPath());

        session.move(sourceNode.getPath(), destPath);
        session.save();

        assertEquals(sourceNode.getPath(), destPath);
    }

    @Test
    public void testMoveVersionableNodeOverDeletedVersionableNode_ComplexHierarchy_2() throws Exception {

        Session session = getAdminSession();
        Node testRootNode = session.getRootNode().addNode("node1");

        // node1 / node_1_1 / node_1_2 / node_1_3 / [source]
        Node sourceParent = testRootNode.addNode("node_1_1").addNode("node_1_2").addNode("node_1_3");
        Node sourceNode = createVersionableNode(sourceParent, "source");

        // node1 / node_1_1 / node_2_1 / node 2_2/ node_2_3 / [dest]
        Node destParent = testRootNode.getNode("node_1_1").addNode("node_2_1").
                addNode("node_2_2").addNode("node_2_3");
        Node destNode = createVersionableNode(destParent, "dest");

        String destPath = destNode.getPath();
        session.removeItem(destNode.getPath());

        session.move(sourceNode.getPath(), destPath);
        session.save();

        assertEquals(sourceNode.getPath(), destPath);
    }
}

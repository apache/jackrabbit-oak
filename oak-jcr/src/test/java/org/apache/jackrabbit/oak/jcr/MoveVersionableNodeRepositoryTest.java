/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.NodeStoreFixtures;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;
import org.junit.runners.Parameterized;

import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionIterator;
import javax.jcr.version.VersionManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_NS;
import static org.junit.Assert.assertEquals;

/**
 * Test for moving versionable nodes over deleted versionable nodes using the DOCUMENT_NS fixture.
 */
public class MoveVersionableNodeRepositoryTest extends AbstractRepositoryTest {

    public MoveVersionableNodeRepositoryTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> memoryFixture() {
        return NodeStoreFixtures.asJunitParameters(singleton(DOCUMENT_NS));
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
        if (newNode.canAddMixin(JcrConstants.MIX_VERSIONABLE)) {
            newNode.addMixin(JcrConstants.MIX_VERSIONABLE);
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
        Node destNode = createVersionableNode(destParent, "destNode"); // nodeName1/nodeName3/sourceNode

        String destPath = destNode.getPath();

        // add version histories for sourceNode and destNode
        setNodePropertyAndCheckIn(sourceNode, "sourceNode_testProp", "sourceNode_testValue_1");

        session.save();

        setNodePropertyAndCheckIn(sourceNode, "sourceNode_testProp", "sourceNode_testValue_2");
        setNodePropertyAndCheckIn(destNode, "destNode_testProp", "destNode_testValue_2");

        session.save();

        VersionHistory delNodeVersionHistory = destNode.getVersionHistory();
        String delNodeVHPath = delNodeVersionHistory.getPath();

        session.removeItem(destNode.getPath());

        session.move(sourceNode.getPath(), destPath);
        session.save();

        // check setting property - via NodeImpl - on moved node
        setNodePropertyAndCheckIn(sourceNode, "testProp", "testValue");

        // check version history
        VersionManager versionManager = session.getWorkspace().getVersionManager();
        VersionIterator versionIterator;

        List<Version> sourceNodeVersions = new ArrayList<>();

        //get the version histories for the removed node and the moved node
        try {
            VersionHistory sourceNodeHistory = (VersionHistory) session.getNode(delNodeVHPath);

            versionIterator = sourceNodeHistory.getAllVersions();
            while (versionIterator.hasNext()) {
                sourceNodeVersions.add(versionIterator.nextVersion());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        VersionHistory destNodeHistory = versionManager.getVersionHistory(destPath);
        versionIterator = destNodeHistory.getAllVersions();

        List<Version> destNodeVersions = new ArrayList<>();
        while (versionIterator.hasNext()) {
            destNodeVersions.add(versionIterator.nextVersion());
        };

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

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

import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.junit.Test;

public class MoveTest extends AbstractJCRTest {

    private void move(String src, String dest, boolean save) throws Exception {
        superuser.move(src, dest);
        if (save) {
            superuser.save();
        }
        assertFalse(superuser.nodeExists(src));
        assertFalse(canGetNode(src));
        assertTrue(superuser.nodeExists(dest));
        assertTrue(canGetNode(dest));
    }

    // See OAK-1040
    private boolean canGetNode(String path) throws RepositoryException {
        try {
            superuser.getNode(path);
            return true;
        } catch (PathNotFoundException e) {
            return false;
        }
    }

    @Test
    public void testRename() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        superuser.save();

        String destPath = testRoot + '/' + nodeName2;
        move(node1.getPath(), destPath, true);

        assertEquals(destPath, node1.getPath());
    }

    @Test
    public void testRenameNewNode() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);

        String destPath = testRoot + '/' + nodeName2;
        move(node1.getPath(), destPath, false);

        assertEquals(destPath, node1.getPath());
    }

    @Test
    public void testMove() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        Node node2 = testRootNode.addNode(nodeName2);
        superuser.save();

        String destPath = node2.getPath() + '/' + nodeName1;
        move(node1.getPath(), destPath, true);

        assertEquals(destPath, node1.getPath());
    }

    /**
     * Simulate a 'rename' call using 3 sessions:
     * - 1st create a node that has a '.tmp' extension 
     * - 2nd remove the '.tmp' by issuing a Session#move call on a new session
     * - 3rd verify the move by issuing a #getNode call on the destination path using a new session
     */
    @Test
    public void testOak898() throws Exception {
        String name = "testMove";
        Node node = testRootNode.addNode(name + ".tmp");
        superuser.save();
        String destPath = testRootNode.getPath() + '/' + name;

        Session session2 = getHelper().getSuperuserSession();
        try {
            assertFalse(session2.hasPendingChanges());
            session2.move(node.getPath(), destPath);
            assertTrue(session2.hasPendingChanges());
            session2.save();

        } finally {
            session2.logout();
        }

        Session session3 = getHelper().getSuperuserSession();
        try {
            assertNotNull(session3.getNode(destPath));
        } finally {
            session3.logout();
        }
    }

    @Test
    public void testMoveReferenceable() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        node1.addMixin(JcrConstants.MIX_REFERENCEABLE);
        Node node2 = testRootNode.addNode(nodeName2);
        superuser.save();

        String destPath = node2.getPath() + '/' + nodeName1;
        move(node1.getPath(), destPath, true);

        assertEquals(destPath, node1.getPath());
    }

    @Test
    public void testMoveNewNode() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        Node node2 = testRootNode.addNode(nodeName2);

        String destPath = node2.getPath() + '/' + nodeName1;
        move(node1.getPath(), destPath, false);

        assertEquals(destPath, node1.getPath());
    }

    @Test
    public void testMoveNewReferenceable() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        node1.addMixin(JcrConstants.MIX_REFERENCEABLE);
        assertTrue(node1.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        Node node2 = testRootNode.addNode(nodeName2);

        String destPath = node2.getPath() + '/' + nodeName1;
        move(node1.getPath(), destPath, false);

        assertEquals(destPath, node1.getPath());

        superuser.save();
        assertEquals(destPath, node1.getPath());
    }
}
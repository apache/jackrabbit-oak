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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import javax.jcr.AccessDeniedException;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.util.Text;
import org.junit.Test;

/**
 * Permission evaluation tests for move operations.
 */
public class SessionMoveTest extends AbstractMoveTest {

    protected void move(String source, String dest) throws RepositoryException {
        move(source, dest, testSession);
    }

    @Override
    protected void move(String source, String dest, Session session) throws RepositoryException {
        session.move(source, dest);
        session.save();
    }

    private void setupMovePermissions() throws Exception {
        allow(path, privilegesFromNames(new String[]{
                Privilege.JCR_REMOVE_NODE,
                Privilege.JCR_REMOVE_CHILD_NODES
        }));
        allow(siblingPath, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));

    }

    @Test
    public void testMoveAndRemoveSubTree() throws Exception {
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        allow(siblingPath, privilegesFromNames(new String[]{
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));

        testSession.move(childNPath, siblingDestPath);

        Node moved = testSession.getNode(siblingDestPath);
        Node child = moved.getNode(nodeName3);

        try {
            child.remove();
            testSession.save();
            fail("Removing subtree after move requires 'jcr:removeNode' privilege on the target");
        } catch (AccessDeniedException e) {
            // success

        }
    }

    @Test
    public void testMoveAndRemoveSubTree2() throws Exception {
        allow(path, privilegesFromNames(new String[] {
                Privilege.JCR_REMOVE_CHILD_NODES,
                Privilege.JCR_REMOVE_NODE}));
        allow(siblingPath, privilegesFromNames(new String[]{
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));
        deny(testSession.getNode(nodePath3).getPath(), privilegesFromName(Privilege.JCR_REMOVE_NODE));

        try {
            testSession.move(childNPath, siblingDestPath);

            Node moved = testSession.getNode(siblingDestPath);
            Node child = moved.getNode(nodeName3);

            child.remove();
            testSession.save();
            fail("Removing subtree after move requires 'jcr:removeNode' on the removed child.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndRemoveSubTree3() throws Exception {
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));
        allow(siblingPath, privilegesFromNames(new String[] {
                PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT
        }));

        testSession.move(childNPath, siblingDestPath);

        Node moved = testSession.getNode(siblingDestPath);
        Node child = moved.getNode(nodeName3);
        child.remove();

        testSession.save();
    }

    @Test
    public void testMoveRemoveSubTreeWithRestriction() throws Exception {
        /* allow READ/WRITE privilege for testUser at 'path' */
        allow(path, testUser.getPrincipal(), readWritePrivileges);
        /* deny REMOVE_NODE privileges at subtree. */
        deny(path, privilegesFromName(PrivilegeConstants.JCR_REMOVE_NODE), createGlobRestriction("*/"+nodeName3));

        assertTrue(testSession.nodeExists(childNPath));
        assertTrue(testSession.hasPermission(childNPath, Session.ACTION_REMOVE));
        assertTrue(testSession.hasPermission(childNPath2, Session.ACTION_ADD_NODE));

        testSession.move(childNPath, childNPath2 + "/dest");
        Node dest = testSession.getNode(childNPath2 + "/dest");
        dest.getNode(nodeName3).remove();

        try {
            testSession.save();
            fail("Removing child node must be denied.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveRemoveSubTreeWithRestriction2() throws Exception {
            /* allow READ/WRITE privilege for testUser at 'path' */
        allow(path, testUser.getPrincipal(), readWritePrivileges);
            /* deny REMOVE_NODE privileges at subtree. */
        deny(path, privilegesFromName(PrivilegeConstants.JCR_REMOVE_CHILD_NODES), createGlobRestriction("*/" + Text.getName(childNPath)));

        assertTrue(testSession.nodeExists(childNPath));
        assertTrue(testSession.hasPermission(childNPath, Session.ACTION_REMOVE));
        assertTrue(testSession.hasPermission(childNPath2, Session.ACTION_ADD_NODE));

        testSession.move(childNPath, childNPath2 + "/dest");
        Node dest = testSession.getNode(childNPath2 + "/dest");
        dest.getNode(nodeName3).remove();

        try {
            testSession.save();
            fail("Removing child node must be denied.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndAddSubTree() throws Exception {
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));
        allow(siblingPath, privilegesFromNames(new String[] {
                PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT
        }));

        testSession.move(childNPath, siblingDestPath);

        Node moved = testSession.getNode(siblingDestPath);
        Node child = moved.getNode(nodeName3);
        child.addNode(nodeName4);

        try {
            testSession.save();
            fail("Adding child node at moved node must be denied: no add_child_node privilege at original location.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndAddSubTree2() throws Exception {
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));
        allow(siblingPath, privilegesFromNames(new String[] {
                PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT
        }));
        allow(nodePath3, privilegesFromName(Privilege.JCR_ADD_CHILD_NODES));

        testSession.move(childNPath, siblingDestPath);

        Node moved = testSession.getNode(siblingDestPath);
        Node child = moved.getNode(nodeName3);
        child.addNode(nodeName4);

        testSession.save();
    }

    @Test
    public void testMoveAndAddSubTree3() throws Exception {
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        allow(childNPath, privilegesFromNames(new String[] {
                Privilege.JCR_REMOVE_NODE, Privilege.JCR_ADD_CHILD_NODES
        }));
        allow(siblingPath, privilegesFromNames(new String[] {
                PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT
        }));

        testSession.move(childNPath, siblingDestPath);

        Node moved = testSession.getNode(siblingDestPath);
        Node child = moved.getNode(nodeName3);
        child.addNode(nodeName4);

        testSession.save();
    }

    @Test
    public void testMoveAddSubTreeWithRestriction() throws Exception {
        /* allow READ/WRITE privilege for testUser at 'path' */
        allow(path, testUser.getPrincipal(), readWritePrivileges);
        /* deny ADD_CHILD_NODES privileges at subtree. */
        deny(path, privilegesFromName(PrivilegeConstants.JCR_ADD_CHILD_NODES), createGlobRestriction("*/"+nodeName3));

        testSession.move(childNPath, childNPath2 + "/dest");
        Node dest = testSession.getNode(childNPath2 + "/dest");
        dest.getNode(nodeName3).addNode(nodeName4);

        try {
            testSession.save();
            fail("Adding child node must be denied.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndAddAtSourceParent() throws Exception {
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));
        allow(siblingPath, privilegesFromNames(new String[]{
                PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT
        }));

        testSession.move(childNPath, siblingDestPath);

        Node sourceParent = testSession.getNode(path);
        sourceParent.addNode(nodeName4);

        try {
            testSession.save();
            fail("Adding child node at source parent be denied: missing add_child_node privilege.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndAddAtSourceParent2() throws Exception {
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));
        allow(siblingPath, privilegesFromNames(new String[] {
                PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT
        }));
        allow(nodePath3, privilegesFromName(Privilege.JCR_ADD_CHILD_NODES));

        testSession.move(childNPath, siblingDestPath);

        Node sourceParent = testSession.getNode(path);
        sourceParent.addNode(nodeName4);

        try {
            testSession.save();
            fail("Adding child node at source parent be denied: missing add_child_node privilege.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndAddAtSourceParent3() throws Exception {
        allow(path, privilegesFromNames(new String[]{
                Privilege.JCR_REMOVE_CHILD_NODES, Privilege.JCR_ADD_CHILD_NODES
        }));
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));
        allow(siblingPath, privilegesFromNames(new String[]{
                PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT
        }));

        testSession.move(childNPath, siblingDestPath);

        Node sourceParent = testSession.getNode(path);
        sourceParent.addNode(nodeName4);

        testSession.save();
    }

    @Test
    public void testMoveAndAddReplacementAtSource() throws Exception {
        allow(path, privilegesFromNames(new String[]{
                Privilege.JCR_REMOVE_CHILD_NODES, Privilege.JCR_ADD_CHILD_NODES
        }));
        allow(siblingPath, privilegesFromNames(new String[] {
                PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT
        }));

        testSession.move(nodePath3, siblingDestPath);

        Node sourceParent = testSession.getNode(childNPath);
        Node replacement = sourceParent.addNode(Text.getName(nodePath3));
        replacement.setProperty("movedProp", "val");

        try {
            testSession.save();
            fail("Missing ADD_NODE and ADD_PROPERTY permission on source parent.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndAddReplacementAtSource2() throws Exception {
        allow(siblingPath, privilegesFromNames(new String[] {
                PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT
        }));

        testSession.move(nodePath3, siblingDestPath);

        Node sourceParent = testSession.getNode(childNPath);
        Node replacement = sourceParent.addNode(Text.getName(nodePath3));
        replacement.setProperty("movedProp", "val");

        try {
            testSession.save();
            fail("Missing REMOVE_NODE permission for move source.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndAddProperty() throws Exception {
        setupMovePermissions();

        testSession.move(nodePath3, siblingDestPath);
        Node destNode = testSession.getNode(siblingDestPath);
        Property p = destNode.setProperty("newProp", "val");
        try {
            testSession.save();
            fail("Missing ADD_PROPERTY permission.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndAddProperty2() throws Exception {
        setupMovePermissions();
        allow(childNPath, privilegesFromName(PrivilegeConstants.REP_ADD_PROPERTIES));

        testSession.move(nodePath3, siblingDestPath);
        Node destNode = testSession.getNode(siblingDestPath);
        Property p = destNode.setProperty("newProp", "val");
        // now save must succeed
        testSession.save();
    }

    @Test
    public void testMoveAndModifyProperty() throws Exception {
        setupMovePermissions();

        testSession.move(nodePath3, siblingDestPath);
        Node destNode = testSession.getNode(siblingDestPath);
        destNode.setProperty("movedProp", "modified");
        try {
            testSession.save();
            fail("Missing MODIFY_PROPERTY permission.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndModifyProperty2() throws Exception {
        setupMovePermissions();
        allow(siblingPath, privilegesFromName(PrivilegeConstants.REP_ALTER_PROPERTIES));

        testSession.move(nodePath3, siblingDestPath);
        Node destNode = testSession.getNode(siblingDestPath);
        destNode.setProperty("movedProp", "modified");
        try {
            testSession.save();
            fail("Missing MODIFY_PROPERTY permission.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndModifyProperty3() throws Exception {
        setupMovePermissions();
        allow(childNPath, privilegesFromName(PrivilegeConstants.REP_ALTER_PROPERTIES));

        testSession.move(nodePath3, siblingDestPath);
        Node destNode = testSession.getNode(siblingDestPath);
        destNode.setProperty("movedProp", "modified");
    }

    @Test
    public void testMoveAndRemoveProperty() throws Exception {
        setupMovePermissions();

        testSession.move(nodePath3, siblingDestPath);
        Node destNode = testSession.getNode(siblingDestPath);
        destNode.getProperty("movedProp").remove();
        try {
            testSession.save();
            fail("Missing REMOVE_PROPERTY permission.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndRemoveProperty2() throws Exception {
        allow(path, privilegesFromNames(new String[]{
                Privilege.JCR_REMOVE_NODE,
                Privilege.JCR_REMOVE_CHILD_NODES,
                PrivilegeConstants.REP_REMOVE_PROPERTIES
        }));
        allow(siblingPath, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));

        testSession.move(nodePath3, siblingDestPath);
        Node destNode = testSession.getNode(siblingDestPath);
        destNode.getProperty("movedProp").remove();
        testSession.save();
    }

    @Test
    public void testMoveAndAddPropertyAtSource() throws Exception {
        setupMovePermissions();

        testSession.move(nodePath3, siblingDestPath);
        Node n = testSession.getNode(childNPath);
        Property p = n.setProperty("newProp", "val");
        try {
            testSession.save();
            fail("Missing ADD_PROPERTY permission.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndAddPropertyAtSource2() throws Exception {
        setupMovePermissions();
        allow(childNPath, privilegesFromName(PrivilegeConstants.REP_ADD_PROPERTIES));

        testSession.move(nodePath3, siblingDestPath);
        Node n = testSession.getNode(childNPath);
        Property p = n.setProperty("newProp", "val");
        testSession.save();
    }

    @Test
    public void testMoveAndModifyPropertyAtSource() throws Exception {
        setupMovePermissions();

        testSession.move(nodePath3, siblingDestPath);
        Node n = testSession.getNode(childNPath);
        assertTrue(n.hasProperty(propertyName1));
        n.setProperty(propertyName1, "modified");
        try {
            testSession.save();
            fail("Missing MODIFY_PROPERTY permission.");
        } catch (AccessDeniedException e) {
            // success
        }

        allow(childNPath, privilegesFromName(PrivilegeConstants.REP_ALTER_PROPERTIES));
        testSession.save();
    }

    @Test
    public void testMoveAndModifyPropertyAtSource2() throws Exception {
        setupMovePermissions();
        allow(childNPath, privilegesFromName(PrivilegeConstants.REP_ALTER_PROPERTIES));

        testSession.move(nodePath3, siblingDestPath);
        Node n = testSession.getNode(childNPath);
        assertTrue(n.hasProperty(propertyName1));
        n.setProperty(propertyName1, "modified");
        testSession.save();
    }

    @Test
    public void testMoveAndRemovePropertyAtSource() throws Exception {
        setupMovePermissions();

        testSession.move(nodePath3, siblingDestPath);
        Node n = testSession.getNode(childNPath);
        assertTrue(n.hasProperty(propertyName1));
        n.getProperty(propertyName1).remove();
        try {
            testSession.save();
            fail("Missing REMOVE_PROPERTY permission.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testMoveAndRemovePropertyAtSource2() throws Exception {
        setupMovePermissions();
        allow(childNPath, privilegesFromName(PrivilegeConstants.REP_REMOVE_PROPERTIES));

        testSession.move(nodePath3, siblingDestPath);
        Node n = testSession.getNode(childNPath);
        assertTrue(n.hasProperty(propertyName1));
        n.getProperty(propertyName1).remove();
        testSession.save();
    }

    /**
     * Moving and removing the moved node at destination should be treated like
     * a simple removal at the original position.
     */
    @Test
    public void testMoveAndRemoveDestination() throws Exception {
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));

        testSession.move(nodePath3, siblingDestPath);
        Node destNode = testSession.getNode(siblingDestPath);
        destNode.remove();
        testSession.save();
    }
}

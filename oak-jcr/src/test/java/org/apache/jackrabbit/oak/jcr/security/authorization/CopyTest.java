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
import javax.jcr.NodeIterator;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.util.Text;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing Workspace#copy with limited permissions both on source and target
 * location.
 */
public class CopyTest extends AbstractEvaluationTest {

    private String targetPath;
    private String destPath;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        Node target = testRootNode.addNode("target");
        targetPath = target.getPath();
        superuser.save();
        testSession.refresh(false);

        destPath = targetPath + "/copy";
    }

    @Test
    public void testCopyNoWritePermissionAtTarget() throws Exception {
        try {
            testSession.getWorkspace().copy(path, destPath);
            fail("no write permission at copy target");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testCopyWithInsufficientPermissions() throws Exception {
        allow(targetPath, privilegesFromName(Privilege.JCR_ADD_CHILD_NODES));
        try {
            testSession.getWorkspace().copy(path, destPath);
            fail("insufficient write permission at copy target");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testCopyWithFullPermissions() throws Exception {
        allow(targetPath, privilegesFromName(Privilege.JCR_ALL));

        testSession.getWorkspace().copy(path, destPath);

        assertTrue(testSession.nodeExists(destPath));
    }

    @Test
    public void testCopyInvisibleSubTree() throws Exception {
        deny(childNPath, privilegesFromName(Privilege.JCR_READ));
        allow(targetPath, privilegesFromName(Privilege.JCR_ALL));

        assertFalse(testSession.nodeExists(childNPath));

        testSession.getWorkspace().copy(path, destPath);

        Node copiedNode = testSession.getNode(destPath);
        String childName = Text.getName(childNPath);
        assertFalse(copiedNode.hasNode(childName));
        assertTrue(copiedNode.hasNode(Text.getName(childNPath2)));

        superuser.refresh(false);
        assertFalse(superuser.nodeExists(destPath + '/' + childName));
    }

    @Test
    public void testCopyInvisibleProperty() throws Exception {
        deny(childNPath, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));
        allow(targetPath, privilegesFromName(Privilege.JCR_ALL));

        testSession.getWorkspace().copy(path, destPath);

        Node copiedNode = testSession.getNode(destPath);
        String childName = Text.getName(childNPath);
        assertTrue(copiedNode.hasNode(childName));
        assertFalse(copiedNode.hasProperty(childName + '/'+ propertyName1));

        superuser.refresh(false);
        assertFalse(superuser.nodeExists(destPath + '/' + childName + '/' + propertyName1));
    }

    @Test
    public void testCopyInvisibleAcContent() throws Exception {
        deny(childNPath, privilegesFromName(Privilege.JCR_READ_ACCESS_CONTROL));
        allow(targetPath, privilegesFromName(PrivilegeConstants.JCR_ALL));

        testSession.getWorkspace().copy(path, destPath);

        Node copiedNode = testSession.getNode(destPath);
        String childName = Text.getName(childNPath);
        assertTrue(copiedNode.hasNode(childName));

        Node child = copiedNode.getNode(childName);
        assertFalse(child.hasNode(AccessControlConstants.REP_POLICY));

        superuser.refresh(false);
        assertFalse(superuser.nodeExists(targetPath + '/' + childName + "/rep:policy"));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2810">OAK-2810</a>
     */
    @Test
    public void testCopyFromNonAccessibleSourceParent() throws Exception {
        // deny read access to the parent of the source to be copied
        deny(path, privilegesFromName(PrivilegeConstants.JCR_ALL));
        // allow copying the node itself
        Privilege[] readWrite = privilegesFromNames(new String[] {PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE});
        allow(childNPath, readWrite);
        allow(targetPath, readWrite);

        testSession.getWorkspace().copy(childNPath, destPath);
        assertTrue(testSession.nodeExists(destPath));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2810">OAK-2810</a>
     */
    @Test
    public void testCopyToProtectedParentDestination() throws Exception {
        // create a protected target parent node (not meant for permission validation this time)
        allow(targetPath, privilegesFromName(Privilege.JCR_ALL));

        Node destParent = null;
        NodeIterator nodeIterator = superuser.getNode(targetPath).getNode(AccessControlConstants.REP_POLICY).getNodes();
        while (nodeIterator.hasNext()) {
            Node n = nodeIterator.nextNode();
            if (n.getDefinition().isProtected()) {
                destParent = n;
                break;
            }
        }

        if (destParent == null) {
            throw new NotExecutableException("No protected parent found");
        }

        try {
            superuser.getWorkspace().copy(childNPath, destParent.getPath() + "/copy");
            fail("Copy must fail with constraint-violation if the destination parent is a protected node.");
        } catch (ConstraintViolationException e) {
            // success
            assertEquals("Node " + destParent.getPath() + " is protected.", e.getMessage());

        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2810">OAK-2810</a>
     */
    @Test
    public void testCopyFromProtectedParentSource() throws Exception {
        // create a protected source parent node (not meant for permission validation this time)
        allow(childNPath, privilegesFromName(PrivilegeConstants.JCR_ALL));

        Node sourceNode = null;
        NodeIterator nodeIterator = superuser.getNode(childNPath).getNode(AccessControlConstants.REP_POLICY).getNodes();
        while (nodeIterator.hasNext()) {
            Node n = nodeIterator.nextNode();
            if (n.getDefinition().isProtected()) {
                sourceNode = n;
                break;
            }
        }

        if (sourceNode == null || !sourceNode.getParent().getDefinition().isProtected()) {
            throw new NotExecutableException("No protected parent found");
        }

        try {
            superuser.getWorkspace().copy(sourceNode.getPath(), destPath);
        } catch (AccessControlException e) {
            // success : the copy fails because an isolated ACE is copied but
            // NOT because the source-parent was a protected node.
        }
    }
}
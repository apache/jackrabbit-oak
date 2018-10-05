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
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

/**
 * Permission evaluation tests for move operations.
 */
public abstract class AbstractMoveTest extends AbstractEvaluationTest {

    protected String nodePath3;

    protected String destPath;
    protected String siblingDestPath;

    protected Privilege[] modifyChildCollection;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        Node node3 = superuser.getNode(childNPath).addNode(nodeName3);
        node3.setProperty("movedProp", "val");
        nodePath3 = node3.getPath();
        superuser.save();
        testSession.refresh(false);

        destPath = path + "/destination";
        siblingDestPath = siblingPath + "/destination";

        modifyChildCollection = privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_REMOVE_CHILD_NODES});
    }

    abstract protected void move(String source, String dest) throws RepositoryException;
    abstract protected void move(String source, String dest, Session session) throws RepositoryException;

    @Test
    public void testMove() throws Exception {
        // give 'add_child_nodes' and 'nt-management' privilege
        // -> not sufficient privileges for a move
        allow(path, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));
        try {
            move(childNPath, destPath);
            fail("Move requires addChildNodes and removeChildNodes privilege.");
        } catch (AccessDeniedException e) {
            // success.
        }
    }

    @Test
    public void testMove2() throws Exception {
        // grant 'add_child_nodes', 'remove_child_nodes' at 'path'
        // -> not sufficient for a move since 'remove_node' privilege is missing
        //    on the move-target
        allow(path, modifyChildCollection);
        try {
            move(childNPath, destPath);
            fail("Move requires addChildNodes and removeChildNodes privilege.");
        } catch (AccessDeniedException e) {
            // success.
        }
    }

    @Test
    public void testMove2b() throws Exception {
        // grant 'add_child_nodes', 'remove_child_nodes' and 'nt_mgt' at 'path'
        // -> not sufficient for a move since 'remove_node' privilege is missing
        //    on the move-target
        allow(path, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_REMOVE_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));
        try {
            move(childNPath, destPath);
            fail("Move requires addChildNodes and removeChildNodes privilege.");
        } catch (AccessDeniedException e) {
            // success.
        }
    }

    @Test
    public void testMove3() throws Exception {
        allow(path, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_REMOVE_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));

        // allow 'remove_node' at childNPath
        // -> now move must succeed
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));

        move(childNPath, destPath);
    }

    @Test
    public void testMove4() throws Exception {
        allow(path, privilegesFromName(PrivilegeConstants.REP_WRITE));
        move(childNPath, destPath);

        // withdraw  'add_child_nodes' privilege on former src-parent
        // -> moving child-node back must fail
        deny(path, privilegesFromName(Privilege.JCR_ADD_CHILD_NODES));

        try {
            move(destPath, childNPath);
            fail("Move requires addChildNodes and removeChildNodes privilege.");
        } catch (AccessDeniedException e) {
            // success.
        }
    }

    @Test
    public void testMissingJcrAddChildNodesAtDestParent() throws Exception {
        allow(path, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_REMOVE_CHILD_NODES}));
        try {
            move(childNPath, siblingDestPath);
            fail("Move requires addChildNodes privilege at dest parent");
        } catch (AccessDeniedException e) {
            // success.
        }
    }

    @Test
    public void testDifferentDestParent() throws Exception {
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        allow(siblingPath, privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES, Privilege.JCR_NODE_TYPE_MANAGEMENT
        }));
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));

        move(childNPath, siblingDestPath);
    }

    @Test
    public void testMoveAccessControlledNode() throws Exception {
        // permissions defined @ childNode
        // -> revoke read permission
        deny(childNPath, readPrivileges);

        assertFalse(testSession.nodeExists(childNPath));
        assertHasPrivileges(childNPath, readPrivileges, false);
        assertFalse(testSession.nodeExists(nodePath3));
        assertHasPrivileges(nodePath3, readPrivileges, false);

        // move the ancestor node
        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        move(childNPath, movedChildNPath, superuser);

        // expected behavior:
        // the AC-content present on childNode is still enforced both on
        // the node itself and on the subtree.
        assertFalse(testSession.nodeExists(movedChildNPath));
        assertHasPrivileges(movedChildNPath, readPrivileges, false);
        assertFalse(testSession.nodeExists(movedNode3Path));
        assertHasPrivileges(movedNode3Path, readPrivileges, false);
    }

    @Test
    public void testMoveAccessControlledNodeInSubtree() throws Exception {
        // permissions defined @ node3Path
        // -> revoke read permission
        deny(nodePath3, readPrivileges);

        assertFalse(testSession.nodeExists(nodePath3));
        assertHasPrivileges(nodePath3, readPrivileges, false);

        // move the ancestor node
        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        move(childNPath, movedChildNPath, superuser);

        // expected behavior:
        // the AC-content present on node3 is still enforced
        assertFalse(testSession.nodeExists(movedNode3Path));
        assertHasPrivileges(movedNode3Path, readPrivileges, false);
    }

    @Test
    public void testMoveWithDifferentEffectiveAc() throws Exception {
        // @path read is denied, @childNode its allowed again
        deny(path, readPrivileges);
        allow(childNPath, readPrivileges);

        assertTrue(testSession.nodeExists(nodePath3));
        assertTrue(testAcMgr.hasPrivileges(nodePath3, readPrivileges));

        // move the ancestor node
        String movedPath = path + "/movedNode";

        move(nodePath3, movedPath, superuser);

        // expected behavior:
        // due to move node3 should not e visible any more
        assertFalse(testSession.nodeExists(movedPath));
        assertHasPrivileges(movedPath, readPrivileges, false);
    }

    @Test
    public void testMoveNodeWithGlobRestriction() throws Exception {
        // permissions defined @ path
        // restriction: remove read priv to nodeName3 node
        deny(childNPath, readPrivileges, createGlobRestriction('/' +nodeName3));

        assertFalse(testSession.nodeExists(nodePath3));
        assertHasPrivileges(nodePath3, readPrivileges, false);

        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        move(childNPath, movedChildNPath, superuser);

        assertFalse(testSession.nodeExists(movedNode3Path));
        assertHasPrivileges(movedNode3Path, readPrivileges, false);
    }

    @Test
    public void testMoveNodeWithGlobRestriction2() throws Exception {
        // permissions defined @ path
        // restriction: remove read priv to nodeName3 node
        deny(childNPath, readPrivileges, createGlobRestriction('/' + nodeName3));

        assertHasPrivileges(nodePath3, readPrivileges, false);

        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        move(childNPath, movedChildNPath, superuser);

        assertFalse(testSession.nodeExists(movedNode3Path));
        assertHasPrivileges(movedNode3Path, readPrivileges, false);
    }
}

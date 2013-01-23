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
import javax.jcr.security.Privilege;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * MoveTest... TODO
 */
@Ignore("OAK-51")
public class MoveTest extends AbstractEvaluationTest {

    private String nodePath3;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        Node node3 = testRootNode.getNode(childNPath).addNode(nodeName3);
        nodePath3 = node3.getPath();
        superuser.save();
    }

    @Test
    public void testSessionMove() throws Exception {
        String destPath = path + '/' + nodeName1;

        // give 'add_child_nodes' and 'nt-management' privilege
        // -> not sufficient privileges for a move
        allow(path, privilegesFromNames(new String[] {Privilege.JCR_ADD_CHILD_NODES, Privilege.JCR_NODE_TYPE_MANAGEMENT}));
        try {
            testSession.move(childNPath, destPath);
            testSession.save();
            fail("Move requires add and remove permission.");
        } catch (AccessDeniedException e) {
            // success.
        }

        // add 'remove_child_nodes' at 'path
        // -> not sufficient for a move since 'remove_node' privilege is missing
        //    on the move-target
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        try {
            testSession.move(childNPath, destPath);
            testSession.save();
            fail("Move requires add and remove permission.");
        } catch (AccessDeniedException e) {
            // success.
        }

        // allow 'remove_node' at childNPath
        // -> now move must succeed
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));
        testSession.move(childNPath, destPath);
        testSession.save();

        // withdraw  'add_child_nodes' privilege on former src-parent
        // -> moving child-node back must fail
        deny(path, privilegesFromName(Privilege.JCR_ADD_CHILD_NODES));
        try {
            testSession.move(destPath, childNPath);
            testSession.save();
            fail("Move requires add and remove permission.");
        } catch (AccessDeniedException e) {
            // success.
        }
    }

    @Test
    public void testWorkspaceMove() throws Exception {
        String destPath = path + '/' + nodeName1;

        // give 'add_child_nodes', 'nt-mgmt' privilege
        // -> not sufficient privileges for a move.
        allow(path, privilegesFromNames(new String[] {Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_NODE_TYPE_MANAGEMENT}));
        try {
            testSession.getWorkspace().move(childNPath, destPath);
            fail("Move requires add and remove permission.");
        } catch (AccessDeniedException e) {
            // success.
        }

        // add 'remove_child_nodes' at 'path
        // -> no sufficient for a move since 'remove_node' privilege is missing
        //    on the move-target
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        try {
            testSession.getWorkspace().move(childNPath, destPath);
            fail("Move requires add and remove permission.");
        } catch (AccessDeniedException e) {
            // success.
        }

        // allow 'remove_node' at childNPath
        // -> now move must succeed
        allow(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE));
        testSession.getWorkspace().move(childNPath, destPath);

        // withdraw  'add_child_nodes' privilege on former src-parent
        // -> moving child-node back must fail
        deny(path, privilegesFromName(Privilege.JCR_ADD_CHILD_NODES));
        try {
            testSession.getWorkspace().move(destPath, childNPath);
            fail("Move requires add and remove permission.");
        } catch (AccessDeniedException e) {
            // success.
        }
    }

    @Test
    public void testMoveAccessControlledNode() throws Exception {
        // permissions defined @ childNode
        // -> revoke read permission
        deny(childNPath, readPrivileges);

        assertFalse(testSession.nodeExists(childNPath));
        assertFalse(testAcMgr.hasPrivileges(childNPath, readPrivileges));
        assertFalse(testSession.nodeExists(nodePath3));
        assertFalse(testAcMgr.hasPrivileges(nodePath3, readPrivileges));

        // move the ancestor node
        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        superuser.move(childNPath, movedChildNPath);
        superuser.save();

        // expected behavior:
        // the AC-content present on childNode is still enforced both on
        // the node itself and on the subtree.
        assertFalse(testSession.nodeExists(movedChildNPath));
        assertFalse(testAcMgr.hasPrivileges(movedChildNPath, readPrivileges));
        assertFalse(testSession.nodeExists(movedNode3Path));
        assertFalse(testAcMgr.hasPrivileges(movedNode3Path, readPrivileges));
    }

    @Test
    public void testMoveAccessControlledNodeInSubtree() throws Exception {
        // permissions defined @ node3Path
        // -> revoke read permission
        deny(nodePath3, readPrivileges);

        assertFalse(testSession.nodeExists(nodePath3));
        assertFalse(testAcMgr.hasPrivileges(nodePath3, readPrivileges));

        // move the ancestor node
        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        superuser.move(childNPath, movedChildNPath);
        superuser.save();

        // expected behavior:
        // the AC-content present on node3 is still enforced
        assertFalse(testSession.nodeExists(movedNode3Path));
        assertFalse(testAcMgr.hasPrivileges(movedNode3Path, readPrivileges));
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

        superuser.move(nodePath3, movedPath);
        superuser.save();

        // expected behavior:
        // due to move node3 should not e visible any more
        assertFalse(testSession.nodeExists(movedPath));
        assertFalse(testAcMgr.hasPrivileges(movedPath, readPrivileges));
    }

    @Test
    public void testMoveNodeWithGlobRestriction() throws Exception {
        // permissions defined @ path
        // restriction: remove read priv to nodeName3 node
        deny(childNPath, readPrivileges, createGlobRestriction('/' +nodeName3));

        assertFalse(testSession.nodeExists(nodePath3));
        assertFalse(testAcMgr.hasPrivileges(nodePath3, readPrivileges));

        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        superuser.move(childNPath, movedChildNPath);
        superuser.save();

        assertFalse(testSession.nodeExists(movedNode3Path));
        assertFalse(testAcMgr.hasPrivileges(movedNode3Path, readPrivileges));
    }

    @Test
    public void testMoveNodeWithGlobRestriction2() throws Exception {
        // permissions defined @ path
        // restriction: remove read priv to nodeName3 node
        deny(childNPath, readPrivileges, createGlobRestriction('/' + nodeName3));

        // don't fill the per-session read-cache by calling Session.nodeExists
        assertFalse(testAcMgr.hasPrivileges(nodePath3, readPrivileges));

        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        superuser.move(childNPath, movedChildNPath);
        superuser.save();

        assertFalse(testSession.nodeExists(movedNode3Path));
        assertFalse(testAcMgr.hasPrivileges(movedNode3Path, readPrivileges));
    }
}
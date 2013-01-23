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

import java.util.Collections;
import java.util.Map;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * MoveTest... TODO
 */
@Ignore("OAK-51")
public class MoveTest extends AbstractEvaluationTest {

    private Privilege[] readPrivileges;

    private Session testSession;
    private AccessControlManager testAcManager;

    private String path;
    private String nodePath2;
    private String nodePath3;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        readPrivileges = privilegesFromName(Privilege.JCR_READ);
        testSession = getTestSession();
        testAcManager = getTestAccessControlManager();

        // create some nodes below the test root in order to apply ac-stuff
        Node node = testRootNode.addNode(nodeName1, testNodeType);
        Node node2 = node.addNode(nodeName2, testNodeType);
        Node node3 = node2.addNode(nodeName3);
        superuser.save();

        nodePath3 = node3.getPath();

        superuser.save();

        path = node.getPath();
        nodePath2 = node2.getPath();

        /*
        precondition:
        testuser must have READ-only permission on test-node and below
        */
        checkReadOnly(path);
        checkReadOnly(nodePath2);
    }

    private Map<String, Value> createGlobRestriction(String value) throws RepositoryException {
        return Collections.singletonMap("rep:glob", getTestSession().getValueFactory().createValue(value));
    }

    @Test
    public void testMoveAccessControlledNode() throws Exception {
        // permissions defined @ childNode
        // -> revoke read permission
        deny(nodePath2, readPrivileges);

        assertFalse(testSession.nodeExists(nodePath2));
        assertFalse(testAcManager.hasPrivileges(nodePath2, readPrivileges));
        assertFalse(testSession.nodeExists(nodePath3));
        assertFalse(testAcManager.hasPrivileges(nodePath3, readPrivileges));

        // move the ancestor node
        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        superuser.move(nodePath2, movedChildNPath);
        superuser.save();

        // expected behavior:
        // the AC-content present on childNode is still enforced both on
        // the node itself and on the subtree.
        assertFalse(testSession.nodeExists(movedChildNPath));
        assertFalse(testAcManager.hasPrivileges(movedChildNPath, readPrivileges));
        assertFalse(testSession.nodeExists(movedNode3Path));
        assertFalse(testAcManager.hasPrivileges(movedNode3Path, readPrivileges));
    }

    @Test
    public void testMoveAccessControlledNodeInSubtree() throws Exception {
        // permissions defined @ node3Path
        // -> revoke read permission
        deny(nodePath3, readPrivileges);

        assertFalse(testSession.nodeExists(nodePath3));
        assertFalse(testAcManager.hasPrivileges(nodePath3, readPrivileges));

        // move the ancestor node
        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        superuser.move(nodePath2, movedChildNPath);
        superuser.save();

        // expected behavior:
        // the AC-content present on node3 is still enforced
        assertFalse(testSession.nodeExists(movedNode3Path));
        assertFalse(testAcManager.hasPrivileges(movedNode3Path, readPrivileges));
    }

    @Test
    public void testMoveWithDifferentEffectiveAc() throws Exception {
        // @path read is denied, @childNode its allowed again
        deny(path, readPrivileges);
        allow(nodePath2, readPrivileges);

        assertTrue(testSession.nodeExists(nodePath3));
        assertTrue(testAcManager.hasPrivileges(nodePath3, readPrivileges));

        // move the ancestor node
        String movedPath = path + "/movedNode";

        superuser.move(nodePath3, movedPath);
        superuser.save();

        // expected behavior:
        // due to move node3 should not e visible any more
        assertFalse(testSession.nodeExists(movedPath));
        assertFalse(testAcManager.hasPrivileges(movedPath, readPrivileges));
    }

    @Test
    public void testMoveNodeWithGlobRestriction() throws Exception {
        // permissions defined @ path
        // restriction: remove read priv to nodeName3 node
        deny(nodePath2, readPrivileges, createGlobRestriction('/' +nodeName3));

        assertFalse(testSession.nodeExists(nodePath3));
        assertFalse(testAcManager.hasPrivileges(nodePath3, readPrivileges));

        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        superuser.move(nodePath2, movedChildNPath);
        superuser.save();

        assertFalse(testSession.nodeExists(movedNode3Path));
        assertFalse(testAcManager.hasPrivileges(movedNode3Path, readPrivileges));
    }

    @Test
    public void testMoveNodeWithGlobRestriction2() throws Exception {
        // permissions defined @ path
        // restriction: remove read priv to nodeName3 node
        deny(nodePath2, readPrivileges, createGlobRestriction('/' + nodeName3));

        // don't fill the per-session read-cache by calling Session.nodeExists
        assertFalse(testAcManager.hasPrivileges(nodePath3, readPrivileges));

        String movedChildNPath = path + "/movedNode";
        String movedNode3Path = movedChildNPath + '/' + nodeName3;

        superuser.move(nodePath2, movedChildNPath);
        superuser.save();

        assertFalse(testSession.nodeExists(movedNode3Path));
        assertFalse(testAcManager.hasPrivileges(movedNode3Path, readPrivileges));
    }
}
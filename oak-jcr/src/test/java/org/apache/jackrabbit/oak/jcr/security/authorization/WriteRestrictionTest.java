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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.test.api.util.Text;
import org.junit.Before;
import org.junit.Test;

import static javax.jcr.security.Privilege.JCR_ADD_CHILD_NODES;
import static javax.jcr.security.Privilege.JCR_REMOVE_CHILD_NODES;
import static javax.jcr.security.Privilege.JCR_REMOVE_NODE;

/**
 * WriteRestrictionTest: tests add and remove node in combination with glob restrictions.
 */
public class WriteRestrictionTest extends AbstractEvaluationTest {

    private String nodePath3;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        Node node3 = superuser.getNode(childNPath).addNode(nodeName3);
        nodePath3 = node3.getPath();
        superuser.save();
        testSession.refresh(false);
    }

    @Test
    public void testGlobRestriction() throws Exception {
        String writeActions = getActions(Session.ACTION_ADD_NODE, Session.ACTION_REMOVE, Session.ACTION_SET_PROPERTY);

        // permissions defined @ path
        // restriction: grants write priv to all nodeName3 children
        allow(path, repWritePrivileges, createGlobRestriction("/*"+nodeName3));

        assertFalse(testAcMgr.hasPrivileges(path, repWritePrivileges));
        assertFalse(testSession.hasPermission(path, javax.jcr.Session.ACTION_SET_PROPERTY));

        assertFalse(testAcMgr.hasPrivileges(childNPath, repWritePrivileges));
        assertFalse(testSession.hasPermission(childNPath, javax.jcr.Session.ACTION_SET_PROPERTY));

        assertTrue(testAcMgr.hasPrivileges(childNPath2, repWritePrivileges));
        assertTrue(testSession.hasPermission(childNPath2, Session.ACTION_SET_PROPERTY));
        assertFalse(testSession.hasPermission(childNPath2, writeActions)); // removal req. rmchildnode privilege on parent.

        assertTrue(testAcMgr.hasPrivileges(nodePath3, repWritePrivileges));
    }

    @Test
    public void testGlobRestriction2() throws Exception {

        Privilege[] addNode = privilegesFromName(JCR_ADD_CHILD_NODES);
        Privilege[] rmNode = privilegesFromName(JCR_REMOVE_NODE);

        // permissions defined @ path
        // restriction: grants write-priv to nodeName3 grand-children but not direct nodeName3 children.
        allow(path, repWritePrivileges, createGlobRestriction("/*/"+nodeName3));

        assertFalse(testAcMgr.hasPrivileges(path, repWritePrivileges));
        assertFalse(testAcMgr.hasPrivileges(path, rmNode));
        assertFalse(testAcMgr.hasPrivileges(childNPath, addNode));
        assertFalse(testAcMgr.hasPrivileges(childNPath2, repWritePrivileges));
        assertTrue(testAcMgr.hasPrivileges(nodePath3, repWritePrivileges));
    }

    @Test
    public void testGlobRestriction3() throws Exception {
        Privilege[] addNode = privilegesFromName(JCR_ADD_CHILD_NODES);

        // permissions defined @ path
        // restriction: allows write to nodeName3 children
        allow(path, repWritePrivileges, createGlobRestriction("/*/"+nodeName3));
        // and grant add-node only at path (no glob restriction)
        allow(path, addNode);

        assertFalse(testAcMgr.hasPrivileges(path, repWritePrivileges));
        assertTrue(testAcMgr.hasPrivileges(path, addNode));

        assertFalse(testAcMgr.hasPrivileges(childNPath, repWritePrivileges));
        assertTrue(testAcMgr.hasPrivileges(childNPath, addNode));

        assertFalse(testAcMgr.hasPrivileges(childNPath2, repWritePrivileges));
        assertTrue(testAcMgr.hasPrivileges(nodePath3, repWritePrivileges));
    }

    @Test
    public void testGlobRestriction4() throws Exception {
        Privilege[] addNode = privilegesFromName(JCR_ADD_CHILD_NODES);

        allow(path, repWritePrivileges, createGlobRestriction("/*"+nodeName3));
        deny(childNPath2, addNode);

        assertFalse(testAcMgr.hasPrivileges(path, repWritePrivileges));
        assertFalse(testSession.hasPermission(path, javax.jcr.Session.ACTION_REMOVE));
        assertFalse(testAcMgr.hasPrivileges(childNPath, repWritePrivileges));
        assertFalse(testSession.hasPermission(childNPath, javax.jcr.Session.ACTION_REMOVE));
        assertFalse(testAcMgr.hasPrivileges(childNPath2, repWritePrivileges));
        assertTrue(testAcMgr.hasPrivileges(nodePath3, repWritePrivileges));
    }

    @Test
    public void testRemoveSubTreeWithRestriction() throws Exception {
        /* allow READ/WRITE privilege for testUser at 'path' */
        allow(path, testUser.getPrincipal(), readWritePrivileges);
        /* deny REMOVE_NODE privileges at subtree. */
        deny(path, privilegesFromName(JCR_REMOVE_NODE), createGlobRestriction("*/" + nodeName3));

        testSession.getNode(childNPath).getNode(nodeName3).remove();
        try {
            testSession.save();
            fail("Removing child node must be denied.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testRemoveSubTreeWithRestriction2() throws Exception {
        /* allow READ/WRITE privilege for testUser at 'path' */
        allow(path, testUser.getPrincipal(), readWritePrivileges);
        /* deny REMOVE_NODE privileges at subtree. */
        deny(path, privilegesFromName(JCR_REMOVE_CHILD_NODES), createGlobRestriction("*/" + Text.getName(childNPath)));

        testSession.getNode(childNPath).getNode(nodeName3).remove();
        try {
            testSession.save();
            fail("Removing child node must be denied.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testAddSubTreeWithRestriction() throws Exception {
        /* allow READ/WRITE privilege for testUser at 'path' */
        allow(path, testUser.getPrincipal(), readWritePrivileges);
        /* deny ADD_CHILD_NODES privileges at subtree. */
        deny(path, privilegesFromName(JCR_ADD_CHILD_NODES), createGlobRestriction("*/"+nodeName3));

        Node node4 = testSession.getNode(nodePath3).addNode(nodeName4);
        try {
            testSession.save();
            fail("Adding child node must be denied.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    /**
     * OAK-9179
     */
    @Test
    public void testGlobTrailingSlash() throws Exception {
        Node grandchild = superuser.getNode(childNPath).addNode("child");
        String ccPath = grandchild.getPath();

        // allow ADD_CHILD_NODES privileges at '/cat/' -> matches descendants of childNPath
        allow(path, privilegesFromName(JCR_ADD_CHILD_NODES), createGlobRestriction("/"+PathUtils.getName(childNPath) + "/"));
        assertGlobTrailingSlashEffect(ccPath);
    }

    /**
     * OAK-9179
     */
    @Test
    public void testGlobTrailingSlashWildcard() throws Exception {
        Node grandchild = superuser.getNode(childNPath).addNode("grandchild");
        String ccPath = grandchild.getPath();

        // allow ADD_CHILD_NODES privileges at '/cat/*' -> matches descendants of childNPath
        allow(path, privilegesFromName(JCR_ADD_CHILD_NODES), createGlobRestriction("/"+PathUtils.getName(childNPath) + "/*"));
        assertGlobTrailingSlashEffect(ccPath);
    }

    private void assertGlobTrailingSlashEffect(String ccPath) throws RepositoryException {
        assertFalse(testSession.hasPermission(path, Session.ACTION_ADD_NODE));
        assertFalse(testSession.hasPermission(childNPath, Session.ACTION_ADD_NODE));
        assertFalse(testSession.hasPermission(childNPath+"/", Session.ACTION_ADD_NODE));
        assertFalse(testSession.hasPermission(ccPath, Session.ACTION_ADD_NODE));
        assertFalse(testSession.hasPermission(ccPath+"/", Session.ACTION_ADD_NODE));
        assertTrue(testSession.hasPermission(ccPath+"/greatgrandchild", Session.ACTION_ADD_NODE));
        assertTrue(testSession.hasPermission(ccPath+"/greatgrandchild/", Session.ACTION_ADD_NODE));
        assertTrue(testSession.hasPermission(ccPath+"/greatgrandchild/descendant", Session.ACTION_ADD_NODE));
        testSession.getNode(ccPath).addNode("greatgrandchild").addNode("descendant");
        testSession.save();
    }
}
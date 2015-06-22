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
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.lock.LockManager;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.Privilege;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Before;
import org.junit.Test;

/**
 * Test access control evaluation for version operations.
 */
public class VersionManagementTest extends AbstractEvaluationTest {

    private static final String SYSTEM = "/jcr:system";
    private static final String VERSIONSTORE = SYSTEM + "/jcr:versionStorage";

    private Privilege[] versionPrivileges;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        versionPrivileges = privilegesFromName(Privilege.JCR_VERSION_MANAGEMENT);
        // must not see version storage or must not have version privilege
        assertTrue(!testSession.nodeExists(VERSIONSTORE)
                || !testAcMgr.hasPrivileges(VERSIONSTORE, versionPrivileges));
    }

    private Node createVersionableNode(Node parent) throws Exception {
        Node n = (parent.hasNode(nodeName1)) ? parent.getNode(nodeName1) : parent.addNode(nodeName1);
        if (n.canAddMixin(mixVersionable)) {
            n.addMixin(mixVersionable);
        } else {
            throw new NotExecutableException();
        }
        n.getSession().save();
        return n;
    }

    @Test
    public void testAddMixVersionable() throws Exception {
        modify(path, REP_WRITE, true);
        modify(path, Privilege.JCR_VERSION_MANAGEMENT, false);

        Node testNode = testSession.getNode(path);
        try {
            createVersionableNode(testNode);
            fail("Test session does not have permission to add mixins -> no version content should be created.");
        } catch (AccessDeniedException e) {
            // success
            // ... but autocreated versionable node properties must not be present
            assertFalse(testNode.isNodeType(mixVersionable));
            assertFalse(testNode.hasProperty("jcr:isCheckedOut"));
            assertFalse(testNode.hasProperty(jcrVersionHistory));
        }
    }

    @Test
    public void testAddMixVersionable2() throws Exception {
        modify(path, REP_WRITE, true);
        modify(path, Privilege.JCR_NODE_TYPE_MANAGEMENT, true);
        modify(path, Privilege.JCR_VERSION_MANAGEMENT, true);

        Node n = createVersionableNode(testSession.getNode(path));
        n.checkin();
        n.checkout();
    }

    @Test
    public void testCheckInCheckout() throws Exception {
        modify(path, REP_WRITE, true);
        modify(path, Privilege.JCR_VERSION_MANAGEMENT, false);

        Node n = createVersionableNode(superuser.getNode(path));
        try {
            testSession.refresh(false);
            Node testNode = testSession.getNode(n.getPath());
            testNode.checkin();
            fail("Missing jcr:versionManagement privilege -> checkin/checkout must fail.");
        } catch (AccessDeniedException e) {
            // success
            // ... but the property must not be modified nor indicating
            // checkedIn status
            Property p = n.getProperty("jcr:isCheckedOut");
            assertFalse(p.isModified());
            assertTrue(n.getProperty("jcr:isCheckedOut").getValue().getBoolean());
        }
    }

    /**
     * @since oak (DIFF: jr required jcr:versionManagement privilege on the version store)
     */
    @Test
    public void testRemoveVersion() throws Exception {
        Node n = createVersionableNode(superuser.getNode(path));

        Node trn = testSession.getNode(path);
        modify(trn.getPath(), Privilege.JCR_VERSION_MANAGEMENT, true);

        Node testNode = trn.getNode(n.getName());
        Version v = testNode.checkin();
        testNode.checkout();
        testNode.checkin();

        // removing a version must be allowed
        testNode.getVersionHistory().removeVersion(v.getName());
    }

    /**
     * @since oak (DIFF: jr required jcr:versionManagement privilege on the version store)
     */
    @Test
    public void testRemoveVersion2() throws Exception {
        Node n = createVersionableNode(superuser.getNode(path));

        Node trn = testSession.getNode(path);
        modify(trn.getPath(), Privilege.JCR_VERSION_MANAGEMENT, true);

        Node testNode = trn.getNode(n.getName());
        Version v = testNode.checkin();
        testNode.checkout();
        testNode.checkin();

        // remove ability to edit version information
        // -> VersionHistory.removeVersion must not be allowed.
        modify(trn.getPath(), Privilege.JCR_VERSION_MANAGEMENT, false);
        try {
            testNode.getVersionHistory().removeVersion(v.getName());
            fail("Missing jcr:versionManagement privilege -> remove a version must fail.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    /**
     * @since oak (DIFF: jr required jcr:versionManagement privilege on the version store)
     */
    @Test
    public void testRemoveVersion3() throws Exception {
        Node n = createVersionableNode(superuser.getNode(path));
        Version v = n.checkin();
        n.checkout();
        n.checkin();

        testSession.refresh(false);
        assertFalse(testAcMgr.hasPrivileges(n.getPath(), versionPrivileges));
        AccessControlList acl = allow(SYSTEM, versionPrivileges);

        try {
            Node testNode = testSession.getNode(n.getPath());
            testNode.getVersionHistory().removeVersion(v.getName());

            fail("Missing jcr:versionManagement privilege -> remove a version must fail.");
        } catch (AccessDeniedException e) {
            // success
        } finally {
            // revert privilege modification (manually remove the ACE added)
            for (AccessControlEntry entry : acl.getAccessControlEntries()) {
                if (entry.getPrincipal().equals(testUser.getPrincipal())) {
                    acl.removeAccessControlEntry(entry);
                }
            }
            acMgr.setPolicy(SYSTEM, acl);
            superuser.save();
        }
    }

    /**
     * @since oak (DIFF: jr required jcr:versionManagement privilege on the version store)
     */
    @Test
    public void testAccessVersionHistory() throws Exception {
        Node n = createVersionableNode(superuser.getNode(path));
        allow(n.getPath(), versionPrivileges);

        Node testNode = testSession.getNode(n.getPath());
        testNode.checkin();
        testNode.checkout();

        // accessing the version history must be allowed if the versionable node
        // is readable to the editing test session.
        VersionHistory vh = testNode.getVersionHistory();
        String vhPath = vh.getPath();
        String vhUUID = vh.getIdentifier();
        assertTrue(vh.isSame(testNode.getSession().getNode(vhPath)));
        assertTrue(vh.isSame(testNode.getSession().getNodeByIdentifier(vhUUID)));
        assertTrue(vh.isSame(testNode.getSession().getNodeByUUID(vhUUID)));
    }

    /**
     * @since oak (DIFF: jr required jcr:versionManagement privilege on the version store)
     */
    @Test
    public void testAccessVersionHistoryVersionableNodeNotAccessible() throws Exception {
        Node n = createVersionableNode(superuser.getNode(path));
        allow(n.getPath(), versionPrivileges);

        Node testNode = testSession.getNode(n.getPath());
        testNode.checkin();
        testNode.checkout();

        VersionHistory vh = testNode.getVersionHistory();
        String vhPath = vh.getPath();
        String vhUUID = vh.getIdentifier();

        // revert read permission on the versionable node
        modify(n.getPath(), Privilege.JCR_READ, false);

        // versionable node is not readable any more for test session.
        assertFalse(testSession.nodeExists(n.getPath()));

        // access version history directly => should fail
        try {
            VersionHistory history = (VersionHistory) testSession.getNode(vhPath);
            fail("Access to version history should be denied if versionable node is not accessible");
        } catch (PathNotFoundException e) {
            // success
        }

        try {
            VersionHistory history = (VersionHistory) testSession.getNodeByIdentifier(vhUUID);
            fail("Access to version history should be denied if versionable node is not accessible");
        } catch (ItemNotFoundException e) {
            // success
        }

        try {
            VersionHistory history = (VersionHistory) testSession.getNodeByUUID(vhUUID);
            fail("Access to version history should be denied if versionable node is not accessible");
        } catch (ItemNotFoundException e) {
            // success
        }
    }

    /**
     * @since oak
     */
    @Test
    public void testAccessVersionHistoryVersionableNodeRemoved() throws Exception {
        Node n = createVersionableNode(superuser.getNode(path));
        allow(n.getPath(), versionPrivileges);

        n.checkin();
        n.checkout();

        String versionablePath = n.getPath();
        VersionHistory vh = n.getVersionHistory();
        String vhPath = vh.getPath();
        String vhUUID = vh.getIdentifier();

        // remove the versionable node
        n.remove();
        superuser.save();

        testSession.refresh(false);
        assertTrue(testSession.nodeExists(path));
        assertFalse(testSession.nodeExists(versionablePath));

        // accessing the version history directly should still succeed as
        // read permission is still granted on the tree defined by the parent.
        VersionHistory history = (VersionHistory) testSession.getNode(vhPath);
        history = (VersionHistory) testSession.getNodeByIdentifier(vhUUID);
        history = (VersionHistory) testSession.getNodeByUUID(vhUUID);

        // revoking read permission on the parent node -> version history
        // must no longer be accessible
        modify(path, Privilege.JCR_READ, false);
        assertFalse(testSession.nodeExists(vhPath));
    }

    /**
     * @since oak
     */
    @Test
    public void testAddVersionLabel() throws Exception {
        Node n = createVersionableNode(superuser.getNode(path));
        allow(n.getPath(), versionPrivileges);

        Node testNode = testSession.getNode(n.getPath());
        Version v = testNode.checkin();
        testNode.checkout();
        Version v2 = testNode.checkin();
        testNode.checkout();

        // -> VersionHistory.addVersionLabel must be allowed
        VersionHistory history = testNode.getVersionHistory();
        history.addVersionLabel(v.getName(), "testLabel", false);
        history.addVersionLabel(v2.getName(), "testLabel", true);

        VersionManager vMgr = testSession.getWorkspace().getVersionManager();
        history = vMgr.getVersionHistory(testNode.getPath());
        history.addVersionLabel(v.getName(), "testLabel", true);
    }

    /**
     * @since oak
     */
    @Test
    public void testRemoveVersionLabel() throws Exception {
        Node n = createVersionableNode(superuser.getNode(path));
        allow(n.getPath(), versionPrivileges);

        Node testNode = testSession.getNode(n.getPath());
        Version v = testNode.checkin();
        testNode.checkout();
        Version v2 = testNode.checkin();
        testNode.checkout();

        // -> VersionHistory.addVersionLabel must be allowed
        VersionHistory history = testNode.getVersionHistory();
        history.addVersionLabel(v.getName(), "testLabel", false);
        history.addVersionLabel(v2.getName(), "testLabel", true);

        VersionManager vMgr = testSession.getWorkspace().getVersionManager();
        history = vMgr.getVersionHistory(testNode.getPath());
        history.removeVersionLabel("testLabel");
    }

    /**
     * @since oak
     */
    @Test
    public void testVersionablePath() throws Exception {
        Node n = createVersionableNode(superuser.getNode(path));

        VersionHistory vh = n.getVersionHistory();
        Property versionablePath = vh.getProperty(superuser.getWorkspace().getName());
        assertEquals(n.getPath(), versionablePath.getString());
    }

    @Test
    public void testAddNewVersionableNode() throws Exception {
        modify(path, REP_WRITE, true);
        modify(path, Privilege.JCR_VERSION_MANAGEMENT, true);

        Node testNode = testSession.getNode(path);
        Node newNode = testNode.addNode("versionable");
        newNode.addMixin("mix:versionable");
        testSession.save();
    }

    /**
     * @since oak
     */
    @Test
    public void testVersionableChildNode() throws Exception {
        Node testNode = superuser.getNode(path).addNode("n1").addNode("n2").addNode("n3").addNode("jcr:content");
        superuser.save();

        testNode.addMixin("mix:versionable");
        superuser.save();

        assertTrue(testNode.isNodeType("mix:versionable"));
        VersionHistory vh = testNode.getVersionHistory();
        Property versionablePath = vh.getProperty(superuser.getWorkspace().getName());
        assertEquals(testNode.getPath(), versionablePath.getString());
    }

    /**
     * @since oak
     */
    @Test
    public void testVersionableChildNode2() throws Exception {
        Node testNode = superuser.getNode(path).addNode("n1").addNode("n2").addNode("n3").addNode("jcr:content");
        testNode.addMixin("mix:versionable");
        superuser.save();


        testNode.remove();
        testNode = superuser.getNode(path).getNode("n1").getNode("n2").getNode("n3").addNode("jcr:content");
        testNode.addMixin("mix:versionable");
        superuser.save();

        assertTrue(testNode.isNodeType("mix:versionable"));
        VersionHistory vh = testNode.getVersionHistory();
        Property versionablePath = vh.getProperty(superuser.getWorkspace().getName());
        assertEquals(testNode.getPath(), versionablePath.getString());
    }
    
    @Test
    public void testCheckInCheckoutLocked() throws Exception {
        
        LockManager lockManager = superuser.getWorkspace().getLockManager();
        VersionManager versionManager = superuser.getWorkspace().getVersionManager();

        // create a versionable and lockable node
        Node n = createVersionableNode(superuser.getNode(path));
        n.addMixin(mixLockable);
        superuser.save();
        
        String nodePath = n.getPath();
        
        // lock
        lockManager.lock(nodePath, true, false, 0, superuser.getUserID());

        // create version
        versionManager.checkin(nodePath);
        versionManager.checkout(nodePath);
    }    
}

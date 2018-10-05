/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.jcr.security.authorization;

import javax.jcr.AccessDeniedException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.Privilege;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionIterator;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.util.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReadVersionContentTest extends AbstractEvaluationTest {

    private Version v;
    private Version v2;
    private VersionHistory vh;

    private String versionablePath;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        deny(VersionConstants.VERSION_STORE_PATH, privilegesFromName(Privilege.JCR_READ));

        Node n = createVersionableNode(superuser.getNode(path));
        versionablePath = n.getPath();
        v = n.checkin();
        vh = n.getVersionHistory();
        n.checkout();
        v2 = n.checkin();
        n.checkout();

        testSession.refresh(false);
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(superuser, VersionConstants.VERSION_STORE_PATH);
        if (acl != null) {
            for (AccessControlEntry entry : acl.getAccessControlEntries()) {
                if (entry.getPrincipal().equals(testUser.getPrincipal())) {
                    acl.removeAccessControlEntry(entry);
                }
            }
            acMgr.setPolicy(VersionConstants.VERSION_STORE_PATH, acl);
            superuser.save();
        }
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


    /**
     * @since oak
     */
    @Test
    public void testHasVersionContentNodes() throws Exception {
        // version information must still be accessible
        assertTrue(testSession.nodeExists(v.getPath()));
        assertTrue(testSession.nodeExists(v2.getPath()));
        assertTrue(testSession.nodeExists(vh.getPath()));
    }

    /**
     * @since oak
     */
    @Test
    public void testGetBaseVersion() throws Exception {
        // version information must still be accessible
        Version base = testSession.getNode(versionablePath).getBaseVersion();
        Version base2 = testSession.getWorkspace().getVersionManager().getBaseVersion(versionablePath);
    }

    /**
     * @since oak
     */
    @Test
    public void testGetVersionHistory() throws Exception {
        // accessing the version history must be allowed if the versionable node
        // is readable to the editing test session.
        Node testNode = testSession.getNode(versionablePath);

        VersionHistory vh = testNode.getVersionHistory();
        VersionHistory vh2 = testSession.getWorkspace().getVersionManager().getVersionHistory(versionablePath);
    }

    /**
     * @since oak
     */
    @Test
    public void testGetVersionHistoryNode() throws Exception {
        // accessing the version history must be allowed if the versionable node
        // is readable to the editing test session.
        Node testNode = testSession.getNode(versionablePath);

        String vhPath = vh.getPath();
        String vhUUID = vh.getIdentifier();

        assertTrue(vh.isSame(testNode.getSession().getNode(vhPath)));
        assertTrue(vh.isSame(testNode.getSession().getNodeByIdentifier(vhUUID)));
        assertTrue(vh.isSame(testNode.getSession().getNodeByUUID(vhUUID)));
    }

    /**
     * @since oak
     */
    @Test
    public void testVersionHistoryGetUUID() throws Exception {
        VersionHistory testVh = testSession.getNode(versionablePath).getVersionHistory();
        testVh.getUUID();
    }

    /**
     * @since oak
     */
    @Test
    public void testVersionHistoryGetIdentifier() throws Exception {
        VersionHistory testVh = testSession.getNode(versionablePath).getVersionHistory();
        testVh.getIdentifier();
    }

    /**
     * @since oak
     */
    @Test
    public void testVersionHistoryGetVersionableIdentifier() throws Exception {
        VersionHistory testVh = testSession.getNode(versionablePath).getVersionHistory();
        testVh.getVersionableIdentifier();
    }

    /**
     * @since oak
     */
    @Test
    public void testVersionHistoryGetVersionableUUID() throws Exception {
        VersionHistory testVh = testSession.getNode(versionablePath).getVersionHistory();
        testVh.getVersionableUUID();
    }

    /**
     * @since oak
     */
    @Test
    public void testVersionHistoryParent() throws Exception {
        // accessing the version history must be allowed if the versionable node
        // is readable to the editing test session.
        Node testNode = testSession.getNode(versionablePath);

        VersionHistory testVh = testNode.getVersionHistory();
        try {
            testVh.getParent();
            fail("version storage intermediate node must not be accessible");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    /**
     * @since oak
     */
    @Test
    public void testGetVersionHistoryParentNode() throws Exception {
        String vhParentPath = Text.getRelativeParent(vh.getPath(), 1);

        assertFalse(testSession.nodeExists(vhParentPath));
        try {
            testSession.getNode(vhParentPath);
            fail("version storage intermediate node must not be accessible");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    /**
     * @since oak
     */
    @Test
    public void testGetVersion() throws Exception {
        // accessing the version history must be allowed if the versionable node
        // is readable to the editing test session.
        Node testNode = testSession.getNode(versionablePath);

        VersionHistory vh = testNode.getVersionHistory();

        Version version = vh.getVersion(v.getName());
        assertTrue(v.isSame(version));
        assertTrue(vh.isSame(version.getContainingHistory()));

        version = vh.getVersion(v2.getName());
        assertTrue(v2.isSame(version));
        assertTrue(vh.isSame(version.getContainingHistory()));
    }

    /**
     * @since oak
     */
    @Test
    public void testGetAllVersions() throws Exception {
        // accessing the version history must be allowed if the versionable node
        // is readable to the editing test session.
        Node testNode = testSession.getNode(versionablePath);

        VersionHistory vh = testNode.getVersionHistory();
        VersionIterator versionIterator = vh.getAllVersions();
    }

    /**
     * @since oak
     */
    @Test
    public void testGetAllLinearVersions() throws Exception {
        // accessing the version history must be allowed if the versionable node
        // is readable to the editing test session.
        Node testNode = testSession.getNode(versionablePath);

        VersionHistory vh = testNode.getVersionHistory();
        VersionIterator versionIterator = vh.getAllLinearVersions();
    }



    /**
     * @since oak
     */
    @Test
    public void testAccessVersionHistoryVersionableNodeNotAccessible() throws Exception {
        // revert read permission on the versionable node
        modify(versionablePath, Privilege.JCR_READ, false);

        // versionable node is not readable any more for test session.
        assertFalse(testSession.nodeExists(versionablePath));

        // access version history directly => should fail
        try {
            VersionHistory history = (VersionHistory) testSession.getNode(vh.getPath());
            fail("Access to version history should be denied if versionable node is not accessible");
        } catch (PathNotFoundException e) {
            // success
        }

        try {
            VersionHistory history = (VersionHistory) testSession.getNodeByIdentifier(vh.getIdentifier());
            fail("Access to version history should be denied if versionable node is not accessible");
        } catch (ItemNotFoundException e) {
            // success
        }

        try {
            VersionHistory history = (VersionHistory) testSession.getNodeByUUID(vh.getUUID());
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
        superuser.getNode(versionablePath).remove();
        superuser.save();

        testSession.refresh(false);
        assertTrue(testSession.nodeExists(path));
        assertFalse(testSession.nodeExists(versionablePath));

        // accessing the version history directly should still succeed as
        // read permission is still granted on the tree defined by the parent.
        VersionHistory history = (VersionHistory) testSession.getNode(vh.getPath());
        history = (VersionHistory) testSession.getNodeByIdentifier(vh.getIdentifier());
        history = (VersionHistory) testSession.getNodeByUUID(vh.getUUID());

        // revoking read permission on the parent node -> version history
        // must no longer be accessible
        modify(path, Privilege.JCR_READ, false);
        assertFalse(testSession.nodeExists(vh.getPath()));
    }
}

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
package org.apache.jackrabbit.oak.jcr.version;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * Contains {@link VersionHistory} related tests.
 */
public class VersionHistoryTest extends AbstractJCRTest {

    private VersionManager versionManager;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        versionManager = superuser.getWorkspace().getVersionManager();
    }

    public void testJcrVersionHistoryProperty() throws Exception {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        assertTrue(n.hasProperty(JcrConstants.JCR_VERSIONHISTORY));
    }

    public void testGetVersionHistoryFromNode() throws Exception {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        VersionHistory vh = n.getVersionHistory();
        assertNotNull(vh);
    }

    public void testGetVersionHistory() throws Exception {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        VersionHistory vh = versionManager.getVersionHistory(n.getPath());
        assertNotNull(vh);
    }

    public void testGetVersionHistory2() throws Exception {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        Session s = getHelper().getSuperuserSession();
        try {
            VersionHistory vh = s.getWorkspace().getVersionManager().getVersionHistory(n.getPath());
            assertNotNull(vh);
        } finally {
            s.logout();
        }
    }

    public void testGetVersionHistoryNodeByUUID() throws Exception {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        Node vh = superuser.getNodeByUUID(n.getProperty(
                JcrConstants.JCR_VERSIONHISTORY).getString());
        assertNotNull(vh);
    }

    public void testGetVersionHistoryAfterMove() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        node1.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        Node node2 = testRootNode.addNode(nodeName2);
        String destPath = node2.getPath() + "/" + nodeName3;
        superuser.move(node1.getPath(), destPath);
        superuser.save();

        assertTrue(superuser.nodeExists(destPath));
        VersionHistory vh = versionManager.getVersionHistory(destPath);
    }

    public void testGetNodeByIdentifier() throws RepositoryException {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        String id = vMgr.getVersionHistory(n.getPath()).getIdentifier();
        assertTrue("Session.getNodeByIdentifier() did not return VersionHistory object for a nt:versionHistory node.",
                superuser.getNodeByIdentifier(id) instanceof VersionHistory);
    }

    @SuppressWarnings("deprecation")
    public void testGetNodeByUUID() throws RepositoryException {
        Node n = testRootNode.addNode(nodeName1, testNodeType);
        n.addMixin(mixVersionable);
        superuser.save();
        VersionManager vMgr = superuser.getWorkspace().getVersionManager();
        String uuid = vMgr.getVersionHistory(n.getPath()).getUUID();
        assertTrue("Session.getNodeByUUID() did not return VersionHistory object for a nt:versionHistory node.",
                superuser.getNodeByUUID(uuid) instanceof VersionHistory);
    }
}
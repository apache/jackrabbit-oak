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
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.junit.Test;

/**
 * VersionablePathsTest... TODO
 * <p/>
 * FIXME: versionablePath only adjusted AFTER move/rename has been persisted.
 */
public class VersionablePathsTest extends AbstractJCRTest {

    // FIXME: shouldn't be needed (OAK-602)
    private Session tmpSession;

    @Override
    protected void tearDown() throws Exception {
        if (tmpSession != null) {
            tmpSession.logout();
        }
        super.tearDown();
    }

    private VersionManager getVersionManager() throws RepositoryException {
        tmpSession = getHelper().getSuperuserSession();
        return tmpSession.getWorkspace().getVersionManager();
    }

    @Test
    public void testVersionablePaths() throws Exception {
        testRootNode.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        Node vh = getVersionManager().getVersionHistory(testRootNode.getPath());

        assertTrue(vh.isNodeType("rep:VersionablePaths"));
        String workspaceName = superuser.getWorkspace().getName();
        assertTrue(vh.hasProperty(workspaceName));
        assertEquals(testRootNode.getPath(), vh.getProperty(workspaceName).getString());
    }

    @Test
    public void testVersionablePathsAfterRename() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        node1.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        String destPath = testRoot + "/" + nodeName2;
        superuser.move(node1.getPath(), destPath);
        superuser.save();

        // FIXME: test node is stale after move
        node1 = testRootNode.getNode(nodeName2);
        Node vh = getVersionManager().getVersionHistory(node1.getPath());
        assertTrue(vh.isNodeType("rep:VersionablePaths"));
        String workspaceName = superuser.getWorkspace().getName();
        assertTrue(vh.hasProperty(workspaceName));
        assertEquals(node1.getPath(), vh.getProperty(workspaceName).getString());
    }

    @Test
    public void testVersionablePathsAfterMove() throws Exception {
        Node node1 = testRootNode.addNode(nodeName1);
        Node node2 = testRootNode.addNode(nodeName2);
        node1.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        String destPath = node2.getPath() + "/" + nodeName1;
        superuser.move(node1.getPath(), destPath);
        superuser.save();

        // FIXME: node1 is stale after move
        node1 = node2.getNode(nodeName1);
        assertEquals(destPath, node1.getPath());

        Node vh = getVersionManager().getVersionHistory(node1.getPath());
        assertTrue(vh.isNodeType("rep:VersionablePaths"));
        String workspaceName = superuser.getWorkspace().getName();
        assertTrue(vh.hasProperty(workspaceName));
        assertEquals(node1.getPath(), vh.getProperty(workspaceName).getString());
    }
}
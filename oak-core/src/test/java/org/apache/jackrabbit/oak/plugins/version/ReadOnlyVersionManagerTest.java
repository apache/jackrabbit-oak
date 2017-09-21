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
package org.apache.jackrabbit.oak.plugins.version;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReadOnlyVersionManagerTest extends AbstractSecurityTest {

    private Tree versionable;
    private String workspaceName;

    private ReadOnlyVersionManager versionManager;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        NodeUtil node = new NodeUtil(root.getTree("/"));
        NodeUtil a = node.addChild("a", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        a.addChild("b", NodeTypeConstants.NT_OAK_UNSTRUCTURED).addChild("c", NodeTypeConstants.NT_OAK_UNSTRUCTURED);


        TreeUtil.addMixin(a.getTree(), JcrConstants.MIX_VERSIONABLE, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
        root.commit();

        versionable = root.getTree("/a");

        // force the creation of a version that has a frozen node
        versionable.setProperty(JCR_ISCHECKEDOUT, Boolean.FALSE, Type.BOOLEAN);
        root.commit();
        versionable.setProperty(JCR_ISCHECKEDOUT, Boolean.TRUE, Type.BOOLEAN);
        root.commit();

        versionManager = ReadOnlyVersionManager.getInstance(root, NamePathMapper.DEFAULT);
        workspaceName = root.getContentSession().getWorkspaceName();
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            root.refresh();

            Tree a = root.getTree("/a");
            if (a.exists()) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testIsVersionStoreTree() throws Exception {
        assertFalse(ReadOnlyVersionManager.isVersionStoreTree(root.getTree("/")));
        assertFalse(ReadOnlyVersionManager.isVersionStoreTree(root.getTree("/a")));
        assertFalse(ReadOnlyVersionManager.isVersionStoreTree(root.getTree("/a/b/c")));

        assertTrue(ReadOnlyVersionManager.isVersionStoreTree(root.getTree(VersionConstants.VERSION_STORE_PATH)));

        Tree versionHistory = versionManager.getVersionHistory(root.getTree("/a"));
        assertNotNull(versionHistory);
        assertFalse(ReadOnlyVersionManager.isVersionStoreTree(versionHistory));
        assertTrue(ReadOnlyVersionManager.isVersionStoreTree(versionHistory.getParent()));
    }

    @Test
    public void testGetVersionable() throws Exception {
        Tree versionHistory = checkNotNull(versionManager.getVersionHistory(root.getTree("/a")));
        assertVersionable("/a", versionHistory);

        Tree rootVersion = versionHistory.getChild(JcrConstants.JCR_ROOTVERSION);
        assertVersionable("/a", rootVersion);

        Tree baseVersion = checkNotNull(versionManager.getBaseVersion(versionable));
        assertVersionable("/a", baseVersion);

        Tree frozen = baseVersion.getChild(VersionConstants.JCR_FROZENNODE);
        assertVersionable("/a", frozen);

        Tree frozenB = frozen.getChild("b");
        assertVersionable("/a/b", frozenB);

        Tree frozenC = frozenB.getChild("c");
        assertVersionable("/a/b/c", frozenC);
    }


    private void assertVersionable(@Nonnull String expectedPath, @Nonnull Tree versionTree) {
        String p = versionTree.getPath();
        assertTrue(p, versionTree.exists());

        Tree versionable = versionManager.getVersionable(versionTree, workspaceName);
        assertNotNull(p, versionable);
        assertEquals(p, expectedPath, versionable.getPath());
    }

    @Test
    public void testGetVersionableForNonVersionTree() throws Exception {
        assertNull(versionManager.getVersionable(versionManager.getVersionStorage(), workspaceName));
        assertNull(versionManager.getVersionable(versionable, workspaceName));
        assertNull(versionManager.getVersionable(root.getTree("/"), workspaceName));
    }

    @Test
    public void testGetVersionableMissingPathProperty() throws Exception {
        Tree versionHistory = checkNotNull(versionManager.getVersionHistory(root.getTree("/a")));
        versionHistory.removeProperty(workspaceName);

        assertNull(versionManager.getVersionable(versionHistory, workspaceName));
        assertNull(versionManager.getVersionable(versionHistory.getChild(JcrConstants.JCR_ROOTVERSION), workspaceName));
    }

    @Test
    public void testGetVersionableNonExistingWorkspace() throws Exception {
        Tree versionHistory = checkNotNull(versionManager.getVersionHistory(root.getTree("/a")));

        assertNull(versionManager.getVersionable(versionHistory, "nonExistingWorkspaceName"));
        assertNull(versionManager.getVersionable(versionHistory.getChild(JcrConstants.JCR_ROOTVERSION), "nonExistingWorkspaceName"));
    }

    @Test
    public void testGetVersionableTargetRemoved() throws Exception {
        Tree baseVersion = checkNotNull(versionManager.getBaseVersion(versionable));

        versionable.remove();
        root.commit();

        Tree t = versionManager.getVersionable(baseVersion, workspaceName);
        assertNotNull(t);
        assertFalse(t.exists());
    }

    @Test
    public void testRemoveEmptyHistoryAfterRemovingVersionable() throws RepositoryException, CommitFailedException {
        NodeUtil node = new NodeUtil(root.getTree("/"));
        NodeUtil testVersionable = node.addChild("testVersionable", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        TreeUtil.addMixin(testVersionable.getTree(), JcrConstants.MIX_VERSIONABLE, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
        root.commit();

        String historyPath = versionManager.getVersionHistory(testVersionable.getTree()).getPath();
        assertTrue(root.getTree(historyPath).exists());

        testVersionable.getTree().remove();
        root.commit();

        assertFalse(root.getTree(historyPath).exists());
    }

    @Test
    public void testPreserveNonEmptyHistoryAfterRemovingVersionable() throws RepositoryException, CommitFailedException {
        String historyPath = versionManager.getVersionHistory(versionable).getPath();
        assertTrue(root.getTree(historyPath).exists());

        versionable.remove();
        root.commit();

        assertTrue(root.getTree(historyPath).exists());
    }

    @Test
    public void testPreserveHistoryAfterMovingVersionable() throws RepositoryException, CommitFailedException {
        NodeUtil node = new NodeUtil(root.getTree("/"));
        NodeUtil testVersionable = node.addChild("testVersionable", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        TreeUtil.addMixin(testVersionable.getTree(), JcrConstants.MIX_VERSIONABLE, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
        root.commit();

        Tree history = versionManager.getVersionHistory(testVersionable.getTree());
        assertTrue(history.exists());
        String historyUuid = history.getProperty(JCR_UUID).getValue(Type.STRING);

        assertTrue(root.move("/testVersionable", "/testVersionable2"));
        root.commit();

        history = versionManager.getVersionHistory(root.getTree("/testVersionable2"));
        assertTrue(history.exists());
        assertEquals(historyUuid, history.getProperty(JCR_UUID).getValue(Type.STRING));
    }
}
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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests related to permission evaluation within the version storage.
 */
public class VersionStorageTest extends AbstractOakCoreTest {

    private String vhPath;

    @Override
    public void before() throws Exception {
        super.before();

        // make sure the version storage has been setup
        Tree a = root.getTree("/a");
        TreeUtil.addMixin(a, JcrConstants.MIX_VERSIONABLE, root.getTree(NODE_TYPES_PATH), adminSession.getAuthInfo().getUserID());
        root.commit();

        Tree vs = root.getTree(VersionConstants.VERSION_STORE_PATH);
        assertTrue(vs.exists());
        String vhUUID = TreeUtil.getString(a, VersionConstants.JCR_VERSIONHISTORY);
        assertNotNull(vhUUID);

        String versionableUuid = TreeUtil.getString(a, JcrConstants.JCR_UUID);
        vhPath = getVersionHistoryPath(versionableUuid, vs);
    }

    private String getVersionHistoryPath(String vUUID, final Tree vs) {
        ReadOnlyVersionManager vMgr = new ReadOnlyVersionManager() {
            @Nonnull
            @Override
            protected Tree getVersionStorage() {
                return vs;
            }

            @Nonnull
            @Override
            protected Root getWorkspaceRoot() {
                return root;
            }

            @Nonnull
            @Override
            protected ReadOnlyNodeTypeManager getNodeTypeManager() {
                throw new UnsupportedOperationException();
            }
        };
        return VersionConstants.VERSION_STORE_PATH + '/' + vMgr.getVersionHistoryPath(vUUID);
    }

    @Override
    public void after() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            if (testPrincipal.equals(ace.getPrincipal())) {
                acl.removeAccessControlEntry(ace);
            }
        }
        acMgr.setPolicy("/", acl);
        root.commit();
    }

    @Test
    public void testGetVersionStorage() throws Exception {
        Tree vs = getTestRoot().getTree(VersionConstants.VERSION_STORE_PATH);
        assertFalse(vs.exists());
    }

    @Test
    public void testGetVersionStorage2() throws Exception {
        setupPermission("/", testPrincipal, true, PrivilegeConstants.JCR_READ);

        Tree vs = getTestRoot().getTree(VersionConstants.VERSION_STORE_PATH);
        assertTrue(vs.exists());
    }

    @Test
    public void testGetVersionHistory() throws Exception {
        Tree vs = getTestRoot().getTree(vhPath);
        assertFalse(vs.exists());
    }

    @Test
    public void testGetVersionHistory2() throws Exception {
        setupPermission("/", testPrincipal, true, PrivilegeConstants.JCR_READ);

        Tree vs = getTestRoot().getTree(vhPath);
        assertTrue(vs.exists());
    }

    @Test
    public void testGetChildrenCountOnVersionStorage() throws Exception {
        Tree vs = getTestRoot().getTree(VersionConstants.VERSION_STORE_PATH);
        vs.getChildrenCount(Long.MAX_VALUE);
    }

    @Test
    public void testGetChildrenCountOnVersionStorage2() throws Exception {
        setupPermission("/", testPrincipal, true, PrivilegeConstants.JCR_READ);
        Tree vs = getTestRoot().getTree(VersionConstants.VERSION_STORE_PATH);
        vs.getChildrenCount(Long.MAX_VALUE);
    }
}
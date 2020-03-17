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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;

import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HiddenTest extends AbstractCugTest {

    private Root readOnlyRoot;
    private Tree hiddenTree;

    private CugPermissionProvider pp;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        createCug(SUPPORTED_PATH, EveryonePrincipal.getInstance());
        root.commit();

        readOnlyRoot = getRootProvider().createReadOnlyRoot(root);
        hiddenTree = readOnlyRoot.getTree("/oak:index/acPrincipalName/:index");
        assertTrue(hiddenTree.exists());

        pp = createCugPermissionProvider(ImmutableSet.of("/"), EveryonePrincipal.getInstance());
    }

    @Test
    public void testSupportedPermissions() {
        assertEquals(Permissions.NO_PERMISSION, pp.supportedPermissions(hiddenTree, null, Permissions.READ));
    }

    @Test
    public void testSupportedPermissionsLocation() {
        assertEquals(Permissions.NO_PERMISSION, pp.supportedPermissions(TreeLocation.create(hiddenTree), Permissions.READ));
    }

    @Test
    public void testSupportedPrivileges() {
        assertSame(PrivilegeBits.EMPTY, pp.supportedPrivileges(hiddenTree, new PrivilegeBitsProvider(readOnlyRoot).getBits(PrivilegeConstants.JCR_READ)));
    }

    @Test
    public void testTreePermission() {
        Tree t = readOnlyRoot.getTree("/");
        TreePermission tp = pp.getTreePermission(t, TreePermission.EMPTY);
        for (String name : PathUtils.elements(hiddenTree.getPath())) {
            assertCugPermission(tp, true);
            t = t.getChild(name);
            tp = pp.getTreePermission(t, tp);
        }
        assertSame(TreePermission.NO_RECOURSE, tp);
        assertEquals(Permissions.NO_PERMISSION, pp.supportedPermissions(tp, null, Permissions.READ));
    }

    @Test
    public void testIsGranted() {
        assertFalse(pp.isGranted(hiddenTree, null, Permissions.READ));
    }

    @Test
    public void testIsGrantedHiddenBelowCug() {
        assertTrue(pp.isGranted(readOnlyRoot.getTree(SUPPORTED_PATH), null, Permissions.READ));
        assertFalse(pp.isGranted(readOnlyRoot.getTree(SUPPORTED_PATH + "/:hidden"), null, Permissions.READ));
    }

    @Test
    public void testIsGrantedPath() {
        assertTrue(pp.isGranted(SUPPORTED_PATH, Permissions.getString(Permissions.READ)));

        assertFalse(pp.isGranted(SUPPORTED_PATH + "/:hidden", Permissions.getString(Permissions.READ)));
        assertFalse(pp.isGranted(SUPPORTED_PATH + "/:hidden/child", Permissions.getString(Permissions.READ)));
        assertFalse(pp.isGranted(hiddenTree.getPath(), Permissions.getString(Permissions.READ)));
    }

    @Test
    public void testHasPrivileges() {
        assertTrue(pp.hasPrivileges(readOnlyRoot.getTree(SUPPORTED_PATH), PrivilegeConstants.JCR_READ));
        assertFalse(pp.hasPrivileges(readOnlyRoot.getTree(SUPPORTED_PATH + "/:hidden"), PrivilegeConstants.JCR_READ));
    }

    @Test
    public void testGetPrivileges() {
        assertFalse(pp.getPrivileges(readOnlyRoot.getTree(SUPPORTED_PATH)).isEmpty());
        assertTrue(pp.getPrivileges(readOnlyRoot.getTree(SUPPORTED_PATH + "/:hidden")).isEmpty());
    }

    @Test
    public void testCombinedSetup() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        try {
            AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
            acl.addAccessControlEntry(EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.JCR_READ));
            acMgr.setPolicy("/", acl);
            root.commit();

            PermissionProvider combined = getConfig(AuthorizationConfiguration.class).getPermissionProvider(readOnlyRoot, root.getContentSession().getWorkspaceName(), ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()));

            assertFalse(combined.hasPrivileges(hiddenTree, PrivilegeConstants.JCR_READ));
            assertTrue(combined.getPrivileges(hiddenTree).isEmpty());

            assertTrue(combined.isGranted(hiddenTree, null, Permissions.ALL));
            assertTrue(combined.isGranted(hiddenTree.getPath(), Permissions.getString(Permissions.ALL)));

            Tree t = readOnlyRoot.getTree("/");
            TreePermission tp = combined.getTreePermission(t, TreePermission.EMPTY);
            for (String name : PathUtils.elements(hiddenTree.getPath())) {
                t = t.getChild(name);
                tp = combined.getTreePermission(t, tp);
            }
            assertTrue(tp.isGranted(Permissions.ALL));

        } finally {
            AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
            acl.addAccessControlEntry(EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.JCR_READ));
            acMgr.removePolicy("/", acl);
            root.commit();
        }
    }
}

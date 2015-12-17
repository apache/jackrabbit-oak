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

import java.util.List;

import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.util.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AccessControlTest extends AbstractCugTest {

    private List<String> acPaths;

    private CugPermissionProvider pp;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        setupCugsAndAcls();

        /**
         * regular acl at
         *   - /content
         *
         * permission store (internal content)
         *   - /jcr:system/rep:permissionStore
         *
         * cugs at
         *   - /content/a     : allow testGroup, deny everyone
         *   - /content/aa/bb : allow testGroup, deny everyone
         *   - /content/a/b/c : allow everyone,  deny testGroup (isolated)
         *   - /content2      : allow everyone,  deny testGroup (isolated)
         *
         */
        acPaths = ImmutableList.of(
                "/content/rep:policy",
                PermissionConstants.PERMISSIONS_STORE_PATH,
                "/content/a/rep:cugPolicy",
                "/content/aa/bb/rep:cugPolicy",
                "/content/a/b/c/rep:cugPolicy",
                "/content2/rep:cugPolicy"
        );

        pp = createCugPermissionProvider(ImmutableSet.of(PathUtils.ROOT_PATH), EveryonePrincipal.getInstance(), getTestGroupPrincipal(), getTestUser().getPrincipal());
    }

    @Test
    public void testSupportedPermissions() {
        for (String acPath : acPaths) {
            assertEquals(Permissions.NO_PERMISSION, pp.supportedPermissions(root.getTree(acPath), null, Permissions.READ));
        }
    }

    @Test
    public void testSupportedPermissionsLocation() {
        for (String acPath : acPaths) {
            assertEquals(Permissions.NO_PERMISSION, pp.supportedPermissions(TreeLocation.create(root, acPath), Permissions.READ));
        }
    }

    @Test
    public void testSupportedPrivileges() {
        PrivilegeBits bts = new PrivilegeBitsProvider(root).getBits(PrivilegeConstants.JCR_READ);
        for (String acPath : acPaths) {
            assertSame(PrivilegeBits.EMPTY, pp.supportedPrivileges(root.getTree(acPath), bts));
        }
    }

    @Test
    public void testTreePermission() {
        for (String acPath : acPaths) {
            Tree t = root.getTree("/");
            TreePermission tp = pp.getTreePermission(t, TreePermission.EMPTY);
            for (String name : PathUtils.elements(acPath)) {
                t = t.getChild(name);
                tp = pp.getTreePermission(t, tp);
            }
            assertSame(TreePermission.NO_RECOURSE, tp);
            assertEquals(Permissions.NO_PERMISSION, pp.supportedPermissions(tp, null, Permissions.READ));
        }
    }

    @Test
    public void testIsGranted() {
        for (String acPath : acPaths) {
            assertFalse(pp.isGranted(root.getTree(acPath), null, Permissions.READ));
        }
    }

    @Test
    public void testIsGrantedPath() {
        for (String acPath : acPaths) {
            assertFalse(pp.isGranted(acPath, Permissions.getString(Permissions.READ)));
        }
    }

    @Test
    public void testHasPrivileges() {
        for (String acPath : acPaths) {
            assertFalse(pp.hasPrivileges(root.getTree(acPath), PrivilegeConstants.JCR_READ));
        }
    }

    @Test
    public void testGetPrivileges() {
        for (String acPath : acPaths) {
            assertTrue(pp.getPrivileges(root.getTree(acPath)).isEmpty());
        }
    }

    @Test
    public void testCombinedSetup() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/content");
        acl.addAccessControlEntry(getTestGroupPrincipal(), AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        PermissionProvider combined = getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, root.getContentSession().getWorkspaceName(), ImmutableSet.of(getTestGroupPrincipal()));
        for (String acPath : acPaths) {
            boolean canReadAc = Text.isDescendantOrEqual("/content", acPath);
            Tree acTree = root.getTree(acPath);

            assertEquals(canReadAc, combined.hasPrivileges(acTree, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
            assertEquals(canReadAc, combined.getPrivileges(acTree).contains(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));

            assertEquals(canReadAc, combined.isGranted(acPath, JackrabbitSession.ACTION_READ_ACCESS_CONTROL));
            assertEquals(canReadAc, combined.isGranted(acTree, null, Permissions.READ_ACCESS_CONTROL));

            Tree t = root.getTree("/");
            TreePermission tp = combined.getTreePermission(t, TreePermission.EMPTY);
            for (String name : PathUtils.elements(acPath)) {
                t = t.getChild(name);
                tp = combined.getTreePermission(t, tp);
            }
            assertEquals(canReadAc, tp.canRead());
            assertEquals(canReadAc, tp.isGranted(Permissions.READ_ACCESS_CONTROL));
        }
    }
}
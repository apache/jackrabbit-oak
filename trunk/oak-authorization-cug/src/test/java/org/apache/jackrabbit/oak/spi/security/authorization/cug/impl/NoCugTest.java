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

import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class NoCugTest extends AbstractCugTest {

    private static final List<String> PATHS = ImmutableList.of(PathUtils.ROOT_PATH, SUPPORTED_PATH, SUPPORTED_PATH + "/subtree", SUPPORTED_PATH3, UNSUPPORTED_PATH, INVALID_PATH);

    private CugPermissionProvider cugPermProvider;


    @Override
    public void before() throws Exception {
        super.before();

        cugPermProvider = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH), getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
    }

    @Test
    public void getPrivileges() {
        for (String p : PATHS) {
            assertTrue(p, cugPermProvider.getPrivileges(root.getTree(p)).isEmpty());
        }
    }

    @Test
    public void hasPrivileges() {
        for (String p : PATHS) {
            assertFalse(p, cugPermProvider.hasPrivileges(root.getTree(p), PrivilegeConstants.JCR_READ));
        }
    }

    @Test
    public void testGetTreePermission() {
        assertSame(TreePermission.NO_RECOURSE, cugPermProvider.getTreePermission(root.getTree(PathUtils.ROOT_PATH), TreePermission.EMPTY));
    }

    @Test
    public void testIsGranted() {
        for (String p : PATHS) {
            assertFalse(p, cugPermProvider.isGranted(root.getTree(p), null, Permissions.READ));
        }
    }

    @Test
    public void testIsGrantedActions() {
        for (String p : PATHS) {
            assertFalse(p, cugPermProvider.isGranted(p, Session.ACTION_READ));
        }
    }

    @Test
    public void testSupportedPrivileges() {
        PrivilegeBits bits = new PrivilegeBitsProvider(root).getBits(PrivilegeConstants.JCR_READ);
        for (String p : PATHS) {
            assertTrue(p, cugPermProvider.supportedPrivileges(root.getTree(p), bits).isEmpty());
        }
    }

    @Test
    public void testSupportedPrivileges2() {
        for (String p : PATHS) {
            assertTrue(p, cugPermProvider.supportedPrivileges(root.getTree(p), null).isEmpty());
        }
    }

    @Test
    public void testSupportedPermissions() {
        for (String p : PATHS) {
            assertEquals(p, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(root.getTree(p), null, Permissions.READ));
        }
    }

    @Test
    public void testSupportedPermissionsTreeLocation() {
        for (String p : PATHS) {
            assertEquals(p, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(TreeLocation.create(root.getTree(p)), Permissions.READ));
        }
    }

    @Test
    public void testIsGrantedTreeLocation() {
        for (String p : PATHS) {
            assertFalse(p, cugPermProvider.isGranted(TreeLocation.create(root.getTree(p)), Permissions.READ));
        }
    }

    @Test
    public void testHiddenProperty() {
        Root immutableRoot = getRootProvider().createReadOnlyRoot(root);
        assertFalse(immutableRoot.getTree(PathUtils.ROOT_PATH).hasProperty(HIDDEN_NESTED_CUGS));
    }
}
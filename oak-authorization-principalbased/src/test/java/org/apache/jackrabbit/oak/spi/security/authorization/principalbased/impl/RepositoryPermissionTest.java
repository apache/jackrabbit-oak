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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.NAMESPACE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.NODE_TYPE_DEFINITION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class RepositoryPermissionTest extends AbstractPrincipalBasedTest {

    private PrincipalBasedPermissionProvider permissionProvider;

    @Before
    public void before() throws Exception {
        super.before();
        permissionProvider = createPermissionProvider(root, getTestSystemUser().getPrincipal());
    }

    @Override
    protected NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    private void setupPermissions(@Nullable String effectivePath, @NotNull String... privNames) throws Exception {
        // set principal-based policy for 'testPrincipal'
        setupPrincipalBasedAccessControl(getTestSystemUser().getPrincipal(), effectivePath, privNames);
        if (root.hasPendingChanges()) {
            root.commit();
        }
    }

    @Test
    public void testGetRepositoryPermissionsTwice() {
        assertSame(permissionProvider.getRepositoryPermission(), permissionProvider.getRepositoryPermission());
    }

    @Test
    public void testGetRepositoryPermissionsAfterRefresh() {
        RepositoryPermission rp = permissionProvider.getRepositoryPermission();
        permissionProvider.refresh();
        assertSame(rp, permissionProvider.getRepositoryPermission());
    }

    @Test
    public void testRefreshResetsRepositoryPermissions() throws Exception {
        RepositoryPermission rp = permissionProvider.getRepositoryPermission();
        Field f = rp.getClass().getDeclaredField("grantedPermissions");
        f.setAccessible(true);
        assertEquals((long) -1, f.get(rp));

        // force evaluation
        rp.isGranted(NAMESPACE_MANAGEMENT);
        assertEquals(Permissions.NO_PERMISSION, f.get(rp));

        // reset permission provider
        permissionProvider.refresh();
        assertEquals((long) -1, f.get(rp));
    }

    @Test
    public void testIsGrantedNoPermissions() {
        assertTrue(permissionProvider.getRepositoryPermission().isGranted(Permissions.NO_PERMISSION));
    }

    @Test
    public void testIsGrantedNoPermissionSetup() {
        assertFalse(permissionProvider.getRepositoryPermission().isGranted(NAMESPACE_MANAGEMENT));
    }

    @Test
    public void testIsGrantedNoRepoPermissionSetup() throws Exception {
        setupPermissions(testContentJcrPath, PrivilegeConstants.JCR_ALL);

        permissionProvider.refresh();
        assertFalse(permissionProvider.getRepositoryPermission().isGranted(NAMESPACE_MANAGEMENT));
    }

    @Test
    public void testIsGrantedRepoPermissionSetup() throws Exception {
        setupPermissions(null, PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT);

        permissionProvider.refresh();

        assertFalse(permissionProvider.getRepositoryPermission().isGranted(NAMESPACE_MANAGEMENT));
        assertFalse(permissionProvider.getRepositoryPermission().isGranted(NAMESPACE_MANAGEMENT| NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(permissionProvider.getRepositoryPermission().isGranted(Permissions.ALL));

        assertTrue(permissionProvider.getRepositoryPermission().isGranted(NODE_TYPE_DEFINITION_MANAGEMENT));
    }

    @Test
    public void getPrivileges() throws Exception {
        assertTrue(permissionProvider.getPrivileges(null).isEmpty());

        setupPermissions(null, JCR_WORKSPACE_MANAGEMENT);
        permissionProvider.refresh();

        Set<String> privNames = permissionProvider.getPrivileges(null);
        assertTrue(Iterables.elementsEqual(Set.of(JCR_WORKSPACE_MANAGEMENT), privNames));
    }

    @Test
    public void hasPrivileges() throws Exception {
        assertFalse(permissionProvider.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT));

        setupPermissions(null, JCR_NAMESPACE_MANAGEMENT);
        permissionProvider.refresh();

        assertTrue(permissionProvider.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT));

        assertFalse(permissionProvider.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT));
        assertFalse(permissionProvider.hasPrivileges(null, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
    }
}
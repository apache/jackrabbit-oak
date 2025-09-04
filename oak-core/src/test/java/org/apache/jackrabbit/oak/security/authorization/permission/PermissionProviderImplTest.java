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
package org.apache.jackrabbit.oak.security.authorization.permission;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.plugins.tree.TreeUtil.addChild;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PermissionProviderImplTest extends AbstractSecurityTest implements AccessControlConstants {

    private static final Set<String> READ_PATHS = Set.of(
            NamespaceConstants.NAMESPACES_PATH,
            NodeTypeConstants.NODE_TYPES_PATH,
            PrivilegeConstants.PRIVILEGES_PATH,
            "/test"
    );

    private ContentSession testSession;
    private PermissionProviderImpl pp;

    @Override
    public void before() throws Exception {
        super.before();

        addChild(root.getTree("/"), "test", JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        testSession = createTestSession();
        pp = createPermissionProvider(testSession);
    }

    @Override
    public void after() throws Exception {
        try {
            testSession.close();
            root.getTree("/test").remove();
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters acConfig = ConfigurationParameters.of(
                PermissionConstants.PARAM_READ_PATHS, READ_PATHS);
        return ConfigurationParameters.of(Map.of(AuthorizationConfiguration.NAME, acConfig));
    }


    private PermissionProviderImpl createPermissionProvider(ContentSession session) {
        AuthorizationConfiguration config = getSecurityProvider().getConfiguration(AuthorizationConfiguration.class);
        assertTrue(config instanceof CompositeAuthorizationConfiguration);

        AuthorizationConfiguration defConfig = ((CompositeAuthorizationConfiguration) config).getDefaultConfig();
        assertTrue(defConfig instanceof AuthorizationConfigurationImpl);

        return new PermissionProviderImpl(session.getLatestRoot(), session.getWorkspaceName(), session.getAuthInfo().getPrincipals(), config.getRestrictionProvider(), config.getParameters(), config.getContext(), (AuthorizationConfigurationImpl) defConfig);
    }

    @Test
    public void testHasPrivileges() {
        assertTrue(pp.hasPrivileges(null));
        assertTrue(pp.hasPrivileges(null, new String[0]));
        assertFalse(pp.hasPrivileges(null, PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT));
    }

    @Test
    public void testTreePermissionsForReadPaths() {
        Root r = testSession.getLatestRoot();
        Tree tree = r.getTree(PathUtils.ROOT_PATH);
        assertFalse(tree.exists());
        assertFalse(pp.getTreePermission(tree, TreePermission.EMPTY).canRead());

        for (String path : READ_PATHS) {
            tree = r.getTree(path);
            assertTrue(tree.exists());
            assertTrue(pp.getTreePermission(tree, TreePermission.EMPTY).canRead());
        }
    }

    @Test
    public void testIsGrantedPathForReadPaths() {
        for (String path : READ_PATHS) {
            assertTrue(pp.isGranted(path, Permissions.getString(Permissions.READ)));
            assertTrue(pp.isGranted(path, Permissions.getString(Permissions.READ_NODE)));
            assertTrue(pp.isGranted(path + '/' + JcrConstants.JCR_PRIMARYTYPE, Permissions.getString(Permissions.READ_PROPERTY)));
            assertFalse(pp.isGranted(path, Permissions.getString(Permissions.READ_ACCESS_CONTROL)));
        }
    }

    @Test
    public void testIsGrantedTreeForReadPaths() {
        for (String path : READ_PATHS) {
            Tree tree = root.getTree(path);
            assertTrue(pp.isGranted(tree, null, Permissions.READ));
            assertTrue(pp.isGranted(tree, null, Permissions.READ_NODE));
            assertTrue(pp.isGranted(tree, tree.getProperty(JcrConstants.JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
            assertFalse(pp.isGranted(tree, null, Permissions.READ_ACCESS_CONTROL));
        }
    }

    @Test
    public void testRepositoryPermissions() {
        RepositoryPermission rp = pp.getRepositoryPermission();
        assertFalse(rp.isGranted(Permissions.READ));
        assertFalse(rp.isGranted(Permissions.READ_NODE));
        assertFalse(rp.isGranted(Permissions.READ_PROPERTY));
        assertFalse(rp.isGranted(Permissions.READ_ACCESS_CONTROL));
    }

    @Test
    public void testGetPrivilegesForReadPaths() {
        for (String path : READ_PATHS) {
            Tree tree = root.getTree(path);
            assertEquals(Collections.singleton(PrivilegeConstants.JCR_READ), pp.getPrivileges(tree));
        }
        assertEquals(Collections.<String>emptySet(), pp.getPrivileges(null));
    }

    @Test
    public void testHasPrivilegesForReadPaths() {
        for (String path : READ_PATHS) {
            Tree tree = root.getTree(path);
            assertTrue(pp.hasPrivileges(tree, PrivilegeConstants.JCR_READ));
            assertTrue(pp.hasPrivileges(tree, PrivilegeConstants.REP_READ_NODES));
            assertTrue(pp.hasPrivileges(tree, PrivilegeConstants.REP_READ_PROPERTIES));
            assertFalse(pp.hasPrivileges(tree, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        }
        assertFalse(pp.hasPrivileges(null, PrivilegeConstants.JCR_READ));
    }

    @Test
    public void testIsGrantedNonExistingLocation() {
        // parent is readable
        TreeLocation location = TreeLocation.create(testSession.getLatestRoot(), "/test/non/existing/tree");
        assertTrue(pp.isGranted(location, Permissions.READ));

        // parent is not readable
        location = TreeLocation.create(testSession.getLatestRoot(), "/non/existing/tree");
        assertFalse(pp.isGranted(location, Permissions.READ));
    }

    @Test
    public void testIsGrantedNonExistingVersionStoreLocation() {
        TreeLocation location = TreeLocation.create(testSession.getLatestRoot(), VersionConstants.VERSION_STORE_PATH + "/non/existing/tree");

        assertFalse(pp.isGranted(location, Permissions.READ));
    }

    @Test
    public void testAdministrativePrincipalSet() {
        PermissionProviderImpl pp = createPermissionProvider(adminSession);
        assertSame(TreePermission.ALL, pp.getTreePermission(root.getTree(PathUtils.ROOT_PATH), TreePermission.EMPTY));
        assertSame(RepositoryPermission.ALL, pp.getRepositoryPermission());
    }
}
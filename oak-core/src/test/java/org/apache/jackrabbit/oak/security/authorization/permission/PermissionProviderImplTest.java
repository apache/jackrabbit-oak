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

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PermissionProviderImplTest extends AbstractSecurityTest implements AccessControlConstants {

    private static final String ADMINISTRATOR_GROUP = "administrators";
    private static final Set<String> READ_PATHS = ImmutableSet.of(
            NamespaceConstants.NAMESPACES_PATH,
            NodeTypeConstants.NODE_TYPES_PATH,
            PrivilegeConstants.PRIVILEGES_PATH,
            "/test"
    );

    private Group adminstrators;
    private AuthorizationConfiguration config;

    @Override
    public void before() throws Exception {
        super.before();

        new NodeUtil(root.getTree("/")).addChild("test", JcrConstants.NT_UNSTRUCTURED);
        UserManager uMgr = getUserManager(root);
        adminstrators = uMgr.createGroup(ADMINISTRATOR_GROUP);
        root.commit();
        config = getSecurityProvider().getConfiguration(AuthorizationConfiguration.class);
    }

    @Override
    public void after() throws Exception {
        try {
            root.getTree("/test").remove();
            UserManager uMgr = getUserManager(root);
            if (adminstrators != null) {
                uMgr.getAuthorizable(adminstrators.getID()).remove();
            }
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
                PermissionConstants.PARAM_READ_PATHS, READ_PATHS,
                PermissionConstants.PARAM_ADMINISTRATIVE_PRINCIPALS, new String[] {ADMINISTRATOR_GROUP});

        return ConfigurationParameters.of(ImmutableMap.of(AuthorizationConfiguration.NAME, acConfig));
    }

    private PermissionProvider createPermissionProvider(ContentSession session) {
        return config.getPermissionProvider(session.getLatestRoot(), session.getWorkspaceName(), session.getAuthInfo().getPrincipals());
    }

    @Test
    public void testHasPrivileges() throws Exception {
        ContentSession testSession = createTestSession();
        try {
            PermissionProvider pp = createPermissionProvider(testSession);

            assertTrue(pp.hasPrivileges(null));
            assertTrue(pp.hasPrivileges(null, new String[0]));
            assertFalse(pp.hasPrivileges(null, PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT));
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testReadPath() throws Exception {
        ContentSession testSession = createTestSession();
        try {
            Root r = testSession.getLatestRoot();
            PermissionProvider pp = createPermissionProvider(testSession);

            Tree tree = r.getTree("/");
            assertFalse(tree.exists());
            assertFalse(pp.getTreePermission(tree, TreePermission.EMPTY).canRead());

            for (String path : READ_PATHS) {
                tree = r.getTree(path);
                assertTrue(tree.exists());
                assertTrue(pp.getTreePermission(tree, TreePermission.EMPTY).canRead());
            }
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testIsGrantedForReadPaths() throws Exception {
        ContentSession testSession = createTestSession();
        try {
            PermissionProvider pp = createPermissionProvider(testSession) ;
            for (String path : READ_PATHS) {
                assertTrue(pp.isGranted(path, Permissions.getString(Permissions.READ)));
                assertTrue(pp.isGranted(path, Permissions.getString(Permissions.READ_NODE)));
                assertTrue(pp.isGranted(path + '/' + JcrConstants.JCR_PRIMARYTYPE, Permissions.getString(Permissions.READ_PROPERTY)));
                assertFalse(pp.isGranted(path, Permissions.getString(Permissions.READ_ACCESS_CONTROL)));
            }

            for (String path : READ_PATHS) {
                Tree tree = root.getTree(path);
                assertTrue(pp.isGranted(tree, null, Permissions.READ));
                assertTrue(pp.isGranted(tree, null, Permissions.READ_NODE));
                assertTrue(pp.isGranted(tree, tree.getProperty(JcrConstants.JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
                assertFalse(pp.isGranted(tree, null, Permissions.READ_ACCESS_CONTROL));
            }

            RepositoryPermission rp = pp.getRepositoryPermission();
            assertFalse(rp.isGranted(Permissions.READ));
            assertFalse(rp.isGranted(Permissions.READ_NODE));
            assertFalse(rp.isGranted(Permissions.READ_PROPERTY));
            assertFalse(rp.isGranted(Permissions.READ_ACCESS_CONTROL));
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testGetPrivilegesForReadPaths() throws Exception {
        ContentSession testSession = createTestSession();
        try {
            PermissionProvider pp = createPermissionProvider(testSession) ;
            for (String path : READ_PATHS) {
                Tree tree = root.getTree(path);
                assertEquals(Collections.singleton(PrivilegeConstants.JCR_READ), pp.getPrivileges(tree));
            }
            assertEquals(Collections.<String>emptySet(), pp.getPrivileges(null));
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testHasPrivilegesForReadPaths() throws Exception {
        ContentSession testSession = createTestSession();
        try {
            PermissionProvider pp = createPermissionProvider(testSession) ;
            for (String path : READ_PATHS) {
                Tree tree = root.getTree(path);
                assertTrue(pp.hasPrivileges(tree, PrivilegeConstants.JCR_READ));
                assertTrue(pp.hasPrivileges(tree, PrivilegeConstants.REP_READ_NODES));
                assertTrue(pp.hasPrivileges(tree, PrivilegeConstants.REP_READ_PROPERTIES));
                assertFalse(pp.hasPrivileges(tree, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
            }
            assertFalse(pp.hasPrivileges(null, PrivilegeConstants.JCR_READ));
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testAdministatorConfig() throws Exception {
        adminstrators.addMember(getTestUser());
        root.commit();

        ContentSession testSession = createTestSession();
        try {
            Root r = testSession.getLatestRoot();
            Root immutableRoot = RootFactory.createReadOnlyRoot(r);

            PermissionProvider pp = createPermissionProvider(testSession) ;
            assertTrue(r.getTree("/").exists());
            TreePermission tp = pp.getTreePermission(immutableRoot.getTree("/"), TreePermission.EMPTY);
            assertSame(TreePermission.ALL, tp);

            for (String path : READ_PATHS) {
                Tree tree = r.getTree(path);
                assertTrue(tree.exists());
                assertSame(TreePermission.ALL, pp.getTreePermission(tree, TreePermission.EMPTY));
            }
        } finally {
            testSession.close();
        }
    }

    @Test
    public void testIsGrantedNonExistingVersionStoreLocation() {
        TreeLocation location = TreeLocation.create(root, VersionConstants.VERSION_STORE_PATH + "/non/existing/tree");
        PermissionProvider pp = createPermissionProvider(adminSession);

        assertTrue(pp instanceof PermissionProviderImpl);
        assertFalse(((PermissionProviderImpl) pp).isGranted(location, Permissions.ALL));
    }
}
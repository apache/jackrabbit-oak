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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Map;

public class AdministrativePermissionProviderTest extends AbstractSecurityTest {

    private static final String ADMINISTRATOR_GROUP = "admins";

    private ContentSession testSession;
    private PermissionProvider permissionProvider;

    @Before
    public void before() throws Exception {
        super.before();

        UserManager uMgr = getUserManager(root);
        Group adminstrators = uMgr.createGroup(ADMINISTRATOR_GROUP);
        adminstrators.addMember(getTestUser());
        root.commit();

        testSession = createTestSession();
        permissionProvider = getConfig(AuthorizationConfiguration.class).getPermissionProvider(testSession.getLatestRoot(), testSession.getWorkspaceName(), testSession.getAuthInfo().getPrincipals());
    }

    @After
    public void after() throws Exception {
        try {
            testSession.close();
            Authorizable a = getUserManager(root).getAuthorizable(ADMINISTRATOR_GROUP);
            if (a != null) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters acConfig = ConfigurationParameters.of(
                PermissionConstants.PARAM_ADMINISTRATIVE_PRINCIPALS, new String[] {ADMINISTRATOR_GROUP});
        return ConfigurationParameters.of(Map.of(AuthorizationConfiguration.NAME, acConfig));
    }

    @Test
    public void testRepositoryPermissions() {
        RepositoryPermission rp = permissionProvider.getRepositoryPermission();
        assertSame(RepositoryPermission.ALL, rp);
    }

    @Test
    public void testRootTreePermissions() {
        Root r = testSession.getLatestRoot();
        assertTrue(r.getTree("/").exists());

        TreePermission tp = permissionProvider.getTreePermission(r.getTree("/"), TreePermission.EMPTY);
        assertSame(TreePermission.ALL, tp);
    }

    @Test
    public void testReadPaths() {
        Root r = testSession.getLatestRoot();
        for (String path : PermissionConstants.DEFAULT_READ_PATHS) {
            Tree tree = r.getTree(path);
            assertTrue(tree.exists());
            assertSame(TreePermission.ALL, permissionProvider.getTreePermission(tree, TreePermission.EMPTY));
        }
    }

    @Test
    public void testIsGrantedNonExistingLocation() {
        assertTrue(permissionProvider instanceof AggregatedPermissionProvider);

        TreeLocation location = TreeLocation.create(testSession.getLatestRoot(), "/test/non/existing/tree");
        assertTrue(((AggregatedPermissionProvider) permissionProvider).isGranted(location, Permissions.ALL));

        location = TreeLocation.create(testSession.getLatestRoot(), "/non/existing/tree");
        assertTrue(((AggregatedPermissionProvider) permissionProvider).isGranted(location, Permissions.ALL));
    }

    @Test
    public void testIsGrantedNonExistingVersionStoreLocation() {
        assertTrue(permissionProvider instanceof AggregatedPermissionProvider);
        TreeLocation location = TreeLocation.create(testSession.getLatestRoot(), VersionConstants.VERSION_STORE_PATH + "/non/existing/tree");

        assertTrue(((AggregatedPermissionProvider) permissionProvider).isGranted(location, Permissions.ALL));
    }
}
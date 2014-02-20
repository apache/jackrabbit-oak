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

import java.util.Set;
import javax.jcr.Session;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PermissionStoreTest extends AbstractSecurityTest {

    private AuthorizationConfiguration acConfig;
    private ContentSession testSession;
    private Root testRoot;

    @Override
    public void before() throws Exception {
        super.before();

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList  acl = AccessControlUtils.getAccessControlList(acMgr, "/");
        if (acl != null) {
            acl.addEntry(getTestUser().getPrincipal(), privilegesFromNames(PrivilegeConstants.JCR_ALL), true);
        }
        acMgr.setPolicy("/", acl);
        root.commit();
        testSession = createTestSession();
        testRoot = testSession.getLatestRoot();
        acConfig = getSecurityProvider().getConfiguration(AuthorizationConfiguration.class);
    }

    @Override
    public void after() throws Exception {
        try {
            if (testSession != null) {
                testSession.close();
            }
            AccessControlManager acMgr = getAccessControlManager(root);
            JackrabbitAccessControlList  acl = AccessControlUtils.getAccessControlList(acMgr, "/");
            if (acl != null) {
                for (AccessControlEntry ace : acl.getAccessControlEntries()) {
                    if (getTestUser().getPrincipal().equals(ace.getPrincipal())) {
                        acl.removeAccessControlEntry(ace);
                    }
                }
            }
            acMgr.setPolicy("/", acl);
            root.commit();
        } finally {
            super.after();
        }
    }

    private PermissionProvider createPermissionProvider() {
        return acConfig.getPermissionProvider(testRoot, testSession.getWorkspaceName(), testSession.getAuthInfo().getPrincipals());
    }

    @Test
    public void testReadAccess() {
        Tree ps = testRoot.getTree(PermissionConstants.PERMISSIONS_STORE_PATH);
        assertFalse(ps.exists());
    }

    @Test
    public void testGetTreePermission() {
        PermissionProvider pp = createPermissionProvider();

        Tree t = root.getTree(PermissionConstants.PERMISSIONS_STORE_PATH);
        assertSame(TreePermission.EMPTY, pp.getTreePermission(t, TreePermission.EMPTY));
    }

    @Test
    public void testIsGranted() {
        PermissionProvider pp = createPermissionProvider();

        Tree t = root.getTree(PermissionConstants.PERMISSIONS_STORE_PATH);

        assertFalse(pp.isGranted(t, null, Permissions.READ));
        assertFalse(pp.isGranted(t, t.getProperty(JcrConstants.JCR_PRIMARYTYPE), Permissions.READ));
    }

    @Test
    public void testIsGrantedAtPath() {
        PermissionProvider pp = createPermissionProvider();

        assertFalse(pp.isGranted(PermissionConstants.PERMISSIONS_STORE_PATH, Session.ACTION_READ));
        assertFalse(pp.isGranted(PermissionConstants.PERMISSIONS_STORE_PATH, Session.ACTION_ADD_NODE));
    }

    @Test
    public void testHasPrivilege() {
        PermissionProvider pp = createPermissionProvider();

        Tree t = root.getTree(PermissionConstants.PERMISSIONS_STORE_PATH);
        assertFalse(pp.hasPrivileges(t, PrivilegeConstants.JCR_READ));
    }

    @Test
    public void testGetPrivilege() {
        PermissionProvider pp = createPermissionProvider();

        Tree t = root.getTree(PermissionConstants.PERMISSIONS_STORE_PATH);
        Set<String> privilegeNames = pp.getPrivileges(t);
        assertTrue(privilegeNames.isEmpty());
    }
}
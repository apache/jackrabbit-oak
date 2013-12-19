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

import java.security.Principal;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TreePermissionImplTest extends AbstractSecurityTest implements AccessControlConstants {

    private AuthorizationConfiguration config;
    private Principal testPrincipal;

    @Override
    public void before() throws Exception {
        super.before();

        new NodeUtil(root.getTree("/")).addChild("test", JcrConstants.NT_UNSTRUCTURED);
        root.commit();
        config = getSecurityProvider().getConfiguration(AuthorizationConfiguration.class);
        testPrincipal = getTestUser().getPrincipal();
    }

    @Override
    public void after() throws Exception {
        try {
            root.getTree("/test").remove();
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    private TreePermission getTreePermission(String path) throws Exception {
        ContentSession testSession = createTestSession();
        PermissionProvider pp = config.getPermissionProvider(testSession.getLatestRoot(), testSession.getWorkspaceName(), testSession.getAuthInfo().getPrincipals());

        return pp.getTreePermission(root.getTree(path), TreePermission.EMPTY);
    }

    @Test
    public void testCanReadProperties() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/test");
        acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_READ), true);
        acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.REP_READ_PROPERTIES), false);
        acMgr.setPolicy("/test", acl);
        root.commit();

        TreePermission tp = getTreePermission("/test");

        assertFalse(tp.canReadProperties());
        assertTrue(tp.canRead());
        assertFalse(tp.canReadProperties());
    }

    @Test
    public void testCanReadProperties2() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/test");
        acl.addEntry(getTestUser().getPrincipal(), privilegesFromNames(PrivilegeConstants.JCR_READ), true);
        acMgr.setPolicy("/test", acl);
        root.commit();

        Tree policyTree = root.getTree("/test/rep:policy");
        NodeUtil ace = new NodeUtil(policyTree).addChild("ace2", NT_REP_DENY_ACE);
        ace.setNames(REP_PRIVILEGES, PrivilegeConstants.REP_READ_PROPERTIES);
        ace.setString(REP_PRINCIPAL_NAME, getTestUser().getPrincipal().getName());
        root.commit();

        TreePermission tp = getTreePermission("/test");

        assertFalse(tp.canReadProperties());
        assertTrue(tp.canRead());
        assertFalse(tp.canReadProperties());
    }
}
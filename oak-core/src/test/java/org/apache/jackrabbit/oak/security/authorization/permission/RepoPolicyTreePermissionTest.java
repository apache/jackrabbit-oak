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
import java.security.PrivilegedAction;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlManager;
import javax.security.auth.Subject;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class RepoPolicyTreePermissionTest extends AbstractSecurityTest implements AccessControlConstants {

    private static final String REPO_POLICY_PATH = '/' + REP_REPO_POLICY;

    private AuthorizationConfiguration config;

    private ContentSession accessSession;
    private ContentSession noAccessSession;

    @Override
    public void before() throws Exception {
        super.before();

        Principal testPrincipal = getTestUser().getPrincipal();

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, null);
        if (acl == null) {
            throw new RuntimeException();
        }

        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy(null, acl);
        root.commit();
        config = getSecurityProvider().getConfiguration(AuthorizationConfiguration.class);

        accessSession = createTestSession();

        Subject notAllowedSubject = new Subject(true, ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()), ImmutableSet.of(), ImmutableSet.of());
        noAccessSession = Subject.doAs(notAllowedSubject, (PrivilegedAction<ContentSession>) () -> {
            try {
                return getContentRepository().login(null, null);
            } catch (Exception e) {
                throw new RuntimeException();
            }
        });
    }

    @Override
    public void after() throws Exception {
        try {
            AccessControlManager acMgr = getAccessControlManager(root);
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, null);
            if (acl != null) {
                acMgr.removePolicy(null, acl);
                root.commit();
            }
            accessSession.close();
            noAccessSession.close();
        } finally {
            super.after();
        }
    }

    @Nonnull
    private TreePermission getTreePermission(@Nonnull ContentSession cs, @Nonnull String path) throws Exception {
        Root r = cs.getLatestRoot();
        PermissionProvider pp = config.getPermissionProvider(r, cs.getWorkspaceName(), cs.getAuthInfo().getPrincipals());

        Tree t = r.getTree(PathUtils.ROOT_PATH);
        TreePermission tp = pp.getTreePermission(t, TreePermission.EMPTY);
        for (String name : PathUtils.elements(path)) {
            t = t.getChild(name);
            tp = pp.getTreePermission(t, tp);
        }
        return tp;
    }

    @Test
    public void testTreePermissionImpl() throws Exception {
        TreePermission tp = getTreePermission(accessSession, REPO_POLICY_PATH);
        assertTrue(tp instanceof RepoPolicyTreePermission);
    }

    @Test
    public void testGetChildPermission() throws Exception {
        TreePermission tp = getTreePermission(accessSession, REPO_POLICY_PATH);
        assertSame(tp, tp.getChildPermission("childName", EmptyNodeState.EMPTY_NODE));
    }

    @Test
    public void testCanRead() throws Exception {
        TreePermission tp = getTreePermission(accessSession, REPO_POLICY_PATH);
        assertTrue(tp.canRead());
    }

    @Test
    public void testCanRead2() throws Exception {
        TreePermission tp = getTreePermission(noAccessSession, REPO_POLICY_PATH);
        assertFalse(tp.canRead());
    }

    @Test
    public void testCanReadAceNode() throws Exception {
        TreePermission tp = getTreePermission(accessSession, root.getTree(REPO_POLICY_PATH).getChildren().iterator().next().getPath());
        assertTrue(tp.canRead());
    }

    @Test
    public void testCanReadAceNode2() throws Exception {
        TreePermission tp = getTreePermission(noAccessSession, root.getTree(REPO_POLICY_PATH).getChildren().iterator().next().getPath());
        assertFalse(tp.canRead());
    }


    @Test
    public void testCanReadProperty() throws Exception {
        TreePermission tp = getTreePermission(accessSession, REPO_POLICY_PATH);
        assertTrue(tp.canRead(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_ACL)));
    }

    @Test
    public void testCanReadProperty2() throws Exception {
        TreePermission tp = getTreePermission(noAccessSession, REPO_POLICY_PATH);
        assertFalse(tp.canRead(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_ACL)));
    }


    @Test
    public void testCanReadPropertyAceNode() throws Exception {
        Tree aceTree = root.getTree(REPO_POLICY_PATH).getChildren().iterator().next();
        PropertyState principalProp = aceTree.getProperty(REP_PRINCIPAL_NAME);

        TreePermission tp = getTreePermission(accessSession, aceTree.getPath());
        assertTrue(tp.canRead(principalProp));
    }

    @Test
    public void testCanReadPropertyAceNode2() throws Exception {
        Tree aceTree = root.getTree(REPO_POLICY_PATH).getChildren().iterator().next();
        PropertyState principalProp = aceTree.getProperty(REP_PRINCIPAL_NAME);

        TreePermission tp = getTreePermission(noAccessSession, aceTree.getPath());
        assertFalse(tp.canRead(principalProp));
    }

    @Test
    public void testCanReadProperties() throws Exception {
        TreePermission tp = getTreePermission(accessSession, REPO_POLICY_PATH);
        assertTrue(tp.canReadProperties());
    }

    @Test
    public void testCanReadProperties2() throws Exception {
        TreePermission tp = getTreePermission(noAccessSession, REPO_POLICY_PATH);
        assertFalse(tp.canReadProperties());
    }

    @Test
    public void testCanReadAll() throws Exception {
        TreePermission tp = getTreePermission(accessSession, REPO_POLICY_PATH);
        assertFalse(tp.canReadAll());
    }

    @Test
    public void testCanReadAll2() throws Exception {
        TreePermission tp = getTreePermission(noAccessSession, REPO_POLICY_PATH);
        assertFalse(tp.canReadAll());
    }

    @Test
    public void testIsGranted() throws Exception {
        TreePermission tp = getTreePermission(accessSession, REPO_POLICY_PATH);
        assertTrue(tp.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertFalse(tp.isGranted(Permissions.WORKSPACE_MANAGEMENT));
        assertFalse(tp.isGranted(Permissions.NAMESPACE_MANAGEMENT|Permissions.WORKSPACE_MANAGEMENT));
    }

    @Test
    public void testIsGranted2() throws Exception {
        TreePermission tp = getTreePermission(noAccessSession, REPO_POLICY_PATH);
        assertFalse(tp.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertFalse(tp.isGranted(Permissions.WORKSPACE_MANAGEMENT));
        assertFalse(tp.isGranted(Permissions.NAMESPACE_MANAGEMENT|Permissions.WORKSPACE_MANAGEMENT));
    }

    @Test
    public void testIsGrantedProperty() throws Exception {
        PropertyState ps = PropertyStates.createProperty("name", "value");
        TreePermission tp = getTreePermission(accessSession, REPO_POLICY_PATH);
        assertTrue(tp.isGranted(Permissions.NAMESPACE_MANAGEMENT, ps));
        assertFalse(tp.isGranted(Permissions.WORKSPACE_MANAGEMENT, ps));
        assertFalse(tp.isGranted(Permissions.NAMESPACE_MANAGEMENT|Permissions.WORKSPACE_MANAGEMENT, ps));
    }

    @Test
    public void testIsGrantedProperty2() throws Exception {
        PropertyState ps = PropertyStates.createProperty("name", "value");
        TreePermission tp = getTreePermission(noAccessSession, REPO_POLICY_PATH);
        assertFalse(tp.isGranted(Permissions.NAMESPACE_MANAGEMENT, ps));
        assertFalse(tp.isGranted(Permissions.WORKSPACE_MANAGEMENT, ps));
        assertFalse(tp.isGranted(Permissions.NAMESPACE_MANAGEMENT|Permissions.WORKSPACE_MANAGEMENT, ps));
    }
}
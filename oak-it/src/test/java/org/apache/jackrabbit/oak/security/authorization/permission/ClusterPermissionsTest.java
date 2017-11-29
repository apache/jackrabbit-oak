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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClusterPermissionsTest {

    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;
    private ContentRepository contentRepository1;
    private ContentRepository contentRepository2;
    private UserManager userManager1;
    private UserManager userManager2;
    private AccessControlManager aclMgr1;
    private AccessControlManager aclMgr2;

    protected NamePathMapper namePathMapper = NamePathMapper.DEFAULT;
    protected SecurityProvider securityProvider1;
    protected SecurityProvider securityProvider2;
    protected ContentSession adminSession1;
    protected ContentSession adminSession2;
    protected Root root1;
    protected Root root2;

    @Before
    public void before() throws Exception {
        MemoryDocumentStore ds = new MemoryDocumentStore();
        MemoryBlobStore bs = new MemoryBlobStore();
        DocumentMK.Builder builder;

        builder = new DocumentMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
        ns1 = builder.setClusterId(1).getNodeStore();
        builder = new DocumentMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
        ns2 = builder.setClusterId(2).getNodeStore();

        Oak oak = new Oak(ns1)
                .with(new InitialContent())
                .with(new ReferenceEditorProvider())
                .with(new ReferenceIndexProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new PropertyIndexProvider())
                .with(new TypeEditorProvider())
                .with(securityProvider1 = new SecurityProviderBuilder().with(getSecurityConfigParameters()).build());
        contentRepository1 = oak.createContentRepository();
        adminSession1 = login1(getAdminCredentials());
        root1 = adminSession1.getLatestRoot();
        userManager1 = securityProvider1.getConfiguration(UserConfiguration.class).getUserManager(root1, namePathMapper);
        aclMgr1 = securityProvider1.getConfiguration(AuthorizationConfiguration.class).getAccessControlManager(root1, namePathMapper);

        // make sure initial content is visible to ns2
        syncClusterNodes();

        oak = new Oak(ns2)
                .with(new InitialContent())
                .with(new ReferenceEditorProvider())
                .with(new ReferenceIndexProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new PropertyIndexProvider())
                .with(new TypeEditorProvider())
                .with(securityProvider2 = new SecurityProviderBuilder().with(getSecurityConfigParameters()).build());

        contentRepository2 = oak.createContentRepository();
        adminSession2 = login2(getAdminCredentials());
        root2 = adminSession2.getLatestRoot();
        userManager2 = securityProvider2.getConfiguration(UserConfiguration.class).getUserManager(root2, namePathMapper);
        aclMgr2 = securityProvider2.getConfiguration(AuthorizationConfiguration.class).getAccessControlManager(root2, namePathMapper);
    }

    @After
    public void after() {
        ns1.dispose();
        ns2.dispose();
    }

    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.EMPTY;
    }

    protected Configuration getConfiguration() {
        return ConfigurationUtil.getDefaultConfiguration(getSecurityConfigParameters());
    }

    protected ContentSession login1(@Nullable Credentials credentials)
            throws LoginException, NoSuchWorkspaceException {
        return contentRepository1.login(credentials, null);
    }
    protected ContentSession login2(@Nullable Credentials credentials)
            throws LoginException, NoSuchWorkspaceException {
        return contentRepository2.login(credentials, null);
    }

    protected Credentials getAdminCredentials() {
        String adminId = "admin";
        return new SimpleCredentials(adminId, adminId.toCharArray());
    }

    @Test
    public void testCreateUser() throws Exception {
        userManager1.createUser("testUser", "testUser");
        root1.commit();
        syncClusterNodes();
        root2.refresh();
        assertNotNull("testUser must exist on 2nd cluster node", userManager2.getAuthorizable("testUser"));
    }

    @Test
    public void testAclPropagation() throws Exception {
        Tree node = root1.getTree("/").addChild("testNode");
        node.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        User user1 = userManager1.createUser("testUser", "testUser");
        JackrabbitAccessControlList acl1 = AccessControlUtils.getAccessControlList(aclMgr1, "/testNode");
        acl1.addEntry(user1.getPrincipal(), AccessControlUtils.privilegesFromNames(aclMgr1, "jcr:all"), true);
        aclMgr1.setPolicy("/testNode", acl1);
        root1.commit();

        syncClusterNodes();
        root2.refresh();
        JackrabbitAccessControlList acl2 = AccessControlUtils.getAccessControlList(aclMgr2, "/testNode");
        AccessControlEntry[] aces = acl2.getAccessControlEntries();
        assertEquals(1, aces.length);
    }

    @Test
    public void testPermissionPropagation() throws Exception {
        // create a "/testNode"
        Tree node = root1.getTree("/").addChild("testNode");
        node.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);

        // create 2 users
        User user1 = userManager1.createUser("testUser1", "testUser1");
        User user2 = userManager1.createUser("testUser2", "testUser2");

        JackrabbitAccessControlList acl1 = AccessControlUtils.getAccessControlList(aclMgr1, "/testNode");

        // deny jcr:all for everyone on /testNode
        acl1.addEntry(EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(aclMgr1, "jcr:all"), false);

        // allow jcr:read for testUser1 on /testNode
        acl1.addEntry(user1.getPrincipal(), AccessControlUtils.privilegesFromNames(aclMgr1, "jcr:read"), true);
        aclMgr1.setPolicy("/testNode", acl1);
        root1.commit();

        syncClusterNodes();
        root2.refresh();

        // login with testUser1 and testUser2 (on cluster node 2)
        ContentSession session1 = contentRepository2.login(new SimpleCredentials("testUser1", "testUser1".toCharArray()), null);
        ContentSession session2 = contentRepository2.login(new SimpleCredentials("testUser2", "testUser2".toCharArray()), null);

        // testUser1 can read /testNode
        assertTrue(session1.getLatestRoot().getTree("/testNode").exists());

        // testUser2 cannot read /testNode
        assertFalse(session2.getLatestRoot().getTree("/testNode").exists());

        // now, allow jcr:read also for 'everyone' (on cluster node 1)
        acl1 = AccessControlUtils.getAccessControlList(aclMgr1, "/testNode");
        acl1.addEntry(EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(aclMgr1, "jcr:read"), true);
        aclMgr1.setPolicy("/testNode", acl1);
        root1.commit();

        syncClusterNodes();
        root2.refresh();

        // testUser1 can read /testNode
        assertTrue(session1.getLatestRoot().getTree("/testNode").exists());

        // testUser2 can also read /testNode
        assertTrue(session2.getLatestRoot().getTree("/testNode").exists());
    }

    private void syncClusterNodes() {
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();
    }

}
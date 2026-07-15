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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginTestBase;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ProtectionConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.Privilege;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_SECOND_USER;
import static org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants.TOKENS_NODE_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants.TOKEN_ATTRIBUTE_DO_CREATE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class ExternalUserValidatorTest extends ExternalLoginTestBase {

    @Parameterized.Parameters(name = "name={2}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { IdentityProtectionType.NONE, false, "None, Default Sync" },
                new Object[] { IdentityProtectionType.WARN, true, "Warn, Dynamic Sync" },
                new Object[] { IdentityProtectionType.WARN, false, "Warn, Default Sync" },
                new Object[] { IdentityProtectionType.PROTECTED, true, "Protected, Dynamic Sync" },
                new Object[] { IdentityProtectionType.PROTECTED, false, "Protected, Default Sync" });
    }
    
    private final IdentityProtectionType type;
    private final boolean isDynamic;
    
    private String localUserPath;
    private String externalUserPath;
    private UserManager userManager;
    
    private Root sysRoot;

    public ExternalUserValidatorTest(@NotNull IdentityProtectionType type, boolean isDynamic, @NotNull String name) {
        this.type = type;
        this.isDynamic = isDynamic;
    }

    @Override
    public void before() throws Exception {
        super.before();

        localUserPath = getTestUser().getPath();

        // force an external user to be synchronized into the repo
        login(new SimpleCredentials(USER_ID, new char[0])).close();
        root.refresh();

        Authorizable a = getUserManager(root).getAuthorizable(USER_ID);
        assertNotNull(a);
        externalUserPath = a.getPath();

        userManager = getUserManager(root);
        sysRoot = getSystemRoot();
    }
    
    @Override
    protected @NotNull Map<String, Object> getExternalPrincipalConfiguration() {
        return Collections.singletonMap(ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDENTITIES, type.label);
    }

    @Override
    @NotNull
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config = super.createSyncConfig();
        config.user().setDynamicMembership(isDynamic);
        return config;
    }
    
    private boolean exceptionExpected() {
        return type == IdentityProtectionType.PROTECTED;
    }
    
    private void assertCommit() {
        try {
            root.commit();
            if (exceptionExpected()) {
                fail("CommitFailedException expected");
            }
        } catch (CommitFailedException e) {
            if (!exceptionExpected()) {
                fail("No CommitFailedException expected.");
            } else {
                assertEquals(76, e.getCode());
            }
        } finally {
            root.refresh();
        }
    }
    
    @Test
    public void testModifyLocalUser() throws Exception {
        Tree userTree = root.getTree(localUserPath);
        userTree.setProperty("test", "value");
        TreeUtil.addChild(userTree, "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        root.commit();
        assertTrue(userManager.getAuthorizableByPath(localUserPath).hasProperty("test"));
        
        userTree.setProperty("test", "modified");
        root.commit();
        assertTrue(userManager.getAuthorizableByPath(localUserPath).hasProperty("test"));

        userTree.removeProperty("test");
        userTree.getChild("child").remove();
        root.commit();
        assertFalse(userManager.getAuthorizableByPath(localUserPath).hasProperty("test"));
    }
    
    @Test
    public void testAddProperty() throws Exception {
        Tree externalUserTree = root.getTree(externalUserPath);
        externalUserTree.setProperty("test", "value");
        
        assertCommit();
        assertEquals(!exceptionExpected(), userManager.getAuthorizableByPath(externalUserPath).hasProperty("test"));
    }

    @Test
    public void testModifyProperty() throws Exception {
        Tree externalUserTree = root.getTree(externalUserPath);
        assertTrue(externalUserTree.hasProperty("name"));
        externalUserTree.setProperty("name", "newValue");

        assertCommit();
        String expected = exceptionExpected() ? "Test User" : "newValue";
        assertEquals(expected, userManager.getAuthorizableByPath(externalUserPath).getProperty("name")[0].getString());
    }
    
    @Test
    public void testRemoveProperty() throws Exception {
        Tree externalUserTree = root.getTree(externalUserPath);
        assertTrue(externalUserTree.hasProperty("email"));
        externalUserTree.removeProperty("email");

        assertCommit();
        assertEquals(exceptionExpected(), userManager.getAuthorizableByPath(externalUserPath).hasProperty("email"));
    }

    @Test
    public void testAddPropertyInSubtree() throws Exception {
        Tree profile = root.getTree(externalUserPath).getChild("profile");
        profile.setProperty("test", "value");

        assertCommit();
        assertEquals(!exceptionExpected(), userManager.getAuthorizableByPath(externalUserPath).hasProperty("profile/test"));
    }

    @Test
    public void testModifyPropertyInSubtree() throws Exception {
        Tree profile = root.getTree(externalUserPath).getChild("profile");
        assertTrue(profile.hasProperty("age"));
        profile.setProperty("age", 90);

        assertCommit();
        long expected = exceptionExpected() ? 72 : 90;
        assertEquals(expected, userManager.getAuthorizableByPath(externalUserPath).getProperty("profile/age")[0].getLong());
    }

    @Test
    public void testRemovePropertyInSubtree() throws Exception {
        Tree profile = root.getTree(externalUserPath).getChild("profile");
        assertTrue(profile.hasProperty("age"));
        profile.removeProperty("age");

        assertCommit();
        assertEquals(exceptionExpected(), userManager.getAuthorizableByPath(externalUserPath).hasProperty("profile/age"));
    }
    
    @Test
    public void testAddChildNode() throws Exception {
        Tree profile = root.getTree(externalUserPath).getChild("profile");
        TreeUtil.addChild(profile, "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        assertCommit();
        assertEquals(!exceptionExpected(), profile.hasChild("child"));
    }

    @Test
    public void testRemoveChildNode() throws Exception {
        Tree profile = root.getTree(externalUserPath).getChild("profile");
        assertTrue(profile.exists());
        profile.remove();

        assertCommit();
        assertEquals(exceptionExpected(), userManager.getAuthorizableByPath(externalUserPath).hasProperty("profile/age"));
    }
    
    @Test
    public void testReorderChildNodes() throws Exception {
        Root sr = getSystemRoot();
        sr.refresh();
        Tree userTree = sr.getTree(externalUserPath);
        Tree profile = userTree.getChild("profile");
        Tree profile2 = TreeUtil.addChild(userTree, "profile2", NT_UNSTRUCTURED, sr.getTree(NodeTypeConstants.NODE_TYPES_PATH), "id");
        sr.commit();
        
        root.refresh();
        Tree t = root.getTree(externalUserPath).getChild("profile2");
        t.orderBefore("profile");
        
        assertCommit();
        String expectedName = exceptionExpected() ? "profile" : "profile2";
        assertEquals(expectedName, root.getTree(externalUserPath).getChildren().iterator().next().getName());
    }
    
    @Test
    public void testModifyGroupMembership() throws Exception {
        Group g = getUserManager(root).getAuthorizable("a", Group.class);
        if (g != null) {
            g.addMember(getTestUser());
            
            assertCommit();
            assertEquals(!exceptionExpected(), g.isMember(getTestUser()));
        } // else: dynamic membership
    }
    
    @Test
    public void testAddEditAcContent() throws Exception {
        JackrabbitAccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, externalUserPath);
        Privilege[] privs = privilegesFromNames(JCR_READ);
        if (acl != null) {
            acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privs);
            acMgr.setPolicy(acl.getPath(), acl);
            root.commit();
            assertTrue(acMgr.hasPrivileges(externalUserPath, Collections.singleton(EveryonePrincipal.getInstance()), privs));

            for (AccessControlEntry entry : acl.getAccessControlEntries()) {
                if (EveryonePrincipal.getInstance().equals(entry.getPrincipal())) {
                    acl.removeAccessControlEntry(entry);
                    acMgr.setPolicy(acl.getPath(), acl);
                    root.commit();
                }
            }
            assertFalse(acMgr.hasPrivileges(externalUserPath, Collections.singleton(EveryonePrincipal.getInstance()), privs));
        }
    }
    
    @Test
    public void testCustomSecurityProperty() throws Exception {
        Context customContext = new Context.Default() {
            @Override
            public boolean definesProperty(@NotNull Tree parent, @NotNull PropertyState property) {
                return "testtoken".equals(property.getName());
            }
        };
        TokenConfiguration customTc = mock(TokenConfiguration.class);
        when(customTc.getContext()).thenReturn(customContext);
        when(customTc.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
        
        TokenConfiguration tc = getSecurityProvider().getConfiguration(TokenConfiguration.class);
        assertTrue(tc instanceof CompositeConfiguration);
        CompositeConfiguration<TokenConfiguration> cc = (CompositeConfiguration<TokenConfiguration>) tc;

        try {
            cc.addConfiguration(customTc);
            
            Tree t = root.getTree(externalUserPath);
            t.setProperty("testtoken", "value");
            root.commit();

            t.setProperty("testtoken", "modified");
            root.commit();
            
            t.removeProperty("testtoken");
            root.commit();
        } finally {
            cc.removeConfiguration(customTc);
        }
    }

    @Test
    public void testRemoveTokens() throws Exception {
        Configuration configuration = getConfiguration();
        try {
            Configuration.setConfiguration(getTokenConfiguration());

            // force creation of login token
            SimpleCredentials sc = new SimpleCredentials(USER_ID, "".toCharArray());
            sc.setAttribute(TokenConstants.TOKEN_ATTRIBUTE, TOKEN_ATTRIBUTE_DO_CREATE);
            getContentRepository().login(sc, null).close();

            root.refresh();
            Tree tokens = root.getTree(externalUserPath).getChild(TOKENS_NODE_NAME);
            assertTrue(tokens.exists());
            
            // remove individual token nodes
            for (Tree t : tokens.getChildren()) {
                t.remove();
                root.commit();
            }

            // remove the .tokens parent
            tokens.remove();
            root.commit();

            assertFalse(root.getTree(externalUserPath).hasChild(TOKENS_NODE_NAME));
        } finally {
            Configuration.setConfiguration(configuration);
        }
    }
    
    private Configuration getTokenConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(
                                TokenLoginModule.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                                Collections.emptyMap()),
                        new AppConfigurationEntry(
                                LoginModuleImpl.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                                Collections.emptyMap()),
                        new AppConfigurationEntry(
                                ExternalLoginModule.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                options)
                };
            }
        };
    }
    
    @Test
    public void testCreateExternalUser() throws Exception {
        ExternalUser extU = idp.getUser(ID_SECOND_USER);
        
        Tree folder = root.getTree(externalUserPath).getParent();
        Tree t = TreeUtil.addChild(folder, "test", UserConstants.NT_REP_USER, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "id");
        t.setProperty(UserConstants.REP_AUTHORIZABLE_ID, extU.getId());
        t.setProperty(UserConstants.REP_PRINCIPAL_NAME, extU.getPrincipalName());
        t.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, extU.getExternalId().getString());
        t.setProperty(JCR_UUID, UUIDUtils.generateUUID(extU.getId().toLowerCase()));
        
        assertCommit();
        Authorizable u = getUserManager(root).getAuthorizable(extU.getId());
        if (exceptionExpected()) {
            assertNull(u);
        } else {
            assertNotNull(u);
        }
    }

    @Test
    public void testCreateExternalUserWithSubtree() throws Exception {
        ExternalUser extU = idp.getUser(ID_SECOND_USER);

        Tree folder = root.getTree(externalUserPath).getParent();
        Tree t = TreeUtil.addChild(folder, "test", UserConstants.NT_REP_USER, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "id");
        t.setProperty(UserConstants.REP_AUTHORIZABLE_ID, extU.getId());
        t.setProperty(UserConstants.REP_PRINCIPAL_NAME, extU.getPrincipalName());
        t.setProperty(ExternalIdentityConstants.REP_EXTERNAL_ID, extU.getExternalId().getString());
        t.setProperty(JCR_UUID, UUIDUtils.generateUUID(extU.getId().toLowerCase()));
        Tree profile = TreeUtil.addChild(t, "profile", NT_UNSTRUCTURED, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "id");
        profile.setProperty("name", "test-user");
        
        assertCommit();
        Authorizable u = getUserManager(root).getAuthorizable(extU.getId());
        if (exceptionExpected()) {
            assertNull(u);
        } else {
            assertNotNull(u);
        }
    }
    
    @Test
    public void testAddMixin() throws Exception {
        Tree externalUserTree = root.getTree(externalUserPath);
        TreeUtil.addMixin(externalUserTree, MIX_VERSIONABLE, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "id");

        assertCommit();
        assertEquals(!exceptionExpected(), root.getTree(externalUserPath).hasProperty(JCR_MIXINTYPES));
    }
    
    @Test
    public void testAddMixinWithProperty() throws Exception {
        Tree externalUserTree = root.getTree(externalUserPath);
        TreeUtil.addMixin(externalUserTree, "mix:language", root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "id");
        externalUserTree.setProperty("jcr:language", "farsi");

        assertCommit();
        assertEquals(!exceptionExpected(), root.getTree(externalUserPath).hasProperty(JCR_MIXINTYPES));
    }
    
    @Test
    public void testRemoveExternalUser() {
        Tree externalUserTree = root.getTree(externalUserPath);
        externalUserTree.remove();

        assertCommit();
        assertEquals(exceptionExpected(), root.getTree(externalUserPath).exists());
    }
    
    @Test
    public void testDisable() throws Exception {
        Authorizable extuser = getUserManager(root).getAuthorizableByPath(externalUserPath);
        assertNotNull(extuser);
        assertFalse(extuser.isGroup());
        
        ((User) extuser).disable("disable");
        assertCommit();
        assertEquals(!exceptionExpected(), root.getTree(externalUserPath).hasProperty(UserConstants.REP_DISABLED));
    }

    @Test
    public void testChangePassword() throws Exception {
        Authorizable extuser = getUserManager(root).getAuthorizableByPath(externalUserPath);
        assertNotNull(extuser);
        assertFalse(extuser.isGroup());
        
        ((User) extuser).changePassword("something");
        assertCommit();
        assertEquals(!exceptionExpected(), root.getTree(externalUserPath).hasProperty(UserConstants.REP_PASSWORD));
    }
    
    @Test
    public void testAddModifyRemoveFolder() throws Exception {
        Tree t = root.getTree(PathUtils.getParentPath(externalUserPath));
        Tree folder = TreeUtil.addChild(t, "folder", UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
        String path = folder.getPath();
        root.commit();
        
        sysRoot.refresh();
        assertTrue(sysRoot.getTree(path).exists());
        
        TreeUtil.addMixin(folder, NodeTypeConstants.MIX_VERSIONABLE, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "id");
        root.commit();

        sysRoot.refresh();
        assertTrue(sysRoot.getTree(path).hasProperty(JCR_MIXINTYPES));
        
        folder.remove();
        root.commit();

        sysRoot.refresh();
        assertFalse(sysRoot.getTree(path).exists());
    }
    
    @Test
    public void testValidatorWithTypeNone() {
        if (type == IdentityProtectionType.NONE) {
            RootProvider rp = mock(RootProvider.class);
            TreeProvider tp = mock(TreeProvider.class);
            SecurityProvider sp = mock(SecurityProvider.class);
            try {
                ExternalUserValidatorProvider vp = new ExternalUserValidatorProvider(rp, tp, sp, type, ProtectionConfig.DEFAULT);
                fail("IllegalArgumentException expected");
            } catch (IllegalArgumentException e) {
                // success
            } finally {
                verifyNoInteractions(rp, tp, sp);
            }
        }
    }
}
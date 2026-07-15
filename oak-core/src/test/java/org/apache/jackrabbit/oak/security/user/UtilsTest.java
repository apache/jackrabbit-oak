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
package org.apache.jackrabbit.oak.security.user;

import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeAware;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;

import static org.apache.jackrabbit.oak.security.user.Utils.canImpersonateAllUsers;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.DEFAULT_ADMIN_ID;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.PARAM_ADMIN_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class UtilsTest extends AbstractSecurityTest {

    private Tree tree;

    @Override
    public void before() throws Exception {
        super.before();

        tree = root.getTree(PathUtils.ROOT_PATH);
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    private void assertEqualPath(@NotNull Tree expected, @NotNull Tree result) {
        assertEquals(expected.getPath(), result.getPath());
    }
    
    private @NotNull Principal getAdminPrincipal(@NotNull UserManager userManager) throws Exception {
        String adminId = getConfig(UserConfiguration.class).getParameters().getConfigValue(PARAM_ADMIN_ID, DEFAULT_ADMIN_ID);
        User admin = userManager.getAuthorizable(adminId, User.class);
        assertNotNull(admin);
        return admin.getPrincipal();
    }

    @Test
    public void testGetOrAddTreeCurrentElement() throws Exception {
        Tree result = Utils.getOrAddTree(tree, ".", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertSame(tree, result);
    }

    @Test
    public void testGetOrAddTreeParentElement() throws Exception {
        Tree child = Utils.getOrAddTree(tree, "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Tree parent = Utils.getOrAddTree(child, "..", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertEqualPath(tree, parent);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetOrAddTreeParentElementFromRoot() throws Exception {
        Utils.getOrAddTree(tree, "..", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
    }

    @Test
    public void testGetOrAddTreeSingleElement() throws Exception {
        Tree child = Utils.getOrAddTree(tree, "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertEqualPath(root.getTree("/child"), child);
    }

    @Test
    public void testGetOrAddTree() throws Exception {
        Map<String, String> map = Map.of(
                "a/b/c", "/a/b/c",
                "a/../b/c", "/b/c",
                "a/b/c/../..", "/a",
                "a/././././b/c", "/a/b/c"
        );
        for (Map.Entry<String, String> entry : map.entrySet()) {
            Tree t = Utils.getOrAddTree(tree, entry.getKey(), NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            assertEqualPath(root.getTree(entry.getValue()), t);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testGetOrAddTreeReachesParentOfRoot() throws Exception {
        Utils.getOrAddTree(tree, "a/../../b", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetOrAddTreeTargetNotAccessible() throws Exception {
        Tree nonExisting = mock(Tree.class);
        when(nonExisting.exists()).thenReturn(false);

        Tree t = mock(Tree.class);
        when(t.exists()).thenReturn(true);
        when(t.getParent()).thenReturn(t);
        when(t.getChild("a")).thenReturn(t);
        when(t.getChild("b")).thenReturn(nonExisting);
        when(t.addChild("b")).thenReturn(nonExisting);

        Utils.getOrAddTree(t, "a/a/b", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
    }
    
    @Test
    public void testIsEveryoneUser() throws Exception {
        AuthorizableImpl user = when(mock(AuthorizableImpl.class).getPrincipal()).thenReturn(EveryonePrincipal.getInstance()).getMock();
        when(user.isGroup()).thenReturn(false);
        assertFalse(Utils.isEveryone(user));
    }

    @Test
    public void testIsEveryoneGroup() throws Exception {
        Group gr = getUserManager(root).createGroup(EveryonePrincipal.getInstance());
        assertTrue(Utils.isEveryone(gr));
    }
    
    @Test
    public void testIsEveryoneOtherAuthorizable() throws Exception {
        Authorizable a = when(mock(Authorizable.class).getPrincipal()).thenReturn(EveryonePrincipal.getInstance()).getMock();
        when(a.isGroup()).thenReturn(false);
        assertFalse(Utils.isEveryone(a));

        when(a.isGroup()).thenReturn(true);
        assertTrue(Utils.isEveryone(a));
    }

    @Test
    public void testIsEveryoneGetPrincipalFails() throws Exception {
        Authorizable a = when(mock(Authorizable.class).getPrincipal()).thenThrow(new RepositoryException()).getMock();
        when(a.isGroup()).thenReturn(true);
        assertFalse(Utils.isEveryone(a));
    }
    
    @Test
    public void testCanImpersonateAllNonExisting() throws Exception {
        Principal principal = new PrincipalImpl("nonExisting");
        UserManager userManager = getUserManager(root);
        
        assertNull(userManager.getAuthorizable(principal));
        assertFalse(canImpersonateAllUsers(principal, userManager));
    }
    
    @Test
    public void testCanImpersonateAllForGroup() throws Exception {
        Principal principal = new PrincipalImpl("aGroup");
        Group gr = getUserManager(root).createGroup(principal);

        UserManager userManager = spy(getUserManager(root));
        assertFalse(canImpersonateAllUsers(principal, userManager));
    }
    
    @Test
    public void testCanImpersonateAllForAdminUser() throws Exception {
        UserManager userManager = getUserManager(root);
        Principal adminPrincipal = getAdminPrincipal(userManager);
        
        assertTrue(canImpersonateAllUsers(adminPrincipal, userManager));
        assertTrue(canImpersonateAllUsers(new PrincipalImpl(adminPrincipal.getName()), userManager));
        
        GroupPrincipal gp = when(mock(GroupPrincipal.class).getName()).thenReturn(adminPrincipal.getName()).getMock();
        assertTrue(canImpersonateAllUsers(gp, userManager));
    }

    @Test
    public void testCanImpersonateAllForNonAdminUser() throws Exception {
        User u = getTestUser();

        UserManager userManager = getUserManager(root);
        String userPrincipalName = u.getPrincipal().getName();

        assertFalse(canImpersonateAllUsers(u.getPrincipal(), userManager));
        assertFalse(canImpersonateAllUsers(new PrincipalImpl(userPrincipalName), userManager));
        assertFalse(canImpersonateAllUsers((AdminPrincipal) () -> userPrincipalName, userManager));
    }
    
    @Test
    public void testCanImpersonateAllLookupFails() throws Exception {
        UserManager umgrMock = when(mock(UserManager.class).getAuthorizable(any(Principal.class))).thenThrow(new RepositoryException()).getMock();

        Principal adminPrincipal = getAdminPrincipal(getUserManager(root));

        assertFalse(canImpersonateAllUsers(adminPrincipal, umgrMock));
    }

    @Test
    public void testCanImpersonateAllByImpersonatorMember() throws Exception {
        String impersonatorGroupId = "impersonator-group";

        UserManagerImpl userManagerSpy = spy((UserManagerImpl)getUserManager(root));
        ConfigurationParameters configs = ImpersonationTestUtil.getMockedConfigs(userManagerSpy.getConfig(), impersonatorGroupId);
        when(userManagerSpy.getConfig()).thenReturn(configs);

        Group impersonatorGroup = userManagerSpy.createGroup(impersonatorGroupId);
        Authorizable authorizable = userManagerSpy.getAuthorizable(getTestUser().getID());
        assertNotNull(authorizable);
        
        assertFalse(canImpersonateAllUsers(authorizable.getPrincipal(), userManagerSpy));
        
        impersonatorGroup.addMember(authorizable);
        root.commit();

        assertTrue(canImpersonateAllUsers(authorizable.getPrincipal(), userManagerSpy));
    }

    @Test
    public void testCanImpersonateAllConfiguredNonExistingPrincipal() throws Exception {
        String impersonatorName = "nonExisting";

        UserManagerImpl userManagerSpy = spy((UserManagerImpl)getUserManager(root));
        ConfigurationParameters configs = ImpersonationTestUtil.getMockedConfigs(userManagerSpy.getConfig(), impersonatorName);
        when(userManagerSpy.getConfig()).thenReturn(configs);

        Authorizable authorizable = userManagerSpy.getAuthorizable(getTestUser().getID());
        assertNotNull(authorizable);
        assertFalse(canImpersonateAllUsers(authorizable.getPrincipal(), userManagerSpy));
    }

    @Test
    public void testIsImpersonatorByPrincipal() throws Exception {
        Principal impersonatorPrincipal = getTestUser().getPrincipal();

        UserManagerImpl userManagerSpy = spy((UserManagerImpl)getUserManager(root));
        ConfigurationParameters configs = ImpersonationTestUtil.getMockedConfigs(userManagerSpy.getConfig(), impersonatorPrincipal.getName());
        when(userManagerSpy.getConfig()).thenReturn(configs);

        User user = userManagerSpy.getAuthorizable(getTestUser().getID(), User.class);
        assertNotNull(user);
        assertTrue(canImpersonateAllUsers(user.getPrincipal(), userManagerSpy));
    }
    @Test
    public void testIsImpersonatorByNonMatchingPrincipal() throws Exception {
        String impersonatorName = "impersonator";
        Principal impersonatorPrincipal = new PrincipalImpl(impersonatorName);

        UserManagerImpl userManagerSpy = spy((UserManagerImpl)getUserManager(root));
        ConfigurationParameters configs = ImpersonationTestUtil.getMockedConfigs(userManagerSpy.getConfig(), impersonatorName);
        when(userManagerSpy.getConfig()).thenReturn(configs);

        PrincipalManager principalManagerMock = ImpersonationTestUtil.getMockedPrincipalManager(impersonatorName, impersonatorPrincipal);
        when(userManagerSpy.getPrincipalManager()).thenReturn(principalManagerMock);

        User user = userManagerSpy.getAuthorizable(getTestUser().getID(), User.class);
        assertNotNull(user);
        assertFalse(canImpersonateAllUsers(user.getPrincipal(), userManagerSpy));
    }
    
    @Test
    public void testIsImpersonatorOtherMgrImpl() throws Exception {
        Principal p = new PrincipalImpl("mockPrincipal");
        
        User userMock = when(mock(User.class).getPrincipal()).thenReturn(p).getMock();
        UserManager umMock = when(mock(UserManager.class).getAuthorizable(p)).thenReturn(userMock).getMock();
        
        assertFalse(canImpersonateAllUsers(p, umMock));
        verify(umMock).getAuthorizable(p);
        verify(userMock).isGroup();
        verify(userMock).isAdmin();
        verifyNoMoreInteractions(userMock, umMock);
    }

    @Test
    public void testIsImpersonatorByGroup() throws Exception {
        String groupName = "impersonator_group";
        Principal impersonatorPrincipal = getTestUser().getPrincipal();
        Principal impersonatorGroupPrincipal = new GroupPrincipal() {
            @Override
            public boolean isMember(@NotNull Principal member) {
                return member.getName().equals(impersonatorPrincipal.getName());
            }

            @Override
            public @NotNull Enumeration<? extends Principal> members() {
                return Collections.emptyEnumeration();
            }

            @Override
            public String getName() {
                return groupName;
            }
        };

        UserManagerImpl userManagerSpy = spy((UserManagerImpl)getUserManager(root));
        ConfigurationParameters configs = ImpersonationTestUtil.getMockedConfigs(userManagerSpy.getConfig(), groupName);
        when(userManagerSpy.getConfig()).thenReturn(configs);
        
        PrincipalManager principalManagerMock = ImpersonationTestUtil.getMockedPrincipalManager(groupName, impersonatorGroupPrincipal);
        when(userManagerSpy.getPrincipalManager()).thenReturn(principalManagerMock);
        
        assertTrue(canImpersonateAllUsers(impersonatorPrincipal, userManagerSpy));
    }
    
    @Test
    public void testGetTreeFromTreeAware() throws Exception {
        Tree t = mock(Tree.class);
        Root r = mock(Root.class);
        
        Authorizable a = mock(Authorizable.class, withSettings().extraInterfaces(TreeAware.class));
        when(((TreeAware) a).getTree()).thenReturn(t);
        
        assertSame(t, Utils.getTree(a, r));
        
        verifyNoInteractions(r);
        verify((TreeAware) a).getTree();
        verifyNoMoreInteractions(a);
    }

    @Test
    public void testGetTree() throws Exception {
        Tree t = mock(Tree.class);
        Root r = when(mock(Root.class).getTree("/user/path")).thenReturn(t).getMock();
        
        Authorizable a = mock(Authorizable.class);
        when(a.getPath()).thenReturn("/user/path");
        
        assertSame(t, Utils.getTree(a, r));
        
        verify(r).getTree(anyString());
        verify(a).getPath();
        verifyNoMoreInteractions(a, r);
    }
}

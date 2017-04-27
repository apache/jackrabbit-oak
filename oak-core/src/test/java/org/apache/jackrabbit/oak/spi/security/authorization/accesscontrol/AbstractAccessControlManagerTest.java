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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.OpenPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class AbstractAccessControlManagerTest extends AbstractAccessControlTest {

    private static final String WSP_NAME = "wspName";
    public static final String TEST_PREFIX = "jr";

    private final String testName = TEST_PREFIX + ":testRoot";
    private final String testPath = '/' + testName;
    private final String nonExistingPath = "/not/existing";

    private final Set<Principal> testPrincipals = ImmutableSet.of(testPrincipal);
    private Privilege[] testPrivileges;
    private Privilege[] allPrivileges;

    private AbstractAccessControlManager acMgr;

    private PrivilegeManager privilegeManager;
    private AuthorizationConfiguration authorizationConfiguration;

    private SecurityProvider securityProvider;
    private ContentSession cs;

    @Before
    public void before() throws Exception {
        testPrivileges = new Privilege[] {mockPrivilege("priv1"), mockPrivilege("priv2")};
        allPrivileges = new Privilege[] {mockPrivilege(PrivilegeConstants.JCR_ALL)};

        cs = Mockito.mock(ContentSession.class);
        when(cs.getWorkspaceName()).thenReturn(WSP_NAME);
        when(cs.getAuthInfo()).thenReturn(new AuthInfoImpl(null, ImmutableMap.of(), testPrincipals));

        when(root.getContentSession()).thenReturn(cs);

        Tree nonExistingTree = Mockito.mock(Tree.class);
        when(nonExistingTree.exists()).thenReturn(false);
        when(root.getTree(nonExistingPath)).thenReturn(nonExistingTree);

        Tree existingTree = Mockito.mock(Tree.class);
        when(existingTree.exists()).thenReturn(true);
        when(root.getTree(testPath)).thenReturn(existingTree);

        Tree rootTree = Mockito.mock(Tree.class);
        when(rootTree.exists()).thenReturn(true);
        when(root.getTree("/")).thenReturn(rootTree);

        privilegeManager = Mockito.mock(PrivilegeManager.class);
        when(privilegeManager.getRegisteredPrivileges()).thenReturn(testPrivileges);
        when(privilegeManager.getPrivilege("priv1")).thenReturn(testPrivileges[0]);
        when(privilegeManager.getPrivilege("priv2")).thenReturn(testPrivileges[1]);
        when(privilegeManager.getPrivilege(PrivilegeConstants.JCR_ALL)).thenReturn(allPrivileges[0]);

        PrivilegeConfiguration privilegeConfiguration = Mockito.mock(PrivilegeConfiguration.class);
        when(privilegeConfiguration.getPrivilegeManager(root, getNamePathMapper())).thenReturn(privilegeManager);

        authorizationConfiguration = Mockito.mock(AuthorizationConfiguration.class);
        when(authorizationConfiguration.getPermissionProvider(root, WSP_NAME, getEveryonePrincipalSet())).thenReturn(EmptyPermissionProvider.getInstance());
        when(authorizationConfiguration.getPermissionProvider(root, WSP_NAME, testPrincipals)).thenReturn(OpenPermissionProvider.getInstance());
        when(authorizationConfiguration.getPermissionProvider(root, WSP_NAME, ImmutableSet.of())).thenReturn(EmptyPermissionProvider.getInstance());
        when(authorizationConfiguration.getContext()).thenReturn(Context.DEFAULT);

        securityProvider = Mockito.mock(SecurityProvider.class);
        when(securityProvider.getConfiguration(PrivilegeConfiguration.class)).thenReturn(privilegeConfiguration);
        when(securityProvider.getConfiguration(AuthorizationConfiguration.class)).thenReturn(authorizationConfiguration);

        acMgr = createAccessControlManager(root, getNamePathMapper());
    }

    private AbstractAccessControlManager createAccessControlManager(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper) {
        return new TestAcMgr(root, namePathMapper, securityProvider);
    }

    private static List<String> getInvalidPaths() {
        List<String> invalid = new ArrayList<String>();
        invalid.add("");
        invalid.add("../../jcr:testRoot");
        invalid.add("jcr:testRoot");
        invalid.add("jcr:test/Root");
        invalid.add("./jcr:testRoot");
        return invalid;
    }

    private static Privilege mockPrivilege(@Nonnull String name) {
        Privilege p = Mockito.mock(Privilege.class);
        when(p.getName()).thenReturn(name);
        return p;
    }

    private static Set<Principal> getEveryonePrincipalSet() {
        return ImmutableSet.<Principal>of(EveryonePrincipal.getInstance());
    }

    //--------------------------------------------------- protected methods >---
    @Test
    public void testGetConfig() {
        assertSame(authorizationConfiguration, acMgr.getConfig());
    }

    @Test
    public void testGetRoot() throws Exception {
        assertSame(root, createAccessControlManager(root, getNamePathMapper()).getRoot());
    }

    @Test
    public void testGetLatestRoot() throws Exception {
        assertNotSame(root, createAccessControlManager(root, getNamePathMapper()).getLatestRoot());
    }

    @Test
    public void testGetNamePathMapper() throws Exception {
        assertSame(getNamePathMapper(), createAccessControlManager(root, getNamePathMapper()).getNamePathMapper());
    }

    @Test
    public void testGetPrivilegeManager() throws Exception {
        assertSame(privilegeManager, acMgr.getPrivilegeManager());
    }

    @Test
    public void testGetOakPathNull() throws Exception {
        assertNull(acMgr.getOakPath(null));
    }

    @Test(expected = RepositoryException.class)
    public void testGetOakPathNotAbsolute() throws Exception {
        acMgr.getOakPath("a/rel/path");
    }

    @Test(expected = RepositoryException.class)
    public void testGetOakPathInvalid() throws Exception {
        NamePathMapper np = new NamePathMapper.Default() {
            @Override
            public String getOakPath(String jcrPath) {
                // mock failing conversion from jcr to oak path
                return null;
            }
        };
        createAccessControlManager(root, np).getOakPath("/any/abs/path");
    }

    @Test
    public void testGetTreeTestPath() throws Exception {
        assertNotNull(acMgr.getTree(testPath, Permissions.NO_PERMISSION, false));
        assertNotNull(acMgr.getTree(testPath, Permissions.NO_PERMISSION, true));
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetTreeNonExstingPath() throws Exception {
        acMgr.getTree(nonExistingPath, Permissions.NO_PERMISSION, false);
    }

    @Test
    public void testGetTreeNullPath() throws Exception {
        assertNotNull(acMgr.getTree(null, Permissions.NO_PERMISSION, false));
    }

    @Test
    public void testGetTreeNullPathCheckPermission() throws Exception {
        assertNotNull(acMgr.getTree(null, Permissions.ALL, false));
    }

    @Test(expected = AccessControlException.class)
    public void testGetTreeDefinesAcContent() throws Exception {
        Context ctx = new Context.Default() {
            @Override
            public boolean definesTree(@Nonnull Tree tree) {
                return true;
            }
        };
        when(authorizationConfiguration.getContext()).thenReturn(ctx);

        acMgr.getTree(testPath, Permissions.NO_PERMISSION, true);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetTreeDefinesNoAccess() throws Exception {
        when(cs.getAuthInfo()).thenReturn(new AuthInfoImpl(null, ImmutableMap.of(), getEveryonePrincipalSet()));

        AbstractAccessControlManager mgr = createAccessControlManager(root, getNamePathMapper());
        mgr.getTree(testPath, Permissions.ALL, true);
    }

    //---------------------------------------------< getSupportedPrivileges >---
    @Test
    public void testGetSupportedPrivileges() throws Exception {
        List<Privilege> allPrivileges = Arrays.asList(privilegeManager.getRegisteredPrivileges());

        Privilege[] supported = acMgr.getSupportedPrivileges(testPath);

        assertNotNull(supported);
        assertEquals(allPrivileges.size(), supported.length);
        assertTrue(allPrivileges.containsAll(Arrays.asList(supported)));
    }

    @Test
    public void testGetSupportedPrivilegesInvalidPath() throws Exception {
        for (String path : getInvalidPaths()) {
            try {
                acMgr.getSupportedPrivileges(path);
                fail("Expects valid node path, found: " + path);
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testGetSupportedPrivilegesNonExistingPath() throws Exception {
        try {
            acMgr.getSupportedPrivileges(nonExistingPath);
            fail("Nonexisting node -> PathNotFoundException expected.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    //--------------------------------------------------< privilegeFromName >---
    @Test
    public void testPrivilegeFromName() throws Exception {
        List<Privilege> allPrivileges = Arrays.asList(privilegeManager.getRegisteredPrivileges());
        for (Privilege privilege : allPrivileges) {
            Privilege p = acMgr.privilegeFromName(privilege.getName());
            assertEquals(privilege, p);
        }
    }

    //------------------------------------------------------< hasPrivileges >---
    @Test
    public void testHasNullPrivileges() throws Exception {
        assertTrue(acMgr.hasPrivileges(testPath, null));
    }

    @Test
    public void testHasEmptyPrivileges() throws Exception {
        assertTrue(acMgr.hasPrivileges(testPath, new Privilege[0]));
    }

    @Test(expected = PathNotFoundException.class)
    public void testHasPrivilegesNonExistingNodePath() throws Exception {
        acMgr.hasPrivileges(nonExistingPath, testPrivileges);
    }

    @Test(expected = PathNotFoundException.class)
    public void testHasPrivilegesNonExistingNodePathEveryoneSet() throws Exception {
        acMgr.hasPrivileges(nonExistingPath, getEveryonePrincipalSet(), testPrivileges);
    }

    @Test(expected = PathNotFoundException.class)
    public void testHasPrivilegesNonExistingNodePathEmptyPrincipalSet() throws Exception {
        acMgr.hasPrivileges(nonExistingPath, ImmutableSet.<Principal>of(), testPrivileges);
    }

    @Test
    public void testHasPrivilegesInvalidPaths() throws Exception {
        for (String path : getInvalidPaths()) {
            try {
                acMgr.hasPrivileges(path, testPrivileges);
                fail("AccessControlManager#hasPrivileges for node that doesn't exist should fail.");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testHasPrivileges() throws Exception {
        assertTrue(acMgr.hasPrivileges(testPath, allPrivileges));
    }

    @Test
    public void testHasPrivilegesSessionSet() throws Exception {
        assertTrue(acMgr.hasPrivileges(testPath, testPrincipals, allPrivileges));
    }

    @Test
    public void testHasPrivilegesInvalidPathsEveryoneSet() throws Exception {
        for (String path : getInvalidPaths()) {
            try {
                acMgr.hasPrivileges(path, ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()), testPrivileges);
                fail("AccessControlManager#hasPrivileges for node that doesn't exist should fail.");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testHasRepoPrivileges() throws Exception {
        assertTrue(acMgr.hasPrivileges(null, testPrivileges));
    }

    @Test
    public void testHasRepoPrivilegesEveryoneSet() throws Exception {
        assertFalse(acMgr.hasPrivileges(null, getEveryonePrincipalSet(), testPrivileges));
    }

    @Test
    public void testHasRepoPrivilegesEmptyPrincipalSet() throws Exception {
        assertFalse(acMgr.hasPrivileges(null, ImmutableSet.<Principal>of(), testPrivileges));
    }

    //------------------------------------------------------< getPrivileges >---
    @Test(expected = PathNotFoundException.class)
    public void testGetPrivilegesNonExistingNodePath() throws Exception {
        acMgr.getPrivileges(nonExistingPath);
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetPrivilegesNonExistingNodePathEmptyPrincipalSet() throws Exception {
        acMgr.getPrivileges(nonExistingPath, ImmutableSet.<Principal>of());
    }

    @Test
    public void testGetPrivilegesInvalidPaths() throws Exception {
        for (String path : getInvalidPaths()) {
            try {
                acMgr.getPrivileges(path);
                fail("AccessControlManager#getPrivileges  for node that doesn't exist should fail.");
            } catch (RepositoryException e) {
                // success
            }
        }

        for (String path : getInvalidPaths()) {
            try {
                acMgr.getPrivileges(path, Collections.singleton(testPrincipal));
                fail("AccessControlManager#getPrivileges  for node that doesn't exist should fail.");
            } catch (RepositoryException e) {
                // success
            }
        }

        for (String path : getInvalidPaths()) {
            try {
                acMgr.getPrivileges(path, ImmutableSet.<Principal>of());
                fail("AccessControlManager#getPrivileges  for node that doesn't exist should fail.");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testGetPrivileges() throws Exception {
        assertArrayEquals(allPrivileges, acMgr.getPrivileges(testPath));
    }

    @Test
    public void testGetPrivilegesEveronePrincipalSet() throws Exception {
        assertArrayEquals(new Privilege[0], acMgr.getPrivileges(testPath, getEveryonePrincipalSet()));
    }

    @Test
    public void testGetPrivilegesEmptyPrincipalSet() throws Exception {
        assertArrayEquals(new Privilege[0], acMgr.getPrivileges(testPath, ImmutableSet.<Principal>of()));
    }

    @Test
    public void testGetRepoPrivileges() throws Exception {
        assertArrayEquals(allPrivileges, acMgr.getPrivileges(null));
    }

    @Test
    public void testGetRepoPrivilegesEveryonePrincipalSet() throws Exception {
        assertArrayEquals(new Privilege[0], acMgr.getPrivileges(null, getEveryonePrincipalSet()));
    }

    @Test
    public void testGetRepoPrivilegesEmptyPrincipalSet() throws Exception {
        assertArrayEquals(new Privilege[0], acMgr.getPrivileges(null, ImmutableSet.<Principal>of()));
    }

    private final class TestAcMgr extends AbstractAccessControlManager {

        protected TestAcMgr(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper, @Nonnull SecurityProvider securityProvider) {
            super(root, namePathMapper, securityProvider);
        }

        @Override
        public JackrabbitAccessControlPolicy[] getApplicablePolicies(Principal principal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public JackrabbitAccessControlPolicy[] getPolicies(Principal principal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AccessControlPolicy[] getEffectivePolicies(Set<Principal> set) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AccessControlPolicy[] getPolicies(String absPath)  {
            throw new UnsupportedOperationException();
        }

        @Override
        public AccessControlPolicy[] getEffectivePolicies(String absPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AccessControlPolicyIterator getApplicablePolicies(String absPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setPolicy(String absPath, AccessControlPolicy policy) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removePolicy(String absPath, AccessControlPolicy policy) {
            throw new UnsupportedOperationException();
        }
    }
}
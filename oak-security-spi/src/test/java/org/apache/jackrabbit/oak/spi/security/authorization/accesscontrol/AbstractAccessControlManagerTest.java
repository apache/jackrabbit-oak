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

import org.apache.jackrabbit.api.security.authorization.PrivilegeCollection;
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
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import javax.jcr.AccessDeniedException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ADD_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ALL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_BITS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class AbstractAccessControlManagerTest extends AbstractAccessControlTest {

    private static final String WSP_NAME = "wspName";
    public static final String TEST_PREFIX = "jr";

    private final String testName = TEST_PREFIX + ":testRoot";
    private final String testPath = '/' + testName;
    private final String nonExistingPath = "/not/existing";

    private final Set<Principal> testPrincipals = Set.of(testPrincipal);
    private Privilege[] testPrivileges;
    private Privilege[] allPrivileges;

    private AbstractAccessControlManager acMgr;

    private PrivilegeManager privilegeManager;
    private AuthorizationConfiguration authorizationConfiguration;

    private SecurityProvider securityProvider;
    private ContentSession cs;

    @Before
    public void before() throws Exception {
        testPrivileges = new Privilege[] {mockPrivilege(JCR_READ), mockPrivilege(JCR_ADD_CHILD_NODES)};
        allPrivileges = new Privilege[] {mockPrivilege(JCR_ALL)};

        cs = mock(ContentSession.class);
        when(cs.getWorkspaceName()).thenReturn(WSP_NAME);
        when(cs.getAuthInfo()).thenReturn(new AuthInfoImpl(null, Map.of(), testPrincipals));

        when(root.getContentSession()).thenReturn(cs);

        Tree nonExistingTree = mock(Tree.class);
        when(nonExistingTree.exists()).thenReturn(false);
        when(root.getTree(nonExistingPath)).thenReturn(nonExistingTree);

        Tree existingTree = mock(Tree.class);
        when(existingTree.exists()).thenReturn(true);
        when(root.getTree(testPath)).thenReturn(existingTree);

        Tree rootTree = mock(Tree.class);
        when(rootTree.exists()).thenReturn(true);
        when(root.getTree("/")).thenReturn(rootTree);
        
        Tree jcrAllTree = mock(Tree.class);
        PrivilegeBits pb = PrivilegeBits.getInstance(PrivilegeBits.BUILT_IN.get(JCR_READ), PrivilegeBits.BUILT_IN.get(JCR_ADD_CHILD_NODES));
        when(jcrAllTree.getProperty(REP_BITS)).thenReturn(pb.asPropertyState(REP_BITS));
        Tree privilegesTree = mock(Tree.class);
        when(privilegesTree.exists()).thenReturn(true);
        when(privilegesTree.hasChild(JCR_ALL)).thenReturn(true);
        when(privilegesTree.getChild(JCR_ALL)).thenReturn(jcrAllTree);
        when(root.getTree(PrivilegeConstants.PRIVILEGES_PATH)).thenReturn(privilegesTree);

        privilegeManager = mock(PrivilegeManager.class);
        when(privilegeManager.getRegisteredPrivileges()).thenReturn(testPrivileges);
        when(privilegeManager.getPrivilege(JCR_READ)).thenReturn(testPrivileges[0]);
        when(privilegeManager.getPrivilege(JCR_ADD_CHILD_NODES)).thenReturn(testPrivileges[1]);
        when(privilegeManager.getPrivilege(JCR_ALL)).thenReturn(allPrivileges[0]);

        PrivilegeConfiguration privilegeConfiguration = mock(PrivilegeConfiguration.class);
        when(privilegeConfiguration.getPrivilegeManager(root, getNamePathMapper())).thenReturn(privilegeManager);

        authorizationConfiguration = mock(AuthorizationConfiguration.class);
        when(authorizationConfiguration.getPermissionProvider(root, WSP_NAME, getEveryonePrincipalSet())).thenReturn(EmptyPermissionProvider.getInstance());
        when(authorizationConfiguration.getPermissionProvider(root, WSP_NAME, testPrincipals)).thenReturn(OpenPermissionProvider.getInstance());
        when(authorizationConfiguration.getPermissionProvider(root, WSP_NAME, Set.of())).thenReturn(EmptyPermissionProvider.getInstance());
        when(authorizationConfiguration.getContext()).thenReturn(Context.DEFAULT);

        securityProvider = mock(SecurityProvider.class);
        when(securityProvider.getConfiguration(PrivilegeConfiguration.class)).thenReturn(privilegeConfiguration);
        when(securityProvider.getConfiguration(AuthorizationConfiguration.class)).thenReturn(authorizationConfiguration);

        acMgr = createAccessControlManager(root, getNamePathMapper());
    }

    private AbstractAccessControlManager createAccessControlManager(@NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        return mock(AbstractAccessControlManager.class, withSettings().useConstructor(root, namePathMapper, securityProvider).defaultAnswer(InvocationOnMock::callRealMethod));
    }

    private static List<String> getInvalidPaths() {
        List<String> invalid = new ArrayList<>();
        invalid.add("");
        invalid.add("../../jcr:testRoot");
        invalid.add("jcr:testRoot");
        invalid.add("jcr:test/Root");
        invalid.add("./jcr:testRoot");
        return invalid;
    }

    private static Privilege mockPrivilege(@NotNull String name) {
        Privilege p = mock(Privilege.class);
        when(p.getName()).thenReturn(name);
        return p;
    }

    private static Set<Principal> getEveryonePrincipalSet() {
        return Set.of(EveryonePrincipal.getInstance());
    }

    //--------------------------------------------------- protected methods >---
    @Test
    public void testGetConfig() {
        assertSame(authorizationConfiguration, acMgr.getConfig());
    }

    @Test
    public void testGetRoot() {
        assertSame(root, createAccessControlManager(root, getNamePathMapper()).getRoot());
    }

    @Test
    public void testGetLatestRoot() {
        assertNotSame(root, createAccessControlManager(root, getNamePathMapper()).getLatestRoot());
    }

    @Test
    public void testGetNamePathMapper() {
        assertSame(getNamePathMapper(), createAccessControlManager(root, getNamePathMapper()).getNamePathMapper());
    }

    @Test
    public void testGetPrivilegeManager() {
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
            public @Nullable String getOakPath(String jcrPath) {
                // mock failing conversion from jcr to oak path
                return null;
            }
        };
        createAccessControlManager(root, np).getOakPath("/any/abs/path");
    }
    
    @Test
    public void testGetOakPathsNull() throws Exception {
        assertTrue(createAccessControlManager(root, NamePathMapper.DEFAULT).getOakPaths(null).isEmpty());
    }

    @Test
    public void testGetOakPathsEmpty() throws Exception {
        assertTrue(createAccessControlManager(root, NamePathMapper.DEFAULT).getOakPaths(new String[0]).isEmpty());
    }

    @Test
    public void testGetOakPaths() throws Exception {
        Collection<String> oakPaths = createAccessControlManager(root, NamePathMapper.DEFAULT).getOakPaths("/path", null, "/path/2");
        assertEquals(Arrays.asList("/path", null, "/path/2"), oakPaths);
    }

    @Test(expected = RepositoryException.class)
    public void testGetOakPathsRelative() throws Exception {
        createAccessControlManager(root, NamePathMapper.DEFAULT).getOakPaths("/valid", "rel/path");
    }

    @Test(expected = RepositoryException.class)
    public void testGetOakPathsMapsToNull() throws Exception {
        NamePathMapper np = when(mock(NamePathMapper.class).getOakPath(anyString())).thenReturn(null).getMock();
        createAccessControlManager(root, np).getOakPaths("/path");
    }

    @Test
    public void testGetTreeTestPath() throws Exception {
        assertNotNull(acMgr.getTree(testPath, Permissions.NO_PERMISSION, false));
        assertNotNull(acMgr.getTree(testPath, Permissions.NO_PERMISSION, true));
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetTreeNonExistingPath() throws Exception {
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
            public boolean definesTree(@NotNull Tree tree) {
                return true;
            }
        };
        when(authorizationConfiguration.getContext()).thenReturn(ctx);

        acMgr.getTree(testPath, Permissions.NO_PERMISSION, true);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetTreeDefinesNoAccess() throws Exception {
        when(cs.getAuthInfo()).thenReturn(new AuthInfoImpl(null, Map.of(), getEveryonePrincipalSet()));

        AbstractAccessControlManager mgr = createAccessControlManager(root, getNamePathMapper());
        mgr.getTree(testPath, Permissions.ALL, true);
    }

    @Test
    public void testGetPrivilegeBitsProvider() {
        PrivilegeBitsProvider pbp = acMgr.getPrivilegeBitsProvider();
        assertNotNull(pbp);
        assertSame(pbp, acMgr.getPrivilegeBitsProvider());
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
    public void testGetSupportedPrivilegesInvalidPath() {
        for (String path : getInvalidPaths()) {
            try {
                acMgr.getSupportedPrivileges(path);
                fail("Expects valid node path, found: " + path);
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetSupportedPrivilegesNonExistingPath() throws Exception {
        acMgr.getSupportedPrivileges(nonExistingPath);
    }

    //--------------------------------------------------< privilegeFromName >---
    @Test
    public void testPrivilegeFromName() throws Exception {
        Privilege[] allPrivileges = privilegeManager.getRegisteredPrivileges();
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
        acMgr.hasPrivileges(nonExistingPath, Set.of(), testPrivileges);
    }

    @Test
    public void testHasPrivilegesInvalidPaths() {
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
    public void testHasPrivilegesInvalidPathsEveryoneSet() {
        for (String path : getInvalidPaths()) {
            try {
                acMgr.hasPrivileges(path, Set.of(EveryonePrincipal.getInstance()), testPrivileges);
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
        assertFalse(acMgr.hasPrivileges(null, Set.of(), testPrivileges));
    }

    //------------------------------------------------------< getPrivileges >---
    @Test(expected = PathNotFoundException.class)
    public void testGetPrivilegesNonExistingNodePath() throws Exception {
        acMgr.getPrivileges(nonExistingPath);
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetPrivilegesNonExistingNodePathEmptyPrincipalSet() throws Exception {
        acMgr.getPrivileges(nonExistingPath, Set.of());
    }

    @Test
    public void testGetPrivilegesInvalidPaths() {
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
                acMgr.getPrivileges(path, Set.of());
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
    public void testGetPrivilegesEveryonePrincipalSet() throws Exception {
        assertArrayEquals(new Privilege[0], acMgr.getPrivileges(testPath, getEveryonePrincipalSet()));
    }

    @Test
    public void testGetPrivilegesEmptyPrincipalSet() throws Exception {
        assertArrayEquals(new Privilege[0], acMgr.getPrivileges(testPath, Set.of()));
    }

    @Test
    public void testGetPrivilegesSessionPrincipalSet() throws Exception {
        Mockito.reset(acMgr);
        Privilege[] privileges = acMgr.getPrivileges(testPath, testPrincipals);

        // getPrivileges(String,Set) for the principals attached to the content session,
        // must result in forwarding the call to getPrivilege(String)
        verify(acMgr, times(1)).getPrivileges(testPath);

        assertArrayEquals(acMgr.getPrivileges(testPath), privileges);
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
        assertArrayEquals(new Privilege[0], acMgr.getPrivileges(null, Set.of()));
    }

    //------------------------------------------------------< getPrivilegeCollection >---
    @Test
    public void testGetPrivilegeCollection() throws Exception {
        PrivilegeCollection pe = acMgr.getPrivilegeCollection(testPath);
        assertArrayEquals(allPrivileges, pe.getPrivileges());
        
        assertTrue(pe.includes(Arrays.stream(testPrivileges).map(Privilege::getName).distinct().toArray(String[]::new)));
        assertTrue(pe.includes());
        assertFalse(pe.includes(PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT));
        try {
            pe.includes("invalid");
            fail();
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetPrivilegeCollectionSamePrincipalSet() throws Exception {
        PrivilegeCollection pc = acMgr.getPrivilegeCollection(testPath, testPrincipals);
        assertArrayEquals(allPrivileges, pc.getPrivileges());
        
        verify(acMgr).getPrivilegeCollection(testPath);
        verify(acMgr).getPrivilegeCollection(testPath, testPrincipals);
    }

    @Test
    public void testGetPrivilegeCollectionEmptyPrincipalSet() throws Exception {
        PrivilegeCollection pc = acMgr.getPrivilegeCollection(testPath, Collections.emptySet());
        assertArrayEquals(new Privilege[0], pc.getPrivileges());
        assertFalse(pc.includes(JCR_READ));
        assertTrue(pc.includes());

        verify(acMgr, never()).getPrivilegeCollection(testPath);
        verify(acMgr).getPrivilegeCollection(testPath, Collections.emptySet());
    }
    
    @Test
    public void testGetPrivilegeCollectionPrincipalSet() throws Exception {
        PrivilegeCollection pc = acMgr.getPrivilegeCollection(testPath, getEveryonePrincipalSet());
        assertArrayEquals(new Privilege[0], pc.getPrivileges());
        assertFalse(pc.includes(Arrays.stream(testPrivileges).map(Privilege::getName).distinct().toArray(String[]::new)));
        assertTrue(pc.includes());
        
        verify(acMgr).getPrivilegeCollection(testPath, getEveryonePrincipalSet());
        verify(acMgr, never()).getPrivilegeCollection(testPath);
    }
    
    @Test
    public void testPrivilegeCollectionFromNames() throws Exception {
        PrivilegeCollection pc = acMgr.privilegeCollectionFromNames();
        assertTrue(pc instanceof AbstractPrivilegeCollection);
        assertArrayEquals(new Privilege[0], pc.getPrivileges());

        pc = acMgr.privilegeCollectionFromNames(JCR_READ);
        assertTrue(pc instanceof AbstractPrivilegeCollection);
        assertArrayEquals(new Privilege[] {acMgr.privilegeFromName(JCR_READ)}, pc.getPrivileges());
    }

    @Test
    public void testPrivilegeCollectionFromInvalidNames() throws Exception {
        try {
            PrivilegeCollection pc = acMgr.privilegeCollectionFromNames("invalid");
            fail("invalid privilege name must be detected");
        } catch (AccessControlException e) {
            // success
            assertEquals("Invalid privilege name contained in [invalid]", e.getMessage());
        }
    }
}

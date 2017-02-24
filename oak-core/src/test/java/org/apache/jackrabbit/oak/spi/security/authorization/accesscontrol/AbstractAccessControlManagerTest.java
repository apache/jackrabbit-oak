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
import javax.jcr.NamespaceRegistry;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractAccessControlManagerTest extends AbstractAccessControlTest {

    public static final String TEST_PREFIX = "jr";
    public static final String TEST_URI = "http://jackrabbit.apache.org";

    private final String testName = TEST_PREFIX + ":testRoot";
    protected final String testPath = '/' + testName;

    protected Principal testPrincipal;
    protected Privilege[] testPrivileges;
    protected Root testRoot;

    protected AbstractAccessControlManager acMgr;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        acMgr = createAccessControlManager(root, getNamePathMapper());

        NodeUtil rootNode = new NodeUtil(root.getTree("/"), getNamePathMapper());
        rootNode.addChild(testName, JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        testPrivileges = privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_READ);
        testPrincipal = getTestPrincipal();
    }

    @After
    public void after() throws Exception {
        try {
            root.refresh();
            root.getTree(testPath).remove();
            root.commit();

            if (testRoot != null) {
                testRoot.getContentSession().close();
                testRoot = null;
            }
        } finally {
            super.after();
        }
    }

    protected AbstractAccessControlManager createAccessControlManager(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper) {
        return new TestAcMgr(root, namePathMapper, getSecurityProvider());
    }

    protected AbstractAccessControlManager getTestAccessControlManager() throws Exception {
        return new TestAcMgr(getTestRoot(), getNamePathMapper(), getSecurityProvider());
    }

    protected List<String> getAcContentPaths() throws RepositoryException {
        // TODO: create ac-content paths
        return ImmutableList.of();
    }

    protected Root getTestRoot() throws Exception {
        if (testRoot == null) {
            testRoot = createTestSession().getLatestRoot();
        }
        return testRoot;
    }

    protected List<String> getInvalidPaths() {
        List<String> invalid = new ArrayList<String>();
        invalid.add("");
        invalid.add("../../jcr:testRoot");
        invalid.add("jcr:testRoot");
        invalid.add("jcr:test/Root");
        invalid.add("./jcr:testRoot");
        return invalid;
    }

    protected static Set<Principal> getPrincipals(ContentSession session) {
        return session.getAuthInfo().getPrincipals();
    }

    protected static Set<Principal> getEveryonePrincipalSet() {
        return ImmutableSet.<Principal>of(EveryonePrincipal.getInstance());
    }

    //--------------------------------------------------- protected methods >---
    @Test
    public void testGetConfig() {
        assertSame(getSecurityProvider().getConfiguration(AuthorizationConfiguration.class), acMgr.getConfig());
    }

    @Test
    public void testGetRoot() throws Exception {
        assertSame(root, createAccessControlManager(root, getNamePathMapper()).getRoot());
        assertSame(getTestRoot(), createAccessControlManager(getTestRoot(), getNamePathMapper()).getRoot());
    }

    @Test
    public void testGetLatestRoot() throws Exception {
        assertNotSame(root, createAccessControlManager(root, getNamePathMapper()).getLatestRoot());
        assertNotSame(getTestRoot(), createAccessControlManager(getTestRoot(), getNamePathMapper()).getLatestRoot());
    }

    @Test
    public void testGetNamePathMapper() throws Exception {
        assertSame(getNamePathMapper(), createAccessControlManager(root, getNamePathMapper()).getNamePathMapper());
        assertSame(getNamePathMapper(), createAccessControlManager(getTestRoot(), getNamePathMapper()).getNamePathMapper());
    }

    @Test
    public void testGetPrivilegeManager() throws Exception {
        PrivilegeManager privMgr = getPrivilegeManager(root);
        assertNotSame(privMgr, acMgr.getPrivilegeManager());
        assertEquals(privMgr.getClass().getName(), acMgr.getPrivilegeManager().getClass().getName());
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

    //---------------------------------------------< getSupportedPrivileges >---
    @Test
    public void testGetSupportedPrivileges() throws Exception {
        List<Privilege> allPrivileges = Arrays.asList(getPrivilegeManager(root).getRegisteredPrivileges());

        List<String> testPaths = new ArrayList<String>();
        testPaths.add(null);
        testPaths.add("/");
        testPaths.add("/jcr:system");
        testPaths.add(testPath);

        for (String path : testPaths) {
            Privilege[] supported = acMgr.getSupportedPrivileges(path);

            assertNotNull(supported);
            assertEquals(allPrivileges.size(), supported.length);
            assertTrue(allPrivileges.containsAll(Arrays.asList(supported)));
        }
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
    public void testGetSupportedPrivilegesPropertyPath() throws Exception {
        try {
            acMgr.getSupportedPrivileges("/jcr:primaryType");
            fail("Property path -> PathNotFoundException expected.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testGetSupportedPrivilegesNonExistingPath() throws Exception {
        try {
            acMgr.getSupportedPrivileges("/non/existing/node");
            fail("Nonexisting node -> PathNotFoundException expected.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    //--------------------------------------------------< privilegeFromName >---
    @Test
    public void testPrivilegeFromName() throws Exception {
        List<Privilege> allPrivileges = Arrays.asList(getPrivilegeManager(root).getRegisteredPrivileges());
        for (Privilege privilege : allPrivileges) {
            Privilege p = acMgr.privilegeFromName(privilege.getName());
            assertEquals(privilege, p);
        }
    }

    @Test
    public void testPrivilegeFromInvalidName() throws Exception {
        List<String> invalid = new ArrayList<String>();
        invalid.add(null);
        invalid.add("");
        invalid.add("test:read");

        for (String privilegeName : invalid) {
            try {
                acMgr.privilegeFromName(privilegeName);
                fail("Invalid privilege name " + privilegeName);
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testPrivilegeFromUnknownName() throws Exception {
        List<String> invalid = new ArrayList<String>();
        invalid.add("unknownPrivilege");
        invalid.add('{' + NamespaceRegistry.NAMESPACE_JCR + "}unknown");

        for (String privilegeName : invalid) {
            try {
                acMgr.privilegeFromName(privilegeName);
                fail("Invalid privilege name " + privilegeName);
            } catch (AccessControlException e) {
                // success
            }
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

    @Test
    public void testHasPrivilegesForPropertyPath() throws Exception {
        String propertyPath = "/jcr:primaryType";
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_ALL);
        try {
            acMgr.hasPrivileges(propertyPath, privs);
            fail("AccessControlManager#hasPrivileges for property should fail.");
        } catch (PathNotFoundException e) {
            // success
        }

        try {
            acMgr.hasPrivileges(propertyPath, getPrincipals(adminSession), privs);
            fail("AccessControlManager#hasPrivileges for property should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testHasPrivilegesNonExistingNodePath() throws Exception {
        String nonExistingPath = "/not/existing";
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_ALL);
        try {
            acMgr.hasPrivileges(nonExistingPath, privs);
            fail("AccessControlManager#hasPrivileges  for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
        try {
            acMgr.hasPrivileges(nonExistingPath, getPrincipals(adminSession), privs);
            fail("AccessControlManager#hasPrivileges  for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
        try {
            acMgr.hasPrivileges(nonExistingPath, getEveryonePrincipalSet(), privs);
            fail("AccessControlManager#hasPrivileges for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
        try {
            acMgr.hasPrivileges(nonExistingPath, ImmutableSet.<Principal>of(), privs);
            fail("AccessControlManager#hasPrivileges for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testHasPrivilegesInvalidPaths() throws Exception {
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_ALL);
        for (String path : getInvalidPaths()) {
            try {
                acMgr.hasPrivileges(path, privs);
                fail("AccessControlManager#hasPrivileges for node that doesn't exist should fail.");
            } catch (RepositoryException e) {
                // success
            }
        }
        for (String path : getInvalidPaths()) {
            try {
                acMgr.hasPrivileges(path, getPrincipals(adminSession), privs);
                fail("AccessControlManager#hasPrivileges for node that doesn't exist should fail.");
            } catch (RepositoryException e) {
                // success
            }
        }
        for (String path : getInvalidPaths()) {
            try {
                acMgr.hasPrivileges(path, ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()), privs);
                fail("AccessControlManager#hasPrivileges for node that doesn't exist should fail.");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testHasPrivilegesAccessControlledNodePath() throws Exception {
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_ALL);
        for (String path : getAcContentPaths()) {
            assertTrue(acMgr.hasPrivileges(path, privs));
            assertTrue(acMgr.hasPrivileges(path, getPrincipals(adminSession), privs));
            assertFalse(acMgr.hasPrivileges(path, ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()), privs));
        }
    }

    /**
     * @since OAK 1.0 As of OAK AccessControlManager#hasPrivilege will throw
     * PathNotFoundException in case the node associated with a given path is
     * not readable to the editing session (compatibility with the specification
     * which was missing in jackrabbit).
     */
    @Test
    public void testHasPrivilegesNotAccessiblePath() throws Exception {
        List<String> notAccessible = new ArrayList();
        notAccessible.add("/");
        notAccessible.addAll(getAcContentPaths());

        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_ALL);
        AbstractAccessControlManager testAcMgr = getTestAccessControlManager();
        for (String path : notAccessible) {
            try {
                testAcMgr.hasPrivileges(path, privs);
                fail("AccessControlManager#hasPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }
        for (String path : notAccessible) {
            try {
                testAcMgr.hasPrivileges(path, getPrincipals(root.getContentSession()), privs);
                fail("AccessControlManager#hasPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }
        for (String path : notAccessible) {
            try {
                testAcMgr.hasPrivileges(path, getPrincipals(getTestRoot().getContentSession()), privs);
                fail("AccessControlManager#hasPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }
        for (String path : notAccessible) {
            try {
                testAcMgr.hasPrivileges(path, ImmutableSet.<Principal>of(), privs);
                fail("AccessControlManager#hasPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }
    }

    @Test
    public void testHasRepoPrivileges() throws Exception {
        assertTrue(acMgr.hasPrivileges(null, privilegesFromNames(PrivilegeConstants.JCR_ALL)));
        assertTrue(acMgr.hasPrivileges(null, getPrincipals(adminSession), privilegesFromNames(PrivilegeConstants.JCR_ALL)));
    }

    @Test
    public void testHasRepoPrivilegesEmptyPrincipalSet() throws Exception {
        assertFalse(acMgr.hasPrivileges(null, ImmutableSet.<Principal>of(), privilegesFromNames(PrivilegeConstants.JCR_ALL)));
    }

    @Test
    public void testTestSessionHasRepoPrivileges() throws Exception {
        AbstractAccessControlManager testAcMgr = getTestAccessControlManager();

        assertFalse(testAcMgr.hasPrivileges(null, testPrivileges));
        assertFalse(testAcMgr.hasPrivileges(null, getPrincipals(getTestRoot().getContentSession()), testPrivileges));
    }

    @Test
    public void testHasRepoPrivilegesNoAccessToPrincipals() throws Exception {
        AbstractAccessControlManager testAcMgr = getTestAccessControlManager();
        // the test-session doesn't have sufficient permissions to read privilege set for admin session.
        try {
            testAcMgr.getPrivileges(null, getPrincipals(adminSession));
            fail("testSession doesn't have sufficient permission to read access control information");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testHasRepoPrivilegesForEmptyPrincipalSet() throws Exception {
        AbstractAccessControlManager testAcMgr = getTestAccessControlManager();
        // the test-session doesn't have sufficient permissions to read privilege set.
        try {
            testAcMgr.getPrivileges(null, ImmutableSet.<Principal>of());
            fail("testSession doesn't have sufficient permission to read access control information");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    //------------------------------------------------------< getPrivileges >---
    @Test
    public void testGetPrivilegesForPropertyPath() throws Exception {
        String propertyPath = "/jcr:primaryType";
        try {
            acMgr.getPrivileges(propertyPath);
            fail("AccessControlManager#getPrivileges for property should fail.");
        } catch (PathNotFoundException e) {
            // success
        }

        try {
            acMgr.getPrivileges(propertyPath, Collections.singleton(testPrincipal));
            fail("AccessControlManager#getPrivileges for property should fail.");
        } catch (PathNotFoundException e) {
            // success
        }

        try {
            acMgr.getPrivileges(propertyPath, getPrincipals(adminSession));
            fail("AccessControlManager#getPrivileges for property should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testGetPrivilegesNonExistingNodePath() throws Exception {
        String nonExistingPath = "/not/existing";
        try {
            acMgr.getPrivileges(nonExistingPath);
            fail("AccessControlManager#getPrivileges  for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }

        try {
            acMgr.getPrivileges(nonExistingPath, getPrincipals(adminSession));
            fail("AccessControlManager#getPrivileges  for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }

        try {
            acMgr.getPrivileges(nonExistingPath, Collections.singleton(testPrincipal));
            fail("AccessControlManager#getPrivileges  for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }

        try {
            acMgr.getPrivileges(nonExistingPath, ImmutableSet.<Principal>of());
            fail("AccessControlManager#getPrivileges  for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
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
                acMgr.getPrivileges(path, getPrincipals(adminSession));
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

    /**
     * @since OAK 1.0 As of OAK AccessControlManager#hasPrivilege will throw
     * PathNotFoundException in case the node associated with a given path is
     * not readable to the editing session.
     */
    @Test
    public void testGetPrivilegesNotAccessiblePath() throws Exception {
        List<String> notAccessible = new ArrayList();
        notAccessible.add("/");
        notAccessible.addAll(getAcContentPaths());

        for (String path : notAccessible) {
            try {
                getTestAccessControlManager().getPrivileges(path);
                fail("AccessControlManager#getPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }

        for (String path : notAccessible) {
            try {
                getTestAccessControlManager().getPrivileges(path, getPrincipals(adminSession));
                fail("AccessControlManager#getPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }

        for (String path : notAccessible) {
            try {
                getTestAccessControlManager().getPrivileges(path, Collections.singleton(testPrincipal));
                fail("AccessControlManager#getPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }

    }

    @Test
    public void testGetPrivilegesAccessControlledNodePath() throws Exception {
        Privilege[] expected = privilegesFromNames(PrivilegeConstants.JCR_ALL);
        for (String path : getAcContentPaths()) {
            assertArrayEquals(expected, acMgr.getPrivileges(path));
            assertArrayEquals(expected, acMgr.getPrivileges(path, getPrincipals(adminSession)));
        }
    }

    @Test
    public void testGetPrivilegesForPrincipalsAccessControlledNodePath() throws Exception {
        Set<Principal> testPrincipals = ImmutableSet.of(testPrincipal);
        Privilege[] expected = new Privilege[0];
        for (String path : getAcContentPaths()) {
            assertArrayEquals(expected, acMgr.getPrivileges(path, testPrincipals));
        }
    }

    @Test
    public void testGetPrivilegesForNoPrincipalsAccessControlledNodePath() throws Exception {
        Privilege[] expected = new Privilege[0];
        for (String path : getAcContentPaths()) {
            assertArrayEquals(expected, acMgr.getPrivileges(path, ImmutableSet.<Principal>of()));
        }
    }

    @Test
    public void testGetRepoPrivileges() throws Exception {
        assertArrayEquals(privilegesFromNames(PrivilegeConstants.JCR_ALL), acMgr.getPrivileges(null));
        assertArrayEquals(privilegesFromNames(PrivilegeConstants.JCR_ALL), acMgr.getPrivileges(null, getPrincipals(adminSession)));
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
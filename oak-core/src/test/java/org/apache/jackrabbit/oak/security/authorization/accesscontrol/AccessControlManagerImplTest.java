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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.PathNotFoundException;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.impl.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.TestACL;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the default {@code AccessControlManager} implementation.
 */
public class AccessControlManagerImplTest extends AbstractSecurityTest implements AccessControlConstants {

    public static final String TEST_LOCAL_PREFIX = "test";
    public static final String TEST_PREFIX = "jr";
    public static final String TEST_URI = "http://jackrabbit.apache.org";

    private final String testName = TEST_PREFIX + ":testRoot";
    private final String testPath = '/' + testName;

    private Principal testPrincipal;
    private Privilege[] testPrivileges;

    private AccessControlManagerImpl acMgr;
    private NamePathMapper npMapper;
    private ValueFactory valueFactory;

    private Root testRoot;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        registerNamespace(TEST_PREFIX, TEST_URI);

        NameMapper nameMapper = new GlobalNameMapper(root);
        npMapper = new NamePathMapperImpl(nameMapper);

        acMgr = new AccessControlManagerImpl(root, npMapper, getSecurityProvider());

        NodeUtil rootNode = new NodeUtil(root.getTree("/"), getNamePathMapper());
        rootNode.addChild(testName, JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        valueFactory = new ValueFactoryImpl(root, npMapper);

        testPrivileges = privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_READ);
        testPrincipal = getTestUser().getPrincipal();
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

    @Override
    protected NamePathMapper getNamePathMapper() {
        return npMapper;
    }

    private void registerNamespace(String prefix, String uri) throws Exception {
        NamespaceRegistry nsRegistry = new ReadWriteNamespaceRegistry(root) {
            @Override
            protected Root getWriteRoot() {
                return root;
            }
        };
        nsRegistry.registerNamespace(prefix, uri);
    }

    private AccessControlManagerImpl createAccessControlManager(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper) {
        return new AccessControlManagerImpl(root, namePathMapper, getSecurityProvider());
    }

    private RestrictionProvider getRestrictionProvider() {
        return getConfig(AuthorizationConfiguration.class).getRestrictionProvider();
    }

    private PrivilegeBitsProvider getBitsProvider() {
        return new PrivilegeBitsProvider(root);
    }

    private AccessControlManagerImpl getTestAccessControlManager() throws Exception {
        return new AccessControlManagerImpl(getTestRoot(), getNamePathMapper(), getSecurityProvider());
    }

    private Root getTestRoot() throws Exception {
        if (testRoot == null) {
            testRoot = createTestSession().getLatestRoot();
        }
        return testRoot;
    }

    private List<String> getInvalidPaths() {
        List<String> invalid = new ArrayList<String>();
        invalid.add("");
        invalid.add("../../jcr:testRoot");
        invalid.add("jcr:testRoot");
        invalid.add("jcr:test/Root");
        invalid.add("./jcr:testRoot");
        return invalid;
    }

    private static Set<Principal> getPrincipals(ContentSession session) {
        return session.getAuthInfo().getPrincipals();
    }

    private static Set<Principal> getEveryonePrincipalSet() {
        return ImmutableSet.<Principal>of(EveryonePrincipal.getInstance());
    }

    private ACL getApplicablePolicy(@Nullable String path) throws RepositoryException {
        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(path);
        if (itr.hasNext()) {
            return (ACL) itr.nextAccessControlPolicy();
        } else {
            throw new RepositoryException("No applicable policy found.");
        }
    }

    private ACL createPolicy(@Nullable String path) {
        final PrincipalManager pm = getPrincipalManager(root);
        final PrivilegeManager pvMgr = getPrivilegeManager(root);
        final RestrictionProvider rp = getRestrictionProvider();
        return new ACL(path, null, getNamePathMapper()) {

            @Override
            ACE createACE(Principal principal, PrivilegeBits privilegeBits, boolean isAllow, Set<Restriction> restrictions) {
                throw new UnsupportedOperationException();
            }

            @Override
            boolean checkValidPrincipal(Principal principal) throws AccessControlException {
                Util.checkValidPrincipal(principal, pm);
                return true;
            }

            @Override
            PrivilegeManager getPrivilegeManager() {
                return pvMgr;
            }

            @Override
            PrivilegeBits getPrivilegeBits(Privilege[] privileges) {
                return getBitsProvider().getBits(privileges, getNamePathMapper());
            }

            @Nonnull
            @Override
            public RestrictionProvider getRestrictionProvider() {
                return rp;
            }
        };
    }

    @Nonnull
    private ACL setupPolicy(@Nullable String path, @Nullable Privilege... privileges) throws RepositoryException {
        Privilege[] privs = (privileges == null || privileges.length == 0) ? testPrivileges : privileges;
        ACL policy = getApplicablePolicy(path);
        if (path == null) {
            policy.addAccessControlEntry(testPrincipal, privs);
        } else {
            policy.addEntry(testPrincipal, privs, true, getGlobRestriction("*"));
        }
        acMgr.setPolicy(path, policy);
        return policy;
    }

    private Map<String, Value> getGlobRestriction(@Nonnull String value) {
        return ImmutableMap.of(REP_GLOB, valueFactory.createValue(value));
    }

    protected List<String> getAcContentPaths() throws RepositoryException {
        ACL policy = getApplicablePolicy(testPath);
        policy.addEntry(testPrincipal, testPrivileges, true, getGlobRestriction("*"));
        acMgr.setPolicy(testPath, policy);

        String aclPath = testPath + '/' + REP_POLICY;
        Tree acl = root.getTree(aclPath);
        assertTrue(acl.exists());
        Iterator<Tree> aces = acl.getChildren().iterator();
        assertTrue(aces.hasNext());
        Tree ace = aces.next();
        assertNotNull(ace);

        List<String> acContentPath = new ArrayList<String>();
        acContentPath.add(aclPath);
        acContentPath.add(ace.getPath());

        Tree rest = ace.getChild(REP_RESTRICTIONS);
        if (rest.exists()) {
            acContentPath.add(rest.getPath());
        }
        return acContentPath;
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

    @Test(expected = PathNotFoundException.class)
    public void testGetSupportedPrivilegesPropertyPath() throws Exception {
        acMgr.getSupportedPrivileges("/jcr:primaryType");
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetSupportedPrivilegesNonExistingPath() throws Exception {
        acMgr.getSupportedPrivileges("/non/existing/node");
    }

    @Test
    public void testGetSupportedPrivilegesIncludingPathConversion() throws Exception {
        List<Privilege> allPrivileges = Arrays.asList(getPrivilegeManager(root).getRegisteredPrivileges());

        List<String> testPaths = new ArrayList();
        testPaths.add('/' + TEST_LOCAL_PREFIX + ":testRoot");
        testPaths.add("/{" + TEST_URI + "}testRoot");

        NameMapper remapped = new LocalNameMapper(
                root, singletonMap(TEST_LOCAL_PREFIX, TEST_URI));

        AccessControlManager acMgr = createAccessControlManager(root, new NamePathMapperImpl(remapped));
        for (String path : testPaths) {
            Privilege[] supported = acMgr.getSupportedPrivileges(path);

            assertNotNull(supported);
            assertEquals(allPrivileges.size(), supported.length);
            assertTrue(allPrivileges.containsAll(Arrays.asList(supported)));
        }
    }

    @Test
    public void testGetSupportedForPrivilegesAcContent() throws Exception {
        List<Privilege> allPrivileges = Arrays.asList(getPrivilegeManager(root).getRegisteredPrivileges());

        for (String acPath : getAcContentPaths()) {
            Privilege[] supported = acMgr.getSupportedPrivileges(acPath);

            assertNotNull(supported);
            assertEquals(allPrivileges.size(), supported.length);
            assertTrue(allPrivileges.containsAll(Arrays.asList(supported)));
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
    public void testPrivilegeFromExpandedName() throws Exception {
        Privilege readPriv = getPrivilegeManager(root).getPrivilege(PrivilegeConstants.JCR_READ);
        assertEquals(readPriv, acMgr.privilegeFromName(Privilege.JCR_READ));
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

    @Test(expected = PathNotFoundException.class)
    public void testHasPrivilegesForPropertyPath() throws Exception {
        String propertyPath = "/jcr:primaryType";
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_ALL);

        acMgr.hasPrivileges(propertyPath, privs);
    }

    @Test(expected = PathNotFoundException.class)
    public void testHasPrivilegesForPropertyPathSessionPrincipals() throws Exception {
        String propertyPath = "/jcr:primaryType";
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_ALL);

        acMgr.hasPrivileges(propertyPath, getPrincipals(adminSession), privs);
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

    @Test(expected = AccessDeniedException.class)
    public void testHasRepoPrivilegesForEmptyPrincipalSet() throws Exception {
        AbstractAccessControlManager testAcMgr = getTestAccessControlManager();
        // the test-session doesn't have sufficient permissions to read privilege set.
        testAcMgr.getPrivileges(null, ImmutableSet.<Principal>of());
    }

    @Test
    public void testTestSessionHasPrivileges() throws Exception {
        setupPolicy(testPath);
        root.commit();

        AccessControlManagerImpl testAcMgr = getTestAccessControlManager();

        // granted privileges
        List<Privilege[]> granted = new ArrayList<Privilege[]>();
        granted.add(privilegesFromNames(PrivilegeConstants.JCR_READ));
        granted.add(privilegesFromNames(PrivilegeConstants.REP_READ_NODES));
        granted.add(privilegesFromNames(PrivilegeConstants.REP_READ_PROPERTIES));
        granted.add(privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES));
        granted.add(testPrivileges);

        for (Privilege[] privileges : granted) {
            assertTrue(testAcMgr.hasPrivileges(testPath, privileges));
            assertTrue(testAcMgr.hasPrivileges(testPath, getPrincipals(getTestRoot().getContentSession()), privileges));
        }

        // denied privileges
        List<Privilege[]> denied = new ArrayList<Privilege[]>();
        denied.add(privilegesFromNames(PrivilegeConstants.JCR_ALL));
        denied.add(privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        denied.add(privilegesFromNames(PrivilegeConstants.JCR_WRITE));
        denied.add(privilegesFromNames(PrivilegeConstants.JCR_LOCK_MANAGEMENT));

        for (Privilege[] privileges : denied) {
            assertFalse(testAcMgr.hasPrivileges(testPath, privileges));
            assertFalse(testAcMgr.hasPrivileges(testPath, getPrincipals(getTestRoot().getContentSession()), privileges));
        }
    }

    @Test(expected = AccessDeniedException.class)
    public void testTestSessionHasPrivilegesForPrincipals() throws Exception {
        setupPolicy(testPath);
        root.commit();

        AccessControlManagerImpl testAcMgr = getTestAccessControlManager();
        // but for 'admin' the test-session doesn't have sufficient privileges
        testAcMgr.getPrivileges(testPath, getPrincipals(adminSession));
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

    @Test
    public void testTestSessionGetPrivileges() throws Exception {
        setupPolicy(testPath);
        root.commit();

        AccessControlManagerImpl testAcMgr = getTestAccessControlManager();
        Set<Principal> testPrincipals = getPrincipals(getTestRoot().getContentSession());

        assertArrayEquals(new Privilege[0], testAcMgr.getPrivileges(null));
        assertArrayEquals(new Privilege[0], testAcMgr.getPrivileges(null, testPrincipals));

        Privilege[] privs = testAcMgr.getPrivileges(testPath);
        assertEquals(ImmutableSet.copyOf(testPrivileges), ImmutableSet.copyOf(privs));

        privs = testAcMgr.getPrivileges(testPath, testPrincipals);
        assertEquals(ImmutableSet.copyOf(testPrivileges), ImmutableSet.copyOf(privs));

        // but for 'admin' the test-session doesn't have sufficient privileges
        try {
            testAcMgr.getPrivileges(testPath, getPrincipals(adminSession));
            fail("testSession doesn't have sufficient permission to read access control information at testPath");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testGetPrivilegesForPrincipals() throws Exception {
        Set<Principal> adminPrincipals = getPrincipals(adminSession);
        Privilege[] expected = privilegesFromNames(PrivilegeConstants.JCR_ALL);
        assertArrayEquals(expected, acMgr.getPrivileges("/", adminPrincipals));
        assertArrayEquals(expected, acMgr.getPrivileges(null, adminPrincipals));
        assertArrayEquals(expected, acMgr.getPrivileges(testPath, adminPrincipals));

        setupPolicy(testPath);
        root.commit();
        Set<Principal> testPrincipals = Collections.singleton(testPrincipal);
        assertArrayEquals(new Privilege[0], acMgr.getPrivileges(null, testPrincipals));
        assertArrayEquals(new Privilege[0], acMgr.getPrivileges("/", testPrincipals));
        assertEquals(
                ImmutableSet.copyOf(testPrivileges),
                ImmutableSet.copyOf(acMgr.getPrivileges(testPath, testPrincipals)));
    }

    //--------------------------------------< getApplicablePolicies(String) >---

    @Test
    public void testGetApplicablePolicies() throws Exception {
        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(testPath);

        assertNotNull(itr);
        assertTrue(itr.hasNext());

        AccessControlPolicy policy = itr.nextAccessControlPolicy();
        assertNotNull(policy);
        assertTrue(policy instanceof ACL);

        ACL acl = (ACL) policy;
        assertTrue(acl.isEmpty());
        assertEquals(testPath, acl.getPath());

        assertFalse(itr.hasNext());
    }

    @Test
    public void testGetApplicablePoliciesOnAccessControllable() throws Exception {
        NodeUtil node = new NodeUtil(root.getTree(testPath));
        node.setNames(JcrConstants.JCR_MIXINTYPES, MIX_REP_ACCESS_CONTROLLABLE);

        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(testPath);

        assertNotNull(itr);
        assertTrue(itr.hasNext());
    }

    @Test
    public void testGetApplicableRepoPolicies() throws Exception {
        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies((String) null);

        assertNotNull(itr);
        assertTrue(itr.hasNext());

        AccessControlPolicy policy = itr.nextAccessControlPolicy();
        assertNotNull(policy);
        assertTrue(policy instanceof ACL);

        ACL acl = (ACL) policy;
        assertTrue(acl.isEmpty());
        assertNull(acl.getPath());

        assertFalse(itr.hasNext());
    }

    @Test
    public void testGetApplicablePoliciesWithCollidingNode() throws Exception {
        NodeUtil testTree = new NodeUtil(root.getTree(testPath));
        testTree.addChild(REP_POLICY, JcrConstants.NT_UNSTRUCTURED);

        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(testPath);
        assertNotNull(itr);
        assertFalse(itr.hasNext());
    }

    @Test
    public void testGetApplicablePoliciesForAccessControlled() throws Exception {
        AccessControlPolicy policy = getApplicablePolicy(testPath);
        acMgr.setPolicy(testPath, policy);

        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(testPath);
        assertNotNull(itr);
        assertFalse(itr.hasNext());
    }

    @Test
    public void testGetApplicablePoliciesInvalidPath() throws Exception {
        for (String invalid : getInvalidPaths()) {
            try {
                acMgr.getPolicies(invalid);
                fail("Getting applicable policies for an invalid path should fail");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testApplicablePoliciesForPropertyPath() throws Exception {
        String propertyPath = "/jcr:primaryType";
        try {
            acMgr.getApplicablePolicies(propertyPath);
            fail("Getting applicable policies for property should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testGetApplicablePoliciesNonExistingNodePath() throws Exception {
        String nonExistingPath = "/not/existing";
        try {
            acMgr.getApplicablePolicies(nonExistingPath);
            fail("Getting applicable policies for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testGetApplicablePoliciesForAcContentPaths() throws Exception {
        for (String path : getAcContentPaths()) {
            try {
                acMgr.getApplicablePolicies(path);
                fail("Getting applicable policies for access control content should fail.");
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    //------------------------------------------------< getPolicies(String) >---
    @Test
    public void testGetPolicies() throws Exception {
        AccessControlPolicy policy = getApplicablePolicy(testPath);
        acMgr.setPolicy(testPath, policy);

        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertNotNull(policies);
        assertEquals(1, policies.length);

        assertTrue(policies[0] instanceof ACL);
        ACL acl = (ACL) policies[0];
        assertTrue(acl.isEmpty());
        assertEquals(testPath, acl.getOakPath());
    }

    @Test
    public void testGetPoliciesNodeNotAccessControlled() throws Exception {
        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertNotNull(policies);
        assertEquals(0, policies.length);
    }

    @Test
    public void testGetPoliciesAfterSet() throws Exception {
        setupPolicy(testPath);

        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertNotNull(policies);
        assertEquals(1, policies.length);

        assertTrue(policies[0] instanceof ACL);
        ACL acl = (ACL) policies[0];
        assertFalse(acl.isEmpty());
    }

    @Test
    public void testGetPoliciesAfterRemove() throws Exception {
        setupPolicy(testPath);

        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertNotNull(policies);
        assertEquals(1, policies.length);

        acMgr.removePolicy(testPath, policies[0]);

        policies = acMgr.getPolicies(testPath);
        assertNotNull(policies);
        assertEquals(0, policies.length);
        assertTrue(acMgr.getApplicablePolicies(testPath).hasNext());
    }

    @Test
    public void testGetPolicyWithInvalidPrincipal() throws Exception {
        ACL policy = getApplicablePolicy(testPath);
        policy.addEntry(testPrincipal, testPrivileges, true, getGlobRestriction("*"));
        acMgr.setPolicy(testPath, policy);

        NodeUtil aclNode = new NodeUtil(root.getTree(testPath + '/' + REP_POLICY));
        NodeUtil aceNode = aclNode.addChild("testACE", NT_REP_DENY_ACE);
        aceNode.setString(REP_PRINCIPAL_NAME, "invalidPrincipal");
        aceNode.setNames(REP_PRIVILEGES, PrivilegeConstants.JCR_READ);

        // reading policies with unknown principal name should not fail.
        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertNotNull(policies);
        assertEquals(1, policies.length);

        ACL acl = (ACL) policies[0];
        List<String> principalNames = new ArrayList<String>();
        for (AccessControlEntry ace : acl.getEntries()) {
            principalNames.add(ace.getPrincipal().getName());
        }
        assertTrue(principalNames.remove("invalidPrincipal"));
        assertTrue(principalNames.remove(testPrincipal.getName()));
        assertTrue(principalNames.isEmpty());
    }

    @Test
    public void testGetRepoPolicies() throws Exception {
        String path = null;

        AccessControlPolicy[] policies = acMgr.getPolicies(path);
        assertNotNull(policies);
        assertEquals(0, policies.length);

        acMgr.setPolicy(null, acMgr.getApplicablePolicies(path).nextAccessControlPolicy());
        assertFalse(acMgr.getApplicablePolicies(path).hasNext());

        policies = acMgr.getPolicies(path);
        assertNotNull(policies);
        assertEquals(1, policies.length);

        assertTrue(policies[0] instanceof ACL);
        ACL acl = (ACL) policies[0];
        assertTrue(acl.isEmpty());
        assertNull(acl.getPath());
        assertNull(acl.getOakPath());
        assertFalse(acMgr.getApplicablePolicies(path).hasNext());

        acMgr.removePolicy(path, acl);
        assertEquals(0, acMgr.getPolicies(path).length);
        assertTrue(acMgr.getApplicablePolicies(path).hasNext());
    }

    @Test
    public void testGetPoliciesInvalidPath() throws Exception {
        for (String invalid : getInvalidPaths()) {
            try {
                acMgr.getPolicies(invalid);
                fail("Getting policies for an invalid path should fail");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testGetPoliciesPropertyPath() throws Exception {
        String propertyPath = "/jcr:primaryType";
        try {
            acMgr.getPolicies(propertyPath);
            fail("Getting policies for property should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testGetPoliciesNonExistingNodePath() throws Exception {
        String nonExistingPath = "/not/existing";
        try {
            acMgr.getPolicies(nonExistingPath);
            fail("Getting policies for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testGetPoliciesAcContentPaths() throws Exception {
        for (String path : getAcContentPaths()) {
            try {
                acMgr.getPolicies(path);
                fail("Getting policies for access control content should fail.");
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    //---------------------------------------< getEffectivePolicies(String) >---
    @Test
    public void testGetEffectivePolicies() throws Exception {
        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(testPath);
        assertNotNull(policies);
        assertEquals(0, policies.length);

        setupPolicy(testPath);
        root.commit();

        policies = acMgr.getEffectivePolicies(testPath);
        assertNotNull(policies);
        assertEquals(1, policies.length);

        NodeUtil child = new NodeUtil(root.getTree(testPath)).addChild("child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getTree().getPath();

        policies = acMgr.getEffectivePolicies(childPath);
        assertNotNull(policies);
        assertEquals(1, policies.length);

        setupPolicy(childPath);
        root.commit();

        policies = acMgr.getEffectivePolicies(childPath);
        assertNotNull(policies);
        assertEquals(2, policies.length);
    }

    @Test
    public void testGetEffectivePoliciesNewPolicy() throws Exception {
        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(testPath);
        assertNotNull(policies);
        assertEquals(0, policies.length);

        setupPolicy(testPath);
        policies = acMgr.getEffectivePolicies(testPath);
        assertNotNull(policies);
        assertEquals(0, policies.length);

        NodeUtil child = new NodeUtil(root.getTree(testPath)).addChild("child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getTree().getPath();

        policies = acMgr.getEffectivePolicies(childPath);
        assertNotNull(policies);
        assertEquals(0, policies.length);

        setupPolicy(childPath);
        policies = acMgr.getEffectivePolicies(childPath);
        assertNotNull(policies);
        assertEquals(0, policies.length);
    }

    @Test
    public void testGetEffectiveModifiedPolicy() throws Exception {
        ACL acl = setupPolicy(testPath);
        AccessControlEntry[] aces = acl.getAccessControlEntries();
        root.commit();

        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT));
        acMgr.setPolicy(testPath, acl);

        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(testPath);
        assertNotNull(policies);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof AccessControlList);
        AccessControlEntry[] effectiveAces = ((AccessControlList) policies[0]).getAccessControlEntries();
        assertArrayEquals(aces, effectiveAces);
        assertFalse(Arrays.equals(effectiveAces, acl.getAccessControlEntries()));
    }

    @Test
    public void testGetEffectivePoliciesInvalidPath() throws Exception {
        for (String invalid : getInvalidPaths()) {
            try {
                acMgr.getEffectivePolicies(invalid);
                fail("Getting policies for an invalid path should fail");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testGetEffectivePoliciesForPropertyPath() throws Exception {
        String propertyPath = "/jcr:primaryType";
        try {
            acMgr.getEffectivePolicies(propertyPath);
            fail("Getting policies for property should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testGetEffectivePoliciesNonExistingNodePath() throws Exception {
        String nonExistingPath = "/not/existing";
        try {
            acMgr.getEffectivePolicies(nonExistingPath);
            fail("Getting policies for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testGetEffectivePoliciesForAcContentPaths() throws Exception {
        for (String path : getAcContentPaths()) {
            try {
                acMgr.getEffectivePolicies(path);
                fail("Getting effective policies for access control content should fail.");
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    /**
     * @since OAK 1.0
     */
    @Test
    public void testTestSessionGetEffectivePolicies() throws Exception {
        // grant 'testUser' READ + READ_AC privileges at 'path'
        Privilege[] privileges = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        setupPolicy(testPath, privileges);
        root.commit();

        Root testRoot = getTestRoot();
        testRoot.refresh();
        AccessControlManager testAcMgr = getTestAccessControlManager();

        assertTrue(testAcMgr.hasPrivileges(testPath, privileges));

        // diff to jr core: getEffectivePolicies will just return the policies
        // accessible for the editing session but not throw an exception.
        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(testPath);
        assertNotNull(effective);
        assertEquals(1, effective.length);
    }

    /**
     * @since OAK 1.0
     */
    @Test
    public void testTestSessionGetEffectivePolicies2() throws Exception {
        NodeUtil child = new NodeUtil(root.getTree(testPath)).addChild("child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getTree().getPath();

        setupPolicy(testPath, privilegesFromNames(PrivilegeConstants.JCR_READ));
        setupPolicy(childPath, privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        root.commit();

        Root testRoot = getTestRoot();
        testRoot.refresh();
        AccessControlManager testAcMgr = getTestAccessControlManager();

        assertTrue(testAcMgr.hasPrivileges(childPath, privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL)));

        // diff to jr core: getEffectivePolicies will just return the policies
        // accessible for the editing session but not throw an exception.
        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(childPath);
        assertNotNull(effective);
        assertEquals(1, effective.length);
    }

    @Test
    public void testTestSessionGetEffectivePoliciesWithoutPrivilege() throws Exception {
        // grant 'testUser' READ + READ_AC privileges at 'path'
        Privilege[] privileges = privilegesFromNames(PrivilegeConstants.JCR_READ);
        setupPolicy(testPath, privileges);
        root.commit();

        Root testRoot = getTestRoot();
        testRoot.refresh();
        AccessControlManager testAcMgr = getTestAccessControlManager();

        List<String> paths = ImmutableList.of(testPath, NodeTypeConstants.NODE_TYPES_PATH);
        for (String path : paths) {
            assertFalse(testAcMgr.hasPrivileges(path, privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL)));
            try {
                testAcMgr.getEffectivePolicies(path);
                fail("READ_ACCESS_CONTROL is not granted at " + path);
            } catch (AccessDeniedException e) {
                // success
            }
        }
    }

    //-----------------------------< setPolicy(String, AccessControlPolicy) >---
    @Test
    public void testSetPolicy() throws Exception {
        ACL acl = getApplicablePolicy(testPath);
        acl.addAccessControlEntry(testPrincipal, testPrivileges);
        acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, getGlobRestriction("*/something"));

        acMgr.setPolicy(testPath, acl);
        root.commit();

        Root root2 = adminSession.getLatestRoot();
        AccessControlPolicy[] policies = getAccessControlManager(root2).getPolicies(testPath);
        assertEquals(1, policies.length);
        assertArrayEquals(acl.getAccessControlEntries(), ((ACL) policies[0]).getAccessControlEntries());
    }

    @Test
    public void testSetRepoPolicy() throws Exception {
        ACL acl = getApplicablePolicy(null);
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT));

        acMgr.setPolicy(null, acl);
        root.commit();

        Root root2 = adminSession.getLatestRoot();
        AccessControlPolicy[] policies = getAccessControlManager(root2).getPolicies((String) null);
        assertEquals(1, policies.length);
        assertArrayEquals(acl.getAccessControlEntries(), ((ACL) policies[0]).getAccessControlEntries());
    }

    @Test
    public void testSetPolicyWritesAcContent() throws Exception {
        ACL acl = getApplicablePolicy(testPath);
        acl.addAccessControlEntry(testPrincipal, testPrivileges);
        acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, getGlobRestriction("*/something"));

        acMgr.setPolicy(testPath, acl);
        root.commit();

        Root root2 = adminSession.getLatestRoot();
        Tree tree = root2.getTree(testPath);
        assertTrue(tree.hasChild(REP_POLICY));
        Tree policyTree = tree.getChild(REP_POLICY);
        assertEquals(NT_REP_ACL, TreeUtil.getPrimaryTypeName(policyTree));
        assertEquals(2, policyTree.getChildrenCount(3));

        Iterator<Tree> children = policyTree.getChildren().iterator();
        Tree ace = children.next();
        assertEquals(NT_REP_GRANT_ACE, TreeUtil.getPrimaryTypeName(ace));
        assertEquals(testPrincipal.getName(), TreeUtil.getString(ace, REP_PRINCIPAL_NAME));
        assertEquals(
                newHashSet(testPrivileges),
                newHashSet(privilegesFromNames(TreeUtil.getStrings(ace, REP_PRIVILEGES))));
        assertFalse(ace.hasChild(REP_RESTRICTIONS));

        Tree ace2 = children.next();
        assertEquals(NT_REP_DENY_ACE, TreeUtil.getPrimaryTypeName(ace2));
        assertEquals(EveryonePrincipal.NAME, ace2.getProperty(REP_PRINCIPAL_NAME).getValue(Type.STRING));
        Privilege[] privs = privilegesFromNames(TreeUtil.getNames(ace2, REP_PRIVILEGES));
        assertEquals(newHashSet(testPrivileges), newHashSet(privs));
        assertTrue(ace2.hasChild(REP_RESTRICTIONS));
        Tree restr = ace2.getChild(REP_RESTRICTIONS);
        assertEquals("*/something", restr.getProperty(REP_GLOB).getValue(Type.STRING));
    }

    @Test
    public void testModifyExistingPolicy() throws Exception {
        ACL acl = getApplicablePolicy(testPath);
        assertTrue(acl.addAccessControlEntry(testPrincipal, testPrivileges));
        AccessControlEntry allowTest = acl.getAccessControlEntries()[0];

        acMgr.setPolicy(testPath, acl);
        root.commit();

        acl = (ACL) acMgr.getPolicies(testPath)[0];
        assertTrue(acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, getGlobRestriction("*/something")));

        AccessControlEntry[] aces = acl.getAccessControlEntries();
        assertEquals(2, aces.length);
        AccessControlEntry denyEveryone = aces[1];
        assertEquals(EveryonePrincipal.getInstance(), denyEveryone.getPrincipal());

        acl.orderBefore(denyEveryone, allowTest);
        acMgr.setPolicy(testPath, acl);
        root.commit();

        acl = (ACL) acMgr.getPolicies(testPath)[0];
        aces = acl.getAccessControlEntries();
        assertEquals(2, aces.length);
        assertEquals(denyEveryone, aces[0]);
        assertEquals(allowTest, aces[1]);

        Privilege[] readAc = new Privilege[]{acMgr.privilegeFromName(PrivilegeConstants.JCR_READ_ACCESS_CONTROL)};
        assertTrue(acl.addEntry(testPrincipal, readAc, false, Collections.<String, Value>emptyMap()));
        assertEquals(3, acl.size());
        AccessControlEntry denyTest = acl.getAccessControlEntries()[2];

        acl.orderBefore(denyTest, allowTest);
        acMgr.setPolicy(testPath, acl);

        acl = (ACL) acMgr.getPolicies(testPath)[0];
        aces = acl.getAccessControlEntries();
        assertEquals(3, aces.length);

        assertEquals(denyEveryone, aces[0]);
        assertEquals(denyTest, aces[1]);
        assertEquals(allowTest, aces[2]);
    }

    @Test
    public void testSetInvalidPolicy() throws Exception {
        try {
            acMgr.setPolicy(testPath, new TestACL(testPath, getRestrictionProvider(), getNamePathMapper()));
            fail("Setting invalid policy must fail");
        } catch (AccessControlException e) {
            // success
        }

        ACL acl = setupPolicy(testPath);
        try {
            acMgr.setPolicy(testPath, new TestACL(testPath, getRestrictionProvider(), getNamePathMapper()));
            fail("Setting invalid policy must fail");
        } catch (AccessControlException e) {
            // success
        }

        ACL repoAcl = setupPolicy(null);
        try {
            acMgr.setPolicy(testPath, repoAcl);
            fail("Setting invalid policy must fail");
        } catch (AccessControlException e) {
            // success
        }

        try {
            acMgr.setPolicy(null, acl);
            fail("Setting invalid policy must fail");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testSetPolicyInvalidPath() throws Exception {
        for (String invalid : getInvalidPaths()) {
            try {
                AccessControlPolicy acl = createPolicy(invalid);
                acMgr.setPolicy(invalid, acl);
                fail("Setting access control policy with invalid path should fail");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testSetPolicyPropertyPath() throws Exception {
        try {
            String path = "/jcr:primaryType";
            AccessControlPolicy acl = createPolicy(path);
            acMgr.setPolicy(path, acl);
            fail("Setting access control policy at property path should fail");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testSetPolicyNonExistingNodePath() throws Exception {
        try {
            String path = "/non/existing";
            AccessControlPolicy acl = createPolicy(path);
            acMgr.setPolicy(path, acl);
            fail("Setting access control policy for non-existing node path should fail");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testSetPolicyAcContent() throws Exception {
        for (String acPath : getAcContentPaths()) {
            try {
                AccessControlPolicy acl = createPolicy(acPath);
                acMgr.setPolicy(acPath, acl);
                fail("Setting access control policy to access control content should fail");
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    @Test
    public void testSetPolicyAtDifferentPath() throws Exception {
        try {
            ACL acl = getApplicablePolicy(testPath);
            acMgr.setPolicy("/", acl);
            fail("Setting access control policy at a different node path must fail");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testSetPolicyModifiesRoot() throws Exception {
        ACL acl = getApplicablePolicy(testPath);
        assertTrue(acl.addAccessControlEntry(testPrincipal, testPrivileges));
        assertFalse(root.hasPendingChanges());

        acMgr.setPolicy(testPath, acl);
        assertTrue(root.hasPendingChanges());

        root.commit();

        acl = (ACL) acMgr.getPolicies(testPath)[0];
        assertEquals(1, acl.getAccessControlEntries().length);

        acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, getGlobRestriction("*/something"));
        acMgr.setPolicy(testPath, acl);
        assertTrue(root.hasPendingChanges());

        root.commit();

        acl = (ACL) acMgr.getPolicies(testPath)[0];
        assertEquals(2, acl.getAccessControlEntries().length);
    }

    @Test
    public void testSetPolicyWithRestrictions() throws Exception {
        ACL acl = getApplicablePolicy(testPath);
        acl.addEntry(testPrincipal, testPrivileges, true, ImmutableMap.of(AccessControlConstants.REP_GLOB, getValueFactory().createValue("/a/b")));
        acl.addEntry(testPrincipal, testPrivileges, true, ImmutableMap.of(AccessControlConstants.REP_GLOB, getValueFactory().createValue("/c/d")));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        ACL l = (ACL) acMgr.getPolicies(testPath)[0];
        assertEquals(2, l.getAccessControlEntries().length);
    }

    @Test
    public void testSetPolicyCreatesIndexedAceNodeNames() throws Exception {
        ACL acl = getApplicablePolicy(testPath);
        assertTrue(acl.addAccessControlEntry(testPrincipal, testPrivileges));
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, ImmutableMap.of(AccessControlConstants.REP_GLOB, getValueFactory().createValue("/*/a"))));
        assertTrue(acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false));
        assertTrue(acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, ImmutableMap.of(AccessControlConstants.REP_GLOB, getValueFactory().createValue("/*/a"))));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        acl = (ACL) acMgr.getPolicies(testPath)[0];
        assertEquals(4, acl.getAccessControlEntries().length);

        Iterable<Tree> aceTrees = root.getTree(testPath).getChild(AccessControlConstants.REP_POLICY).getChildren();
        String[] aceNodeNames = Iterables.toArray(Iterables.transform(aceTrees, aceTree -> aceTree.getName()), String.class);
        assertArrayEquals(new String[]{"allow", "allow1", "deny2", "deny3"}, aceNodeNames);
    }

    @Test
    public void testSetPolicyWithExistingMixins() throws Exception {
        TreeUtil.addMixin(root.getTree(testPath), JcrConstants.MIX_LOCKABLE, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);

        ACL acl = getApplicablePolicy(testPath);
        assertTrue(acl.addAccessControlEntry(testPrincipal, testPrivileges));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        assertEquals(ImmutableSet.of(JcrConstants.MIX_LOCKABLE, MIX_REP_ACCESS_CONTROLLABLE),
                ImmutableSet.copyOf(TreeUtil.getNames(root.getTree(testPath), JcrConstants.JCR_MIXINTYPES)));
    }

    //--------------------------< removePolicy(String, AccessControlPolicy) >---
    @Test
    public void testRemovePolicy() throws Exception {
        ACL acl = setupPolicy(testPath);

        acMgr.removePolicy(testPath, acl);

        assertEquals(0, acMgr.getPolicies(testPath).length);
        assertTrue(acMgr.getApplicablePolicies(testPath).hasNext());
    }

    @Test
    public void testRemoveRepoPolicy() throws Exception {
        ACL acl = setupPolicy(null);

        acMgr.removePolicy(null, acl);

        assertEquals(0, acMgr.getPolicies((String) null).length);
        assertTrue(acMgr.getApplicablePolicies((String) null).hasNext());
    }

    @Test
    public void testRemoveInvalidPolicy() throws Exception {
        ACL acl = setupPolicy(testPath);
        try {
            acMgr.removePolicy(testPath, new TestACL(testPath, getRestrictionProvider(), getNamePathMapper()));
            fail("Invalid policy -> removal must fail");
        } catch (AccessControlException e) {
            // success
        }

        ACL repoAcl = setupPolicy(null);
        try {
            acMgr.removePolicy(testPath, repoAcl);
            fail("Setting invalid policy must fail");
        } catch (AccessControlException e) {
            // success
        }

        try {
            acMgr.removePolicy(null, acl);
            fail("Setting invalid policy must fail");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testRemovePolicyInvalidPath() throws Exception {
        for (String invalid : getInvalidPaths()) {
            try {
                AccessControlPolicy acl = createPolicy(invalid);
                acMgr.removePolicy(invalid, acl);
                fail("Removing access control policy with invalid path should fail");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testRemovePolicyPropertyPath() throws Exception {
        try {
            String path = "/jcr:primaryType";
            AccessControlPolicy acl = createPolicy(path);
            acMgr.removePolicy(path, acl);
            fail("Removing access control policy at property path should fail");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testRemovePolicyNonExistingNodePath() throws Exception {
        try {
            String path = "/non/existing";
            AccessControlPolicy acl = createPolicy(path);
            acMgr.removePolicy(path, acl);
            fail("Removing access control policy for non-existing node path should fail");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testRemovePolicyAcContent() throws Exception {
        for (String acPath : getAcContentPaths()) {
            try {
                AccessControlPolicy acl = createPolicy(acPath);
                acMgr.removePolicy(acPath, acl);
                fail("Removing access control policy to access control content should fail");
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePolicyAtDifferentPath() throws Exception {
        setupPolicy(testPath);
        ACL acl = getApplicablePolicy("/");
        acMgr.removePolicy(testPath, acl);
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePolicyNodeRemoved() throws Exception {
        setupPolicy(testPath);
        AccessControlPolicy acl = acMgr.getPolicies(testPath)[0];
        root.getTree(testPath + "/" + REP_POLICY).remove();

        acMgr.removePolicy(testPath, acl);
    }

    //-----------------------------------< getApplicablePolicies(Principal) >---
    @Test
    public void testGetApplicablePoliciesNullPrincipal() throws Exception {
        try {
            acMgr.getApplicablePolicies((Principal) null);
            fail("Null is not a valid principal");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetApplicablePoliciesInvalidPrincipal() throws Exception {
        Principal unknown = getPrincipalManager(root).getPrincipal("unknown");
        int i = 0;
        while (unknown != null) {
            unknown = getPrincipalManager(root).getPrincipal("unknown"+i);
        }
        unknown = new InvalidTestPrincipal("unknown" + i);
        try {
            acMgr.getApplicablePolicies(unknown);
            fail("Unknown principal should be detected.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetApplicablePoliciesInternalPrincipal() throws Exception {
        Principal unknown = getPrincipalManager(root).getPrincipal("unknown");
        int i = 0;
        while (unknown != null) {
            unknown = getPrincipalManager(root).getPrincipal("unknown"+i);
        }
        unknown = new PrincipalImpl("unknown" + i);

        assertEquals(1, acMgr.getApplicablePolicies(unknown).length);
    }

    @Test
    public void testGetApplicablePoliciesByPrincipal() throws Exception {
        List<Principal> principals = ImmutableList.of(testPrincipal, EveryonePrincipal.getInstance());
        for (Principal principal : principals) {
            AccessControlPolicy[] applicable = acMgr.getApplicablePolicies(principal);

            assertNotNull(applicable);
            assertEquals(1, applicable.length);
            assertTrue(applicable[0] instanceof ACL);
        }
    }

    @Test
    public void testGetApplicablePoliciesByPrincipal2() throws Exception {
        setupPolicy(testPath);

        // changes not yet persisted -> no existing policies found for user
        AccessControlPolicy[] applicable = acMgr.getApplicablePolicies(testPrincipal);
        assertNotNull(applicable);
        assertEquals(1, applicable.length);
        assertTrue(applicable[0] instanceof ACL);

        // after persisting changes -> no applicable policies present any more.
        root.commit();
        applicable = acMgr.getApplicablePolicies(testPrincipal);
        assertNotNull(applicable);
        assertEquals(0, applicable.length);
    }

    @Test
    public void testTestSessionGetApplicablePolicies() throws Exception {
        setupPolicy(testPath);
        root.commit();

        Root testRoot = getTestRoot();
        testRoot.refresh();
        JackrabbitAccessControlManager testAcMgr = getTestAccessControlManager();
        List<Principal> principals = ImmutableList.of(testPrincipal, EveryonePrincipal.getInstance());
        for (Principal principal : principals) {
            // testRoot can't read access control content -> doesn't see
            // the existing policies and creates a new applicable policy.
            AccessControlPolicy[] applicable = testAcMgr.getApplicablePolicies(principal);
            assertNotNull(applicable);
            assertEquals(1, applicable.length);
            assertTrue(applicable[0] instanceof ACL);
        }
    }

    //---------------------------------------------< getPolicies(Principal) >---
    @Test
    public void testGetPoliciesNullPrincipal() throws Exception {
        try {
            acMgr.getPolicies((Principal) null);
            fail("Null is not a valid principal");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetPoliciesInvalidPrincipal() throws Exception {
        Principal unknown = getPrincipalManager(root).getPrincipal("unknown");
        int i = 0;
        while (unknown != null) {
            unknown = getPrincipalManager(root).getPrincipal("unknown"+i);
        }
        unknown = new InvalidTestPrincipal("unknown" + i);
        try {
            acMgr.getPolicies(unknown);
            fail("Unknown principal should be detected.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetPoliciesInternalPrincipal() throws Exception {
        Principal unknown = getPrincipalManager(root).getPrincipal("unknown");
        int i = 0;
        while (unknown != null) {
            unknown = getPrincipalManager(root).getPrincipal("unknown"+i);
        }
        unknown = new PrincipalImpl("unknown" + i);
        assertEquals(0, acMgr.getPolicies(unknown).length);
    }

    @Test
    public void testGetPoliciesByPrincipal() throws Exception {
        List<Principal> principals = ImmutableList.of(testPrincipal, EveryonePrincipal.getInstance());
        for (Principal principal : principals) {
            AccessControlPolicy[] policies = acMgr.getPolicies(principal);

            assertNotNull(policies);
            assertEquals(0, policies.length);
        }
    }

    @Test
    public void testGetPoliciesByPrincipal2() throws Exception {
        setupPolicy(testPath);

        // changes not yet persisted -> no existing policies found for user
        AccessControlPolicy[] policies = acMgr.getPolicies(testPrincipal);
        assertNotNull(policies);
        assertEquals(0, policies.length);

        // after persisting changes -> policies must be found
        root.commit();
        policies = acMgr.getPolicies(testPrincipal);
        assertNotNull(policies);
        assertEquals(1, policies.length);
    }

    @Test
    public void testTestSessionGetPolicies() throws Exception {
        setupPolicy(testPath);
        root.commit();

        Root testRoot = getTestRoot();
        testRoot.refresh();
        JackrabbitAccessControlManager testAcMgr = getTestAccessControlManager();
        PrincipalManager testPrincipalMgr = getPrincipalManager(testRoot);

        List<Principal> principals = ImmutableList.of(testPrincipal, EveryonePrincipal.getInstance());
        for (Principal principal : principals) {
            if (testPrincipalMgr.hasPrincipal(principal.getName())) {
                // testRoot can't read access control content -> doesn't see
                // the existing policies and creates a new applicable policy.
                AccessControlPolicy[] policies = testAcMgr.getPolicies(principal);
                assertNotNull(policies);
                assertEquals(0, policies.length);
            } else {
                // testRoot can't read principal -> no policies for that principal
                assertEquals(0, testAcMgr.getPolicies(principal).length);
            }
        }
    }

    //-------------------------------< getEffectivePolicies(Set<Principal>) >---
    @Test
    public void testGetEffectivePoliciesNullPrincipal() throws Exception {
        try {
            acMgr.getEffectivePolicies((Set) null);
            fail("Null principal set not allowed");
        } catch (AccessControlException e) {
            // success
        }

        try {
            acMgr.getEffectivePolicies(new HashSet<Principal>(Arrays.asList(EveryonePrincipal.getInstance(), null, testPrincipal)));
            fail("Null principal set not allowed");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetEffectivePoliciesInvalidPrincipals() throws Exception {
        Principal unknown = getPrincipalManager(root).getPrincipal("unknown");
        int i = 0;
        while (unknown != null) {
            unknown = getPrincipalManager(root).getPrincipal("unknown"+i);
        }
        unknown = new InvalidTestPrincipal("unknown" + i);
        try {
            acMgr.getEffectivePolicies(Collections.singleton(unknown));
            fail("Unknown principal should be detected.");
        } catch (AccessControlException e) {
            // success
        }
        try {
            acMgr.getEffectivePolicies(ImmutableSet.of(unknown, EveryonePrincipal.getInstance(), testPrincipal));
            fail("Unknown principal should be detected.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetEffectivePoliciesByPrincipal() throws Exception {
        // no ACLs containing entries for the specified principals
        // -> no effective policies expected
        Set<Set<Principal>> principalSets = new HashSet<Set<Principal>>();
        principalSets.add(Collections.singleton(testPrincipal));
        principalSets.add(Collections.<Principal>singleton(EveryonePrincipal.getInstance()));
        principalSets.add(ImmutableSet.of(testPrincipal, EveryonePrincipal.getInstance()));

        for (Set<Principal> principals : principalSets) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principals);
            assertNotNull(policies);
            assertEquals(0, policies.length);
        }

        setupPolicy(testPath);
        // changes not yet persisted -> no effecitve policies found for testprincipal
        for (Set<Principal> principals : principalSets) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principals);
            assertNotNull(policies);
            assertEquals(0, policies.length);
        }

        root.commit();
        // after persisting changes -> the policy must be found
        for (Set<Principal> principals : principalSets) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principals);
            assertNotNull(policies);
            if (principals.contains(testPrincipal)) {
                assertEquals(1, policies.length);
            } else {
                assertEquals(0, policies.length);
            }
        }

        NodeUtil child = new NodeUtil(root.getTree(testPath)).addChild("child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getTree().getPath();
        setupPolicy(childPath);
        // changes not yet persisted -> no effecitve policies found for testprincipal
        for (Set<Principal> principals : principalSets) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principals);
            assertNotNull(policies);
            if (principals.contains(testPrincipal)) {
                assertEquals(1, policies.length);
            } else {
                assertEquals(0, policies.length);
            }
        }

        root.commit();
        // after persisting changes -> the policy must be found
        for (Set<Principal> principals : principalSets) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principals);
            assertNotNull(policies);
            if (principals.contains(testPrincipal)) {
                assertEquals(2, policies.length);
            } else {
                assertEquals(0, policies.length);
            }
        }
    }

    @Test
    public void testNoEffectiveDuplicateEntries() throws Exception {
        Set<Principal> principalSet = ImmutableSet.of(testPrincipal, EveryonePrincipal.getInstance());

        // create first policy with multiple ACEs for the test principal set.
        ACL policy = getApplicablePolicy(testPath);
        policy.addEntry(testPrincipal, testPrivileges, true, getGlobRestriction("*"));
        policy.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT), false);
        policy.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT), false);
        assertEquals(3, policy.getAccessControlEntries().length);
        acMgr.setPolicy(testPath, policy);
        root.commit();

        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principalSet);
        assertEquals(1, policies.length);

        // add another policy
        NodeUtil child = new NodeUtil(root.getTree(testPath)).addChild("child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getTree().getPath();
        setupPolicy(childPath);
        root.commit();

        policies = acMgr.getEffectivePolicies(principalSet);
        assertEquals(2, policies.length);
    }

    @Test
    public void testEffectiveSorting() throws Exception {
        Set<Principal> principalSet = ImmutableSet.of(testPrincipal, EveryonePrincipal.getInstance());

        ACL nullPathPolicy = null;
        try {
            // 1. policy at 'testPath'
            ACL policy = getApplicablePolicy(testPath);
            policy.addEntry(testPrincipal, testPrivileges, true, getGlobRestriction("*"));
            policy.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT), false);
            policy.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT), false);
            acMgr.setPolicy(testPath, policy);

            // 2. policy at child node
            NodeUtil child = new NodeUtil(root.getTree(testPath)).addChild("child", JcrConstants.NT_UNSTRUCTURED);
            String childPath = child.getTree().getPath();
            setupPolicy(childPath);

            // 3. policy for null-path
            nullPathPolicy = getApplicablePolicy(null);
            assertNotNull(nullPathPolicy);

            nullPathPolicy.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.REP_PRIVILEGE_MANAGEMENT), true);
            acMgr.setPolicy(null, nullPathPolicy);
            root.commit();

            AccessControlPolicy[] effectivePolicies = acMgr.getEffectivePolicies(principalSet);
            assertEquals(3, effectivePolicies.length);

            assertNull(((JackrabbitAccessControlPolicy) effectivePolicies[0]).getPath());
            assertEquals(testPath, ((JackrabbitAccessControlPolicy) effectivePolicies[1]).getPath());
            assertEquals(childPath, ((JackrabbitAccessControlPolicy) effectivePolicies[2]).getPath());

        } finally {
            if (nullPathPolicy != null) {
                acMgr.removePolicy(null, nullPathPolicy);
                root.commit();
            }
        }

    }

    @Test
    public void testEffectivePoliciesFiltering() throws Exception {
        // create first policy with multiple ACEs for the test principal set.
        ACL policy = getApplicablePolicy(testPath);
        policy.addEntry(testPrincipal, testPrivileges, true, getGlobRestriction("*"));
        policy.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT), false);
        policy.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT), false);
        assertEquals(3, policy.getAccessControlEntries().length);
        acMgr.setPolicy(testPath, policy);
        root.commit();

        // different ways to create the principal-set to make sure the filtering
        // doesn't rely on principal equality but rather on the name.
        List<Principal> principals = ImmutableList.of(
                testPrincipal,
                new PrincipalImpl(testPrincipal.getName()),
                new Principal() {
                    @Override
                    public String getName() {
                        return testPrincipal.getName();
                    }
                });

        for (Principal princ : principals) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(ImmutableSet.of(princ));
            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof AccessControlList);

            AccessControlList acl = (AccessControlList) policies[0];
            assertEquals(2, acl.getAccessControlEntries().length);
            for (AccessControlEntry ace : acl.getAccessControlEntries()) {
                assertEquals(princ.getName(), ace.getPrincipal().getName());
            }
        }
    }

    @Test
    public void testTestSessionGetEffectivePoliciesByPrincipal() throws Exception {
        NodeUtil child = new NodeUtil(root.getTree(testPath)).addChild("child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getTree().getPath();

        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        setupPolicy(testPath, privs);
        setupPolicy(childPath, privs);
        root.commit();

        Root testRoot = getTestRoot();
        testRoot.refresh();
        JackrabbitAccessControlManager testAcMgr = getTestAccessControlManager();

        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(Collections.singleton(testPrincipal));
        assertNotNull(effective);
        assertEquals(2, effective.length);
    }

    /**
     * @since OAK 1.0 Policy at testPath not accessible -> getEffectivePolicies
     * only returns the readable policy but doesn't fail.
     */
    @Test
    public void testTestSessionGetEffectivePoliciesByPrincipal2() throws Exception {
        NodeUtil child = new NodeUtil(root.getTree(testPath)).addChild("child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getTree().getPath();

        // policy at testPath: ac content was visible but the policy can't be
        // retrieved from AcMgr as the accesscontrolled node is not visible.
        setupPolicy(testPath, privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        // policy at childPath: will be found by the getEffectivePolicies
        setupPolicy(childPath, privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        root.commit();

        Root testRoot = getTestRoot();
        testRoot.refresh();
        JackrabbitAccessControlManager testAcMgr = getTestAccessControlManager();

        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(Collections.singleton(testPrincipal));
        assertNotNull(effective);
        assertEquals(1, effective.length);
    }

    /**
     * @since OAK 1.0 Policy at testPath not accessible -> getEffectivePolicies
     * only returns the readable policy but doesn't fail.
     */
    @Test
    public void testTestSessionGetEffectivePoliciesByPrincipal3() throws Exception {
        NodeUtil child = new NodeUtil(root.getTree(testPath)).addChild("child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getTree().getPath();

        setupPolicy(testPath, privilegesFromNames(PrivilegeConstants.JCR_READ));
        setupPolicy(childPath, privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        root.commit();

        Root testRoot = getTestRoot();
        testRoot.refresh();
        JackrabbitAccessControlManager testAcMgr = getTestAccessControlManager();

        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(Collections.singleton(testPrincipal));
        assertNotNull(effective);
        assertEquals(1, effective.length);
    }

    @Test
    public void testTestSessionGetEffectivePoliciesByPrincipals() throws Exception {
        NodeUtil child = new NodeUtil(root.getTree(testPath)).addChild("child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getTree().getPath();

        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        setupPolicy(testPath, privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));

        JackrabbitAccessControlList acl = getApplicablePolicy(childPath);
        acl.addEntry(EveryonePrincipal.getInstance(), privs, true);
        acMgr.setPolicy(childPath, acl);

        root.commit();

        Root testRoot = getTestRoot();
        testRoot.refresh();
        JackrabbitAccessControlManager testAcMgr = getTestAccessControlManager();

        Set<Principal> principals = ImmutableSet.of(testPrincipal, EveryonePrincipal.getInstance());
        AccessControlPolicy[] policies = testAcMgr.getEffectivePolicies(principals);
        assertNotNull(policies);
        assertEquals(2, policies.length);
    }

    /**
     * @since OAK 1.0 : only accessible policies are returned but not exception
     * is raised.
     */
    @Test
    public void testTestSessionGetEffectivePoliciesByPrincipals2() throws Exception {
        NodeUtil child = new NodeUtil(root.getTree(testPath)).addChild("child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getTree().getPath();

        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);

        // create policy on testPath -> but deny access to test session
        JackrabbitAccessControlList acl = getApplicablePolicy(testPath);
        acl.addEntry(testPrincipal, privs, false);
        acMgr.setPolicy(testPath, acl);

        // grant access at childpath
        setupPolicy(childPath, privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        root.commit();

        Root testRoot = getTestRoot();
        testRoot.refresh();
        JackrabbitAccessControlManager testAcMgr = getTestAccessControlManager();

        Set<Principal> principals = ImmutableSet.of(testPrincipal, EveryonePrincipal.getInstance());
        AccessControlPolicy[] policies = testAcMgr.getEffectivePolicies(principals);
        assertNotNull(policies);
        assertEquals(1, policies.length);
    }

    //-----------------------------------------------< setPrincipalPolicy() >---
    @Test
    public void testSetPrincipalPolicy() throws Exception {
        JackrabbitAccessControlPolicy[] applicable = acMgr.getApplicablePolicies(testPrincipal);
        assertNotNull(applicable);
        assertEquals(1, applicable.length);
        assertTrue(applicable[0] instanceof ACL);

        ACL acl = (ACL) applicable[0];
        Value pathValue = getValueFactory().createValue(testPath, PropertyType.PATH);
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, Collections.singletonMap(REP_NODE_PATH, pathValue)));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        Root root2 = adminSession.getLatestRoot();
        AccessControlPolicy[] policies = getAccessControlManager(root2).getPolicies(testPath);
        assertEquals(1, policies.length);
        assertEquals(1, ((ACL) policies[0]).getAccessControlEntries().length);

        policies = getAccessControlManager(root2).getPolicies(testPrincipal);
        assertEquals(1, policies.length);
        assertArrayEquals(acl.getAccessControlEntries(), ((ACL) policies[0]).getAccessControlEntries());
    }

    @Test
    public void testSetPrincipalPolicy2() throws Exception {
        setupPolicy(testPath);
        root.commit();

        JackrabbitAccessControlPolicy[] policies = acMgr.getPolicies(testPrincipal);
        ACL acl = (ACL) policies[0];

        Map<String, Value> restrictions = new HashMap<String, Value>();
        restrictions.put(REP_NODE_PATH, getValueFactory().createValue(testPath, PropertyType.PATH));

        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, restrictions));

        restrictions.putAll(getGlobRestriction("*"));
        assertFalse(acl.addEntry(testPrincipal, testPrivileges, true, restrictions));

        acMgr.setPolicy(acl.getPath(), acl);
        assertEquals(2, ((ACL) acMgr.getPolicies(testPath)[0]).getAccessControlEntries().length);
    }

    @Test
    public void testSetPrincipalPolicyWithNewMvRestriction() throws Exception {
        setupPolicy(testPath);
        root.commit();

        JackrabbitAccessControlPolicy[] policies = acMgr.getPolicies(testPrincipal);
        ACL acl = (ACL) policies[0];

        Map<String, Value> restrictions = new HashMap();
        restrictions.put(REP_NODE_PATH, getValueFactory().createValue(testPath, PropertyType.PATH));

        Map<String, Value[]> mvRestrictions = new HashMap();
        ValueFactory vf = getValueFactory(root);
        Value[] restrValues = new Value[] {vf.createValue("itemname", PropertyType.NAME), vf.createValue("propName", PropertyType.NAME)};
        mvRestrictions.put(REP_ITEM_NAMES, restrValues);

        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, restrictions, mvRestrictions));

        acMgr.setPolicy(acl.getPath(), acl);
        AccessControlEntry[] entries = ((ACL) acMgr.getPolicies(testPath)[0]).getAccessControlEntries();
        assertEquals(2, entries.length);
        ACE newEntry = (ACE) entries[1];
        assertEquals(1, newEntry.getRestrictions().size());
        assertArrayEquals(restrValues, newEntry.getRestrictions(REP_ITEM_NAMES));
    }

    @Test
    public void testSetPrincipalPolicyRemovesEntries() throws Exception {
        setupPolicy(testPath);
        root.commit();

        ACL acl = (ACL) acMgr.getPolicies(testPrincipal)[0];
        acl.getEntries().clear();
        acMgr.setPolicy(acl.getPath(), acl);

        assertEquals(0, ((ACL) acMgr.getPolicies(testPath)[0]).getAccessControlEntries().length);
    }

    @Test
    public void testSetPrincipalPolicyRemovedACL() throws Exception {
        setupPolicy(testPath);
        root.commit();

        AccessControlPolicy nodeBased = acMgr.getPolicies(testPath)[0];

        ACL acl = (ACL) acMgr.getPolicies(testPrincipal)[0];
        acl.getEntries().clear();

        // remove policy at test-path before writing back the principal-based policy
        acMgr.removePolicy(testPath, nodeBased);

        // now write it back
        acMgr.setPolicy(acl.getPath(), acl);

        // ... which must not have an effect and the policy must not be re-added.
        assertEquals(0, acMgr.getPolicies(testPath).length);
    }

    //--------------------------------------------< removePrincipalPolicy() >---

    @Test
    public void testRemovePrincipalPolicy() throws Exception {
        JackrabbitAccessControlPolicy[] applicable = acMgr.getApplicablePolicies(testPrincipal);
        assertNotNull(applicable);
        assertEquals(1, applicable.length);
        assertTrue(applicable[0] instanceof ACL);

        ACL acl = (ACL) applicable[0];
        Value pathValue = getValueFactory().createValue(testPath, PropertyType.PATH);
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, Collections.singletonMap(REP_NODE_PATH, pathValue)));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        acMgr.removePolicy(acl.getPath(), acl);
        root.commit();

        assertEquals(0, acMgr.getPolicies(testPrincipal).length);
        assertEquals(0, acMgr.getPolicies(testPath).length);
    }

    @Test
    public void testRemovePrincipalPolicy2() throws Exception {
        setupPolicy(testPath);
        root.commit();

        AccessControlPolicy[] policies = acMgr.getPolicies(testPrincipal);
        assertNotNull(policies);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof ACL);

        ACL acl = (ACL) policies[0];
        acMgr.removePolicy(acl.getPath(), acl);

        policies = acMgr.getPolicies(testPath);
        assertEquals(0, policies.length);

        policies = acMgr.getPolicies(testPrincipal);
        assertEquals(0, policies.length);
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePrincipalPolicyRemovedACL() throws Exception {
        setupPolicy(testPath);
        root.commit();

        AccessControlPolicy nodeBased = acMgr.getPolicies(testPath)[0];

        ACL acl = (ACL) acMgr.getPolicies(testPrincipal)[0];

        // remove policy at test-path before writing back the principal-based policy
        acMgr.removePolicy(testPath, nodeBased);

        // now try to write it back, which is expected to throw AccessControlException
        acMgr.removePolicy(acl.getPath(), acl);
    }
}

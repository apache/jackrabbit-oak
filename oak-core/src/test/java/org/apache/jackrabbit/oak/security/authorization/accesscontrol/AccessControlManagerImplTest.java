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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.authorization.PrivilegeCollection;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlList;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ImmutableACL;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.NamespaceRegistry;
import javax.jcr.PathNotFoundException;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.Collections.singletonMap;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ALL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_LOCK_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_VERSION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for the default {@code AccessControlManager} implementation.
 */
public class AccessControlManagerImplTest extends AbstractAccessControlTest implements AccessControlConstants {

    private static final String TEST_LOCAL_PREFIX = "test";
    private static final String TEST_PREFIX = "jr";
    private static final String TEST_URI = "http://jackrabbit.apache.org";

    private final String testName = TEST_PREFIX + ":testRoot";
    private final String testPath = '/' + testName;

    private Principal testPrincipal;
    private Privilege[] testPrivileges;

    private AccessControlManagerImpl acMgr;
    private NamePathMapper npMapper;
    private ValueFactory valueFactory;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        NamespaceRegistry nsRegistry = new ReadWriteNamespaceRegistry(root) {
            @Override
            protected Root getWriteRoot() {
                return root;
            }
        };
        nsRegistry.registerNamespace(TEST_PREFIX, TEST_URI);
        
        acMgr = new AccessControlManagerImpl(root, npMapper, getSecurityProvider());

        TreeUtil.addChild(root.getTree(PathUtils.ROOT_PATH), testName, JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        valueFactory = getValueFactory(root);

        testPrivileges = privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES, JCR_READ);
        testPrincipal = getTestUser().getPrincipal();
    }

    @After
    public void after() throws Exception {
        try {
            root.refresh();
            root.getTree(testPath).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @NotNull
    @Override
    protected NamePathMapper getNamePathMapper() {
        if (npMapper == null) {
            NameMapper nameMapper = new GlobalNameMapper(root);
            npMapper = new NamePathMapperImpl(nameMapper);
        }
        return npMapper;
    }

    @NotNull
    private NamePathMapper getNamePathMapperWithLocalRemapping() {
        NameMapper remapped = new LocalNameMapper(root, singletonMap(TEST_LOCAL_PREFIX, TEST_URI));
        return new NamePathMapperImpl(remapped);
    }

    @NotNull
    private AccessControlManagerImpl createAccessControlManager(@NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        return new AccessControlManagerImpl(root, namePathMapper, getSecurityProvider());
    }
    
    @NotNull
    private static Set<Principal> getEveryonePrincipalSet() {
        return Set.of(EveryonePrincipal.getInstance());
    }

    @NotNull
    private ACL createPolicy(@Nullable String path) throws RepositoryException {
        return createACL(path, Collections.emptyList(), getNamePathMapper(), getRestrictionProvider());
    }

    @NotNull
    private ACL setupPolicy(@Nullable String path, @Nullable Privilege... privileges) throws RepositoryException {
        Privilege[] privs = (privileges == null || privileges.length == 0) ? testPrivileges : privileges;
        return TestUtility.setupPolicy(acMgr, path, testPrincipal, privs, true, TestUtility.getGlobRestriction("*", valueFactory), null);
    }

    @NotNull
    private Map<String, Value[]> getMvRestriction(@NotNull String name, @NotNull String... values) throws ValueFormatException {
        List<Value> list = new ArrayList<>();
        for (String v : values) {
            list.add(valueFactory.createValue(v, PropertyType.NAME));
        }
        return ImmutableMap.of(name, list.toArray(new Value[0]));
    }

    @NotNull
    private List<String> getInvalidPaths() {
        List<String> invalid = new ArrayList<>();
        invalid.add("");
        invalid.add("../../jcr:testRoot");
        invalid.add("jcr:testRoot");
        invalid.add("jcr:test/Root");
        invalid.add("./jcr:testRoot");
        return invalid;
    }

    @NotNull
    private List<String> getAcContentPaths() throws RepositoryException {
        ACL policy = TestUtility.getApplicablePolicy(acMgr, testPath);
        policy.addEntry(testPrincipal, testPrivileges, true, TestUtility.getGlobRestriction("*", valueFactory));
        acMgr.setPolicy(testPath, policy);

        String aclPath = testPath + '/' + REP_POLICY;
        Tree acl = root.getTree(aclPath);
        assertTrue(acl.exists());
        Iterator<Tree> aces = acl.getChildren().iterator();
        assertTrue(aces.hasNext());
        Tree ace = aces.next();
        assertNotNull(ace);

        List<String> acContentPath = new ArrayList<>();
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
        List<String> testPaths = new ArrayList<>();
        testPaths.add(null);
        testPaths.add("/");
        testPaths.add("/jcr:system");
        testPaths.add(testPath);

        assertSupportedPrivileges(acMgr, testPaths);
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
    public void testGetSupportedPrivilegesPropertyPath() throws Exception {
        acMgr.getSupportedPrivileges("/jcr:primaryType");
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetSupportedPrivilegesNonExistingPath() throws Exception {
        acMgr.getSupportedPrivileges("/non/existing/node");
    }

    @Test
    public void testGetSupportedPrivilegesIncludingPathConversion() throws Exception {
        List<String> testPaths = new ArrayList<>();
        testPaths.add('/' + TEST_LOCAL_PREFIX + ":testRoot");
        testPaths.add("/{" + TEST_URI + "}testRoot");

        AccessControlManager acMgr = createAccessControlManager(root, getNamePathMapperWithLocalRemapping());
        assertSupportedPrivileges(acMgr, testPaths);
    }

    @Test
    public void testGetSupportedForPrivilegesAcContent() throws Exception {
        assertSupportedPrivileges(acMgr, getAcContentPaths());
    }

    private void assertSupportedPrivileges(@NotNull AccessControlManager acMgr, @NotNull Iterable<String> paths) throws Exception {
        List<Privilege> allPrivileges = Arrays.asList(getPrivilegeManager(root).getRegisteredPrivileges());
        for (String acPath : paths) {
            Privilege[] supported = acMgr.getSupportedPrivileges(acPath);

            assertNotNull(supported);
            assertEquals(allPrivileges.size(), supported.length);
            assertTrue(allPrivileges.containsAll(Arrays.asList(supported)));
        }
    }

    //--------------------------------------------------< privilegeFromName >---

    @Test
    public void testPrivilegeFromName() throws Exception {
        for (Privilege privilege : getPrivilegeManager(root).getRegisteredPrivileges()) {
            Privilege p = acMgr.privilegeFromName(privilege.getName());
            assertEquals(privilege, p);
        }
    }

    @Test
    public void testPrivilegeFromExpandedName() throws Exception {
        Privilege readPriv = getPrivilegeManager(root).getPrivilege(JCR_READ);
        assertEquals(readPriv, acMgr.privilegeFromName(Privilege.JCR_READ));
    }

    @Test
    public void testPrivilegeFromInvalidName() {
        List<String> invalid = new ArrayList<>();
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
        List<String> invalid = new ArrayList<>();
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
            acMgr.hasPrivileges(nonExistingPath, Set.of(), privs);
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
                acMgr.hasPrivileges(path, Set.of(EveryonePrincipal.getInstance()), privs);
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
            assertFalse(acMgr.hasPrivileges(path, Set.of(EveryonePrincipal.getInstance()), privs));
        }
    }

    @Test
    public void testHasRepoPrivileges() throws Exception {
        assertTrue(acMgr.hasPrivileges(null, privilegesFromNames(PrivilegeConstants.JCR_ALL)));
        assertTrue(acMgr.hasPrivileges(null, getPrincipals(adminSession), privilegesFromNames(PrivilegeConstants.JCR_ALL)));
    }

    @Test
    public void testHasRepoPrivilegesEmptyPrincipalSet() throws Exception {
        assertFalse(acMgr.hasPrivileges(null, Collections.emptySet(), privilegesFromNames(PrivilegeConstants.JCR_ALL)));
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
            acMgr.getPrivileges(nonExistingPath, Collections.emptySet());
            fail("AccessControlManager#getPrivileges  for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
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
                acMgr.getPrivileges(path, Collections.emptySet());
                fail("AccessControlManager#getPrivileges  for node that doesn't exist should fail.");
            } catch (RepositoryException e) {
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
        Set<Principal> testPrincipals = Set.of(testPrincipal);
        Privilege[] expected = new Privilege[0];
        for (String path : getAcContentPaths()) {
            assertArrayEquals(expected, acMgr.getPrivileges(path, testPrincipals));
        }
    }

    @Test
    public void testGetPrivilegesForNoPrincipalsAccessControlledNodePath() throws Exception {
        Privilege[] expected = new Privilege[0];
        for (String path : getAcContentPaths()) {
            assertArrayEquals(expected, acMgr.getPrivileges(path, Collections.emptySet()));
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
        assertArrayEquals(new Privilege[0], acMgr.getPrivileges(null, Collections.emptySet()));
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
        Tree node = root.getTree(testPath);
        node.setProperty(JcrConstants.JCR_MIXINTYPES, ImmutableList.of(MIX_REP_ACCESS_CONTROLLABLE), Type.NAMES);

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
        Tree testTree = root.getTree(testPath);
        TreeUtil.addChild(testTree, REP_POLICY, JcrConstants.NT_UNSTRUCTURED);

        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(testPath);
        assertNotNull(itr);
        assertFalse(itr.hasNext());
    }

    @Test
    public void testGetApplicablePoliciesForAccessControlled() throws Exception {
        AccessControlPolicy policy = TestUtility.getApplicablePolicy(acMgr, testPath);
        acMgr.setPolicy(testPath, policy);

        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(testPath);
        assertNotNull(itr);
        assertFalse(itr.hasNext());
    }

    @Test
    public void testGetApplicablePoliciesInvalidPath() {
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
    public void testGetApplicablePoliciesNullPath() throws RepositoryException{
        assertArrayEquals( new AccessControlPolicy[0], acMgr.getPolicies((String) null));
    }

    @Test(expected = PathNotFoundException.class)
    public void testApplicablePoliciesForPropertyPath() throws Exception {
        String propertyPath = "/jcr:primaryType";
        acMgr.getApplicablePolicies(propertyPath);
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetApplicablePoliciesNonExistingNodePath() throws Exception {
        String nonExistingPath = "/not/existing";
        acMgr.getApplicablePolicies(nonExistingPath);
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
        AccessControlPolicy policy = TestUtility.getApplicablePolicy(acMgr, testPath);
        acMgr.setPolicy(testPath, policy);

        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertPolicies(policies, 1);

        assertTrue(policies[0] instanceof ACL);
        ACL acl = (ACL) policies[0];
        assertTrue(acl.isEmpty());
        assertEquals(testPath, acl.getOakPath());
    }

    @Test
    public void testGetPoliciesNodeNotAccessControlled() throws Exception {
        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertPolicies(policies, 0);
    }

    @Test
    public void testGetPoliciesAfterSet() throws Exception {
        setupPolicy(testPath);

        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertPolicies(policies, 1);

        assertTrue(policies[0] instanceof ACL);
        ACL acl = (ACL) policies[0];
        assertFalse(acl.isEmpty());
    }

    @Test
    public void testGetPoliciesAfterRemove() throws Exception {
        setupPolicy(testPath);

        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertPolicies(policies, 1);

        acMgr.removePolicy(testPath, policies[0]);

        policies = acMgr.getPolicies(testPath);
        assertPolicies(policies, 0);
        assertTrue(acMgr.getApplicablePolicies(testPath).hasNext());
    }

    @Test
    public void testGetPolicyWithInvalidPrincipal() throws Exception {
        ACL policy = TestUtility.getApplicablePolicy(acMgr, testPath);
        policy.addEntry(testPrincipal, testPrivileges, true, TestUtility.getGlobRestriction("*", valueFactory));
        acMgr.setPolicy(testPath, policy);

        Tree aclNode = root.getTree(testPath + '/' + REP_POLICY);
        Tree aceNode = TreeUtil.addChild(aclNode, "testACE", NT_REP_DENY_ACE);
        aceNode.setProperty(REP_PRINCIPAL_NAME, "invalidPrincipal");
        aceNode.setProperty(REP_PRIVILEGES, ImmutableList.of(PrivilegeConstants.JCR_READ), Type.NAMES);

        // reading policies with unknown principal name should not fail.
        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertPolicies(policies, 1);

        ACL acl = (ACL) policies[0];
        List<String> principalNames = new ArrayList<>();
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
        assertPolicies(policies, 0);

        acMgr.setPolicy(null, acMgr.getApplicablePolicies(path).nextAccessControlPolicy());
        assertFalse(acMgr.getApplicablePolicies(path).hasNext());

        policies = acMgr.getPolicies(path);
        assertPolicies(policies, 1);

        assertTrue(policies[0] instanceof ACL);
        ACL acl = (ACL) policies[0];
        assertTrue(acl.isEmpty());
        assertNull(acl.getPath());
        assertNull(acl.getOakPath());
        assertFalse(acMgr.getApplicablePolicies(path).hasNext());

        acMgr.removePolicy(path, acl);
        assertPolicies(acMgr.getPolicies(path), 0);
        assertTrue(acMgr.getApplicablePolicies(path).hasNext());
    }

    @Test
    public void testGetPoliciesInvalidPath() {
        for (String invalid : getInvalidPaths()) {
            try {
                acMgr.getPolicies(invalid);
                fail("Getting policies for an invalid path should fail");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetPoliciesPropertyPath() throws Exception {
        String propertyPath = "/jcr:primaryType";
        acMgr.getPolicies(propertyPath);
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetPoliciesNonExistingNodePath() throws Exception {
        String nonExistingPath = "/not/existing";
        acMgr.getPolicies(nonExistingPath);
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

    @Test
    public void testGetPoliciesLimitsPrincipalLookup() throws Exception {
        ACL policy = TestUtility.getApplicablePolicy(acMgr, testPath);
        policy.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_READ));
        policy.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_REMOVE_CHILD_NODES), true, ImmutableMap.of(REP_GLOB, getValueFactory(root).createValue("")));
        policy.addAccessControlEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_REMOVE_NODE));
        acMgr.setPolicy(policy.getPath(), policy);

        // read policy again
        policy = (ACL) acMgr.getPolicies(policy.getPath())[0];
        assertEquals(3, policy.size());
        AccessControlEntry[] entries = policy.getAccessControlEntries();
        // reading policies attempts to lookup principals (see OAK-xxx)
        assertTrue(entries[0].getPrincipal() instanceof GroupPrincipal);
        // reading policies must only lookup a given principal once
        assertSame(entries[1].getPrincipal(), entries[2].getPrincipal());
    }

    @Test
    public void testGetPoliciesWithNonAceChild() throws Exception {
        ACL policy = setupPolicy(testPath, testPrivileges);
        assertEquals(1, policy.size());

        Tree policyTree = root.getTree(testPath).getChild(REP_POLICY);
        TreeUtil.addChild(policyTree, "child", NT_OAK_UNSTRUCTURED);

        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertPolicies(policies, 1);
        assertEquals(1, ((ACL) policies[0]).size());
    }

    @Test
    public void testGetPoliciesWithInvalidatedPrivileges() throws Exception {
        ACL policy = setupPolicy(testPath, privilegesFromNames(JCR_LOCK_MANAGEMENT));
        assertEquals(1, policy.size());

        Tree privTree = root.getTree(PrivilegeConstants.PRIVILEGES_PATH).getChild(JCR_LOCK_MANAGEMENT);
        privTree.remove();

        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertPolicies(policies, 1);
        ACL acl = (ACL) policies[0];
        assertEquals(1, acl.size());
        Privilege[] privs = acl.getAccessControlEntries()[0].getPrivileges();
        assertArrayEquals(new Privilege[0], privs);
    }

    //---------------------------------------< getEffectivePolicies(String) >---
    @Test
    public void testGetEffectivePoliciesNoPoliciesSet() throws Exception {
        assertPolicies(acMgr.getEffectivePolicies(testPath), 0);
    }

    @Test
    public void testGetEffectivePoliciesEmptyACL() throws Exception {
        // set empty policy -> no effective ACEs
        acMgr.setPolicy(testPath, acMgr.getApplicablePolicies(testPath).nextAccessControlPolicy());
        root.commit();

        // resulting effective policies should be empty array
        assertPolicies(acMgr.getEffectivePolicies(testPath), 0);
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        setupPolicy(testPath);
        root.commit();

        assertPolicies(acMgr.getEffectivePolicies(testPath), 1);
    }

    @Test
    public void testGetEffectivePoliciesRootPath() throws Exception {
        assertPolicies(acMgr.getEffectivePolicies(PathUtils.ROOT_PATH), 0);
    }

    @Test
    public void testGetEffectivePoliciesNullPath() throws Exception {
        assertPolicies(acMgr.getEffectivePolicies((String) null), 0);
        ACL acl = null;
        try {
            acl = setupPolicy(null, privilegesFromNames(PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT));
            root.commit();
            assertPolicies(acMgr.getEffectivePolicies((String) null), 1);
        } finally {
            if (acl != null) {
                acMgr.removePolicy(null, acl);
                root.commit();
            }
        }
    }

    @Test
    public void testGetEffectivePoliciesOnChild() throws Exception {
        setupPolicy(testPath);
        Tree child = TreeUtil.addChild(root.getTree(testPath), "child", JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        String childPath = child.getPath();
        assertPolicies(acMgr.getEffectivePolicies(childPath), 1);
    }

    @Test
    public void testGetEffectivePoliciesOnNewChild() throws Exception {
        setupPolicy(testPath);
        root.commit();

        Tree child = TreeUtil.addChild(root.getTree(testPath), "child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getPath();

        assertPolicies(acMgr.getEffectivePolicies(childPath), 1);
    }

    @Test
    public void testGetEffectivePoliciesOnChild2() throws Exception {
        Tree child = TreeUtil.addChild(root.getTree(testPath), "child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getPath();
        setupPolicy(childPath);
        setupPolicy(testPath);
        root.commit();

        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(childPath);
        assertPolicies(policies, 2);

        for (AccessControlPolicy policy : policies) {
            assertTrue(policy instanceof ImmutableACL);
        }
    }

    @Test
    public void testGetEffectivePoliciesMatchingRestriction() throws Exception {
        ACL policy = TestUtility.setupPolicy(acMgr, testPath, testPrincipal, privilegesFromNames(JCR_READ), true, null,
                getMvRestriction(REP_ITEM_NAMES, "child"));
        policy.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, null,
                getMvRestriction(REP_ITEM_NAMES, "notMatching"));
        acMgr.setPolicy(policy.getPath(), policy);

        Tree child = TreeUtil.addChild(root.getTree(testPath), "child", JcrConstants.NT_UNSTRUCTURED);
        Tree grandChild = TreeUtil.addChild(child, "child", JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        assertPolicies(acMgr.getEffectivePolicies(testPath), 1);
        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(child.getPath());
        assertPolicies(policies, 1);

        // only the matching ACE must be included in the effective policy
        AccessControlPolicy effective = policies[0];
        assertTrue(effective instanceof ImmutableACL);
        assertEquals(2, ((ImmutableACL) effective).getAccessControlEntries().length);

        assertPolicies(acMgr.getEffectivePolicies(grandChild.getPath()), 1);
    }

    /**
     * An ACE restriction matching only a property (like e.g. jcr:primaryType) would not be included in the effective
     * policies, becauce AccessControlManager.getEffectivePolicies(String) requires the path to point to an existing
     * JCR node.
     */
    @Test
    public void testGetEffectivePoliciesMatchingPropertyRestriction() throws Exception {
        TestUtility.setupPolicy(acMgr, testPath, testPrincipal, privilegesFromNames(JCR_READ), false, null,
                getMvRestriction(REP_ITEM_NAMES, JcrConstants.JCR_PRIMARYTYPE));

        Tree child = TreeUtil.addChild(root.getTree(testPath), "child", JcrConstants.NT_UNSTRUCTURED);
        Tree grandChild = TreeUtil.addChild(child, "child", JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        assertPolicies(acMgr.getEffectivePolicies(testPath), 1);
        assertPolicies(acMgr.getEffectivePolicies(child.getPath()), 1);
        assertPolicies(acMgr.getEffectivePolicies(grandChild.getPath()), 1);
    }

    @Test
    public void testGetEffectivePoliciesNotMatchingRestriction() throws Exception {
        TestUtility.setupPolicy(acMgr, testPath, testPrincipal, privilegesFromNames(JCR_READ), true, null,
                getMvRestriction(REP_NT_NAMES, NT_OAK_UNSTRUCTURED));

        Tree child = TreeUtil.addChild(root.getTree(testPath), "child", NT_OAK_UNSTRUCTURED);
        Tree grandChild = TreeUtil.addChild(child, "grandChild", JcrConstants.NT_UNSTRUCTURED);
        Tree other = TreeUtil.addChild(root.getTree(testPath), "other", JcrConstants.NT_UNSTRUCTURED);

        root.commit();

        assertPolicies(acMgr.getEffectivePolicies(testPath), 1);
        assertPolicies(acMgr.getEffectivePolicies(grandChild.getPath()), 1);
        assertPolicies(acMgr.getEffectivePolicies(other.getPath()), 1);
    }

    @Test
    public void testGetEffectivePoliciesNewPolicy() throws Exception {
        assertPolicies(acMgr.getEffectivePolicies(testPath), 0);

        setupPolicy(testPath);
        assertPolicies(acMgr.getEffectivePolicies(testPath), 0);

        Tree child = TreeUtil.addChild(root.getTree(testPath),"child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getPath();

        assertPolicies(acMgr.getEffectivePolicies(childPath), 0);

        setupPolicy(childPath);
        assertPolicies(acMgr.getEffectivePolicies(childPath), 0);
    }

    @Test
    public void testGetEffectiveModifiedPolicy() throws Exception {
        ACL acl = setupPolicy(testPath);
        AccessControlEntry[] aces = acl.getAccessControlEntries();
        root.commit();

        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT));
        acMgr.setPolicy(testPath, acl);

        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(testPath);
        assertPolicies(acMgr.getEffectivePolicies(testPath), 1);

        assertTrue(policies[0] instanceof AccessControlList);
        AccessControlEntry[] effectiveAces = ((AccessControlList) policies[0]).getAccessControlEntries();
        assertArrayEquals(aces, effectiveAces);
        assertFalse(Arrays.equals(effectiveAces, acl.getAccessControlEntries()));
    }

    @Test
    public void testGetEffectivePoliciesInvalidPath() {
        for (String invalid : getInvalidPaths()) {
            try {
                acMgr.getEffectivePolicies(invalid);
                fail("Getting policies for an invalid path should fail");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetEffectivePoliciesForPropertyPath() throws Exception {
        String propertyPath = "/jcr:primaryType";
        acMgr.getEffectivePolicies(propertyPath);
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetEffectivePoliciesNonExistingNodePath() throws Exception {
        String nonExistingPath = "/not/existing";
        acMgr.getEffectivePolicies(nonExistingPath);
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

    //-----------------------------< setPolicy(String, AccessControlPolicy) >---
    @Test
    public void testSetPolicy() throws Exception {
        ACL acl = TestUtility.getApplicablePolicy(acMgr, testPath);
        acl.addAccessControlEntry(testPrincipal, testPrivileges);
        acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, TestUtility.getGlobRestriction("*/something", valueFactory));

        acMgr.setPolicy(testPath, acl);
        root.commit();

        Root root2 = adminSession.getLatestRoot();
        AccessControlPolicy[] policies = getAccessControlManager(root2).getPolicies(testPath);
        assertPolicies(policies, 1);
        assertArrayEquals(acl.getAccessControlEntries(), ((ACL) policies[0]).getAccessControlEntries());
    }

    @Test
    public void testSetRepoPolicy() throws Exception {
        ACL acl = TestUtility.getApplicablePolicy(acMgr, null);
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT));

        acMgr.setPolicy(null, acl);
        root.commit();

        Root root2 = adminSession.getLatestRoot();
        AccessControlPolicy[] policies = getAccessControlManager(root2).getPolicies((String) null);
        assertPolicies(policies, 1);
        assertArrayEquals(acl.getAccessControlEntries(), ((ACL) policies[0]).getAccessControlEntries());
    }

    @Test
    public void testSetPolicyWritesAcContent() throws Exception {
        ACL acl = TestUtility.getApplicablePolicy(acMgr, testPath);
        acl.addAccessControlEntry(testPrincipal, testPrivileges);
        acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, TestUtility.getGlobRestriction("*/something", valueFactory));

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
                CollectionUtils.toSet(testPrivileges),
                CollectionUtils.toSet(privilegesFromNames(TreeUtil.getStrings(ace, REP_PRIVILEGES))));
        assertFalse(ace.hasChild(REP_RESTRICTIONS));

        Tree ace2 = children.next();
        assertEquals(NT_REP_DENY_ACE, TreeUtil.getPrimaryTypeName(ace2));
        assertEquals(EveryonePrincipal.NAME, requireNonNull(ace2.getProperty(REP_PRINCIPAL_NAME)).getValue(Type.STRING));
        Privilege[] privs = privilegesFromNames(TreeUtil.getNames(ace2, REP_PRIVILEGES));
        assertEquals(CollectionUtils.toSet(testPrivileges), CollectionUtils.toSet(privs));
        assertTrue(ace2.hasChild(REP_RESTRICTIONS));
        Tree restr = ace2.getChild(REP_RESTRICTIONS);
        assertEquals("*/something", requireNonNull(restr.getProperty(REP_GLOB)).getValue(Type.STRING));
    }

    @Test
    public void testModifyExistingPolicy() throws Exception {
        ACL acl = TestUtility.getApplicablePolicy(acMgr, testPath);
        assertTrue(acl.addAccessControlEntry(testPrincipal, testPrivileges));
        AccessControlEntry allowTest = acl.getAccessControlEntries()[0];

        acMgr.setPolicy(testPath, acl);
        root.commit();

        acl = (ACL) acMgr.getPolicies(testPath)[0];
        assertTrue(acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, TestUtility.getGlobRestriction("*/something", valueFactory)));

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
        assertTrue(acl.addEntry(testPrincipal, readAc, false, Collections.emptyMap()));
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
    public void testSetPolicyInvalidPath() {
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

    @Test(expected = PathNotFoundException.class)
    public void testSetPolicyPropertyPath() throws Exception {
        String path = "/jcr:primaryType";
        AccessControlPolicy acl = createPolicy(path);
        acMgr.setPolicy(path, acl);
    }

    @Test(expected = PathNotFoundException.class)
    public void testSetPolicyNonExistingNodePath() throws Exception {
        String path = "/non/existing";
        AccessControlPolicy acl = createPolicy(path);
        acMgr.setPolicy(path, acl);
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

    @Test(expected = AccessControlException.class)
    public void testSetPolicyAtDifferentPath() throws Exception {
        ACL acl = TestUtility.getApplicablePolicy(acMgr, testPath);
        acMgr.setPolicy("/", acl);
    }

    @Test
    public void testSetPolicyModifiesRoot() throws Exception {
        ACL acl = TestUtility.getApplicablePolicy(acMgr, testPath);
        assertTrue(acl.addAccessControlEntry(testPrincipal, testPrivileges));
        assertFalse(root.hasPendingChanges());

        acMgr.setPolicy(testPath, acl);
        assertTrue(root.hasPendingChanges());

        root.commit();

        acl = (ACL) acMgr.getPolicies(testPath)[0];
        assertEquals(1, acl.getAccessControlEntries().length);

        acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, TestUtility.getGlobRestriction("*/something", valueFactory));
        acMgr.setPolicy(testPath, acl);
        assertTrue(root.hasPendingChanges());

        root.commit();

        acl = (ACL) acMgr.getPolicies(testPath)[0];
        assertEquals(2, acl.getAccessControlEntries().length);
    }

    @Test
    public void testSetPolicyWithRestrictions() throws Exception {
        ACL acl = TestUtility.getApplicablePolicy(acMgr, testPath);
        acl.addEntry(testPrincipal, testPrivileges, true, TestUtility.getGlobRestriction("/a/b", valueFactory));
        acl.addEntry(testPrincipal, testPrivileges, true, null, Collections.singletonMap(REP_GLOBS, new Value[] {valueFactory.createValue("/c/d")}));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        ACL l = (ACL) acMgr.getPolicies(testPath)[0];
        assertEquals(2, l.getAccessControlEntries().length);
    }

    @Test
    public void testSetPolicyCreatesIndexedAceNodeNames() throws Exception {
        ACL acl = TestUtility.getApplicablePolicy(acMgr, testPath);
        assertTrue(acl.addAccessControlEntry(testPrincipal, testPrivileges));
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, TestUtility.getGlobRestriction("/*/a", valueFactory)));
        assertTrue(acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false));
        assertTrue(acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, TestUtility.getGlobRestriction("/*/a", valueFactory)));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        acl = (ACL) acMgr.getPolicies(testPath)[0];
        assertEquals(4, acl.getAccessControlEntries().length);

        Iterable<Tree> aceTrees = root.getTree(testPath).getChild(REP_POLICY).getChildren();
        String[] aceNodeNames = Iterables.toArray(Iterables.transform(aceTrees, Tree::getName), String.class);
        assertArrayEquals(new String[]{"allow", "allow1", "deny2", "deny3"}, aceNodeNames);
    }

    @Test
    public void testSetPolicyWithExistingMixins() throws Exception {
        TreeUtil.addMixin(root.getTree(testPath), JcrConstants.MIX_LOCKABLE, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);

        ACL acl = TestUtility.getApplicablePolicy(acMgr, testPath);
        assertTrue(acl.addAccessControlEntry(testPrincipal, testPrivileges));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        assertEquals(Set.of(JcrConstants.MIX_LOCKABLE, MIX_REP_ACCESS_CONTROLLABLE),
                ImmutableSet.copyOf(TreeUtil.getNames(root.getTree(testPath), JcrConstants.JCR_MIXINTYPES)));
    }

    //--------------------------< removePolicy(String, AccessControlPolicy) >---
    @Test
    public void testRemovePolicy() throws Exception {
        ACL acl = setupPolicy(testPath);

        acMgr.removePolicy(testPath, acl);

        assertPolicies(acMgr.getPolicies(testPath), 0);
        assertTrue(acMgr.getApplicablePolicies(testPath).hasNext());
    }

    @Test
    public void testRemoveRepoPolicy() throws Exception {
        ACL acl = setupPolicy(null);

        acMgr.removePolicy(null, acl);

        assertPolicies(acMgr.getPolicies((String) null), 0);
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
    public void testRemovePolicyInvalidPath() {
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

    @Test(expected = PathNotFoundException.class)
    public void testRemovePolicyPropertyPath() throws Exception {
        String path = "/jcr:primaryType";
        AccessControlPolicy acl = createPolicy(path);
        acMgr.removePolicy(path, acl);
    }

    @Test(expected = PathNotFoundException.class)
    public void testRemovePolicyNonExistingNodePath() throws Exception {
        String path = "/non/existing";
        AccessControlPolicy acl = createPolicy(path);
        acMgr.removePolicy(path, acl);
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
        ACL acl = TestUtility.getApplicablePolicy(acMgr, PathUtils.ROOT_PATH);
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
    @Test(expected = AccessControlException.class)
    public void testGetApplicablePoliciesNullPrincipal() throws Exception {
        acMgr.getApplicablePolicies((Principal) null);
    }

    @Test
    public void testGetApplicablePoliciesByPrincipal() throws Exception {
        List<Principal> principals = ImmutableList.of(testPrincipal, EveryonePrincipal.getInstance());
        for (Principal principal : principals) {
            AccessControlPolicy[] applicable = acMgr.getApplicablePolicies(principal);
            assertPolicies(applicable, 1);
            assertTrue(applicable[0] instanceof ACL);
        }
    }

    @Test
    public void testGetApplicablePoliciesByPrincipal2() throws Exception {
        setupPolicy(testPath);

        // changes not yet persisted -> no existing policies found for user
        AccessControlPolicy[] applicable = acMgr.getApplicablePolicies(testPrincipal);
        assertPolicies(applicable, 1);
        assertTrue(applicable[0] instanceof ACL);

        // after persisting changes -> no applicable policies present any more.
        root.commit();
        assertPolicies(acMgr.getApplicablePolicies(testPrincipal), 0);
    }

    //---------------------------------------------< getPolicies(Principal) >---
    @Test(expected = AccessControlException.class)
    public void testGetPoliciesNullPrincipal() throws Exception {
        acMgr.getPolicies((Principal) null);
    }

    @Test
    public void testGetPoliciesByPrincipal() throws Exception {
        List<Principal> principals = ImmutableList.of(testPrincipal, EveryonePrincipal.getInstance());
        for (Principal principal : principals) {
            assertPolicies(acMgr.getPolicies(principal), 0);
        }
    }

    @Test
    public void testGetPoliciesByPrincipal2() throws Exception {
        setupPolicy(testPath);

        // changes not yet persisted -> no existing policies found for user
        assertPolicies(acMgr.getPolicies(testPrincipal), 0);

        // after persisting changes -> policies must be found
        root.commit();
        assertPolicies(acMgr.getPolicies(testPrincipal), 1);
    }

    @Test
    public void testGetPoliciesByPrincipalRemapped() throws Exception {
        setupPolicy(testPath);
        root.commit();

        NamePathMapper mapper = getNamePathMapperWithLocalRemapping();
        AccessControlPolicy[] policies = createAccessControlManager(root, mapper).getPolicies(testPrincipal);
        assertNotNull(policies);
        assertEquals(1, policies.length);

        List<ACE> entries = ((ACL) policies[0]).getEntries();
        Value rest = entries.get(0).getRestriction(REP_NODE_PATH);
        assertNotNull(rest);
        assertEquals(mapper.getJcrPath(testPath), rest.getString());
    }

    @Test
    public void testGetPoliciesByPrincipalRepositoryLevel() throws Exception {
        setupPolicy(null, privilegesFromNames(PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT));

        // changes not yet persisted -> no existing policies found for user
        AccessControlPolicy[] policies = acMgr.getPolicies(testPrincipal);
        assertNotNull(policies);
        assertEquals(0, policies.length);

        // after persisting changes -> policies must be found
        root.commit();
        policies = acMgr.getPolicies(testPrincipal);
        assertNotNull(policies);
        assertEquals(1, policies.length);
        JackrabbitAccessControlList acl = (JackrabbitAccessControlList) policies[0];
        AccessControlEntry[] entries = acl.getAccessControlEntries();
        assertEquals(1, entries.length);
        JackrabbitAccessControlEntry entry = (JackrabbitAccessControlEntry) entries[0];
        assertTrue(entry.getRestriction(REP_NODE_PATH).getString().isEmpty());
    }

    //-------------------------------< getEffectivePolicies(Set<Principal>) >---

    @Test(expected = AccessControlException.class)
    public void testGetEffectivePoliciesNullPrincipal() throws Exception {
        acMgr.getEffectivePolicies((Set) null);
    }

    @Test(expected = AccessControlException.class)
    public void testGetEffectivePoliciesSetContainingNullPrincipal() throws Exception {
        acMgr.getEffectivePolicies(new HashSet<>(Arrays.asList(EveryonePrincipal.getInstance(), null, testPrincipal)));
    }

    @Test
    public void testGetEffectivePoliciesByPrincipal() throws Exception {
        // no ACLs containing entries for the specified principals
        // -> no effective policies expected
        Set<Set<Principal>> principalSets = new HashSet<>();
        principalSets.add(Collections.singleton(testPrincipal));
        principalSets.add(Collections.singleton(EveryonePrincipal.getInstance()));
        principalSets.add(Set.of(testPrincipal, EveryonePrincipal.getInstance()));

        for (Set<Principal> principals : principalSets) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principals);
            assertPolicies(policies, 1, true);
        }

        setupPolicy(testPath);
        // changes not yet persisted -> no effecitve policies found for testprincipal
        for (Set<Principal> principals : principalSets) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principals);
            assertPolicies(policies, 1, true);
        }

        root.commit();
        // after persisting changes -> the policy must be found
        for (Set<Principal> principals : principalSets) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principals);
            if (principals.contains(testPrincipal)) {
                assertPolicies(policies, 2, true);
            } else {
                assertPolicies(policies, 1, true);
            }
        }

        Tree child = TreeUtil.addChild(root.getTree(testPath), "child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getPath();
        setupPolicy(childPath);
        // changes not yet persisted -> no effecitve policies found for testprincipal
        for (Set<Principal> principals : principalSets) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principals);
            if (principals.contains(testPrincipal)) {
                assertPolicies(policies, 2, true);
            } else {
                assertPolicies(policies, 1, true);
            }
        }

        root.commit();
        // after persisting changes -> the policy must be found
        for (Set<Principal> principals : principalSets) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principals);
            if (principals.contains(testPrincipal)) {
                assertPolicies(policies, 3, true);
            } else {
                assertPolicies(policies, 1, true);
            }
        }
    }

    @Test
    public void testNoEffectiveDuplicateEntries() throws Exception {
        Set<Principal> principalSet = Set.of(testPrincipal, EveryonePrincipal.getInstance());

        // create first policy with multiple ACEs for the test principal set.
        ACL policy = TestUtility.getApplicablePolicy(acMgr, testPath);
        policy.addEntry(testPrincipal, testPrivileges, true, TestUtility.getGlobRestriction("*", valueFactory));
        policy.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT), false);
        policy.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT), false);
        assertEquals(3, policy.getAccessControlEntries().length);
        acMgr.setPolicy(testPath, policy);
        root.commit();

        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principalSet);
        assertPolicies(policies, 2, true);

        // add another policy
        Tree child = TreeUtil.addChild(root.getTree(testPath), "child", JcrConstants.NT_UNSTRUCTURED);
        String childPath = child.getPath();
        setupPolicy(childPath);
        root.commit();

        assertPolicies(acMgr.getEffectivePolicies(principalSet), 3, true);
    }

    @Test
    public void testEffectiveSorting() throws Exception {
        Set<Principal> principalSet = Set.of(testPrincipal, EveryonePrincipal.getInstance());

        ACL nullPathPolicy = null;
        try {
            // 1. policy at 'testPath'
            ACL policy = TestUtility.getApplicablePolicy(acMgr, testPath);
            policy.addEntry(testPrincipal, testPrivileges, true, TestUtility.getGlobRestriction("*", valueFactory));
            policy.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT), false);
            policy.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT), false);
            acMgr.setPolicy(testPath, policy);

            // 2. policy at child node
            Tree child = TreeUtil.addChild(root.getTree(testPath), "child", JcrConstants.NT_UNSTRUCTURED);
            String childPath = child.getPath();
            setupPolicy(childPath);

            // 3. policy for null-path
            nullPathPolicy = TestUtility.getApplicablePolicy(acMgr, null);
            assertNotNull(nullPathPolicy);

            nullPathPolicy.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.REP_PRIVILEGE_MANAGEMENT), true);
            acMgr.setPolicy(null, nullPathPolicy);
            root.commit();

            AccessControlPolicy[] effectivePolicies = acMgr.getEffectivePolicies(principalSet);
            assertPolicies(effectivePolicies, 4, true);

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
        ACL policy = TestUtility.getApplicablePolicy(acMgr, testPath);
        policy.addEntry(testPrincipal, testPrivileges, true, TestUtility.getGlobRestriction("*", valueFactory));
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
                () -> testPrincipal.getName());

        for (Principal princ : principals) {
            AccessControlPolicy[] policies = acMgr.getEffectivePolicies(Set.of(princ));
            assertPolicies(policies, 2, true);
            assertTrue(policies[0] instanceof AccessControlList);

            AccessControlList acl = (AccessControlList) policies[0];
            assertEquals(2, acl.getAccessControlEntries().length);
            for (AccessControlEntry ace : acl.getAccessControlEntries()) {
                assertEquals(princ.getName(), ace.getPrincipal().getName());
            }
        }
    }

    @Test
    public void testEffectivePolicyIsolatedAce() throws Exception {
        Root r = spy(root);
        ContentSession cs = when(spy(adminSession).getLatestRoot()).thenReturn(r).getMock();
        when(r.getContentSession()).thenReturn(cs);

        Tree testTree = r.getTree(testPath);
        Tree ace = TreeUtil.addChild(testTree, "ace", NT_REP_GRANT_ACE);
        ace.setProperty(REP_PRINCIPAL_NAME, testPrincipal.getName());
        ace.setProperty(REP_PRIVILEGES, ImmutableList.of(JCR_READ), Type.NAMES);

        QueryEngine qe = mockQueryEngine(ace);
        when(r.getQueryEngine()).thenReturn(qe);

        AccessControlManagerImpl mgr = createAccessControlManager(r, getNamePathMapper());
        AccessControlPolicy[] policies = mgr.getEffectivePolicies(Set.of(testPrincipal));
        assertPolicies(policies, 1, true);
    }

    @Test
    public void testEffectivePolicyWrongPrimaryType() throws Exception {
        Root r = spy(root);
        ContentSession cs = when(spy(adminSession).getLatestRoot()).thenReturn(r).getMock();
        when(r.getContentSession()).thenReturn(cs);

        Tree testTree = r.getTree(testPath);
        Tree aclWithWrongType = TreeUtil.addChild(testTree, REP_POLICY, NT_OAK_UNSTRUCTURED);
        Tree ace = TreeUtil.addChild(aclWithWrongType, "ace", NT_REP_GRANT_ACE);
        ace.setProperty(REP_PRINCIPAL_NAME, testPrincipal.getName());
        ace.setProperty(REP_PRIVILEGES, ImmutableList.of(JCR_READ), Type.NAMES);

        QueryEngine qe = mockQueryEngine(ace);
        when(r.getQueryEngine()).thenReturn(qe);

        AccessControlManagerImpl mgr = createAccessControlManager(r, getNamePathMapper());
        AccessControlPolicy[] policies = mgr.getEffectivePolicies(Set.of(testPrincipal));
        assertPolicies(policies, 1, true);
    }
    
    private static QueryEngine mockQueryEngine(@NotNull Tree aceTree) throws Exception {
        ResultRow row = when(mock(ResultRow.class).getTree(null)).thenReturn(aceTree).getMock();
        Iterable rows = ImmutableList.of(row);
        Result res = mock(Result.class);
        when(res.getRows()).thenReturn(rows).getMock();
        QueryEngine qe = mock(QueryEngine.class);
        when(qe.executeQuery(anyString(), anyString(), any(Map.class), any(Map.class))).thenReturn(res);
        return qe;
    }

    @Test(expected = RepositoryException.class)
    public void testEffectivePolicyFailingQuery() throws Exception {
        Root r = spy(root);
        ContentSession cs = when(spy(adminSession).getLatestRoot()).thenReturn(r).getMock();
        when(r.getContentSession()).thenReturn(cs);

        Tree testTree = r.getTree(testPath);
        Tree ace = TreeUtil.addChild(testTree, "ace", NT_REP_GRANT_ACE);
        ace.setProperty(REP_PRINCIPAL_NAME, testPrincipal.getName());
        ace.setProperty(REP_PRIVILEGES, ImmutableList.of(JCR_READ), Type.NAMES);

        QueryEngine qe = mock(QueryEngine.class);
        when(qe.executeQuery(anyString(), anyString(), any(Map.class), any(Map.class))).thenThrow(new ParseException("test", 0));
        when(r.getQueryEngine()).thenReturn(qe);

        AccessControlManagerImpl mgr = createAccessControlManager(r, getNamePathMapper());
        mgr.getEffectivePolicies(Set.of(testPrincipal));
    }

    //-----------------------------------------------< setPrincipalPolicy() >---
    @Test
    public void testSetPrincipalPolicy() throws Exception {
        JackrabbitAccessControlPolicy[] applicable = acMgr.getApplicablePolicies(testPrincipal);
        assertPolicies(applicable, 1);
        assertTrue(applicable[0] instanceof ACL);

        ACL acl = (ACL) applicable[0];
        Value pathValue = getValueFactory().createValue(testPath, PropertyType.PATH);
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, Collections.singletonMap(REP_NODE_PATH, pathValue)));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        Root root2 = adminSession.getLatestRoot();
        AccessControlPolicy[] policies = getAccessControlManager(root2).getPolicies(testPath);
        assertPolicies(policies, 1);
        assertEquals(1, ((ACL) policies[0]).getAccessControlEntries().length);

        policies = getAccessControlManager(root2).getPolicies(testPrincipal);
        assertPolicies(policies, 1);
        assertArrayEquals(acl.getAccessControlEntries(), ((ACL) policies[0]).getAccessControlEntries());
    }

    @Test
    public void testSetPrincipalPolicy2() throws Exception {
        setupPolicy(testPath);
        root.commit();

        JackrabbitAccessControlPolicy[] policies = acMgr.getPolicies(testPrincipal);
        ACL acl = (ACL) policies[0];

        Value nodePathValue = getValueFactory().createValue(testPath, PropertyType.PATH);
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, ImmutableMap.of(REP_NODE_PATH, nodePathValue)));

        // entry with * glob has already been created in the setup
        Map<String, Value> restrictions = ImmutableMap.of(REP_NODE_PATH, nodePathValue, REP_GLOB, valueFactory.createValue("*"));
        assertFalse(acl.addEntry(testPrincipal, testPrivileges, true, restrictions));

        // entry with different glob -> should be added
        restrictions = ImmutableMap.of(REP_NODE_PATH, nodePathValue, REP_GLOB, valueFactory.createValue("*/a/b"));
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, false, restrictions));

        acMgr.setPolicy(acl.getPath(), acl);
        assertEquals(3, ((ACL) acMgr.getPolicies(testPath)[0]).getAccessControlEntries().length);
    }

    @Test
    public void testSetPrincipalPolicyRemapped() throws Exception {
        setupPolicy(testPath);
        root.commit();

        NamePathMapper mapper = getNamePathMapperWithLocalRemapping();
        JackrabbitAccessControlManager remappedAcMgr = createAccessControlManager(root, mapper);
        JackrabbitAccessControlPolicy[] policies = remappedAcMgr.getPolicies(testPrincipal);
        assertNotNull(policies);
        assertEquals(1, policies.length);

        ACL acl = (ACL) policies[0];
        Value pathValue = new ValueFactoryImpl(root, mapper).createValue(mapper.getJcrPath(testPath), PropertyType.PATH);
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, Collections.singletonMap(REP_NODE_PATH, pathValue)));
        remappedAcMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        AccessControlPolicy[] acps = remappedAcMgr.getPolicies(mapper.getJcrPath(testPath));
        assertEquals(1, acps.length);
        assertEquals(2, ((ACL) acps[0]).getAccessControlEntries().length);

        acps = acMgr.getPolicies(testPath);
        assertEquals(1, acps.length);
        assertEquals(2, ((ACL) acps[0]).getAccessControlEntries().length);
    }

    @Test
    public void testSetPrincipalPolicyForRepositoryLevel() throws Exception {
        assertEquals(0, acMgr.getPolicies((String)null).length);

        JackrabbitAccessControlPolicy[] policies = acMgr.getApplicablePolicies(testPrincipal);
        ACL acl = (ACL) policies[0];

        Map<String, Value> restrictions = new HashMap<>();
        restrictions.put(REP_NODE_PATH, getValueFactory().createValue("", PropertyType.STRING));
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT);
        assertTrue(acl.addEntry(testPrincipal, privs, true, restrictions));

        acMgr.setPolicy(acl.getPath(), acl);

        AccessControlPolicy[] repoLevelPolicies = acMgr.getPolicies((String)null);
        assertEquals(1, repoLevelPolicies.length);

        AccessControlEntry[] entries = ((JackrabbitAccessControlList) repoLevelPolicies[0]).getAccessControlEntries();
        assertEquals(1, entries.length);

        assertArrayEquals(privs, entries[0].getPrivileges());
        assertEquals(testPrincipal, entries[0].getPrincipal());
    }

    @Test
    public void testSetPrincipalPolicyWithNewMvRestriction() throws Exception {
        setupPolicy(testPath);
        root.commit();

        JackrabbitAccessControlPolicy[] policies = acMgr.getPolicies(testPrincipal);
        ACL acl = (ACL) policies[0];

        Map<String, Value> restrictions = new HashMap<>();
        restrictions.put(REP_NODE_PATH, getValueFactory().createValue(testPath, PropertyType.PATH));

        Map<String, Value[]> mvRestrictions = new HashMap<>();
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
    public void testSetEmptyPrincipalPolicyRemovedACL() throws Exception {
        setupPolicy(testPath);
        root.commit();

        ACL acl = (ACL) acMgr.getPolicies(testPrincipal)[0];
        acl.getEntries().clear();

        // remove policy at test-path before writing back the principal-based policy
        AccessControlPolicy nodeBased = acMgr.getPolicies(testPath)[0];
        acMgr.removePolicy(testPath, nodeBased);

        // now write back the empty principal-based policy
        acMgr.setPolicy(acl.getPath(), acl);

        // ... which must not have an effect and the policy must not be re-added.
        assertPolicies(acMgr.getPolicies(testPath), 0);
    }

    @Test
    public void testSetPrincipalPolicyRemovedACL() throws Exception {
        setupPolicy(testPath);
        root.commit();

        ACL acl = (ACL) acMgr.getPolicies(testPrincipal)[0];

        // remove policy at test-path before writing back the principal-based policy
        ACL nodeBased = (ACL) acMgr.getPolicies(testPath)[0];
        int expectedSize = nodeBased.size();
        acMgr.removePolicy(testPath, nodeBased);

        // now write it back
        acMgr.setPolicy(acl.getPath(), acl);

        // entries needed to be added again
        assertPolicies(acMgr.getPolicies(testPath), expectedSize);
    }

    @Test(expected = AccessControlException.class)
    public void testSetPrincipalPolicyWithIncompleteEntry() throws Exception {
        setupPolicy(testPath);
        root.commit();

        ACL acl = (ACL) acMgr.getPolicies(testPrincipal)[0];
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT);
        acl.getEntries().add(new ACE(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT), false, Collections.emptySet(), getNamePathMapper()) {
            @Override
            protected @NotNull PrivilegeBitsProvider getPrivilegeBitsProvider() {
                return AccessControlManagerImplTest.this.getBitsProvider();
            }

            @Override
            public Privilege[] getPrivileges() {
                return privs;
            }
        });

        acMgr.setPolicy(acl.getPath(), acl);
    }

    //--------------------------------------------< removePrincipalPolicy() >---

    @Test
    public void testRemovePrincipalPolicy() throws Exception {
        JackrabbitAccessControlPolicy[] applicable = acMgr.getApplicablePolicies(testPrincipal);
        assertPolicies(applicable, 1);
        assertTrue(applicable[0] instanceof ACL);

        ACL acl = (ACL) applicable[0];
        Value pathValue = getValueFactory().createValue(testPath, PropertyType.PATH);
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, Collections.singletonMap(REP_NODE_PATH, pathValue)));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        acMgr.removePolicy(acl.getPath(), acl);
        root.commit();

        assertPolicies(acMgr.getPolicies(testPrincipal), 0);
        assertPolicies(acMgr.getPolicies(testPath), 0);
    }

    @Test
    public void testRemovePrincipalPolicy2() throws Exception {
        ACL acl = setupPolicy(testPath);
        assertTrue(acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.REP_READ_PROPERTIES)));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        JackrabbitAccessControlPolicy[] policies = acMgr.getPolicies(testPrincipal);
        assertPolicies(policies, 1);

        // removing the 'testPrincipal' policy will not remove the entry for everyone
        acMgr.removePolicy(policies[0].getPath(), policies[0]);

        assertPolicies(acMgr.getPolicies(testPath), 1);
        assertPolicies(acMgr.getPolicies(testPrincipal), 0);
        assertPolicies(acMgr.getPolicies(EveryonePrincipal.getInstance()), 1);
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

    @Test
    public void testRemovePoliciesByPrincipalRemapped() throws Exception {
        setupPolicy(testPath);
        root.commit();

        NamePathMapper mapper = getNamePathMapperWithLocalRemapping();
        JackrabbitAccessControlManager remappedAcMgr = createAccessControlManager(root, mapper);
        JackrabbitAccessControlPolicy[] policies = remappedAcMgr.getPolicies(testPrincipal);
        assertNotNull(policies);
        assertEquals(1, policies.length);

        remappedAcMgr.removePolicy(policies[0].getPath(), policies[0]);
        root.commit();

        assertEquals(0, acMgr.getPolicies(testPath).length);
    }

    @Test
    public void testRemovePrincipalPolicyForRepositoryLevel() throws Exception {
        setupPolicy(null, privilegesFromNames(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT));
        root.commit();

        JackrabbitAccessControlPolicy[] policies = acMgr.getPolicies(testPrincipal);
        assertEquals(1, policies.length);

        acMgr.removePolicy(policies[0].getPath(), policies[0]);
        root.commit();

        AccessControlPolicy[] repoLevelPolicies = acMgr.getPolicies((String)null);
        assertEquals(0, repoLevelPolicies.length);
    }

    //------------------------------------< privilegeCollectionFromNames() >---
    @Test
    public void testPrivilegeCollectionFromNames() throws Exception {
        PrivilegeCollection pc = acMgr.privilegeCollectionFromNames(Privilege.JCR_READ, Privilege.JCR_WRITE, Privilege.JCR_VERSION_MANAGEMENT);
        
        assertEquals(pc, acMgr.privilegeCollectionFromNames(Privilege.JCR_READ, Privilege.JCR_WRITE, Privilege.JCR_VERSION_MANAGEMENT));
        assertEquals(pc, acMgr.privilegeCollectionFromNames(JCR_READ, PrivilegeConstants.JCR_WRITE, JCR_VERSION_MANAGEMENT));
        assertEquals(pc, acMgr.privilegeCollectionFromNames(REP_READ_NODES, REP_READ_PROPERTIES, PrivilegeConstants.JCR_WRITE, JCR_VERSION_MANAGEMENT));
        
        assertNotEquals(pc, acMgr.privilegeCollectionFromNames());
        assertNotEquals(pc, acMgr.privilegeCollectionFromNames(JCR_READ));
        assertNotEquals(pc, acMgr.privilegeCollectionFromNames(JCR_ALL));
        assertNotEquals(pc, acMgr.privilegeCollectionFromNames(JCR_READ, PrivilegeConstants.JCR_WRITE));
        assertNotEquals(pc, acMgr.privilegeCollectionFromNames(JCR_READ, JCR_VERSION_MANAGEMENT));
        
        assertTrue(pc.includes());
        assertTrue(pc.includes(Privilege.JCR_READ, Privilege.JCR_WRITE));
        assertTrue(pc.includes(PrivilegeConstants.JCR_WRITE, Privilege.JCR_VERSION_MANAGEMENT));
        assertTrue(pc.includes(REP_READ_NODES, PrivilegeConstants.JCR_ADD_CHILD_NODES));
        
        assertFalse(pc.includes(PrivilegeConstants.JCR_ALL));
        assertFalse(pc.includes(PrivilegeConstants.REP_WRITE));
        assertFalse(pc.includes(PrivilegeConstants.REP_USER_MANAGEMENT, PrivilegeConstants.REP_ALTER_PROPERTIES));
    }

    private final static class TestACL extends AbstractAccessControlList {

        private final List<JackrabbitAccessControlEntry> entries = new ArrayList<>();
        private final RestrictionProvider restrictionProvider;

        private TestACL(@Nullable String jcrPath,
                       @NotNull RestrictionProvider restrictionProvider,
                       @NotNull NamePathMapper namePathMapper,
                       @NotNull List<JackrabbitAccessControlEntry> entries) {
            super((jcrPath == null) ? null : namePathMapper.getOakPath(jcrPath), namePathMapper);
            this.entries.addAll(entries);
            this.restrictionProvider = restrictionProvider;
        }

        private TestACL(@Nullable String jcrPath,
                       @NotNull RestrictionProvider restrictionProvider,
                       @NotNull NamePathMapper namePathMapper,
                       @NotNull JackrabbitAccessControlEntry... entry) {
            this(jcrPath, restrictionProvider, namePathMapper, List.of(entry));
        }

        @Override
        public boolean isEmpty() {
            return entries.isEmpty();
        }

        @Override
        public int size() {
            return entries.size();
        }

        @Override
        public boolean addEntry(@NotNull Principal principal, @NotNull Privilege[] privileges,
                                boolean isAllow, @Nullable Map<String, Value> restrictions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addEntry(@NotNull Principal principal, @NotNull Privilege[] privileges, boolean isAllow, @Nullable Map<String, Value> restrictions, @Nullable Map<String, Value[]> mvRestrictions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void orderBefore(@NotNull AccessControlEntry srcEntry, @Nullable AccessControlEntry destEntry) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeAccessControlEntry(AccessControlEntry ace) {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public List<JackrabbitAccessControlEntry> getEntries() {
            return entries;
        }

        @NotNull
        @Override
        public RestrictionProvider getRestrictionProvider() {
            return restrictionProvider;
        }
    }
}

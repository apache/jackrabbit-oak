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
package org.apache.jackrabbit.oak.security.authorization;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.jcr.NamespaceRegistry;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.TestNameMapper;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AbstractAccessControlTest;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * AccessControlManagerImplTest... TODO
 */
public class AccessControlManagerImplTest extends AbstractAccessControlTest implements AccessControlConstants {

    private final String testName = TestNameMapper.TEST_PREFIX + ":testRoot";
    private final String testPath = '/' + testName;

    private Principal testPrincipal;
    private Privilege[] testPrivileges;

    private TestNameMapper nameMapper;
    private NamePathMapper npMapper;

    private AccessControlManagerImpl acMgr;
    private PrivilegeManager privilegeManager;
    private ValueFactory valueFactory;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        registerNamespace(TestNameMapper.TEST_PREFIX, TestNameMapper.TEST_URI);
        nameMapper = new TestNameMapper(Namespaces.getNamespaceMap(root.getTree("/")));
        npMapper = new NamePathMapperImpl(nameMapper);

        acMgr = getAccessControlManager(npMapper);
        privilegeManager = getSecurityProvider().getPrivilegeConfiguration().getPrivilegeManager(root, npMapper);
        valueFactory = new ValueFactoryImpl(root.getBlobFactory(), npMapper);

        NodeUtil rootNode = new NodeUtil(root.getTree("/"), npMapper);
        rootNode.addChild(testName, JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        // TODO
        testPrincipal = new PrincipalImpl("admin");
        testPrivileges = privilegesFromNames(Privilege.JCR_ADD_CHILD_NODES, Privilege.JCR_READ);
    }

    @After
    public void after() throws Exception {
        root.refresh();
        root.getTree(testPath).remove();
        root.commit();
    }

    @Override
    protected NamePathMapper getNamePathMapper() {
        return npMapper;
    }

    private AccessControlManagerImpl getAccessControlManager(NamePathMapper npMapper) {
        return new AccessControlManagerImpl(root, npMapper, getSecurityProvider());
    }

    private NamePathMapper getLocalNamePathMapper() {
        NameMapper remapped = new TestNameMapper(nameMapper, TestNameMapper.LOCAL_MAPPING);
        return new NamePathMapperImpl(remapped);
    }

    private ACL getApplicablePolicy(String path) throws RepositoryException {
        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(path);
        if (itr.hasNext()) {
            return (ACL) itr.nextAccessControlPolicy();
        } else {
            throw new RepositoryException("No applicable policy found.");
        }
    }

    private void setupPolicy(String path) throws RepositoryException {
        ACL policy = getApplicablePolicy(path);
        if (path == null) {
            policy.addAccessControlEntry(testPrincipal, testPrivileges);
        } else {
            policy.addEntry(testPrincipal, testPrivileges, true, getGlobRestriction("*"));
        }
        acMgr.setPolicy(path, policy);
    }

    private Map<String, Value> getGlobRestriction(String value) {
        return ImmutableMap.of(REP_GLOB, valueFactory.createValue(value));
    }

    @Test
    public void testGetSupportedPrivileges() throws Exception {
        List<Privilege> allPrivileges = Arrays.asList(privilegeManager.getRegisteredPrivileges());

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
    public void testGetSupportedPrivilegesWithInvalidPath() throws Exception {
        List<String> invalid = new ArrayList<String>();
        invalid.add("");
        invalid.add("../../jcr:testRoot");
        invalid.add("jcr:testRoot");
        invalid.add("./jcr:testRoot");

        for (String path : invalid) {
            try {
                acMgr.getSupportedPrivileges(path);
                fail("Expects valid node path, found: " + path);
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testGetSupportedPrivilegesFromPropertyPath() throws Exception {
        try {
            acMgr.getSupportedPrivileges("/jcr:primaryType");
            fail("Property path -> PathNotFoundException expected.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testGetSupportedPrivilegesWithNonExistingPath() throws Exception {
        try {
            acMgr.getSupportedPrivileges("/non/existing/node");
            fail("Nonexisting node -> PathNotFoundException expected.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testGetSupportedPrivilegesIncludingPathConversion() throws Exception {
        List<Privilege> allPrivileges = Arrays.asList(privilegeManager.getRegisteredPrivileges());

        List<String> testPaths = new ArrayList<String>();
        testPaths.add('/' + TestNameMapper.TEST_LOCAL_PREFIX + ":testRoot");
        testPaths.add("/{" + TestNameMapper.TEST_URI + "}testRoot");

        AccessControlManager acMgr = getAccessControlManager(getLocalNamePathMapper());
        for (String path : testPaths) {
            Privilege[] supported = acMgr.getSupportedPrivileges(path);

            assertNotNull(supported);
            assertEquals(allPrivileges.size(), supported.length);
            assertTrue(allPrivileges.containsAll(Arrays.asList(supported)));
        }
    }

    @Test
    public void testPrivilegeFromName() throws Exception {
        List<Privilege> allPrivileges = Arrays.asList(privilegeManager.getRegisteredPrivileges());
        for (Privilege privilege : allPrivileges) {
            Privilege p = acMgr.privilegeFromName(privilege.getName());
            assertEquals(privilege, p);
        }
    }

    @Test
    public void testPrivilegeFromExpandedName() throws Exception {
        Privilege readPriv = privilegeManager.getPrivilege(PrivilegeConstants.JCR_READ);

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

    @Test
    public void testHasPrivileges() throws Exception {
        // TODO
    }

    @Test
    public void testGetPrivileges() throws Exception {
        // TODO
    }

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
    public void testGetPoliciesNodeNotAccessControlled() throws Exception {
        AccessControlPolicy[] policies = acMgr.getPolicies(testPath);
        assertNotNull(policies);
        assertEquals(0, policies.length);
    }

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
    public void testAccessControlContentPaths() throws Exception {
        ACL policy = getApplicablePolicy(testPath);
        policy.addEntry(testPrincipal, testPrivileges, true, getGlobRestriction("*"));
        acMgr.setPolicy(testPath, policy);

        String aclPath = testPath + '/' + REP_POLICY;
        Tree acl = root.getTree(aclPath);
        assertNotNull(acl);
        Iterator<Tree> aces = acl.getChildren().iterator();
        assertTrue(aces.hasNext());
        Tree ace = aces.next();
        assertNotNull(ace);

        List<String> accessControlledPaths = new ArrayList<String>();
        accessControlledPaths.add(aclPath);
        accessControlledPaths.add(ace.getPath());

        Tree rest = ace.getChild(REP_RESTRICTIONS);
        if (rest != null) {
            accessControlledPaths.add(rest.getPath());
        }

        for (String path : accessControlledPaths) {
            try {
                acMgr.getApplicablePolicies(path);
                fail("Getting applicable policies for access control content should fail.");
            } catch (AccessControlException e) {
                // success
            }

            try {
                acMgr.getPolicies(path);
                fail("Getting policies for access control content should fail.");
            } catch (AccessControlException e) {
                // success
            }

            try {
                acMgr.getEffectivePolicies(path);
                fail("Getting effective policies for access control content should fail.");
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    @Test
    public void testNonExistingNodePath() throws Exception {
        String propertyPath = "/not/existing";

        try {
            acMgr.getApplicablePolicies(propertyPath);
            fail("Getting applicable policies for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
        try {
            acMgr.getPolicies(propertyPath);
            fail("Getting policies for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }

        try {
            acMgr.getEffectivePolicies(propertyPath);
            fail("Getting policies for node that doesn't exist should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testPropertyPath() throws Exception {
        String propertyPath = "/jcr:primaryType";

        try {
            acMgr.getApplicablePolicies(propertyPath);
            fail("Getting applicable policies for property should fail.");
        } catch (PathNotFoundException e) {
            // success
        }

        try {
            acMgr.getPolicies(propertyPath);
            fail("Getting policies for property should fail.");
        } catch (PathNotFoundException e) {
            // success
        }

        try {
            acMgr.getEffectivePolicies(propertyPath);
            fail("Getting policies for property should fail.");
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        // TODO
    }

    @Ignore("OAK-396")
    @Test
    public void testSetPolicy() throws Exception {
        ACL acl = getApplicablePolicy(testPath);
        acl.addAccessControlEntry(testPrincipal, testPrivileges);
        acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false, getGlobRestriction("*/something"));

        acMgr.setPolicy(testPath, acl);
        root.commit();

        Root root2 = adminSession.getLatestRoot();
        AccessControlPolicy[] policies = getAccessControlManager(root2).getPolicies(testPath);
        assertNotNull(policies);
        assertEquals(1, policies.length);

        ACL acl2 = (ACL) policies[0];
        assertArrayEquals(acl.getAccessControlEntries(), acl2.getAccessControlEntries());
    }

    @Ignore("OAK-396")
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
        assertEquals(2, policyTree.getChildrenCount());

        Iterator<Tree> children = policyTree.getChildren().iterator();
        Tree ace = children.next();
        assertEquals(NT_REP_GRANT_ACE, TreeUtil.getPrimaryTypeName(ace));
        assertEquals(testPrincipal.getName(), TreeUtil.getString(ace, REP_PRINCIPAL_NAME));
        assertArrayEquals(testPrivileges, privilegesFromNames(TreeUtil.getStrings(ace, REP_PRIVILEGES)));
        assertFalse(ace.hasChild(REP_RESTRICTIONS));

        NodeUtil ace2 = new NodeUtil(children.next());
        assertEquals(NT_REP_DENY_ACE, ace2.getPrimaryNodeTypeName());
        assertEquals(EveryonePrincipal.NAME, ace2.getString(REP_PRINCIPAL_NAME, null));
        assertArrayEquals(testPrivileges, privilegesFromNames(ace2.getNames(REP_PRIVILEGES)));
        assertTrue(ace2.hasChild(REP_RESTRICTIONS));
        NodeUtil restr = ace2.getChild(REP_RESTRICTIONS);
        assertEquals("*/something", restr.getString(REP_GLOB, null));
    }

    @Test
    public void testSetInvalidPolicy() throws Exception {
        // TODO
    }

    @Test
    public void testSetPolicyWithInvalidPath() throws Exception {
        // TODO
    }

    @Test
    public void testSetPolicyOnAclNode() throws Exception {
        // TODO
    }

    @Test
    public void testSetRepoPolicy() throws Exception {
        // TODO
    }

    @Test
    public void testRemovePolicy() throws Exception {
        // TODO
    }

    @Test
    public void testRemoveInvalidPolicy() throws Exception {
        // TODO
    }

    @Test
    public void testRemovePolicyWithInvalidPath() throws Exception {
        // TODO
    }

    @Test
    public void testRemovePolicyOnAclNode() throws Exception {
        // TODO
    }

    @Test
    public void testRemoveRepoPolicy() throws Exception {
        // TODO
    }
}
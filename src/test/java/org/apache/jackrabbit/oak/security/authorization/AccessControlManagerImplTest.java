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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.jcr.NamespaceRegistry;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.TestNameMapper;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * AccessControlManagerImplTest... TODO
 */
public class AccessControlManagerImplTest extends AbstractSecurityTest {

    final String testName = TestNameMapper.TEST_PREFIX + ":testRoot";
    final String testPath = "/"+testName;

    TestNameMapper nameMapper;
    NamePathMapper npMapper;

    AccessControlManagerImpl acMgr;
    PrivilegeManager privilegeManager;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        NamespaceRegistry nsRegistry = new ReadWriteNamespaceRegistry() {
            @Override
            protected Root getWriteRoot() {
                return root;
            }

            @Override
            protected Tree getReadTree() {
                return root.getTree("/");
            }
        };
        nsRegistry.registerNamespace(TestNameMapper.TEST_PREFIX, TestNameMapper.TEST_URI);

        nameMapper = new TestNameMapper(Namespaces.getNamespaceMap(root.getTree("/")));
        npMapper = new NamePathMapperImpl(nameMapper);

        acMgr = getAccessControlManager(npMapper);
        privilegeManager = getSecurityProvider().getPrivilegeConfiguration().getPrivilegeManager(root, npMapper);

        NodeUtil rootNode = new NodeUtil(root.getTree("/"), npMapper);
        rootNode.addChild(testName, JcrConstants.NT_UNSTRUCTURED);

        root.commit();
    }

    @After
    public void after() throws Exception {
        root.getTree(testPath).remove();
        root.commit();
    }

    private AccessControlManagerImpl getAccessControlManager(NamePathMapper npMapper) {
        return new AccessControlManagerImpl(root, npMapper, getSecurityProvider());
    }

    private NamePathMapper getLocalNamePathMapper() {
        NameMapper remapped = new TestNameMapper(nameMapper, TestNameMapper.LOCAL_MAPPING);
        return new NamePathMapperImpl(remapped);
    }

    private AccessControlPolicy getApplicablePolicy(String path) throws RepositoryException {
        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(path);
        if (itr.hasNext()) {
            return itr.nextAccessControlPolicy();
        } else {
            throw new RepositoryException("No applicable policy found.");
        }
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
        testPaths.add("/"+ TestNameMapper.TEST_LOCAL_PREFIX + ":testRoot");
        testPaths.add("/{"+ TestNameMapper.TEST_URI+"}testRoot");

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
        invalid.add("{"+ NamespaceRegistry.NAMESPACE_JCR+"}unknown");

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
        assertTrue(((ACL) policy).isEmpty());

        assertFalse(itr.hasNext());
    }

    @Test
    public void testGetApplicablePoliciesNodeAccessControlled() throws Exception {
        AccessControlPolicy policy = getApplicablePolicy(testPath);
        acMgr.setPolicy(testPath, policy);

        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(testPath);
        assertNotNull(itr);
        assertFalse(itr.hasNext());
    }

    @Test
    public void testGetApplicablePoliciesWithCollidingNode() throws Exception {
        NodeUtil testTree = new NodeUtil(root.getTree(testPath));
        testTree.addChild(AccessControlConstants.REP_POLICY, JcrConstants.NT_UNSTRUCTURED);

        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(testPath);
        assertNotNull(itr);
        assertFalse(itr.hasNext());
    }

    @Test
    public void testGetApplicablePoliciesOnAclNode() throws Exception {
        AccessControlPolicy policy = getApplicablePolicy(testPath);
        acMgr.setPolicy(testPath, policy);

        String aclPath = testPath + "/rep:policy";
        assertNotNull(root.getTree(aclPath));

        try {
            AccessControlPolicyIterator itr = acMgr.getApplicablePolicies(aclPath);
            fail("Getting applicable policies for ACL node.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetApplicablePoliciesOnAceNode() throws Exception {
        // TODO
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
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        // TODO
    }

    @Test
    public void testSetPolicy() throws Exception {
        // TODO
    }

    @Test
    public void testRemovePolicy() throws Exception {
        // TODO
    }
}
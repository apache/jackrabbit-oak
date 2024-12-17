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
package org.apache.jackrabbit.oak.exercise.security.authorization.principalbased;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlPolicy;
import java.security.Principal;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * <pre>
 * Module: Principal-Based Authorization
 * =============================================================================
 *
 * Title: Access Control Management with Principal-Based Authorization
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Learn how to manage access control by principal and how to use it combination with path-based access control
 * management.
 *
 * Reading:
 * http://jackrabbit.apache.org/oak/docs/security/authorization/principalbased.html#details_access_mgt
 *
 * Exercises:
 *
 * Applicable Access Control Policies
 * Complete the following 3 tests to train your understanding of applicable access control policies. Compare the
 * policies obtained from the principal-based authorization module with the actual repository setup.
 *
 * - {@link #testApplicablePoliciesSupportedPrincipal()}
 * - {@link #testApplicablePoliciesUnsupportedPrincipal()}
 * - {@link #testApplicablePoliciesByPath()}
 *
 *   Questions: What are the main differences between the isolated principal-based access control manager and the real-world composite setup?
 *              Can you explain the differences between accessing policies by principal and by path?
 *
 * Editing Access Control
 * The following tests focus on editing access control by principal. Complete the tests such that they both pass.
 * Notice, that the for the assertion of the effective permission setup, two slightly different content sessions are
 * created (one containing an unsupported principal).
 *
 * - {@link #testEditAccssControl()}
 * - {@link #testEditAccssControl2()}
 *
 *   Questions: Can you change the access control setup using alternative ways to get the expected effective permissions (not changing the assertion)?
 *              Can you explain the main difference between 'testEditAccssControl' and 'testEditAccssControl2'?
 *              Could you use AccessControlManager.getApplicablePolicies(testPath) in 'testEditAccssControl' and/or 'testEditAccssControl2'? Explain why or why not.
 *
 * Effective Access Control Policies
 * Walk through the tests to understand how effective policies are computed when accessed by a set of principals or by path.
 *
 * - {@link #testEffectivePolicies()}: The same access control setup results in different effective policies depending
 *                                     on the set of principals passed to the method. Complete the assertions such that the
 *                                     test passes.
 * - {@link #testEffectivePoliciesByPath()}: This time effective policies are access by path. Complete the assertions and
 *                                     compare the results with the previous test.
 *
 *   Questions: What are the differences between effective policies by principals and by path?
 *              Discuss how the notion of supported principal is reflected in each case.
 * </pre>
 */
public class L2_AccessControlManagementTest extends AbstractPrincipalBasedTest {

    private Principal supportedPrincipal;
    private Principal unsupportedPrincipal;

    private JackrabbitAccessControlManager principalBasedAcMgr;
    private JackrabbitAccessControlManager compositeAcMgr;

    private String testPath;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        supportedPrincipal = getSystemUserPrincipal("systemUser", getSupportedIntermediatePath());
        unsupportedPrincipal = getGroupPrincipal();

        principalBasedAcMgr = (JackrabbitAccessControlManager) getPrincipalBasedAuthorizationConfiguration().getAccessControlManager(root, getNamePathMapper());
        compositeAcMgr = (JackrabbitAccessControlManager) getConfig(AuthorizationConfiguration.class).getAccessControlManager(root, getNamePathMapper());

        Tree testTree = TreeUtil.addChild(root.getTree(PathUtils.ROOT_PATH), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        testPath = getNamePathMapper().getJcrPath(testTree.getPath());
        root.commit();
    }

    @After
    @Override
    public void after() throws Exception {
        try {
            root.getTree(testPath).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testApplicablePoliciesSupportedPrincipal() throws Exception {
        AccessControlPolicy[] applicable = principalBasedAcMgr.getApplicablePolicies(supportedPrincipal);
        assertEquals(-1 /*EXERCISE: fix assertion */, applicable.length);
        assertTrue(applicable[0] instanceof PrincipalAccessControlList);

        applicable = compositeAcMgr.getApplicablePolicies(supportedPrincipal);
        assertEquals(-1 /*EXERCISE: fix assertion */, applicable.length);

        // EXERCISE: inspect the applicable policies and write assertions
        // EXERCISE: explain the difference between the composite setup and the principal-based-only access control manager.
    }

    @Test
    public void testApplicablePoliciesUnsupportedPrincipal() throws Exception {
        AccessControlPolicy[] applicable = principalBasedAcMgr.getApplicablePolicies(unsupportedPrincipal);
        assertEquals(-1 /*EXERCISE: fix assertion */, applicable.length);

        applicable = compositeAcMgr.getApplicablePolicies(unsupportedPrincipal);
        assertEquals(-1 /*EXERCISE: fix assertion */, applicable.length);

        // EXERCISE: inspect the applicable policies and write test-assertions
        // EXERCISE: explain the difference between the composite setup and the principal-based-only access control manager.
    }

    @Test
    public void testApplicablePoliciesByPath() throws Exception {
        Iterator<AccessControlPolicy> applicable = principalBasedAcMgr.getApplicablePolicies(testPath);
        assertEquals(null /*EXERCISE: complete assertion */, applicable.hasNext());

        applicable = compositeAcMgr.getApplicablePolicies(testPath);
        assertEquals(null /*EXERCISE: complete assertion */, applicable.hasNext());

        // EXERCISE: inspect the available applicable policies
        // EXERCISE: explain the difference between the composite setup and the principal-based-only access control manager.
    }

    @Test
    public void testEditAccssControl() throws Exception {
        PrincipalAccessControlList acl = getApplicablePrincipalAccessControlList(compositeAcMgr, supportedPrincipal);
        assertNotNull(acl);

        // EXERCISE: modify the PrincipalAccessControlList such that the test passes.
        // EXERCISE: can you identify an alternative way to edit the access control entries?
        compositeAcMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        try (ContentSession cs = getTestSession(supportedPrincipal)) {
            Root testRoot = cs.getLatestRoot();
            assertTrue(testRoot.getTree(testPath).exists());
        }
    }

    @Test
    public void testEditAccssControl2() throws Exception {
        // EXERCISE: Obtain the applicable policy that results in effective permissions for the Subject containing
        //           both the supportedPrincipal and unsupportedPrincipal
        JackrabbitAccessControlList acl = null; // EXERCISE
        acl.addEntry(supportedPrincipal, privilegesFromNames(PrivilegeConstants.JCR_READ), true,
                Map.of(AccessControlConstants.REP_NODE_PATH, getValueFactory(root).createValue(testPath)));
        compositeAcMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        // EXERCISE: Can you identify another way of obtaining _and_ modifying the access control setup to achieve
        //           the same effective setup?

        try (ContentSession cs = getTestSession(supportedPrincipal, unsupportedPrincipal)) {
            Root testRoot = cs.getLatestRoot();
            assertTrue(testRoot.getTree(testPath).exists());
        }
    }

    @Test
    public void testEffectivePolicies() throws Exception {
        JackrabbitAccessControlList acl = requireNonNull(AccessControlUtils.getAccessControlList(compositeAcMgr, testPath));
        acl.addAccessControlEntry(supportedPrincipal, privilegesFromNames(PrivilegeConstants.JCR_REMOVE_CHILD_NODES));
        acl.addAccessControlEntry(unsupportedPrincipal, privilegesFromNames(PrivilegeConstants.JCR_MODIFY_PROPERTIES));
        compositeAcMgr.setPolicy(acl.getPath(), acl);

        PrincipalAccessControlList pacl = requireNonNull(getApplicablePrincipalAccessControlList(compositeAcMgr, supportedPrincipal));
        pacl.addEntry(testPath, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES));
        compositeAcMgr.setPolicy(pacl.getPath(), pacl);
        root.commit();

        // 1. get effective policies for just 'supportedPrincipal'

        // EXERCISE: what is the expected result? verify by filling in the missing assertions.
        AccessControlPolicy[] effective = compositeAcMgr.getEffectivePolicies(Set.of(supportedPrincipal));
        assertEquals(-1 /*EXERCISE*/, effective.length);
        // EXERCISE: inspect the nature of the policies

        assertTrue(effective[0] instanceof AccessControlList);
        AccessControlEntry[] entries = ((AccessControlList) effective[0]).getAccessControlEntries();
        assertEquals(-1 /*EXERCISE*/, entries.length);
        assertArrayEquals(null /*EXERCISE*/, entries[0].getPrivileges());
        // EXERCISE: inspect the entries. where do they take effect?

        // 2. get effective policies for the set containing both 'supportedPrincipal' and 'unsupportedPrincipal'

        // EXERCISE: what is the expected result? verify it by filling in the missing assertions.
        effective = compositeAcMgr.getEffectivePolicies(Set.of(supportedPrincipal, unsupportedPrincipal));
        assertEquals(-1 /*EXERCISE*/, effective.length);
        // EXERCISE: inspect the nature of the policies

        assertTrue(effective[0] instanceof AccessControlList);
        entries = ((AccessControlList) effective[0]).getAccessControlEntries();
        assertEquals(-1 /*EXERCISE*/, entries.length);
        assertArrayEquals(null /*EXERCISE*/, entries[0].getPrivileges());
        // EXERCISE: inspect the entries. where do they take effect?
    }

    @Test
    public void testEffectivePoliciesByPath() throws Exception {
        JackrabbitAccessControlList acl = requireNonNull(AccessControlUtils.getAccessControlList(compositeAcMgr, testPath));
        acl.addAccessControlEntry(unsupportedPrincipal, privilegesFromNames(PrivilegeConstants.REP_REMOVE_PROPERTIES));
        compositeAcMgr.setPolicy(acl.getPath(), acl);

        PrincipalAccessControlList pacl = requireNonNull(getApplicablePrincipalAccessControlList(compositeAcMgr, supportedPrincipal));
        pacl.addEntry(testPath, privilegesFromNames(PrivilegeConstants.REP_ADD_PROPERTIES));
        compositeAcMgr.setPolicy(pacl.getPath(), pacl);
        root.commit();

        // notice, that effective policies are accessed by path!
        AccessControlPolicy[] effective = compositeAcMgr.getEffectivePolicies(testPath);
        assertEquals(-1 /*EXERCISE*/, effective.length);
        // EXERCISE: inspect the policies
    }
}

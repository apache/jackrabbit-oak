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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ReadPolicy;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.MIX_REP_PRINCIPAL_BASED_MIXIN;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.MockUtility.mockFilterProvider;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ADD_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_LOCK_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_REMOVE_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_REMOVE_NODE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_RETENTION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_VERSION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_WRITE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PrincipalBasedAccessControlManagerTest extends AbstractPrincipalBasedTest {

    private PrincipalBasedAccessControlManager acMgr;
    private ItemBasedPrincipal validPrincipal;

    @Before
    public void testBefore() throws Exception {
        super.before();

        acMgr = createAccessControlManager(root, getFilterProvider());
        validPrincipal = (ItemBasedPrincipal) getTestSystemUser().getPrincipal();
    }

    private PrincipalBasedAccessControlManager createAccessControlManager(Root root, @NotNull FilterProvider filterProvider) {
        return new PrincipalBasedAccessControlManager(getMgrProvider(this.root), filterProvider);
    }

    private PrincipalPolicyImpl createValidPolicy() throws RepositoryException {
        String oakPath = getNamePathMapper().getOakPath(validPrincipal.getPath());
        return new PrincipalPolicyImpl(validPrincipal, oakPath, getMgrProvider(root));
    }

    @Test(expected = AccessControlException.class)
    public void testGetApplicablePoliciesNullPrincipal() throws Exception {
        acMgr.getApplicablePolicies((Principal) null);
    }

    @Test(expected = AccessControlException.class)
    public void testGetApplicablePoliciesEmptyPrincipalName() throws Exception {
        acMgr.getApplicablePolicies(new PrincipalImpl(""));
    }

    @Test
    public void testGetApplicablePoliciesPrincipalNotHandled() throws Exception {
        PrincipalBasedAccessControlManager mgr = createAccessControlManager(root, mockFilterProvider(false));
        assertEquals(0, mgr.getApplicablePolicies(validPrincipal).length);
    }

    @Test
    public void testGetApplicablePoliciesPrincipalHandled() throws Exception {
        PrincipalBasedAccessControlManager mgr = createAccessControlManager(root, mockFilterProvider(true));
        AccessControlPolicy[] applicable = mgr.getApplicablePolicies(validPrincipal);
        assertEquals(1, applicable.length);
        assertTrue(applicable[0] instanceof PrincipalPolicyImpl);
    }

    @Test
    public void testGetSetPolicy() throws Exception {
        AccessControlPolicy[] applicable = acMgr.getApplicablePolicies(validPrincipal);
        assertEquals(1, applicable.length);
        assertEquals(0, acMgr.getPolicies(validPrincipal).length);

        PrincipalPolicyImpl policy = (PrincipalPolicyImpl) applicable[0];
        policy.addEntry(testContentJcrPath, privilegesFromNames(JCR_READ));

        acMgr.setPolicy(policy.getPath(), policy);
        assertEquals(0,  acMgr.getApplicablePolicies(validPrincipal).length);
        assertEquals(1, acMgr.getPolicies(validPrincipal).length);
    }

    @Test(expected = AccessControlException.class)
    public void testGetPoliciesNullPrincipal() throws Exception {
        acMgr.getPolicies((Principal) null);
    }

    @Test(expected = AccessControlException.class)
    public void testGetPoliciesEmptyPrincipalName() throws Exception {
        acMgr.getPolicies(new PrincipalImpl(""));
    }

    @Test
    public void testGetPoliciesPrincipalNotHandled() throws Exception {
        PrincipalBasedAccessControlManager mgr = createAccessControlManager(root, mockFilterProvider(false));
        assertEquals(0, mgr.getPolicies(validPrincipal).length);
    }

    @Test
    public void testGetPoliciesAccessControlledTree() throws Exception {
        Tree tree = root.getTree(getNamePathMapper().getOakPath(validPrincipal.getPath()));
        TreeUtil.addMixin(tree, MIX_REP_PRINCIPAL_BASED_MIXIN, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "uid");
        assertEquals(0, acMgr.getPolicies(validPrincipal).length);
    }

    @Test(expected = AccessControlException.class)
    public void testGetEffectivePoliciesEmptyPrincipalName() throws Exception {
        acMgr.getEffectivePolicies(Set.of(validPrincipal, new PrincipalImpl("")));
    }

    @Test
    public void testGetEffectivePoliciesNothingSet() throws Exception {
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(Set.of(validPrincipal));
        assertEffectivePolicies(effective, 1, -1, true);
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        PrincipalPolicyImpl policy = (PrincipalPolicyImpl) acMgr.getApplicablePolicies(validPrincipal)[0];
        policy.addEntry(testContentJcrPath, privilegesFromNames(PrivilegeConstants.JCR_ALL));
        acMgr.setPolicy(policy.getPath(), policy);

        // transient changes => no effective policy
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(Set.of(validPrincipal));
        assertEffectivePolicies(effective, 1, -1, true);

        // after commit => effective policy present
        root.commit();
        effective = acMgr.getEffectivePolicies(Set.of(validPrincipal));
        assertEffectivePolicies(effective, 2, 1, true);
    }

    @Test
    public void testGetEffectivePoliciesEmptyPolicySet() throws Exception {
        JackrabbitAccessControlPolicy emptyPolicy = acMgr.getApplicablePolicies(validPrincipal)[0];
        acMgr.setPolicy(emptyPolicy.getPath(), emptyPolicy);
        root.commit();
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(Set.of(validPrincipal));
        assertEffectivePolicies(effective, 1, -1, true);
    }

    @Test
    public void testGetEffectivePoliciesRemovedPrincipal() throws Exception {
        setupPrincipalBasedAccessControl(validPrincipal, null, JCR_WORKSPACE_MANAGEMENT);
        root.commit();

        String id = getTestSystemUser().getID();
        Root latestRoot = adminSession.getLatestRoot();
        getUserManager(latestRoot).getAuthorizable(validPrincipal).remove();
        latestRoot.commit();
        try {
            assertEffectivePolicies(acMgr.getEffectivePolicies(Set.of(validPrincipal)), 1, -1, true);
        } finally {
            root.refresh();
            getUserManager(root).createSystemUser(id, INTERMEDIATE_PATH);
            root.commit();
        }
    }

    /**
     * Since principal-based permissions are only evaluated if the complete set of principals is supported, the same
     * should apply for {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getEffectivePolicies(Set)}.
     */
    @Test
    public void testGetEffectivePoliciesMixedPrincipalSet() throws Exception {
        setupPrincipalBasedAccessControl(validPrincipal, testJcrPath, JCR_READ);
        root.commit();

        Set<Principal> mixedPrincipalSet = Set.of(validPrincipal, getTestUser().getPrincipal());
        assertEquals(0, acMgr.getEffectivePolicies(mixedPrincipalSet).length);
    }

    @Test
    public void testGetEffectivePoliciesRemovedPolicy() throws Exception {
        setupPrincipalBasedAccessControl(validPrincipal, null, JCR_WORKSPACE_MANAGEMENT);
        root.commit();

        Root latestRoot = adminSession.getLatestRoot();
        latestRoot.getTree(getNamePathMapper().getOakPath(validPrincipal.getPath())).getChild(REP_PRINCIPAL_POLICY).remove();
        latestRoot.commit();

        assertEffectivePolicies(acMgr.getEffectivePolicies(Set.of(validPrincipal)), 1, -1, true);
    }

    @Test(expected = AccessControlException.class)
    public void testSetInvalidPolicy() throws Exception {
        PrincipalAccessControlList policy = mock(PrincipalAccessControlList.class);
        acMgr.setPolicy(validPrincipal.getPath(), policy);
    }

    @Test(expected = AccessControlException.class)
    public void testSetEffectivePolicy() throws Exception {
        setupPrincipalBasedAccessControl(validPrincipal, testContentJcrPath, REP_WRITE);
        root.commit();

        ImmutablePrincipalPolicy effective = (ImmutablePrincipalPolicy) acMgr.getEffectivePolicies(Set.of(validPrincipal))[0];
        acMgr.setPolicy(effective.getPath(), effective);
    }

    @Test(expected = AccessControlException.class)
    public void testSetPolicyPathMismatch() throws Exception {
        PrincipalPolicyImpl policy = createValidPolicy();
        acMgr.setPolicy(policy.getOakPath(), policy);
    }

    @Test(expected = AccessControlException.class)
    public void testSetPolicyNullPath() throws Exception {
        PrincipalAccessControlList policy = mock(PrincipalAccessControlList.class);
        acMgr.setPolicy(null, policy);
    }

    @Test(expected = AccessControlException.class)
    public void testSetPolicyUnsupportedPath() throws Exception {
        String unsupportedJcrPath = getNamePathMapper().getJcrPath(PathUtils.getParentPath(SUPPORTED_PATH));
        acMgr.setPolicy(unsupportedJcrPath, createValidPolicy());
    }

    @Test
    public void testSetEmptyPolicy() throws Exception {
        PrincipalPolicyImpl policy = createValidPolicy();
        acMgr.setPolicy(policy.getPath(), policy);

        assertEquals(1, acMgr.getPolicies(validPrincipal).length);
        assertEquals(0, acMgr.getApplicablePolicies(validPrincipal).length);
        assertTrue(root.getTree(policy.getOakPath()).hasChild(REP_PRINCIPAL_POLICY));
    }

    @Test
    public void testSetPolicyNonExistingEffectivePath() throws Exception {
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(validPrincipal, acMgr);
        policy.addEntry(testJcrPath, privilegesFromNames(JCR_READ));
        acMgr.setPolicy(policy.getPath(), policy);
        root.commit();
    }

    @Test
    public void testSetPolicyRemovedEffectivePath() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        root.commit();

        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(validPrincipal, acMgr);
        policy.addEntry(testJcrPath, privilegesFromNames(JCR_READ));

        root.getTree(TEST_OAK_PATH).remove();
        root.commit();

        acMgr.setPolicy(policy.getPath(), policy);
        root.commit();
    }

    @Test
    public void testSetPolicy() throws Exception {
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(validPrincipal, PathUtils.ROOT_PATH, JCR_READ_ACCESS_CONTROL);
        addPrincipalBasedEntry(policy, null, JCR_WORKSPACE_MANAGEMENT);

        Tree policyTree = root.getTree(policy.getOakPath()).getChild(REP_PRINCIPAL_POLICY);
        assertTrue(policyTree.exists());
        assertEquals(2, policyTree.getChildrenCount(10));

        AccessControlPolicy[] policies = acMgr.getPolicies(validPrincipal);
        assertEquals(1, policies.length);
        assertEquals(2, ((PrincipalPolicyImpl)policies[0]).size());
        assertEquals(0, acMgr.getApplicablePolicies(validPrincipal).length);
    }

    @Test
    public void testSetPolicyRemovesAllChildNodes() throws Exception {
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(validPrincipal, testJcrPath, JCR_READ);

        Tree policyTree = root.getTree(policy.getOakPath()).getChild(REP_PRINCIPAL_POLICY);
        TreeUtil.addChild(policyTree, "nonEntryChild", NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        policy.removeAccessControlEntry(policy.getEntries().get(0));
        policy.addEntry(testJcrPath, privilegesFromNames(REP_READ_NODES));
        acMgr.setPolicy(policy.getPath(), policy);

        policyTree = root.getTree(policy.getOakPath()).getChild(REP_PRINCIPAL_POLICY);
        assertFalse(policyTree.hasChild("nonEntryChild"));
        policy = (PrincipalPolicyImpl) acMgr.getPolicies(validPrincipal)[0];
        assertArrayEquals(privilegesFromNames(REP_READ_NODES), policy.getEntries().get(0).getPrivileges());
    }

    @Test(expected = AccessControlException.class)
    public void testRemoveInvalidPolicy() throws Exception {
        PrincipalAccessControlList policy = mock(PrincipalAccessControlList.class);
        acMgr.removePolicy(validPrincipal.getPath(), policy);
    }

    @Test(expected = AccessControlException.class)
    public void testRemoveEffectivePolicy() throws Exception {
        setupPrincipalBasedAccessControl(validPrincipal, testContentJcrPath, REP_WRITE);
        root.commit();

        ImmutablePrincipalPolicy effective = (ImmutablePrincipalPolicy) acMgr.getEffectivePolicies(Set.of(validPrincipal))[0];
        acMgr.removePolicy(effective.getPath(), effective);
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePolicyPathMismatch() throws Exception {
        PrincipalPolicyImpl policy = createValidPolicy();
        acMgr.removePolicy(PathUtils.getParentPath(policy.getPath()), policy);
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePolicyNullPath() throws Exception {
        PrincipalAccessControlList policy = mock(PrincipalAccessControlList.class);
        acMgr.removePolicy(null, policy);
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePolicyUnsupportedPath() throws Exception {
        String unsupportedJcrPath = getNamePathMapper().getJcrPath(PathUtils.getParentPath(SUPPORTED_PATH));
        acMgr.removePolicy(unsupportedJcrPath, createValidPolicy());
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePolicyTreeAlreadyRemoved() throws Exception {
        PrincipalPolicyImpl policy = createValidPolicy();
        acMgr.setPolicy(policy.getPath(), policy);

        // remove tree => then try to remove the policy
        root.getTree(policy.getOakPath()).getChild(REP_PRINCIPAL_POLICY).remove();
        acMgr.removePolicy(policy.getPath(), policy);
    }

    @Test
    public void testRemoveEmptyPolicy() throws Exception {
        PrincipalPolicyImpl policy = createValidPolicy();
        acMgr.setPolicy(policy.getPath(), policy);

        acMgr.removePolicy(policy.getPath(), policy);
        assertEquals(0, acMgr.getPolicies(validPrincipal).length);
        assertEquals(1, acMgr.getApplicablePolicies(validPrincipal).length);
        assertFalse(root.getTree(policy.getOakPath()).hasChild(REP_PRINCIPAL_POLICY));
    }

    @Test
    public void testRemovePolicy() throws Exception {
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(validPrincipal, getNamePathMapper().getJcrPath(UserConstants.DEFAULT_USER_PATH), PrivilegeConstants.REP_USER_MANAGEMENT);
        addPrincipalBasedEntry(policy, null, PrivilegeConstants.REP_PRIVILEGE_MANAGEMENT);

        acMgr.removePolicy(policy.getPath(), policy);
        assertEquals(0, acMgr.getPolicies(validPrincipal).length);
        assertEquals(1, acMgr.getApplicablePolicies(validPrincipal).length);
        assertFalse(root.getTree(policy.getOakPath()).hasChild(REP_PRINCIPAL_POLICY));
    }

    @Test
    public void testGetApplicableByPath() throws RepositoryException {
        assertFalse(acMgr.getApplicablePolicies(validPrincipal.getPath()).hasNext());
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetApplicableByNonExistingPath() throws RepositoryException {
        acMgr.getApplicablePolicies(testContentJcrPath);
    }

    @Test
    public void testGetPoliciesByPath() throws RepositoryException {
        assertEquals(0, acMgr.getPolicies(validPrincipal.getPath()).length);
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetPoliciesByNonExistingPath() throws RepositoryException {
        acMgr.getPolicies(testContentJcrPath);
    }

    @Test
    public void testGetEffectivePoliciesByPathNothingSet() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        assertEquals(0, acMgr.getEffectivePolicies(testContentJcrPath).length);
        assertEquals(0, acMgr.getEffectivePolicies((String) null).length);
    }

    @Test
    public void testGetEffectivePoliciesByPathTransientPolicy() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        setupPrincipalBasedAccessControl(validPrincipal, testContentJcrPath, JCR_VERSION_MANAGEMENT);

        assertEquals(0, acMgr.getEffectivePolicies(testContentJcrPath).length);
        assertEquals(0, acMgr.getEffectivePolicies(testJcrPath).length);
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetEffectivePoliciesByNonExistingPath() throws Exception {
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(validPrincipal, testContentJcrPath, JCR_VERSION_MANAGEMENT);
        addPrincipalBasedEntry(policy, PathUtils.ROOT_PATH, JCR_READ);
        root.commit();

        acMgr.getEffectivePolicies(testContentJcrPath);
    }

    @Test
    public void testGetEffectivePoliciesByPath() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(validPrincipal, testContentJcrPath, JCR_REMOVE_CHILD_NODES);
        addPrincipalBasedEntry(policy, PathUtils.ROOT_PATH, JCR_READ);
        root.commit();

        assertEffectivePolicies(acMgr.getEffectivePolicies(testJcrPath), 1,2, false);
        assertEffectivePolicies(acMgr.getEffectivePolicies(testContentJcrPath), 1, 2, false);
        assertEffectivePolicies(acMgr.getEffectivePolicies(PathUtils.ROOT_PATH), 1,1, false);
    }

    @Test
    public void testGetPolicyWithNonAceChild() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(validPrincipal, testContentJcrPath, JCR_RETENTION_MANAGEMENT);
        addPrincipalBasedEntry(policy, PathUtils.ROOT_PATH, JCR_READ);

        Tree policyTree = root.getTree(getNamePathMapper().getOakPath(policy.getPath())).getChild(REP_PRINCIPAL_POLICY);
        TreeUtil.addChild(policyTree, "nonAceChild", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        // don't commit as adding such child without extra mixins was invalid

        // read policy again -> must only contain 2 entries and ignore the invalid child
        policy = (PrincipalPolicyImpl) acMgr.getPolicies(validPrincipal)[0];
        assertEquals(2, policy.size());
    }

    @Test
    public void testGetPolicyMissingMixinType() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(validPrincipal, testContentJcrPath, JCR_LOCK_MANAGEMENT);
        addPrincipalBasedEntry(policy, PathUtils.ROOT_PATH, JCR_READ);

        Tree policyTree = root.getTree(getNamePathMapper().getOakPath(policy.getPath()));
        policyTree.removeProperty(JcrConstants.JCR_MIXINTYPES);
        // don't commit as access controlled node without mixin is invalid

        // read policy again -> princial policy must not be read due to missing mixin
        assertEquals(0, acMgr.getPolicies(validPrincipal).length);
    }

    @Test
    public void testHasPrivilegesByPrincipals() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        setupPrincipalBasedAccessControl(validPrincipal, testContentJcrPath, JCR_NODE_TYPE_MANAGEMENT);

        // setup default ac mgt only on subtree (testJcrPath)
        addDefaultEntry(testJcrPath, validPrincipal, JCR_NODE_TYPE_MANAGEMENT);
        root.commit();

        // priv is only granted where both models are granting.
        assertFalse(acMgr.hasPrivileges(testContentJcrPath, Set.of(validPrincipal), privilegesFromNames(JCR_NODE_TYPE_MANAGEMENT)));
        assertTrue(acMgr.hasPrivileges(testJcrPath, Set.of(validPrincipal), privilegesFromNames(JCR_NODE_TYPE_MANAGEMENT)));

        // set of principals not supported by principalbased-authorization => only default impl takes effect.
        assertFalse(acMgr.hasPrivileges(testContentJcrPath, Set.of(validPrincipal, EveryonePrincipal.getInstance()), privilegesFromNames(JCR_NODE_TYPE_MANAGEMENT)));
        assertTrue(acMgr.hasPrivileges(testJcrPath, Set.of(validPrincipal, EveryonePrincipal.getInstance()), privilegesFromNames(JCR_NODE_TYPE_MANAGEMENT)));
    }

    @Test
    public void testGetPrivilegesByPrincipals() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        setupPrincipalBasedAccessControl(validPrincipal, testContentJcrPath, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE);

        // grant different set of privs using the default ac mgt
        addDefaultEntry(testContentJcrPath, validPrincipal, JCR_READ, JCR_REMOVE_NODE);
        root.commit();

        // only subset is is granted
        assertPrivileges(acMgr.getPrivileges(testContentJcrPath, Set.of(validPrincipal)), JCR_REMOVE_NODE);
        assertPrivileges(acMgr.getPrivileges(testJcrPath, Set.of(validPrincipal)), JCR_REMOVE_NODE);

        // set of principals not supported by principalbased-authorization => only default impl takes effect.
        assertPrivileges(acMgr.getPrivileges(testContentJcrPath, Set.of(validPrincipal, EveryonePrincipal.getInstance())), JCR_READ, JCR_REMOVE_NODE);
        assertPrivileges(acMgr.getPrivileges(testJcrPath, Set.of(validPrincipal, EveryonePrincipal.getInstance())), JCR_READ, JCR_REMOVE_NODE);
    }

    private void assertPrivileges(@NotNull Privilege[] privs, @NotNull String... expectedOakPrivNames) throws Exception {
        assertEquals(Set.of(privilegesFromNames(expectedOakPrivNames)), Set.of(privs));
    }
    
    @Test
    public void testGetEffectivePrincipalsPaths() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        setupPrincipalBasedAccessControl(validPrincipal, testJcrPath, JCR_READ);
        root.commit();

        // unsupported principal
        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(Collections.singleton(getTestUser().getPrincipal()), testJcrPath);
        assertFalse(effective.hasNext());

        // non-matching paths
        effective = acMgr.getEffectivePolicies(Collections.singleton(validPrincipal), 
                PathUtils.ROOT_PATH, null, testContentJcrPath);
        assertFalse(effective.hasNext());
        
        // matching paths
        effective = acMgr.getEffectivePolicies(Collections.singleton(validPrincipal), testJcrPath);
        assertTrue(effective.hasNext());

        effective = acMgr.getEffectivePolicies(Collections.singleton(validPrincipal), testJcrPath + "/non/existing/child");
        assertTrue(effective.hasNext());
    }
    
    @Test
    public void testGetEffectivePrincipalsEmptyPaths() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        setupPrincipalBasedAccessControl(validPrincipal, testJcrPath, JCR_READ);
        root.commit();

        Set<Principal> principals = Collections.singleton(validPrincipal); 
        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(principals, new String[0]);
        AccessControlPolicy[] expected = acMgr.getEffectivePolicies(principals);
        
        assertArrayEquals(expected, ImmutableList.copyOf(effective).toArray(new AccessControlPolicy[0]));
    }

    @Test
    public void testGetEffectivePrincipalsReadablePaths() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        setupPrincipalBasedAccessControl(validPrincipal, testJcrPath, JCR_READ);
        root.commit();

        Set<String> readablePaths = getConfig(AuthorizationConfiguration.class).getParameters().getConfigValue(PermissionConstants.PARAM_READ_PATHS, PermissionConstants.DEFAULT_READ_PATHS);
        String[] paths = readablePaths.stream().map(oakPath -> namePathMapper.getJcrPath(oakPath)).distinct().toArray(String[]::new);
        assertEquals(3, paths.length);
        
        List<AccessControlPolicy> effective = ImmutableList.copyOf(acMgr.getEffectivePolicies(Collections.singleton(validPrincipal), paths));

        assertEquals(1, effective.size());
        assertEquals(ReadPolicy.INSTANCE, effective.get(0));
    }
}
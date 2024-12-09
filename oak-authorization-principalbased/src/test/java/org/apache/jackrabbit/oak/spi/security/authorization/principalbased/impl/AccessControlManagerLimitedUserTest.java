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
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ImmutableACL;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ReadPolicy;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_ENTRY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_EFFECTIVE_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_POLICY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for PrincipalBasedAccessControlManager where the editing session (based on a regular user with default
 * permission evaluation) lacks permissions to read/modify access control on the target system-principal.
 */
public class AccessControlManagerLimitedUserTest extends AbstractPrincipalBasedTest implements PrivilegeConstants {

    Principal systemPrincipal;
    String systemPrincipalPath;

    Principal testPrincipal;
    Root testRoot;
    JackrabbitAccessControlManager testAcMgr;

    @Before
    public void before() throws Exception {
        super.before();

        User systemUser = getTestSystemUser();
        systemPrincipalPath = systemUser.getPath();
        systemPrincipal = getTestSystemUser().getPrincipal();

        testPrincipal = createTestPrincipal();

        setupContentTrees(TEST_OAK_PATH);

        // grant test-user full read access (but not read-access control!)
        grant(testPrincipal, PathUtils.ROOT_PATH, JCR_READ);

        // trigger creation of principal policy with testPrincipal with 2 random entries
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(systemPrincipal, testContentJcrPath, JCR_NODE_TYPE_MANAGEMENT);
        addPrincipalBasedEntry(policy, null, JCR_NAMESPACE_MANAGEMENT);
        root.commit();

        testRoot = createTestRoot();
        testAcMgr = createAccessControlManager(testRoot);
    }

    @Override
    public void after() throws Exception {
        try {
            if (testRoot != null) {
                testRoot.getContentSession().close();
            }
        } finally {
            super.after();
        }
    }

    Principal createTestPrincipal() throws Exception {
        return getTestUser().getPrincipal();
    }

    Root createTestRoot() throws Exception {
        User testUser = getTestUser();
        return login(new SimpleCredentials(testUser.getID(), testUser.getID().toCharArray())).getLatestRoot();
    }

    void grant(@NotNull Principal principal, @Nullable String path, @NotNull String... privNames) throws Exception {
        addDefaultEntry(path, principal, privNames);
    }

    private static void assertEmptyPolicies(@NotNull AccessControlPolicy[] policies) {
        assertEquals(0, policies.length);
    }

    private static void assertPolicies(@NotNull AccessControlPolicy[] policies, Class<? extends JackrabbitAccessControlList> expectedClass, int expectedSize, int expectedEntrySize) {
        assertEquals(expectedSize, policies.length);
        if (expectedSize > 0) {
            assertTrue(expectedClass.isAssignableFrom(policies[0].getClass()));
            assertEquals(expectedEntrySize, ((JackrabbitAccessControlList) policies[0]).size());
        }
    }

    @NotNull
    private String getPolicyPath() throws Exception {
        JackrabbitAccessControlPolicy[] policies = createAccessControlManager(root).getPolicies(systemPrincipal);
        assertEquals(1, policies.length);
        return PathUtils.concat(((PrincipalPolicyImpl) policies[0]).getOakPath(), REP_PRINCIPAL_POLICY);
    }

    @NotNull
    private String getEntryPath() throws Exception {
        Tree policyTree = root.getTree(getPolicyPath());
        assertTrue(policyTree.exists());

        for (Tree child : policyTree.getChildren()) {
            if (Utils.isPrincipalEntry(child)) {
                return child.getPath();
            }
        }
        throw new RepositoryException("unable to locate policy entry");
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetApplicableByPath() throws RepositoryException {
        testAcMgr.getApplicablePolicies(testJcrPath);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetPoliciesByPath() throws RepositoryException {
        testAcMgr.getPolicies(testJcrPath);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetEffectiveByPathNoAccess() throws RepositoryException {
        testAcMgr.getEffectivePolicies(testJcrPath);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetEffectiveByNullPath() throws RepositoryException {
        testAcMgr.getEffectivePolicies((String) null);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetEffectiveByRooyPath() throws RepositoryException {
        testAcMgr.getEffectivePolicies(PathUtils.ROOT_PATH);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetEffectiveByPathReadAccessControlOnPrincipal() throws Exception {
        // grant testuser read-access control on testPrincipal-path but NOT on effective paths null and /oak:content
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        // since default permission evaluation is in charge for 'testUser' -> access to full principal policy is now
        // granted
        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(testJcrPath);
        assertEquals(1, effective.length);
        assertTrue(effective[0] instanceof PrincipalPolicyImpl);
    }

    @Test
    public void testGetEffectiveByPathMissingReadAccessControlOnPrincipal() throws Exception {
        // test-user: granted read-access-control on effective null-path
        grant(testPrincipal, null, JCR_READ_ACCESS_CONTROL);
        // test-user: granted read-access-control on effective /oak:content
        grant(testPrincipal, PathUtils.getAncestorPath(testJcrPath, 3), JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        assertEmptyPolicies(testAcMgr.getEffectivePolicies((String) null));
        assertEmptyPolicies(testAcMgr.getEffectivePolicies(testJcrPath));
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetApplicableByPrincipalNoAccess() throws RepositoryException {
        testAcMgr.getApplicablePolicies(systemPrincipal);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetPoliciesByPrincipalNoAccess() throws RepositoryException {
        testAcMgr.getPolicies(systemPrincipal);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetEffectiveByPrincipalNoAccess() throws RepositoryException {
        testAcMgr.getEffectivePolicies(Set.of(systemPrincipal));
    }

    @Test
    public void testGetPoliciesByPrincipal() throws Exception {
        // grant testuser read-access control on testPrincipal-path but NOT on effective paths null and /oak:content
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        // no read-ac permission on effective paths
        assertPolicies(testAcMgr.getPolicies(systemPrincipal), PrincipalPolicyImpl.class, 1, 2);

        // grant testuser read-access control on /oak:content
        grant(testPrincipal, testContentJcrPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();
        assertPolicies(testAcMgr.getPolicies(systemPrincipal), PrincipalPolicyImpl.class, 1, 2);

        // additionally grant testuser read-access control on null
        grant(testPrincipal, null, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();
        assertPolicies(testAcMgr.getPolicies(systemPrincipal), PrincipalPolicyImpl.class, 1, 2);
    }

    @Test
    public void testGetEffectiveByPrincipal() throws Exception {
        // grant testuser read-access control on testPrincipal-path but NOT on effective paths null and /oak:content
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        // read-access-control is only granted for the principalpolicy itself
        assertPolicies(testAcMgr.getEffectivePolicies(Set.of(systemPrincipal)), ImmutableACL.class, 1, 2);

        // grant testuser read-access control on /oak:content
        grant(testPrincipal, testContentJcrPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();
        assertPolicies(testAcMgr.getEffectivePolicies(Set.of(systemPrincipal)), ImmutableACL.class, 1, 2);

        // additionally grant testuser read-access control on null
        grant(testPrincipal, null, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();
        assertPolicies(testAcMgr.getEffectivePolicies(Set.of(systemPrincipal)), ImmutableACL.class, 1, 2);

        // make sure test-user has read-accesscontrol permission on any readable-path
        grant(testPrincipal, PathUtils.ROOT_PATH, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(Set.of(systemPrincipal));;
        assertPolicies(effective, ImmutableACL.class, 2, 2);
        assertTrue(effective[1] instanceof ReadPolicy);
    }

    @Test(expected = AccessDeniedException.class)
    public void testSetPolicyMissingModifyAccessControlOnPrincipal() throws Exception {
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        PrincipalPolicyImpl policy = (PrincipalPolicyImpl) testAcMgr.getPolicies(systemPrincipal)[0];
        policy.addEntry(null, privilegesFromNames(JCR_WORKSPACE_MANAGEMENT));

        testAcMgr.setPolicy(policy.getPath(), policy);
    }

    @Test(expected = AccessDeniedException.class)
    public void testSetPolicyMissingModifyAccessControlOnEffectivePath() throws Exception {
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        PrincipalPolicyImpl policy = (PrincipalPolicyImpl) testAcMgr.getPolicies(systemPrincipal)[0];
        policy.addEntry(null, privilegesFromNames(JCR_WORKSPACE_MANAGEMENT));
        policy.addEntry(testJcrPath, privilegesFromNames(JCR_READ));

        testAcMgr.setPolicy(policy.getPath(), policy);
    }

    @Test(expected = AccessDeniedException.class)
    public void testSetPolicyMissingModifyAccessControlOnEffectivePath2() throws Exception {
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        grant(testPrincipal, testContentJcrPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        PrincipalPolicyImpl policy = (PrincipalPolicyImpl) testAcMgr.getPolicies(systemPrincipal)[0];
        policy.addEntry(null, privilegesFromNames(JCR_WORKSPACE_MANAGEMENT));
        policy.addEntry(testJcrPath, privilegesFromNames(JCR_READ));

        testAcMgr.setPolicy(policy.getPath(), policy);
    }

    @Test
    public void testSetPolicy() throws Exception {
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        grant(testPrincipal, testContentJcrPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        grant(testPrincipal, null, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        PrincipalPolicyImpl policy = (PrincipalPolicyImpl) testAcMgr.getPolicies(systemPrincipal)[0];
        policy.addEntry(null, privilegesFromNames(JCR_WORKSPACE_MANAGEMENT));
        policy.addEntry(testJcrPath, privilegesFromNames(JCR_READ));

        testAcMgr.setPolicy(policy.getPath(), policy);
        testRoot.commit();
    }

    @Test(expected = AccessDeniedException.class)
    public void testRemovePolicyMissingModifyAccessControlOnPrincipal() throws Exception {
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        PrincipalPolicyImpl policy = (PrincipalPolicyImpl) testAcMgr.getPolicies(systemPrincipal)[0];
        testAcMgr.removePolicy(policy.getPath(), policy);
    }

    @Test(expected = AccessDeniedException.class)
    public void testRemovePolicyMissingModifyAccessControlOnEffectivePath() throws Exception {
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        PrincipalPolicyImpl policy = (PrincipalPolicyImpl) testAcMgr.getPolicies(systemPrincipal)[0];
        testAcMgr.removePolicy(policy.getPath(), policy);
    }

    @Test(expected = AccessDeniedException.class)
    public void testRemovePolicyMissingModifyAccessControlOnEffectivePath2() throws Exception {
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        grant(testPrincipal, testContentJcrPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        PrincipalPolicyImpl policy = (PrincipalPolicyImpl) testAcMgr.getPolicies(systemPrincipal)[0];
        testAcMgr.removePolicy(policy.getPath(), policy);
    }

    @Test
    public void testRemovePolicy() throws Exception {
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        grant(testPrincipal, null, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        grant(testPrincipal, testContentJcrPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        PrincipalPolicyImpl policy = (PrincipalPolicyImpl) testAcMgr.getPolicies(systemPrincipal)[0];
        testAcMgr.removePolicy(policy.getPath(), policy);
        testRoot.commit();
    }

    @Test
    public void testRemovePolicyWithNonEntryChild() throws Exception {
        // clear all entries from policy
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(systemPrincipal, getAccessControlManager(root));
        for (AccessControlEntry entry : policy.getAccessControlEntries()) {
            policy.removeAccessControlEntry(entry);
        }
        getAccessControlManager(root).setPolicy(policy.getPath(), policy);
        // grant permission to read/modify policy
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        // transiently add non-entry tree
        Tree policyTree = testRoot.getTree(getPolicyPath());
        assertFalse(policyTree.getChildren().iterator().hasNext());
        TreeUtil.addChild(policyTree, "nonEntry", NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        policy = getPrincipalPolicyImpl(systemPrincipal, testAcMgr);
        testAcMgr.removePolicy(policy.getPath(), policy);
        testRoot.commit();
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePolicyMissingEffectivePaths() throws Exception {
        // grant permission to read/modify policy
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        // first read policy
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(systemPrincipal, testAcMgr);

        // transiently remove rep:effectivePath properties from all entries.
        Tree policyTree = testRoot.getTree(getPolicyPath());
        for (Tree child : policyTree.getChildren()) {
            child.removeProperty(REP_EFFECTIVE_PATH);
        }

        // removing policy must fail, because effective paths cannot be evaluated
        testAcMgr.removePolicy(policy.getPath(), policy);
    }

    @Test(expected = AccessDeniedException.class)
    public void testHasPrivilegeSystemUser() throws Exception {
        // test session has no access
        testAcMgr.hasPrivileges(testContentJcrPath, Set.of(systemPrincipal), privilegesFromNames(JCR_NODE_TYPE_MANAGEMENT));
    }

    @Test(expected = AccessDeniedException.class)
    public void testHasPrivilegeSystemUserWithPartialReadAc() throws Exception {
        // grant read-ac access on principal policy (but not on targetPath)
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        testAcMgr.hasPrivileges(testContentJcrPath, Set.of(systemPrincipal), privilegesFromNames(JCR_NODE_TYPE_MANAGEMENT));
    }

    @Test
    public void testHasPrivilegeSystemUserWithPartialReadAc2() throws Exception {
        // grant read-ac access on effective path -> no entries accessible
        grant(testPrincipal, testContentJcrPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        assertFalse(testAcMgr.hasPrivileges(testContentJcrPath, Set.of(systemPrincipal), privilegesFromNames(JCR_NODE_TYPE_MANAGEMENT)));
    }

    @Test
    public void testHasPrivilegeSystemUserWithReadAc() throws Exception {
        // grant read-ac access on effective path and on principal policy
        grant(testPrincipal, testContentJcrPath, JCR_READ_ACCESS_CONTROL);
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        // default model lacks jcr:nodeTypeManagement privilege -> not granted
        assertFalse(testAcMgr.hasPrivileges(testContentJcrPath, Set.of(systemPrincipal), privilegesFromNames(JCR_NODE_TYPE_MANAGEMENT)));

        addDefaultEntry(testContentJcrPath, systemPrincipal, JCR_READ, JCR_NODE_TYPE_MANAGEMENT);
        root.commit();
        testRoot.refresh();

        // once default model grants permissions as well -> granted
        assertTrue(testAcMgr.hasPrivileges(testContentJcrPath, Set.of(systemPrincipal), privilegesFromNames(JCR_NODE_TYPE_MANAGEMENT)));

        // but combination read/nt-mgt is not granted because jcr:read is missing on principal-based setup.
        assertFalse(testAcMgr.hasPrivileges(testContentJcrPath, Set.of(systemPrincipal), privilegesFromNames(JCR_READ, JCR_NODE_TYPE_MANAGEMENT)));
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetPrivilegeSystemUser() throws Exception {
        // test session has no access
        testAcMgr.getPrivileges(null, Set.of(systemPrincipal));
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetPrivilegeSystemUserWithPartialReadAc() throws Exception {
        // grant read ac on principal policy but not on target path
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        testAcMgr.getPrivileges(null, Set.of(systemPrincipal));
    }

    @Test
    public void testGetPrivilegeSystemUserWithPartialReadAc2() throws Exception {
        // grant read-ac access on target path (but not on principal policy)
        grant(testPrincipal, null, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        assertEquals(0, testAcMgr.getPrivileges(null, Set.of(systemPrincipal)).length);
    }

    @Test
    public void testGetPrivilegeSystemUserWithWithReadAc() throws Exception {
        // grant read-ac access on effective path and on principal policy
        grant(testPrincipal, null, JCR_READ_ACCESS_CONTROL);
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        // default model lacks jcr:nodeTypeManagement privilege -> not granted
        assertArrayEquals(new Privilege[0], testAcMgr.getPrivileges(null, Set.of(systemPrincipal)));

        addDefaultEntry(null, systemPrincipal, JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT);
        root.commit();
        testRoot.refresh();

        // once default model grants namespace-mgt privilege as well -> granted
        // but not rep:privilegeMgt because the principal-based model doesn't grant that one
        assertArrayEquals(privilegesFromNames(JCR_NAMESPACE_MANAGEMENT), testAcMgr.getPrivileges(null, Set.of(systemPrincipal)));
    }

    @Test
    public void testReadPolicyTree() throws Exception {
        Tree policyTree = testRoot.getTree(getPolicyPath());
        assertNotNull(policyTree);
        assertFalse(policyTree.exists());
    }

    @Test
    public void testReadPolicyTreeWithReadAc() throws Exception {
        // grant read-ac access and check again
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        Tree policyTree = testRoot.getTree(getPolicyPath());
        assertNotNull(policyTree);
        assertTrue(policyTree.exists());
    }

    @Test
    public void testReadEntryTree() throws Exception {
        Tree entryTree = testRoot.getTree(getEntryPath());
        assertNotNull(entryTree);
        assertFalse(entryTree.exists());
    }

    @Test
    public void testReadEntryTreeWithReadAc() throws Exception {
        // grant read-ac access and check again
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        Tree entryTree = testRoot.getTree(getEntryPath());
        assertNotNull(entryTree);
        assertTrue(entryTree.exists());
    }

    @Test(expected = CommitFailedException.class)
    public void testAddEntryTree() throws Exception {
        // grant read-ac access on principal policy
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        try {
            Tree policyTree = testRoot.getTree(getPolicyPath());
            Tree entry = TreeUtil.addChild(policyTree, "entry", NT_REP_PRINCIPAL_ENTRY);
            entry.setProperty(REP_EFFECTIVE_PATH, TEST_OAK_PATH, Type.PATH);
            entry.setProperty(Constants.REP_PRIVILEGES, ImmutableList.of(JCR_ADD_CHILD_NODES), Type.NAMES);
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(3, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddEntryTreeModAcOnSystemPrincipal() throws Exception {
        // grant read-ac + mod-ac access on principal policy
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        try {
            Tree policyTree = testRoot.getTree(getPolicyPath());
            Tree entry = TreeUtil.addChild(policyTree, "entry", NT_REP_PRINCIPAL_ENTRY);
            entry.setProperty(REP_EFFECTIVE_PATH, TEST_OAK_PATH, Type.PATH);
            entry.setProperty(Constants.REP_PRIVILEGES, ImmutableList.of(JCR_ADD_CHILD_NODES), Type.NAMES);
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(3, e.getCode());
            throw e;
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testAddEntryTreeModAcOnEffectivePath() throws Exception {
        // grant read-ac + mod-ac access on effective path only -> cannot read principal policy
        grant(testPrincipal, testJcrPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        Tree policyTree = testRoot.getTree(getPolicyPath());
        Tree entry = TreeUtil.addChild(policyTree, "entry", NT_REP_PRINCIPAL_ENTRY);

    }

    @Test
    public void testAddEntryTreeFullModAc() throws Exception {
        grant(testPrincipal, testJcrPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        Tree policyTree = testRoot.getTree(getPolicyPath());
        Tree entry = TreeUtil.addChild(policyTree, "entry", NT_REP_PRINCIPAL_ENTRY);
        entry.setProperty(REP_EFFECTIVE_PATH, TEST_OAK_PATH, Type.PATH);
        entry.setProperty(Constants.REP_PRIVILEGES, ImmutableList.of(JCR_ADD_CHILD_NODES), Type.NAMES);
        testRoot.commit();
    }

    @Test(expected = CommitFailedException.class)
    public void testRemovePolicyTree() throws Exception {
        // grant read-ac access on principal policy
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        try {
            testRoot.getTree(getPolicyPath()).remove();
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(0, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRemovePolicyTreeWithModAcOnSystemPrincipal() throws Exception {
        // grant read-ac + mod-ac access on principal policy but not on target paths
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        try {
            testRoot.getTree(getPolicyPath()).remove();
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(3, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRemovePolicyTreeWithModAcOnOneEffectivePath() throws Exception {
        // grant read-ac + mod-ac access on principal policy and on testcontent target paths (but not on null path)
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        grant(testPrincipal, testContentJcrPath, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        try {
            testRoot.getTree(getPolicyPath()).remove();
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(3, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRemovePolicyTreeWithModAcOnOneEffectivePath2() throws Exception {
        // grant read-ac + mod-ac access on principal policy and on nul target paths (but not on testcontent path)
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        grant(testPrincipal, null, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        try {
            testRoot.getTree(getPolicyPath()).remove();
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(3, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveEmptyPolicyTree() throws Exception {
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(systemPrincipal, getAccessControlManager(root));
        for (AccessControlEntry entry : policy.getEntries()) {
            policy.removeAccessControlEntry(entry);
        }
        getAccessControlManager(root).setPolicy(policy.getPath(), policy);
        // grant permission to read policy
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);

        root.commit();
        testRoot.refresh();

        Tree policyTree = testRoot.getTree(getPolicyPath());
        assertTrue(policyTree.exists());
        policyTree.remove();
        testRoot.commit();
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveEntryTree() throws Exception {
        // grant read-ac access on principal policy
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        try {
            testRoot.getTree(getEntryPath()).remove();
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(3, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveEntryTreeModAcOnSystemPrincipal() throws Exception {
        // grant read-ac + mod-ac access on principal policy but not on target path
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        try {
            testRoot.getTree(getEntryPath()).remove();
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(3, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveEntryTreeModAcOnEffectivePath() throws Exception {
        // grant read-ac on principal policy but not mod-ac
        // on target path grant mod-ac
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL);
        grant(testPrincipal, testContentJcrPath, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        try {
            testRoot.getTree(getEntryPath()).remove();
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertEquals(CommitFailedException.ACCESS, e.getType());
            assertEquals(0, e.getCode());
            throw e;
        }
    }

    @Test
    public void testRemoveEntryTreeFullModAc() throws Exception {
        // grant read-ac and mod-ac on principal policy
        // on target path grant mod-ac
        grant(testPrincipal, systemPrincipalPath, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        grant(testPrincipal, testContentJcrPath, JCR_MODIFY_ACCESS_CONTROL);
        root.commit();
        testRoot.refresh();

        testRoot.getTree(getEntryPath()).remove();
        testRoot.commit();
    }

    @Test
    public void testGetEffectivePrincipalsReadablePaths() throws Exception {
        Set<String> readablePaths = getConfig(AuthorizationConfiguration.class).getParameters().getConfigValue(PermissionConstants.PARAM_READ_PATHS, PermissionConstants.DEFAULT_READ_PATHS);
        String[] paths = readablePaths.stream().map(oakPath -> namePathMapper.getJcrPath(oakPath)).distinct().toArray(String[]::new);
        assertEquals(3, paths.length);

        // readpolicy must not be included if editing session doesn't have sufficient permission on readable paths.
        try {
            getAccessControlManager(testRoot).getEffectivePolicies(Collections.singleton(systemPrincipal), paths);
            fail("AccessDenied exception expected");
        } catch (AccessDeniedException e) {
            // success
        }
    }
}
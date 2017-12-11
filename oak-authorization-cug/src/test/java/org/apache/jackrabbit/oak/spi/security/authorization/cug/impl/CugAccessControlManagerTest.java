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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.NamedAccessControlPolicy;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CugAccessControlManagerTest extends AbstractCugTest {

    private CugAccessControlManager cugAccessControlManager;

    @Override
    public void before() throws Exception {
        super.before();

        cugAccessControlManager = new CugAccessControlManager(root, NamePathMapper.DEFAULT, getSecurityProvider(), ImmutableSet.copyOf(SUPPORTED_PATHS));
    }

    private CugPolicy createCug(@Nonnull String path) {
        return new CugPolicyImpl(path, NamePathMapper.DEFAULT, getPrincipalManager(root), ImportBehavior.ABORT);
    }

    private CugPolicy getApplicableCug(@Nonnull String path) throws RepositoryException {
        return (CugPolicy) cugAccessControlManager.getApplicablePolicies(path).nextAccessControlPolicy();
    }

    @Test
    public void testGetSupportedPrivileges() throws Exception {
        Privilege[] readPrivs = privilegesFromNames(PrivilegeConstants.JCR_READ);
        Map<String, Privilege[]> pathMap = ImmutableMap.of(
                SUPPORTED_PATH, readPrivs,
                SUPPORTED_PATH + "/subtree", readPrivs,
                UNSUPPORTED_PATH, new Privilege[0],
                NodeTypeConstants.NODE_TYPES_PATH, new Privilege[0]
        );

        for (String path : pathMap.keySet()) {
            Privilege[] expected = pathMap.get(path);
            assertArrayEquals(expected, cugAccessControlManager.getSupportedPrivileges(path));
        }
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetSupportedPrivilegesInvalidPath() throws Exception {
        cugAccessControlManager.getSupportedPrivileges(INVALID_PATH);
    }

    @Test
    public void testGetApplicablePolicies() throws Exception {
        AccessControlPolicyIterator it = cugAccessControlManager.getApplicablePolicies(SUPPORTED_PATH);
        assertTrue(it.hasNext());

        AccessControlPolicy policy = cugAccessControlManager.getApplicablePolicies(SUPPORTED_PATH).nextAccessControlPolicy();
        assertTrue(policy instanceof CugPolicyImpl);

    }

    @Test
    public void testGetApplicablePoliciesAfterSet() throws Exception {
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, getApplicableCug(SUPPORTED_PATH));
        AccessControlPolicyIterator it = cugAccessControlManager.getApplicablePolicies(SUPPORTED_PATH);
        assertFalse(it.hasNext());
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetApplicablePoliciesInvalidPath() throws Exception {
        cugAccessControlManager.getApplicablePolicies(INVALID_PATH);
    }

    @Test
    public void testGetApplicablePoliciesUnsupportedPath() throws Exception {
        AccessControlPolicyIterator it = cugAccessControlManager.getApplicablePolicies(UNSUPPORTED_PATH);
        assertFalse(it.hasNext());
    }

    @Test
    public void testGetApplicablePoliciesNullPath() throws Exception {
        AccessControlPolicyIterator it = cugAccessControlManager.getApplicablePolicies((String) null);
        assertFalse(it.hasNext());
    }

    @Test
    public void testGetPolicies() throws Exception {
        AccessControlPolicy[] policies = cugAccessControlManager.getPolicies(SUPPORTED_PATH);
        assertEquals(0, policies.length);
    }

    @Test
    public void testGetPoliciesAfterSet() throws Exception {
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, getApplicableCug(SUPPORTED_PATH));

        AccessControlPolicy[] policies = cugAccessControlManager.getPolicies(SUPPORTED_PATH);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof CugPolicyImpl);
    }

    @Test
    public void testGetPoliciesAfterManualCreation() throws Exception {
        NodeUtil n = new NodeUtil(root.getTree(SUPPORTED_PATH));
        NodeUtil cug = n.addChild(REP_CUG_POLICY, NT_REP_CUG_POLICY);

        AccessControlPolicy[] policies = cugAccessControlManager.getPolicies(SUPPORTED_PATH);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof CugPolicy);
        CugPolicy cugPolicy = (CugPolicy) policies[0];
        assertTrue(cugPolicy.getPrincipals().isEmpty());

        cug.setStrings(REP_PRINCIPAL_NAMES, "unknownPrincipalName", EveryonePrincipal.NAME);

        policies = cugAccessControlManager.getPolicies(SUPPORTED_PATH);
        cugPolicy = (CugPolicy) policies[0];
        assertEquals(2, cugPolicy.getPrincipals().size());
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetPoliciesInvalidPath() throws Exception {
        cugAccessControlManager.getPolicies(INVALID_PATH);
    }

    @Test
    public void testGetPoliciesUnsupportedPath() throws Exception {
        AccessControlPolicy[] policies = cugAccessControlManager.getPolicies(UNSUPPORTED_PATH);
        assertEquals(0, policies.length);
    }

    @Test
    public void testGetPoliciesNullPath() throws Exception {
        AccessControlPolicy[] policies = cugAccessControlManager.getPolicies((String) null);
        assertEquals(0, policies.length);
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        AccessControlPolicy[] policies = cugAccessControlManager.getEffectivePolicies(SUPPORTED_PATH);
        assertEquals(0, policies.length);

        cugAccessControlManager.setPolicy(SUPPORTED_PATH, createCug(SUPPORTED_PATH));
        root.commit();

        policies = cugAccessControlManager.getEffectivePolicies(SUPPORTED_PATH);
        assertEquals(1, policies.length);

        AccessControlPolicy[] effectiveOnChild = cugAccessControlManager.getEffectivePolicies(SUPPORTED_PATH + "/subtree");
        assertEquals(1, policies.length);

        assertEquals(policies.length, effectiveOnChild.length);
        assertEquals(((JackrabbitAccessControlPolicy) policies[0]).getPath(), ((JackrabbitAccessControlPolicy) effectiveOnChild[0]).getPath());
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetEffectivePoliciesInvalidPath() throws Exception {
        cugAccessControlManager.getEffectivePolicies(INVALID_PATH);
    }

    @Test
    public void testGetEffectivePoliciesUnsupportedPath() throws Exception {
        AccessControlPolicy[] policies = cugAccessControlManager.getEffectivePolicies(UNSUPPORTED_PATH);
        assertEquals(0, policies.length);
    }

    @Test
    public void testGetEffectivePoliciesNullPath() throws Exception {
        AccessControlPolicy[] policies = cugAccessControlManager.getEffectivePolicies((String) null);
        assertEquals(0, policies.length);
    }

    @Test
    public void testGetEffectivePoliciesNotEnabled() throws Exception {
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, createCug(SUPPORTED_PATH));
        root.commit();

        ConfigurationParameters config = ConfigurationParameters.of(AuthorizationConfiguration.NAME, ConfigurationParameters.of(
                    CugConstants.PARAM_CUG_SUPPORTED_PATHS, SUPPORTED_PATHS,
                    CugConstants.PARAM_CUG_ENABLED, false));
        CugAccessControlManager acMgr = new CugAccessControlManager(root, NamePathMapper.DEFAULT, CugSecurityProvider.newTestSecurityProvider(config), ImmutableSet.copyOf(SUPPORTED_PATHS));
        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(SUPPORTED_PATH);
        assertEquals(0, policies.length);

        AccessControlPolicy[] effectiveOnChild = acMgr.getEffectivePolicies(SUPPORTED_PATH + "/subtree");
        assertEquals(0, policies.length);

        assertEquals(policies.length, effectiveOnChild.length);
    }

    @Test
    public void testSetPolicy() throws Exception {
        CugPolicy cug = getApplicableCug(SUPPORTED_PATH);
        cug.addPrincipals(EveryonePrincipal.getInstance());

        cugAccessControlManager.setPolicy(SUPPORTED_PATH, cug);
        AccessControlPolicy[] policies = cugAccessControlManager.getPolicies(SUPPORTED_PATH);
        assertEquals(1, policies.length);
        AccessControlPolicy policy = policies[0];
        assertTrue(policy instanceof CugPolicyImpl);
        Set<Principal> principals = ((CugPolicy) policy).getPrincipals();
        assertEquals(1, principals.size());
        assertEquals(EveryonePrincipal.getInstance(), principals.iterator().next());
    }


    @Test
    public void testSetPolicyPersisted() throws Exception {
        CugPolicy cug = getApplicableCug(SUPPORTED_PATH);
        cug.addPrincipals(EveryonePrincipal.getInstance());
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, cug);
        root.commit();

        Tree tree = root.getTree(SUPPORTED_PATH);
        assertTrue(TreeUtil.isNodeType(tree, CugConstants.MIX_REP_CUG_MIXIN, root.getTree(NodeTypeConstants.NODE_TYPES_PATH)));
        Tree cugTree = tree.getChild(CugConstants.REP_CUG_POLICY);
        assertTrue(cugTree.exists());
        assertEquals(CugConstants.NT_REP_CUG_POLICY, TreeUtil.getPrimaryTypeName(cugTree));

        PropertyState prop = cugTree.getProperty(CugConstants.REP_PRINCIPAL_NAMES);
        assertNotNull(prop);
        assertTrue(prop.isArray());
        assertEquals(Type.STRINGS, prop.getType());
        assertEquals(1, prop.count());
        assertEquals(EveryonePrincipal.NAME, prop.getValue(Type.STRING, 0));
    }

    @Test
    public void testSetInvalidPolicy() throws Exception {
        List<AccessControlPolicy> invalidPolicies = ImmutableList.of(
                new AccessControlPolicy() {},
                new NamedAccessControlPolicy() {
                    public String getName() {
                        return "name";
                    }
                },
                InvalidCug.INSTANCE
        );

        for (AccessControlPolicy policy : invalidPolicies) {
            try {
                cugAccessControlManager.setPolicy(SUPPORTED_PATH, policy);
                fail("Invalid cug policy must be detected.");
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    @Test(expected = PathNotFoundException.class)
    public void testSetPolicyInvalidPath() throws Exception {
        cugAccessControlManager.setPolicy(INVALID_PATH, createCug(INVALID_PATH));
    }

    @Test(expected = AccessControlException.class)
    public void testSetPolicyUnsupportedPath() throws Exception {
        cugAccessControlManager.setPolicy(UNSUPPORTED_PATH, createCug(UNSUPPORTED_PATH));
    }

    @Test(expected = AccessControlException.class)
    public void testSetPolicyPathMismatch() throws Exception {
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, createCug(SUPPORTED_PATH + "/subtree"));
    }

    @Test(expected = AccessControlException.class)
    public void testSetInvalidCugNode() throws Exception {
        Tree supportedTree = root.getTree(SUPPORTED_PATH);
        new NodeUtil(supportedTree).addChild(REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        cugAccessControlManager.setPolicy(SUPPORTED_PATH, new CugPolicyImpl(SUPPORTED_PATH, NamePathMapper.DEFAULT, getPrincipalManager(root), ImportBehavior.BESTEFFORT));
    }

    @Test
    public void testRemovePolicy() throws Exception {
        CugPolicy cug = getApplicableCug(SUPPORTED_PATH);
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, cug);
        cugAccessControlManager.removePolicy(SUPPORTED_PATH, cugAccessControlManager.getPolicies(SUPPORTED_PATH)[0]);

        assertArrayEquals(new AccessControlPolicy[0], cugAccessControlManager.getPolicies(SUPPORTED_PATH));
    }

    @Test
    public void testRemovePolicyPersisted() throws Exception {
        CugPolicy cug = getApplicableCug(SUPPORTED_PATH);
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, cug);
        root.commit();
        cugAccessControlManager.removePolicy(SUPPORTED_PATH, cugAccessControlManager.getPolicies(SUPPORTED_PATH)[0]);
        root.commit();

        Tree tree = root.getTree(SUPPORTED_PATH);
        assertFalse(tree.hasChild(CugConstants.REP_CUG_POLICY));
    }

    @Test
    public void testRemovePolicyRemovesMixin() throws Exception {
        ReadOnlyNodeTypeManager ntMgr = ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT);

        CugPolicy cug = getApplicableCug(SUPPORTED_PATH);
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, cug);
        root.commit();

        assertTrue(ntMgr.isNodeType(root.getTree(SUPPORTED_PATH), MIX_REP_CUG_MIXIN));

        cugAccessControlManager.removePolicy(SUPPORTED_PATH, cugAccessControlManager.getPolicies(SUPPORTED_PATH)[0]);
        root.commit();

        assertFalse(ntMgr.isNodeType(root.getTree(SUPPORTED_PATH), MIX_REP_CUG_MIXIN));
    }

    @Test
    public void testRemoveInvalidPolicy() throws Exception {
        List<AccessControlPolicy> invalidPolicies = ImmutableList.of(
                new AccessControlPolicy() {},
                new NamedAccessControlPolicy() {
                    public String getName() {
                        return "name";
                    }
                },
                InvalidCug.INSTANCE
        );

        for (AccessControlPolicy policy : invalidPolicies) {
            try {
                cugAccessControlManager.removePolicy(SUPPORTED_PATH, policy);
                fail("Invalid cug policy must be detected.");
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    @Test(expected = AccessControlException.class)
    public void testRemoveInvalidCugNode() throws Exception {
        Tree supportedTree = root.getTree(SUPPORTED_PATH);
        new NodeUtil(supportedTree).addChild(REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        cugAccessControlManager.removePolicy(SUPPORTED_PATH, new CugPolicyImpl(SUPPORTED_PATH, NamePathMapper.DEFAULT, getPrincipalManager(root), ImportBehavior.BESTEFFORT));
    }

    @Test(expected = PathNotFoundException.class)
    public void testRemovePolicyInvalidPath() throws Exception {
        cugAccessControlManager.removePolicy(INVALID_PATH, createCug(INVALID_PATH));
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePolicyUnsupportedPath() throws Exception {
        cugAccessControlManager.removePolicy(UNSUPPORTED_PATH, createCug(UNSUPPORTED_PATH));
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePolicyPathMismatch() throws Exception {
        cugAccessControlManager.removePolicy(SUPPORTED_PATH, createCug(SUPPORTED_PATH + "/subtree"));
    }

    @Test
    public void testGetApplicablePoliciesByPrincipal() throws Exception {
        AccessControlPolicy[] applicable = cugAccessControlManager.getApplicablePolicies(EveryonePrincipal.getInstance());
        assertNotNull(applicable);
        assertEquals(0, applicable.length);
    }

    @Test
    public void testGetPoliciesByPrincipal() throws Exception {
        AccessControlPolicy[] applicable = cugAccessControlManager.getPolicies(EveryonePrincipal.getInstance());
        assertNotNull(applicable);
        assertEquals(0, applicable.length);
    }

    @Test
    public void testGetEffectivePoliciesByPrincipal() throws Exception {
        AccessControlPolicy[] applicable = cugAccessControlManager.getEffectivePolicies(ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()));
        assertNotNull(applicable);
        assertEquals(0, applicable.length);
    }

    /**
     * An invalid (unsupported) implementation of {@link CugPolicy}.
     */
    private static final class InvalidCug implements CugPolicy {

        private static final InvalidCug INSTANCE = new InvalidCug();

        @Nonnull
        @Override
        public Set<Principal> getPrincipals() {
            return Collections.emptySet();
        }

        @Override
        public boolean addPrincipals(@Nonnull Principal... principals) {
            return false;
        }

        @Override
        public boolean removePrincipals(@Nonnull Principal... principals) {
            return false;
        }

        @Override
        public String getPath() {
            return null;
        }
    }
}
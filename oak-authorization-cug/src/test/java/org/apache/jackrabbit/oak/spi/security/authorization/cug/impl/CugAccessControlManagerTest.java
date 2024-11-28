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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.NamedAccessControlPolicy;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.jackrabbit.oak.api.Type.NAMES;
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

        cugAccessControlManager = new CugAccessControlManager(root, NamePathMapper.DEFAULT, getSecurityProvider(), ImmutableSet.copyOf(SUPPORTED_PATHS), getExclude(), getRootProvider());
    }

    private CugPolicy createCug(@NotNull String path) {
        return new CugPolicyImpl(path, NamePathMapper.DEFAULT, getPrincipalManager(root), ImportBehavior.ABORT, getExclude());
    }

    private CugPolicy getApplicableCug() throws RepositoryException {
        return (CugPolicy) cugAccessControlManager.getApplicablePolicies(AbstractCugTest.SUPPORTED_PATH).nextAccessControlPolicy();
    }

    @Test
    public void testGetSupportedPrivileges() throws Exception {
        Privilege[] readPrivs = privilegesFromNames(PrivilegeConstants.JCR_READ);
        Map<String, Privilege[]> pathMap = Map.of(
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

    @Test
    public void testGetSupportedPrivilegesNullPath() throws Exception {
        assertArrayEquals(new Privilege[0], cugAccessControlManager.getSupportedPrivileges(null));
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
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, getApplicableCug());
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
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, getApplicableCug());

        AccessControlPolicy[] policies = cugAccessControlManager.getPolicies(SUPPORTED_PATH);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof CugPolicyImpl);
    }

    @Test
    public void testGetPoliciesAfterManualCreation() throws Exception {
        Tree n = root.getTree(SUPPORTED_PATH);
        Tree cug = TreeUtil.addChild(n, REP_CUG_POLICY, NT_REP_CUG_POLICY);

        AccessControlPolicy[] policies = cugAccessControlManager.getPolicies(SUPPORTED_PATH);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof CugPolicy);
        CugPolicy cugPolicy = (CugPolicy) policies[0];
        assertTrue(cugPolicy.getPrincipals().isEmpty());

        cug.setProperty(REP_PRINCIPAL_NAMES, ImmutableList.of("unknownPrincipalName", EveryonePrincipal.NAME), Type.STRINGS);

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
        CugAccessControlManager acMgr = new CugAccessControlManager(root, NamePathMapper.DEFAULT, CugSecurityProvider.newTestSecurityProvider(config), ImmutableSet.copyOf(SUPPORTED_PATHS), getExclude(), getRootProvider());
        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(SUPPORTED_PATH);
        assertEquals(0, policies.length);

        AccessControlPolicy[] effectiveOnChild = acMgr.getEffectivePolicies(SUPPORTED_PATH + "/subtree");
        assertEquals(0, policies.length);

        assertEquals(policies.length, effectiveOnChild.length);
    }

    @Test(expected = AccessControlException.class)
    public void testGetEffectivePoliciesNoReadAcPermission() throws Exception {
        setupCugsAndAcls();
        // test-user only has read-access on /content (no read-ac permission)
        try (ContentSession cs = createTestSession()) {
            Root r = cs.getLatestRoot();
            CugAccessControlManager m = new CugAccessControlManager(r, NamePathMapper.DEFAULT, getSecurityProvider(), ImmutableSet.copyOf(SUPPORTED_PATHS), getExclude(), getRootProvider());
            AccessControlPolicy[] effective = m.getEffectivePolicies("/content");
            assertEquals(0, effective.length);
        }
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetEffectivePoliciesNoReadPermission() throws Exception {
        setupCugsAndAcls();
        // test-user only has read-access on /content (no read-ac permission)
        try (ContentSession cs = createTestSession()) {
            Root r = cs.getLatestRoot();
            CugAccessControlManager m = new CugAccessControlManager(r, NamePathMapper.DEFAULT, getSecurityProvider(), ImmutableSet.copyOf(SUPPORTED_PATHS), getExclude(), getRootProvider());
            m.getEffectivePolicies("/content2");
        }
    }

    @Test
    public void testGetEffectivePoliciesLimitedReadAcPermission() throws Exception {
        setupCugsAndAcls();

        // test-user2 only has read-ac permission on /content but not on /content2
        try (ContentSession cs = createTestSession2()) {
            Root r = cs.getLatestRoot();
            CugAccessControlManager m = new CugAccessControlManager(r, NamePathMapper.DEFAULT, getSecurityProvider(), ImmutableSet.copyOf(SUPPORTED_PATHS), getExclude(), getRootProvider());
            AccessControlPolicy[] effective = m.getEffectivePolicies("/content/a/b/c");
            // [/content/a, /content/a/b/c]
            assertEquals(2, effective.length);
        }
    }

    @Test
    public void testSetPolicy() throws Exception {
        CugPolicy cug = getApplicableCug();
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
        CugPolicy cug = getApplicableCug();
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
                (NamedAccessControlPolicy) () -> "name",
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
        TreeUtil.addChild(supportedTree, REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        cugAccessControlManager.setPolicy(SUPPORTED_PATH, new CugPolicyImpl(SUPPORTED_PATH, NamePathMapper.DEFAULT, getPrincipalManager(root), ImportBehavior.BESTEFFORT, getExclude()));
    }

    @Test
    public void testSetPolicyMixinAlreadyPresent() throws Exception {
        CugPolicy cug = getApplicableCug();
        cug.addPrincipals(EveryonePrincipal.getInstance());

        TreeUtil.addMixin(root.getTree(SUPPORTED_PATH), MIX_REP_CUG_MIXIN, root.getTree(NODE_TYPES_PATH), "uid");

        cugAccessControlManager.setPolicy(SUPPORTED_PATH, cug);
        root.commit();

        ReadOnlyNodeTypeManager ntMgr = ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT);
        assertTrue(ntMgr.isNodeType(root.getTree(SUPPORTED_PATH), MIX_REP_CUG_MIXIN));
    }

    @Test
    public void testResetPolicy() throws Exception {
        CugPolicy cug = getApplicableCug();
        cug.addPrincipals(EveryonePrincipal.getInstance());
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, cug);

        cug = (CugPolicy) cugAccessControlManager.getPolicies(SUPPORTED_PATH)[0];
        assertTrue(cug.removePrincipals(EveryonePrincipal.getInstance()));
        assertTrue(cug.getPrincipals().isEmpty());

        Principal testprincipal = getTestGroupPrincipal();
        cug.addPrincipals(testprincipal);

        cugAccessControlManager.setPolicy(SUPPORTED_PATH, cug);

        cug = (CugPolicy) cugAccessControlManager.getPolicies(SUPPORTED_PATH)[0];
        assertEquals(Set.of(testprincipal), cug.getPrincipals());
    }

    @Test
    public void testRemovePolicy() throws Exception {
        CugPolicy cug = getApplicableCug();
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, cug);
        cugAccessControlManager.removePolicy(SUPPORTED_PATH, cugAccessControlManager.getPolicies(SUPPORTED_PATH)[0]);

        assertArrayEquals(new AccessControlPolicy[0], cugAccessControlManager.getPolicies(SUPPORTED_PATH));
    }

    @Test
    public void testRemovePolicyPersisted() throws Exception {
        CugPolicy cug = getApplicableCug();
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

        CugPolicy cug = getApplicableCug();
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, cug);
        root.commit();

        assertTrue(ntMgr.isNodeType(root.getTree(SUPPORTED_PATH), MIX_REP_CUG_MIXIN));

        cugAccessControlManager.removePolicy(SUPPORTED_PATH, cugAccessControlManager.getPolicies(SUPPORTED_PATH)[0]);
        root.commit();

        assertFalse(ntMgr.isNodeType(root.getTree(SUPPORTED_PATH), MIX_REP_CUG_MIXIN));
    }

    @Test
    public void testRemovePolicyMixinAlreadyRemoved() throws Exception {
        ReadOnlyNodeTypeManager ntMgr = ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT);

        CugPolicy cug = getApplicableCug();
        cugAccessControlManager.setPolicy(SUPPORTED_PATH, cug);
        root.commit();

        Tree tree = root.getTree(SUPPORTED_PATH);
        Set<String> mixins = CollectionUtils.toSet(TreeUtil.getNames(tree, NodeTypeConstants.JCR_MIXINTYPES));
        mixins.remove(MIX_REP_CUG_MIXIN);
        tree.setProperty(JcrConstants.JCR_MIXINTYPES, mixins, NAMES);

        assertFalse(ntMgr.isNodeType(root.getTree(SUPPORTED_PATH), MIX_REP_CUG_MIXIN));

        cugAccessControlManager.removePolicy(SUPPORTED_PATH, cug);
        root.commit();
    }

    @Test
    public void testRemoveInvalidPolicy() throws Exception {
        List<AccessControlPolicy> invalidPolicies = ImmutableList.of(
                new AccessControlPolicy() {},
                (NamedAccessControlPolicy) () -> "name",
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
        TreeUtil.addChild(supportedTree, REP_CUG_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        cugAccessControlManager.removePolicy(SUPPORTED_PATH, new CugPolicyImpl(SUPPORTED_PATH, NamePathMapper.DEFAULT, getPrincipalManager(root), ImportBehavior.BESTEFFORT, getExclude()));
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
    public void testGetApplicablePoliciesByPrincipal() {
        AccessControlPolicy[] applicable = cugAccessControlManager.getApplicablePolicies(EveryonePrincipal.getInstance());
        assertNotNull(applicable);
        assertEquals(0, applicable.length);
    }

    @Test
    public void testGetPoliciesByPrincipal() {
        AccessControlPolicy[] applicable = cugAccessControlManager.getPolicies(EveryonePrincipal.getInstance());
        assertNotNull(applicable);
        assertEquals(0, applicable.length);
    }


    @Test
    public void testGetEffectiveByPrincipalNoCugPresent() {
        AccessControlPolicy[] effective = cugAccessControlManager.getEffectivePolicies(Collections.singleton(EveryonePrincipal.getInstance()));
        assertEquals(0, effective.length);
    }

    @Test
    public void testGetEffectiveByPrincipalTransientChanges() throws Exception {
        Principal p = getTestGroupPrincipal();
        createCug(SUPPORTED_PATH, p);
        createCug(SUPPORTED_PATH + "/subtree", p);

        AccessControlPolicy[] effective = cugAccessControlManager.getEffectivePolicies(Collections.singleton(p));
        assertEquals(0, effective.length);
    }

    @Test
    public void testGetEffectiveByPrincipal() throws Exception {
        Principal p = getTestGroupPrincipal();
        createCug(SUPPORTED_PATH, p);
        createCug(SUPPORTED_PATH + "/subtree", p);
        root.commit();

        Principal na = new PrincipalImpl("testNA");
        Set<Principal> pp = Set.of(p, na);
        AccessControlPolicy[] effective = cugAccessControlManager.getEffectivePolicies(pp);
        assertEquals(2, effective.length);

        AccessControlPolicy policy = effective[0];
        assertTrue(policy instanceof CugPolicy);
        assertEquals(Collections.singleton(p), ((CugPolicy) policy).getPrincipals());
        assertEquals(SUPPORTED_PATH, ((CugPolicy) policy).getPath());

        AccessControlPolicy policy2 = effective[1];
        assertTrue(policy2 instanceof CugPolicy);
        assertEquals(Collections.singleton(p), ((CugPolicy) policy2).getPrincipals());
        assertEquals(SUPPORTED_PATH + "/subtree", ((CugPolicy) policy2).getPath());
    }

    @Test
    public void testGetEffectiveByPrincipalNotListed() throws Exception {
        createCug(SUPPORTED_PATH + "/subtree", getTestGroupPrincipal());
        root.commit();

        Principal na = new PrincipalImpl("testNA");
        AccessControlPolicy[] effective = cugAccessControlManager.getEffectivePolicies(Set.of(na));
        assertEquals(0, effective.length);
    }

    @Test(expected = AccessControlException.class)
    public void testGetEffectiveByPrincipalIsImmutable() throws Exception {
        Principal p = getTestGroupPrincipal();
        createCug(SUPPORTED_PATH, p);
        root.commit();

        Principal na = new PrincipalImpl("testNA");
        Set<Principal> pp = Set.of(p, na);
        AccessControlPolicy[] effective = cugAccessControlManager.getEffectivePolicies(pp);
        assertEquals(1, effective.length);
        assertTrue(effective[0] instanceof CugPolicy);
        // modifying policy must fail
        ((CugPolicy) effective[0]).addPrincipals(EveryonePrincipal.getInstance());
    }

    @Test
    public void testGetEffectivePrincipalDistributed() throws Exception {
        // [/content/a : testgroup, /content/aa/bb : testgroup, /content/a/b/c : everyone, /content2: everyone]
        setupCugsAndAcls();

        AccessControlPolicy[] effective = cugAccessControlManager.getEffectivePolicies(Set.of(getTestGroupPrincipal(), EveryonePrincipal.getInstance()));
        assertEquals(4, effective.length);

        AccessControlPolicy[] everyoneEffective = cugAccessControlManager.getEffectivePolicies(Set.of(EveryonePrincipal.getInstance()));
        assertEquals(2, everyoneEffective.length);

        AccessControlPolicy[] testgroupEffective = cugAccessControlManager.getEffectivePolicies(Set.of(getTestGroupPrincipal()));
        assertEquals(2, testgroupEffective.length);

        assertTrue(Sets.intersection(ImmutableSet.copyOf(everyoneEffective), ImmutableSet.copyOf(testgroupEffective)).isEmpty());
    }

    @Test
    public void testGetEffectivePrincipalNoReadAcPermission() throws Exception {
        setupCugsAndAcls();
        // test-user only has read-access on /content (no read-ac permission)
        try (ContentSession cs = createTestSession()) {
            Root r = cs.getLatestRoot();
            CugAccessControlManager m = new CugAccessControlManager(r, NamePathMapper.DEFAULT, getSecurityProvider(),
                    ImmutableSet.copyOf(SUPPORTED_PATHS), getExclude(), getRootProvider());
            AccessControlPolicy[] effective = m.getEffectivePolicies(Set.of(getTestGroupPrincipal(), EveryonePrincipal.getInstance()));
            assertEquals(0, effective.length);
        }
    }

    @Test
    public void testGetEffectivePrincipalLimitedReadAcPermission() throws Exception {
        setupCugsAndAcls();

        // test-user3 only has read-ac permission on /content but not on /content2
        try (ContentSession cs = createTestSession2()) {
            Root r = cs.getLatestRoot();
            CugAccessControlManager m = new CugAccessControlManager(r, NamePathMapper.DEFAULT, getSecurityProvider(),
                    ImmutableSet.copyOf(SUPPORTED_PATHS), getExclude(), getRootProvider());
            AccessControlPolicy[] effective = m.getEffectivePolicies(Set.of(getTestGroupPrincipal(), EveryonePrincipal.getInstance()));
            // [/content/a, /content/a/b/c, /content/aa/bb] but not: /content2
            assertEquals(3, effective.length);
        }
    }

    @Test
    public void tesetGetEffectiveByPrincipalRootIncludedIsSupportedPath() throws Exception {
        setupCugsAndAcls();

        CugAccessControlManager acMgr = new CugAccessControlManager(root, NamePathMapper.DEFAULT, getSecurityProvider(), Set.of(PathUtils.ROOT_PATH), getExclude(), getRootProvider());
        assertEquals(4, acMgr.getEffectivePolicies(Set.of(getTestGroupPrincipal(), EveryonePrincipal.getInstance())).length);
    }
    
    @Test
    public void testGetEffectiveEmptyPrincipals() throws Exception {
        setupCugsAndAcls();

        Iterator<AccessControlPolicy> effectiveByPrincipalsPaths = cugAccessControlManager.getEffectivePolicies(Collections.emptySet(), new String[0]);
        assertFalse(effectiveByPrincipalsPaths.hasNext());

        AccessControlPolicy[] effectiveByPrincipals = cugAccessControlManager.getEffectivePolicies(Collections.emptySet());
        assertEquals(0, effectiveByPrincipals.length);
    }
    
    @Test
    public void testGetEffectiveByPrincipalNotEnabled() throws Exception {
        setupCugsAndAcls();

        ConfigurationParameters config = ConfigurationParameters.of(AuthorizationConfiguration.NAME, ConfigurationParameters.of(
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, SUPPORTED_PATHS,
                CugConstants.PARAM_CUG_ENABLED, false));
        CugAccessControlManager acMgr = new CugAccessControlManager(root, NamePathMapper.DEFAULT, CugSecurityProvider.newTestSecurityProvider(config), ImmutableSet.copyOf(SUPPORTED_PATHS), getExclude(), getRootProvider());
        assertEquals(0, acMgr.getEffectivePolicies(Set.of(getTestGroupPrincipal(), EveryonePrincipal.getInstance())).length);
    }
    
    @Test
    public void testGetEffectivePrincipalsEmptyPaths() throws Exception {
        setupCugsAndAcls();

        Set<Principal> principalSet = Collections.singleton(getTestGroupPrincipal());
        Iterator<AccessControlPolicy> effective = cugAccessControlManager.getEffectivePolicies(principalSet, new String[0]);
        assertTrue(effective.hasNext());
        assertEquivalentCugs(cugAccessControlManager.getEffectivePolicies(principalSet), Iterators.toArray(effective, AccessControlPolicy.class));
    }

    @Test
    public void testGetEffectivePrincipalsNullPaths() throws Exception {
        setupCugsAndAcls();

        Set<Principal> principalSet = Collections.singleton(getTestGroupPrincipal());
        String[] paths = null;
        Iterator<AccessControlPolicy> effective = cugAccessControlManager.getEffectivePolicies(principalSet, paths);
        assertTrue(effective.hasNext());
        assertEquivalentCugs(cugAccessControlManager.getEffectivePolicies(principalSet), Iterators.toArray(effective, AccessControlPolicy.class));
    }

    @Test
    public void testGetEffectivePrincipalsPathsDisabled() throws Exception {
        setupCugsAndAcls();

        Set<Principal> principalSet = Collections.singleton(getTestGroupPrincipal());

        ConfigurationParameters config = ConfigurationParameters.of(AuthorizationConfiguration.NAME, ConfigurationParameters.of(
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, SUPPORTED_PATHS,
                CugConstants.PARAM_CUG_ENABLED, false));
        CugAccessControlManager acMgr = new CugAccessControlManager(root, NamePathMapper.DEFAULT, CugSecurityProvider.newTestSecurityProvider(config), ImmutableSet.copyOf(SUPPORTED_PATHS), getExclude(), getRootProvider());

        Iterator<AccessControlPolicy> effective = acMgr.getEffectivePolicies(principalSet, "/content/a");
        assertFalse(effective.hasNext());
    }

    @Test
    public void testGetEffectivePrincipalsDuplicatePath() throws Exception {
        setupCugsAndAcls();

        String cugPath = "/content/a";
        Set<Principal> principalSet = Collections.singleton(getTestGroupPrincipal());
        Iterator<AccessControlPolicy> effective = cugAccessControlManager.getEffectivePolicies(principalSet, cugPath, cugPath);
        assertTrue(effective.hasNext());
        assertEquivalentCugs(cugAccessControlManager.getEffectivePolicies(cugPath), Iterators.toArray(effective, AccessControlPolicy.class));
    }

    @Test
    public void testGetEffectivePrincipalsUnsupportedPath() throws Exception {
        setupCugsAndAcls();

        Set<Principal> principalSet = Collections.singleton(getTestGroupPrincipal());
        Iterator<AccessControlPolicy> effective = cugAccessControlManager.getEffectivePolicies(principalSet, UNSUPPORTED_PATH);
        assertFalse(effective.hasNext());
    }

    @Test
    public void testGetEffectivePrincipalsPropertyPath() throws Exception {
        setupCugsAndAcls();

        String cugPath = "/content/a";
        Set<Principal> principalSet = Collections.singleton(getTestGroupPrincipal());
        Iterator<AccessControlPolicy> effective = cugAccessControlManager.getEffectivePolicies(principalSet, cugPath+"/jcr:primaryType");
        AccessControlPolicy[] expected = Iterators.toArray(cugAccessControlManager.getEffectivePolicies(principalSet, cugPath), AccessControlPolicy.class);
        assertEquivalentCugs(expected, Iterators.toArray(effective, AccessControlPolicy.class));
    }

    @Test
    public void testGetEffectivePrincipalsNullPath() throws Exception {
        setupCugsAndAcls();

        Set<Principal> principalSet = Collections.singleton(getTestGroupPrincipal());
        Iterator<AccessControlPolicy> effective = cugAccessControlManager.getEffectivePolicies(principalSet, new String[] {null});
        assertFalse(effective.hasNext());
    }

    @Test
    public void testGetEffectivePrincipalsPathsMissingPermission() throws Exception {
        setupCugsAndAcls();

        // test-user only has read-access on /content (no read-ac permission)
        try (ContentSession cs = createTestSession()) {
            Root r = cs.getLatestRoot();
            CugAccessControlManager m = new CugAccessControlManager(r, NamePathMapper.DEFAULT, getSecurityProvider(), ImmutableSet.copyOf(SUPPORTED_PATHS), getExclude(), getRootProvider());
            Iterator<AccessControlPolicy> effective = m.getEffectivePolicies(Set.of(getTestGroupPrincipal()), "/content/a");
            assertFalse(effective.hasNext());
        }
    }

    @Test
    public void testDefinesPathMismatch() {
        assertFalse(cugAccessControlManager.defines(null, createCug(SUPPORTED_PATH)));
        assertFalse(cugAccessControlManager.defines(SUPPORTED_PATH2, createCug(SUPPORTED_PATH)));
    }

    @Test
    public void testDefinesUnsupportedPolicy() {
        assertFalse(cugAccessControlManager.defines(InvalidCug.INSTANCE.getPath(), InvalidCug.INSTANCE));
    }

    @Test
    public void testDefines() {
        CugPolicy cugPolicy = createCug(SUPPORTED_PATH);
        assertTrue(cugAccessControlManager.defines(cugPolicy.getPath(), cugPolicy));
    }

    /**
     * An invalid (unsupported) implementation of {@link CugPolicy}.
     */
    private static final class InvalidCug implements CugPolicy {

        private static final InvalidCug INSTANCE = new InvalidCug();

        @NotNull
        @Override
        public Set<Principal> getPrincipals() {
            return Collections.emptySet();
        }

        @Override
        public boolean addPrincipals(@NotNull Principal... principals) {
            return false;
        }

        @Override
        public boolean removePrincipals(@NotNull Principal... principals) {
            return false;
        }

        @Nullable
        @Override
        public String getPath() {
            return null;
        }
    }
}

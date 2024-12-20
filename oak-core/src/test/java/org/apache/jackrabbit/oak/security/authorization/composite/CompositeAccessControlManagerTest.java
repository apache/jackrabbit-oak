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
package org.apache.jackrabbit.oak.security.authorization.composite;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregationFilter;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class CompositeAccessControlManagerTest extends AbstractSecurityTest {

    private static final String TEST_PATH = "/test";

    private CompositeAccessControlManager acMgr;

    @Override
    public void before() throws Exception {
        super.before();

        acMgr = createComposite(getAccessControlManager(root), new TestAcMgr());

        Tree tree = root.getTree(ROOT_PATH);
        TreeUtil.addChild(tree,"test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            root.getTree(TEST_PATH).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @NotNull
    private CompositeAccessControlManager createComposite(@NotNull AccessControlManager... acMgrs) {
        return createComposite(AggregationFilter.DEFAULT, acMgrs);
    }

    @NotNull
    private CompositeAccessControlManager createComposite(@NotNull AggregationFilter aggregationFilter, @NotNull AccessControlManager... acMgrs) {
        return new CompositeAccessControlManager(root, NamePathMapper.DEFAULT, getSecurityProvider(), ImmutableList.copyOf(acMgrs), aggregationFilter);
    }

    @Test
    public void testGetSupportedPrivileges() throws Exception {
        Set<Privilege> expected = Set.of(getPrivilegeManager(root).getRegisteredPrivileges());
        Set<Privilege> result = Set.of(acMgr.getSupportedPrivileges("/"));
        assertEquals(expected, result);

        result = CollectionUtils.toSet(acMgr.getSupportedPrivileges(TEST_PATH));
        assertTrue(result.containsAll(expected));
        assertTrue(result.contains(TestPrivilege.INSTANCE));
    }

    @Test
    public void testGetApplicablePolicies() throws Exception {
        AccessControlPolicyIterator it = acMgr.getApplicablePolicies("/");
        while (it.hasNext()) {
            if (it.nextAccessControlPolicy() == TestPolicy.INSTANCE) {
                fail("TestPolicy should only be applicable at /test.");
            }
        }

        Set<AccessControlPolicy> applicable = CollectionUtils.toSet(acMgr.getApplicablePolicies(TEST_PATH));
        assertEquals(2, applicable.size());
        assertTrue(applicable.contains(TestPolicy.INSTANCE));
    }

    @Test
    public void testGetApplicablePoliciesNotPolicyOwner() throws Exception {
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        AccessControlManager mgr = when(mock(AccessControlManager.class).getApplicablePolicies(anyString())).thenReturn(new AccessControlPolicyIteratorAdapter(Set.of(policy))).getMock();

        CompositeAccessControlManager composite = createComposite(mgr);
        AccessControlPolicyIterator it = composite.getApplicablePolicies(ROOT_PATH);
        assertFalse(it.hasNext());

        verify(mgr, never()).getApplicablePolicies(ROOT_PATH);
    }

    @Test
    public void testGetPolicies() throws Exception {
        int len = 0;
        AccessControlPolicy[] policies = acMgr.getPolicies(TEST_PATH);
        assertEquals(len, policies.length);

        acMgr.setPolicy(TEST_PATH, TestPolicy.INSTANCE);
        len++;

        policies = acMgr.getPolicies(TEST_PATH);

        assertEquals(len, policies.length);
        assertSame(TestPolicy.INSTANCE, policies[0]);

        AccessControlPolicyIterator it = acMgr.getApplicablePolicies(TEST_PATH);
        while (it.hasNext()) {
            AccessControlPolicy plc = it.nextAccessControlPolicy();
            if (plc == TestPolicy.INSTANCE) {
                fail("TestPolicy should only be applicable at /test.");
            } else {
                acMgr.setPolicy(TEST_PATH, plc);
                len++;

                Set<AccessControlPolicy> policySet = Set.of(acMgr.getPolicies(TEST_PATH));
                assertEquals(len, policySet.size());
                assertTrue(policySet.contains(TestPolicy.INSTANCE));
                assertTrue(policySet.contains(plc));
            }
        }
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(TEST_PATH);
        assertEquals(0, effective.length);

        AccessControlPolicyIterator it = acMgr.getApplicablePolicies(TEST_PATH);
        while (it.hasNext()) {
            AccessControlPolicy plc = it.nextAccessControlPolicy();
            if (plc instanceof AccessControlList) {
                ((AccessControlList) plc).addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_READ));
            }
            acMgr.setPolicy(TEST_PATH, plc);
        }
        root.commit();
        assertEquals(2, acMgr.getEffectivePolicies(TEST_PATH).length);

        Tree child = root.getTree(TEST_PATH).addChild("child");
        child.setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        assertEquals(1, acMgr.getEffectivePolicies(child.getPath()).length);
    }

    @Test
    public void testGetEffectivePoliciesFiltersDuplicates() throws Exception {
        TestAcMgr test = new TestAcMgr();
        test.hasPolicy = true;
        
        // create a composite that would result in duplicate effective policies
        AccessControlManager composite = createComposite(test, test);
        assertEquals(1, composite.getEffectivePolicies(TEST_PATH).length);
    }

    @Test
    public void testSetPolicyAtRoot() throws Exception {
        AccessControlPolicyIterator it = acMgr.getApplicablePolicies("/");
        int cnt = 0;
        while (it.hasNext()) {
            AccessControlPolicy plc = it.nextAccessControlPolicy();
            assertTrue(plc instanceof JackrabbitAccessControlList);
            acMgr.setPolicy("/", plc);
            cnt++;
        }
        assertEquals(1, cnt);
    }

    @Test
    public void testSetPolicyAtTestPath() throws Exception {
        AccessControlPolicyIterator it = acMgr.getApplicablePolicies(TEST_PATH);
        int cnt = 0;
        while (it.hasNext()) {
            AccessControlPolicy plc = it.nextAccessControlPolicy();
            acMgr.setPolicy(TEST_PATH, plc);
            cnt++;
        }
        assertEquals(2, cnt);
    }

    @Test(expected = AccessControlException.class)
    public void testSetPoliciesNotPolicyOwner() throws Exception {
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        AccessControlManager mgr = when(mock(AccessControlManager.class).getPolicies(anyString())).thenReturn(new AccessControlPolicy[] {policy}).getMock();

        try {
            CompositeAccessControlManager composite = createComposite(mgr);
            composite.setPolicy(ROOT_PATH, policy);
        } finally {
            verify(mgr, never()).setPolicy(ROOT_PATH, policy);
        }
    }

    @Test(expected = AccessControlException.class)
    public void testSetPoliciesPolicyOwnerPathNotDefined() throws Exception {
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        AccessControlManager mgr = mock(AccessControlManager.class, withSettings().extraInterfaces(PolicyOwner.class));
        when(mgr.getPolicies(anyString())).thenReturn(new AccessControlPolicy[] {policy}).getMock();
        when(((PolicyOwner) mgr).defines(anyString(), any(AccessControlPolicy.class))).thenReturn(false);

        try {
            CompositeAccessControlManager composite = createComposite(mgr);
            composite.setPolicy(ROOT_PATH, policy);
        } finally {
            verify(mgr, never()).setPolicy(ROOT_PATH, policy);
            verify(((PolicyOwner) mgr), times(1)).defines(ROOT_PATH, policy);
        }
    }

    @Test
    public void testRemovePolicy() throws Exception {
        AccessControlPolicyIterator it = acMgr.getApplicablePolicies(TEST_PATH);
        while (it.hasNext()) {
            AccessControlPolicy plc = it.nextAccessControlPolicy();
            acMgr.setPolicy(TEST_PATH, plc);
        }
        root.commit();

        acMgr.removePolicy(TEST_PATH, TestPolicy.INSTANCE);
        root.commit();

        assertEquals(1, acMgr.getPolicies(TEST_PATH).length);

        acMgr.removePolicy(TEST_PATH, acMgr.getPolicies(TEST_PATH)[0]);
        root.commit();

        assertEquals(0, acMgr.getPolicies(TEST_PATH).length);
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePoliciesNotPolicyOwner() throws Exception {
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        AccessControlManager mgr = when(mock(AccessControlManager.class).getPolicies(anyString())).thenReturn(new AccessControlPolicy[] {policy}).getMock();

        try {
            CompositeAccessControlManager composite = createComposite(mgr);
            composite.removePolicy(ROOT_PATH, policy);
        } finally {
            verify(mgr, never()).removePolicy(ROOT_PATH, policy);
        }
    }

    @Test(expected = AccessControlException.class)
    public void testRemovePoliciesPolicyOwnerPathNotDefined() throws Exception {
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        AccessControlManager mgr = mock(AccessControlManager.class, withSettings().extraInterfaces(PolicyOwner.class));
        when(mgr.getPolicies(anyString())).thenReturn(new AccessControlPolicy[] {policy}).getMock();
        when(((PolicyOwner) mgr).defines(anyString(), any(AccessControlPolicy.class))).thenReturn(false);

        try {
            CompositeAccessControlManager composite = createComposite(mgr);
            composite.removePolicy(ROOT_PATH, policy);
        } finally {
            verify(mgr, never()).removePolicy(ROOT_PATH, policy);
            verify(((PolicyOwner) mgr), times(1)).defines(ROOT_PATH, policy);
        }
    }

    @Test
    public void testGetApplicablePoliciesByPrincipalNotOwnerPolicy() throws Exception {
        JackrabbitAccessControlPolicy policy = mock(JackrabbitAccessControlPolicy.class);
        JackrabbitAccessControlManager mgr = mock(JackrabbitAccessControlManager.class);
        when(mgr.getApplicablePolicies(any(Principal.class))).thenReturn(new JackrabbitAccessControlPolicy[] {policy}).getMock();

        CompositeAccessControlManager composite = createComposite(mgr);
        JackrabbitAccessControlPolicy[] applicable = composite.getApplicablePolicies(EveryonePrincipal.getInstance());
        assertEquals(0, applicable.length);

        verify(mgr, never()).getApplicablePolicies(EveryonePrincipal.getInstance());
    }

    @Test
    public void testGetApplicablePoliciesByPrincipalNotJackrabbitAcMgr() throws Exception {
        AccessControlManager mgr = mock(AccessControlManager.class, withSettings().extraInterfaces(PolicyOwner.class));

        CompositeAccessControlManager composite = createComposite(mgr);
        JackrabbitAccessControlPolicy[] applicable = composite.getApplicablePolicies(EveryonePrincipal.getInstance());
        assertEquals(0, applicable.length);

        verifyNoInteractions(mgr);
    }

    @Test
    public void testGetApplicablePoliciesByPrincipal() throws Exception {
        JackrabbitAccessControlPolicy policy = mock(JackrabbitAccessControlPolicy.class);
        JackrabbitAccessControlManager mgr = mock(JackrabbitAccessControlManager.class, withSettings().extraInterfaces(PolicyOwner.class));
        when(mgr.getApplicablePolicies(any(Principal.class))).thenReturn(new JackrabbitAccessControlPolicy[] {policy}).getMock();

        CompositeAccessControlManager composite = createComposite(mgr);
        JackrabbitAccessControlPolicy[] applicable = composite.getApplicablePolicies(EveryonePrincipal.getInstance());
        assertArrayEquals(new JackrabbitAccessControlPolicy[] {policy}, applicable);

        verify(mgr, times(1)).getApplicablePolicies(EveryonePrincipal.getInstance());
    }

    @Test
    public void testGetPoliciesByPrincipalNotPolicyOwner() throws Exception {
        AccessControlManager mgr = mock(AccessControlManager.class);

        CompositeAccessControlManager composite = createComposite(mgr);
        assertEquals(0, composite.getPolicies(EveryonePrincipal.getInstance()).length);

        verifyNoInteractions(mgr);
    }

    @Test
    public void testGetPoliciesByPrincipalNotJackrabbitAcMgr() throws Exception {
        JackrabbitAccessControlPolicy policy = mock(JackrabbitAccessControlPolicy.class);
        JackrabbitAccessControlManager mgr = mock(JackrabbitAccessControlManager.class);
        when(mgr.getPolicies(any(Principal.class))).thenReturn(new JackrabbitAccessControlPolicy[] {policy}).getMock();

        CompositeAccessControlManager composite = createComposite(mgr);
        assertArrayEquals(new JackrabbitAccessControlPolicy[] {policy}, composite.getPolicies(EveryonePrincipal.getInstance()));

        verify(mgr, times(1)).getPolicies(EveryonePrincipal.getInstance());
    }

    @Test
    public void testGetPoliciesByPrincipal() throws Exception {
        JackrabbitAccessControlPolicy policy = mock(JackrabbitAccessControlPolicy.class);
        JackrabbitAccessControlManager mgr = mock(JackrabbitAccessControlManager.class, withSettings().extraInterfaces(PolicyOwner.class));
        when(mgr.getPolicies(any(Principal.class))).thenReturn(new JackrabbitAccessControlPolicy[] {policy}).getMock();

        CompositeAccessControlManager composite = createComposite(mgr);
        assertArrayEquals(new JackrabbitAccessControlPolicy[] {policy}, composite.getPolicies(EveryonePrincipal.getInstance()));

        verify(mgr, times(1)).getPolicies(EveryonePrincipal.getInstance());
    }

    @Test
    public void testEffectivePoliciesByPrincipalNotJackrabbitAcMgr() throws Exception {
        AccessControlManager mgr = mock(AccessControlManager.class);

        Set<Principal> principalSet = Set.of(EveryonePrincipal.getInstance());
        CompositeAccessControlManager composite = createComposite(mgr);
        assertEquals(0, composite.getEffectivePolicies(principalSet).length);

        verifyNoInteractions(mgr);
    }

    @Test
    public void testEffectivePoliciesByPrincipal() throws Exception {
        JackrabbitAccessControlPolicy policy = mock(JackrabbitAccessControlPolicy.class);
        JackrabbitAccessControlManager mgr = mock(JackrabbitAccessControlManager.class, withSettings().extraInterfaces(PolicyOwner.class));
        when(mgr.getEffectivePolicies(any(Set.class))).thenReturn(new JackrabbitAccessControlPolicy[] {policy});

        Set<Principal> principalSet = Set.of(EveryonePrincipal.getInstance());
        CompositeAccessControlManager composite = createComposite(mgr);
        assertArrayEquals(new JackrabbitAccessControlPolicy[] {policy}, composite.getEffectivePolicies(principalSet));

        verify(mgr, times(1)).getEffectivePolicies(principalSet);
    }

    @Test
    public void testEffectivePoliciesByPrincipalAndPathsNotJackrabbitAcMgr() throws Exception {
        AccessControlManager mgr = mock(AccessControlManager.class);

        Set<Principal> principalSet = Set.of(EveryonePrincipal.getInstance());
        CompositeAccessControlManager composite = createComposite(mgr);
        assertFalse(composite.getEffectivePolicies(principalSet, ROOT_PATH).hasNext());

        verifyNoInteractions(mgr);
    }

    @Test
    public void testEffectivePoliciesByPrincipalAndPaths() throws Exception {
        JackrabbitAccessControlPolicy policy = mock(JackrabbitAccessControlPolicy.class);
        JackrabbitAccessControlManager mgr = mock(JackrabbitAccessControlManager.class, withSettings().extraInterfaces(PolicyOwner.class));
        when(mgr.getEffectivePolicies(any(Set.class), anyString())).thenReturn(Iterators.singletonIterator(policy));

        Set<Principal> principalSet = Set.of(EveryonePrincipal.getInstance());
        CompositeAccessControlManager composite = createComposite(mgr);
        
        assertTrue(Iterators.elementsEqual(Iterators.singletonIterator(policy), composite.getEffectivePolicies(principalSet, ROOT_PATH)));

        verify(mgr, times(0)).getEffectivePolicies(principalSet);
        verify(mgr, times(1)).getEffectivePolicies(principalSet, ROOT_PATH);
    }

    @Test
    public void testAggregationFilterByPath() throws Exception {
        String matchingPath = TEST_PATH;

        JackrabbitAccessControlManager acMgr1 = mock(JackrabbitAccessControlManager.class, withSettings().extraInterfaces(PolicyOwner.class));
        JackrabbitAccessControlManager acMgr2 = mock(JackrabbitAccessControlManager.class, withSettings().extraInterfaces(PolicyOwner.class));
        for (JackrabbitAccessControlManager acMgr : new JackrabbitAccessControlManager[] {acMgr1, acMgr2}) {
            when(acMgr.getApplicablePolicies(anyString())).thenReturn(AccessControlPolicyIteratorAdapter.EMPTY);
            when(acMgr.getPolicies(anyString())).thenReturn(new AccessControlPolicy[0]);
            when(acMgr.getEffectivePolicies(anyString())).thenReturn(new AccessControlPolicy[0]);
        }

        AggregationFilter aggregationFilter = mock(AggregationFilter.class);
        when(aggregationFilter.stop(acMgr1, matchingPath)).thenReturn(true);

        CompositeAccessControlManager composite = createComposite(aggregationFilter, acMgr1, acMgr2);
        composite.getApplicablePolicies(TEST_PATH); // must not call AggregationFilter
        composite.getPolicies(TEST_PATH);           // must not call AggregationFilter
        composite.getEffectivePolicies(TEST_PATH);  // must call AggregationFilter

        verify(aggregationFilter, times(1)).stop(acMgr1, TEST_PATH);
        verify(aggregationFilter, never()).stop(acMgr2, TEST_PATH);

        verify(acMgr1, times(1)).getEffectivePolicies(TEST_PATH);
        verify(acMgr2, never()).getEffectivePolicies(TEST_PATH);

        composite.getEffectivePolicies(ROOT_PATH);  // must call AggregationFilter but not trigger stop

        verify(aggregationFilter, times(1)).stop(acMgr1, ROOT_PATH);
        verify(aggregationFilter, times(1)).stop(acMgr2, ROOT_PATH);

        verify(acMgr1, times(1)).getEffectivePolicies(ROOT_PATH);
        verify(acMgr2, times(1)).getEffectivePolicies(ROOT_PATH);
    }

    @Test
    public void testAggregationFilterByPrincipals() throws Exception {
        Principal principal = new PrincipalImpl("matchingPrincipal");
        Set<Principal> matchingSet = Set.of(principal);
        Set<Principal> notMatching = Set.of(EveryonePrincipal.getInstance());

        JackrabbitAccessControlManager acMgr1 = mock(JackrabbitAccessControlManager.class, withSettings().extraInterfaces(PolicyOwner.class));
        JackrabbitAccessControlManager acMgr2 = mock(JackrabbitAccessControlManager.class, withSettings().extraInterfaces(PolicyOwner.class));
        for (JackrabbitAccessControlManager acMgr : new JackrabbitAccessControlManager[] {acMgr1, acMgr2}) {
            when(acMgr.getApplicablePolicies(any(Principal.class))).thenReturn(new JackrabbitAccessControlPolicy[0]);
            when(acMgr.getPolicies(any(Principal.class))).thenReturn(new JackrabbitAccessControlPolicy[0]);
            when(acMgr.getEffectivePolicies(any(Set.class))).thenReturn(new JackrabbitAccessControlPolicy[0]);
        }

        AggregationFilter aggregationFilter = mock(AggregationFilter.class);
        when(aggregationFilter.stop(acMgr1, matchingSet)).thenReturn(true);

        CompositeAccessControlManager composite = createComposite(aggregationFilter, acMgr1, acMgr2);
        composite.getApplicablePolicies(principal);  // must not call AggregationFilter
        composite.getPolicies(principal);            // must not call AggregationFilter
        composite.getEffectivePolicies(matchingSet); // must call AggregationFilter

        verify(aggregationFilter, times(1)).stop(acMgr1, matchingSet);
        verify(aggregationFilter, never()).stop(acMgr2, matchingSet);

        verify(acMgr1, times(1)).getEffectivePolicies(matchingSet);
        verify(acMgr2, never()).getEffectivePolicies(matchingSet);

        composite.getEffectivePolicies(notMatching);  // must call AggregationFilter but not trigger stop

        verify(aggregationFilter, times(1)).stop(acMgr1, notMatching);
        verify(aggregationFilter, times(1)).stop(acMgr2, notMatching);

        verify(acMgr1, times(1)).getEffectivePolicies(notMatching);
        verify(acMgr2, times(1)).getEffectivePolicies(notMatching);
    }

    private final static class TestAcMgr implements AccessControlManager, PolicyOwner {

        private boolean hasPolicy = false;

        private final Privilege[] supported;

        private TestAcMgr() {
            this.supported = new Privilege[] {TestPrivilege.INSTANCE};
        }

        @Override
        public Privilege[] getSupportedPrivileges(String absPath) {
            if (TEST_PATH.equals(absPath)) {
                return supported;
            } else {
                return new Privilege[0];
            }
        }

        @Override
        public Privilege privilegeFromName(String privilegeName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasPrivileges(String absPath, Privilege[] privileges) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Privilege[] getPrivileges(String absPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AccessControlPolicy[] getPolicies(String absPath) {
            if (TEST_PATH.equals(absPath) && hasPolicy) {
                return TestPolicy.asPolicyArray();
            } else {
                return new AccessControlPolicy[0];
            }
        }

        @Override
        public AccessControlPolicy[] getEffectivePolicies(String absPath) {
            if (TEST_PATH.equals(absPath) && hasPolicy) {
                return TestPolicy.asPolicyArray();
            } else {
                return new AccessControlPolicy[0];
            }
        }

        @Override
        public AccessControlPolicyIterator getApplicablePolicies(String absPath) {
            if (TEST_PATH.equals(absPath) && !hasPolicy) {
                return new AccessControlPolicyIteratorAdapter(Collections.singleton(TestPolicy.INSTANCE));
            } else {
                return AccessControlPolicyIteratorAdapter.EMPTY;
            }
        }

        @Override
        public void setPolicy(String absPath, AccessControlPolicy policy) throws AccessControlException {
            if (hasPolicy || !TEST_PATH.equals(absPath) || policy != TestPolicy.INSTANCE)  {
                throw new AccessControlException();
            } else {
                hasPolicy = true;
            }
        }

        @Override
        public void removePolicy(String absPath, AccessControlPolicy policy) throws AccessControlException {
            if (!hasPolicy || !TEST_PATH.equals(absPath) || policy != TestPolicy.INSTANCE)  {
                throw new AccessControlException();
            } else {
                hasPolicy = false;
            }
        }

        //----------------------------------------------------< PolicyOwner >---
        @Override
        public boolean defines(String absPath, @NotNull AccessControlPolicy accessControlPolicy) {
            return TEST_PATH.equals(absPath) && accessControlPolicy == TestPolicy.INSTANCE;
        }
    }

    private static final class TestPolicy implements AccessControlPolicy {

        static final TestPolicy INSTANCE = new TestPolicy();

        static AccessControlPolicy[] asPolicyArray() {
            return new AccessControlPolicy[] {INSTANCE};
        }
    }

    private static final class TestPrivilege implements Privilege {

        static final String NAME = TestPrivilege.class.getName() + "-privilege";
        static final Privilege INSTANCE = new TestPrivilege();

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public boolean isAbstract() {
            return false;
        }

        @Override
        public boolean isAggregate() {
            return false;
        }

        @Override
        public Privilege[] getDeclaredAggregatePrivileges() {
            return new Privilege[0];
        }

        @Override
        public Privilege[] getAggregatePrivileges() {
            return new Privilege[0];
        }
    }
}

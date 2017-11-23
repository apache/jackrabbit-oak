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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompositeAccessControlManagerTest extends AbstractSecurityTest {

    private static final String TEST_PATH = "/test";

    private CompositeAccessControlManager acMgr;

    @Override
    public void before() throws Exception {
        super.before();

        List<AccessControlManager> acMgrs = ImmutableList.of(getAccessControlManager(root), new TestAcMgr());
        acMgr = new CompositeAccessControlManager(root, NamePathMapper.DEFAULT, getSecurityProvider(), acMgrs);

        NodeUtil node = new NodeUtil(root.getTree("/"));
        node.addChild("test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);

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

    @Test
    public void testGetSupportedPrivileges() throws Exception {
        Set<Privilege> expected = ImmutableSet.copyOf(getPrivilegeManager(root).getRegisteredPrivileges());
        Set<Privilege> result = ImmutableSet.copyOf(acMgr.getSupportedPrivileges("/"));
        assertEquals(expected, result);

        result = Sets.newHashSet(acMgr.getSupportedPrivileges(TEST_PATH));
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

        Set<AccessControlPolicy> applicable = ImmutableSet.copyOf(acMgr.getApplicablePolicies(TEST_PATH));
        assertEquals(2, applicable.size());
        assertTrue(applicable.contains(TestPolicy.INSTANCE));
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

                Set<AccessControlPolicy> policySet = ImmutableSet.copyOf(acMgr.getPolicies(TEST_PATH));
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
            acMgr.setPolicy(TEST_PATH, plc);
        }
        root.commit();
        assertEquals(2, acMgr.getEffectivePolicies(TEST_PATH).length);

        Tree child = root.getTree(TEST_PATH).addChild("child");
        child.setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        assertEquals(1, acMgr.getEffectivePolicies(child.getPath()).length);
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
        public boolean defines(String absPath, @Nonnull AccessControlPolicy accessControlPolicy) {
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
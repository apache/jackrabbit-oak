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
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
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
            root.getTree(TEST_PATH).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testGetSupportedPrivileges() throws Exception {
        Set<Privilege> expected = ImmutableSet.copyOf(getPrivilegeManager(root).getRegisteredPrivileges());
        Set<Privilege> result = ImmutableSet.copyOf(acMgr.getSupportedPrivileges(TEST_PATH));
        assertEquals(expected, result);
    }

    @Test
    public void testPrivilegeFromName() {
        // TODO
    }

    @Test
    public void testGetApplicablePolicies() throws Exception {
        AccessControlPolicyIterator it = acMgr.getApplicablePolicies("/");
        while (it.hasNext()) {
            if (it.nextAccessControlPolicy() == TestPolicy.INSTANCE) {
                fail("TestPolicy should only be applicable at /test.");
            }
        }

        boolean found = false;
        it = acMgr.getApplicablePolicies(TEST_PATH);
        while (!found && it.hasNext()) {
            found = (it.nextAccessControlPolicy() == TestPolicy.INSTANCE);
        }
        assertTrue(found);
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
    public void testGetEffectivePolicies() {
        // TODO
    }

    @Test
    public void testSetPolicy() {
        // TODO
    }

    @Test
    public void testRemovePolicy() {
        // TODO
    }


    private final static class TestAcMgr implements AccessControlManager, PolicyOwner {

        private boolean hasPolicy = false;

        @Override
        public Privilege[] getSupportedPrivileges(String absPath) {
            return new Privilege[0];
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
                return new AccessControlPolicy[] {TestPolicy.INSTANCE};
            } else {
                return new AccessControlPolicy[0];
            }
        }

        @Override
        public AccessControlPolicy[] getEffectivePolicies(String absPath) {
            if (TEST_PATH.equals(absPath) && hasPolicy) {
                return new AccessControlPolicy[] {TestPolicy.INSTANCE};
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

        private static final TestPolicy INSTANCE = new TestPolicy();
    }
}
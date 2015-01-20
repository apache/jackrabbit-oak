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
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner;
import org.junit.Test;

public class CompositeAccessControlManagerTest extends AbstractCompositeTest {

    private CompositeAccessControlManager acMgr;

    @Override
    public void before() throws Exception {
        super.before();

        List<AccessControlManager> acMgrs = ImmutableList.of(getAccessControlManager(root), new TestAcMgr());

        acMgr = new CompositeAccessControlManager(root, NamePathMapper.DEFAULT, getSecurityProvider(), acMgrs);
    }

    @Test
    public void testGetApplicablePolicies() {
        // TODO
    }

    @Test
    public void testGetPolicies() {
        // TODO
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

        private static final String PATH = "/test";

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
            return false;
        }

        @Override
        public Privilege[] getPrivileges(String absPath) {
            return new Privilege[0];
        }

        @Override
        public AccessControlPolicy[] getPolicies(String absPath) {
            if (PATH.equals(absPath)) {
                return new AccessControlPolicy[] {TestPolicy.INSTANCE};
            } else {
                return new AccessControlPolicy[0];
            }
        }

        @Override
        public AccessControlPolicy[] getEffectivePolicies(String absPath) {
            if (PATH.equals(absPath)) {
                return new AccessControlPolicy[] {TestPolicy.INSTANCE};
            } else {
                return new AccessControlPolicy[0];
            }
        }

        @Override
        public AccessControlPolicyIterator getApplicablePolicies(String absPath) {
            if (PATH.equals(absPath)) {
                return new AccessControlPolicyIteratorAdapter(Collections.singleton(TestPolicy.INSTANCE));
            } else {
                return AccessControlPolicyIteratorAdapter.EMPTY;
            }
        }

        @Override
        public void setPolicy(String absPath, AccessControlPolicy policy) {
            // TODO

        }

        @Override
        public void removePolicy(String absPath, AccessControlPolicy policy) {
            // TODO

        }

        //----------------------------------------------------< PolicyOwner >---
        @Override
        public boolean defines(String absPath, AccessControlPolicy accessControlPolicy) {
            // TODO
            return false;
        }
    }

    private static final class TestPolicy implements AccessControlPolicy {

        private static final TestPolicy INSTANCE = new TestPolicy();
    }
}
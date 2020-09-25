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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlPolicy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class NodeACLTest extends AbstractAccessControlTest {

    private ACL nodeAcl;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        JackrabbitAccessControlManager acMgr = getAccessControlManager(root);
        AccessControlList policy = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        policy.addAccessControlEntry(testPrincipal, testPrivileges);
        policy.addAccessControlEntry(EveryonePrincipal.getInstance(), testPrivileges);
        acMgr.setPolicy(TEST_PATH, policy);

        nodeAcl = getNodeAcl(acMgr);
    }

    @After
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    @NotNull
    private static ACL getNodeAcl(@NotNull JackrabbitAccessControlManager acMgr) throws RepositoryException {
        for (AccessControlPolicy acp : acMgr.getPolicies(AbstractAccessControlTest.TEST_PATH)) {
            if (acp instanceof ACL) {
                return (ACL) acp;
            }
        }
        throw new RuntimeException("no node acl found");
    }

    @Test
    public void testEquals() throws Exception {
        assertEquals(nodeAcl, nodeAcl);
        assertEquals(nodeAcl, getNodeAcl(getAccessControlManager(root)));
    }

    @Test
    public void testEqualsDifferentPath() throws Exception {
        getAccessControlManager(root);
        AccessControlList acl = AccessControlUtils.getAccessControlList(getAccessControlManager(root), null);
        acl.addAccessControlEntry(testPrincipal, testPrivileges);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), testPrivileges);
        assertNotEquals(nodeAcl, acl);
    }

    @Test
    public void testEqualsDifferentEntries() throws Exception {
        ACL acl = getNodeAcl(getAccessControlManager(root));
        acl.removeAccessControlEntry(acl.getAccessControlEntries()[0]);
        assertNotEquals(nodeAcl, acl);
    }

    @Test
    public void testEqualsDifferentAcessControlList() {
        assertNotEquals(nodeAcl, createACL(TEST_PATH, nodeAcl.getEntries(), getNamePathMapper(), getRestrictionProvider()));
    }

    @Test
    public void testHashCode() {
        assertEquals(0, nodeAcl.hashCode());
    }

}
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

import java.security.Principal;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlPolicy;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PrincipalACLTest extends AbstractAccessControlTest {

    private ACL principalAcl;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        JackrabbitAccessControlManager acMgr = getAccessControlManager(root);
        AccessControlList policy = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        policy.addAccessControlEntry(testPrincipal, testPrivileges);
        policy.addAccessControlEntry(EveryonePrincipal.getInstance(), testPrivileges);
        acMgr.setPolicy(TEST_PATH, policy);
        root.commit();

        principalAcl = getPrincipalAcl(acMgr, testPrincipal);
    }

    @Nonnull
    private static ACL getPrincipalAcl(@Nonnull JackrabbitAccessControlManager acMgr, @Nonnull Principal testPrincipal) throws RepositoryException {
        for (AccessControlPolicy acp : acMgr.getPolicies(testPrincipal)) {
            if (acp instanceof ACL) {
                return (ACL) acp;
            }
        }
        throw new RuntimeException("no principal acl found");
    }

    @Test(expected = UnsupportedRepositoryOperationException.class)
    public void testReorder() throws Exception {
        AccessControlEntry[] entries = principalAcl.getAccessControlEntries();
        principalAcl.orderBefore(entries[0], null);
    }

    @Test
    public void testEquals() throws Exception {
        assertEquals(principalAcl, principalAcl);
        assertEquals(principalAcl, getPrincipalAcl(getAccessControlManager(root), testPrincipal));
    }

    @Test
    public void testEqualsDifferentPrincipal() throws Exception {
        assertNotEquals(principalAcl, getPrincipalAcl(getAccessControlManager(root), EveryonePrincipal.getInstance()));
    }

    @Test
    public void testEqualsDifferentACL() throws Exception {
        assertNotEquals(principalAcl, AccessControlUtils.getAccessControlList(getAccessControlManager(root), TEST_PATH));
    }

    @Test
    public void testHashCode() {
        assertEquals(0, principalAcl.hashCode());
    }
}
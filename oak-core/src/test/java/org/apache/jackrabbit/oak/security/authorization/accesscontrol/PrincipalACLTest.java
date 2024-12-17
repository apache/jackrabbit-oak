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
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NODE_PATH;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_VERSION_MANAGEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PrincipalACLTest extends AbstractAccessControlTest {

    private ACL principalAcl;
    private Privilege[] privileges;

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
        privileges = privilegesFromNames(JCR_VERSION_MANAGEMENT);
    }

    @NotNull
    private static ACL getPrincipalAcl(@NotNull JackrabbitAccessControlManager acMgr, @NotNull Principal testPrincipal) throws RepositoryException {
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
    public void testEqualsDifferentPath() throws Exception {
        ACL acl = getPrincipalAcl(getAccessControlManager(root), new PrincipalImpl(testPrincipal.getName()));
        assertNotEquals(principalAcl, acl);
    }

    @Test
    public void testEqualsDifferentEntries() throws Exception {
        ValueFactory vf = getValueFactory(root);
        ACL acl = getPrincipalAcl(getAccessControlManager(root), testPrincipal);
        acl.addEntry(testPrincipal, privileges, true,
                Map.of(REP_GLOB, vf.createValue("/subtree/*"), REP_NODE_PATH, vf.createValue(TEST_PATH)));
        assertNotEquals(principalAcl, acl);
    }

    @Test
    public void testHashCode() {
        assertEquals(0, principalAcl.hashCode());
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryMissingNodePath() throws Exception  {
        principalAcl.addAccessControlEntry(testPrincipal, privileges);
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryDifferentPrincipal() throws Exception  {
        principalAcl.addEntry(EveryonePrincipal.getInstance(), privileges, true, Collections.singletonMap(REP_NODE_PATH, getValueFactory(root).createValue(TEST_PATH)));
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryNullPrincipal() throws Exception  {
        principalAcl.addEntry(null, privileges, true, Collections.singletonMap(REP_NODE_PATH, getValueFactory(root).createValue(TEST_PATH)));
    }
}

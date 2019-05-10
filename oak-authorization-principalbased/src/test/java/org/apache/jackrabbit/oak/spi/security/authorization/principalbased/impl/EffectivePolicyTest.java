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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ImmutableACL;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.security.AccessControlPolicy;
import java.security.Principal;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NT_NAMES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_WRITE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EffectivePolicyTest extends AbstractPrincipalBasedTest {

    private PrincipalBasedAccessControlManager acMgr;
    private Principal validPrincipal;
    private Principal validPrincipal2;
    private String jcrEffectivePath;

    @Before
    public void testBefore() throws Exception {
        super.before();

        setupContentTrees(TEST_OAK_PATH);
        jcrEffectivePath = PathUtils.getAncestorPath(getNamePathMapper().getJcrPath(TEST_OAK_PATH), 3);

        validPrincipal2 = getUserManager(root).createSystemUser("anotherValidPrincipal", INTERMEDIATE_PATH).getPrincipal();
        root.commit();

        acMgr = createAccessControlManager(root);
        validPrincipal = getTestSystemUser().getPrincipal();

        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(validPrincipal, jcrEffectivePath, JCR_READ, REP_WRITE);
        addPrincipalBasedEntry(policy, null, JCR_NAMESPACE_MANAGEMENT);

        policy = (PrincipalPolicyImpl) acMgr.getApplicablePolicies(validPrincipal2)[0];
        Map<String, Value> restrictions = ImmutableMap.of(getNamePathMapper().getJcrName(REP_GLOB), getValueFactory(root).createValue("/*/glob"));
        policy.addEntry(jcrEffectivePath, privilegesFromNames(JCR_READ), restrictions, ImmutableMap.of());

        String ntJcrName = getNamePathMapper().getJcrName(JcrConstants.NT_RESOURCE);
        Map<String, Value[]> mvRestrictions = ImmutableMap.of(getNamePathMapper().getJcrName(REP_NT_NAMES), new Value[] {getValueFactory(root).createValue(ntJcrName, PropertyType.NAME)});
        policy.addEntry(PathUtils.ROOT_PATH, privilegesFromNames(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT), ImmutableMap.of(), mvRestrictions);

        acMgr.setPolicy(policy.getPath(), policy);

        root.commit();
    }

    @Test
    public void testEffectivePolicyByPrincipal() throws Exception {
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(ImmutableSet.of(validPrincipal));
        assertEquals(1, effective.length);
        assertTrue(effective[0] instanceof ImmutableACL);

        List<JackrabbitAccessControlEntry> entries = ((ImmutableACL)effective[0]).getEntries();
        assertEquals(2, entries.size());

        assertTrue(entries.get(0) instanceof PrincipalAccessControlList.Entry);
        assertEquals(validPrincipal, entries.get(0).getPrincipal());
        assertArrayEquals(privilegesFromNames(JCR_READ, REP_WRITE), entries.get(0).getPrivileges());
        assertEquals(jcrEffectivePath, ((PrincipalAccessControlList.Entry) entries.get(0)).getEffectivePath());

        assertNull(((PrincipalAccessControlList.Entry) entries.get(1)).getEffectivePath());
    }

    @Test
    public void testEffectivePolicyByPath() throws Exception {
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(getNamePathMapper().getJcrPath(TEST_OAK_PATH));
        assertEquals(2, effective.length);

        for (AccessControlPolicy effectivePolicy : effective) {
            assertTrue(effectivePolicy instanceof ImmutableACL);

            ImmutableACL acl = (ImmutableACL) effectivePolicy;
            if (jcrEffectivePath.equals(acl.getPath())) {
                List<JackrabbitAccessControlEntry> entries = acl.getEntries();
                assertEquals(2, entries.size());

                for (JackrabbitAccessControlEntry entry : entries) {
                    if (validPrincipal.equals(entry.getPrincipal())) {
                        assertArrayEquals(privilegesFromNames(JCR_READ, REP_WRITE), entry.getPrivileges());
                        assertEquals(0, entry.getRestrictionNames().length);
                    } else {
                        assertEquals(validPrincipal2, entry.getPrincipal());
                        assertArrayEquals(privilegesFromNames(JCR_READ), entry.getPrivileges());
                        assertArrayEquals(new String[] {getNamePathMapper().getJcrName(REP_GLOB)}, entry.getRestrictionNames());
                    }
                }
            } else {
                assertEquals(PathUtils.ROOT_PATH, acl.getPath());

                List<JackrabbitAccessControlEntry> entries = acl.getEntries();
                assertEquals(1, entries.size());

                JackrabbitAccessControlEntry entry = entries.get(0);
                assertTrue(entry instanceof ACE);
                assertArrayEquals(privilegesFromNames(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT), entry.getPrivileges());
                assertEquals(1, ((ACE) entry).getRestrictions().size());
                assertArrayEquals(new String[] {getNamePathMapper().getJcrName(REP_NT_NAMES)}, entry.getRestrictionNames());
            }
        }
    }

    @Test
    public void testEffectivePolicyByNullPath() throws Exception {
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies((String) null);
        assertEquals(1, effective.length);
        assertTrue(effective[0] instanceof ImmutableACL);

        List<JackrabbitAccessControlEntry> entries = ((ImmutableACL)effective[0]).getEntries();
        assertEquals(1, entries.size());

        assertTrue(entries.get(0) instanceof ACE);
        assertEquals(validPrincipal, entries.get(0).getPrincipal());
        assertArrayEquals(privilegesFromNames(JCR_NAMESPACE_MANAGEMENT), entries.get(0).getPrivileges());
    }
}

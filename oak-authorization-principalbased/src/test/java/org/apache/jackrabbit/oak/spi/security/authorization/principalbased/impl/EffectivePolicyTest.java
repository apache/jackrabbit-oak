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

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.util.Text;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.security.AccessControlPolicy;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NT_NAMES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_WRITE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

        // create 2 entries for 'validPrincipal'
        // - jcrEffectivePath : read, write
        // - null : namespaceMgt
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(validPrincipal, jcrEffectivePath, JCR_READ, REP_WRITE);
        addPrincipalBasedEntry(policy, null, JCR_NAMESPACE_MANAGEMENT);

        // create 2 entries for 'validPrincipal2'
        // - jcrEffectivePath : read
        // - root : lifecycleMgt
        policy = (PrincipalPolicyImpl) acMgr.getApplicablePolicies(validPrincipal2)[0];
        Map<String, Value> restrictions = Map.of(getNamePathMapper().getJcrName(REP_GLOB), getValueFactory(root).createValue("/*/glob"));
        policy.addEntry(jcrEffectivePath, privilegesFromNames(JCR_READ), restrictions, Map.of());

        String ntJcrName = getNamePathMapper().getJcrName(JcrConstants.NT_RESOURCE);
        Map<String, Value[]> mvRestrictions = Map.of(getNamePathMapper().getJcrName(REP_NT_NAMES), new Value[] {getValueFactory(root).createValue(ntJcrName, PropertyType.NAME)});
        policy.addEntry(PathUtils.ROOT_PATH, privilegesFromNames(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT), Map.of(), mvRestrictions);

        acMgr.setPolicy(policy.getPath(), policy);

        root.commit();
    }

    @Test
    public void testEffectivePolicyByPrincipal() throws Exception {
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(Set.of(validPrincipal));
        assertEffectivePolicies(effective, 2, 2, true);

        List<JackrabbitAccessControlEntry> entries = ((ImmutablePrincipalPolicy)effective[0]).getEntries();
        assertEquals(2, entries.size());

        assertTrue(entries.get(0) instanceof PrincipalAccessControlList.Entry);
        assertEquals(validPrincipal, entries.get(0).getPrincipal());
        assertArrayEquals(privilegesFromNames(JCR_READ, REP_WRITE), entries.get(0).getPrivileges());
        assertEquals(jcrEffectivePath, ((PrincipalAccessControlList.Entry) entries.get(0)).getEffectivePath());

        assertNull(((PrincipalAccessControlList.Entry) entries.get(1)).getEffectivePath());
    }

    @Test
    public void testEffectivePolicyByPrincipal2() throws Exception {
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(Set.of(validPrincipal2));
        assertEffectivePolicies(effective, 2, 2, true);

        List<JackrabbitAccessControlEntry> entries = ((ImmutablePrincipalPolicy)effective[0]).getEntries();
        assertEquals(2, entries.size());

        assertTrue(entries.get(0) instanceof PrincipalAccessControlList.Entry);
        assertEquals(validPrincipal2, entries.get(0).getPrincipal());
        assertArrayEquals(privilegesFromNames(JCR_READ), entries.get(0).getPrivileges());
        assertEquals(jcrEffectivePath, ((PrincipalAccessControlList.Entry) entries.get(0)).getEffectivePath());

        assertEquals(validPrincipal2, entries.get(1).getPrincipal());
        assertArrayEquals(privilegesFromNames(JCR_LIFECYCLE_MANAGEMENT), entries.get(1).getPrivileges());
        assertEquals(PathUtils.ROOT_PATH, ((PrincipalAccessControlList.Entry) entries.get(1)).getEffectivePath());
    }

    @Test
    public void testEffectivePolicyByPath() throws Exception {
        String path = getNamePathMapper().getJcrPath(TEST_OAK_PATH);
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(path);
        assertEquals(2, effective.length);

        for (AccessControlPolicy policy : effective) {
            assertTrue(policy instanceof ImmutablePrincipalPolicy);
            ImmutablePrincipalPolicy effectivePolicy = (ImmutablePrincipalPolicy) policy;

            // filter expected entries: only entries that take effect at the target path should be taken into consideration
            ImmutablePrincipalPolicy byPrincipal = (ImmutablePrincipalPolicy) acMgr.getEffectivePolicies(Set.of(effectivePolicy.getPrincipal()))[0];
            Set<JackrabbitAccessControlEntry> expected = ImmutableSet.copyOf(Iterables.filter(byPrincipal.getEntries(), entry -> {
                String effectivePath = ((PrincipalAccessControlList.Entry) entry).getEffectivePath();
                return effectivePath != null && Text.isDescendantOrEqual(effectivePath, path);
            }));

            assertEquals(expected.size(), effectivePolicy.size());
            List<JackrabbitAccessControlEntry> entries = effectivePolicy.getEntries();
            for (JackrabbitAccessControlEntry entry : expected) {
                assertTrue(entries.contains(entry));
            }
        }
    }

    @Test
    public void testEffectivePolicyByPathVerifiesPrincipals() throws Exception {
        PrincipalManager principalMgr = mock(PrincipalManager.class);
        when(principalMgr.getPrincipal(validPrincipal.getName())).thenReturn(null);
        when(principalMgr.getPrincipal(validPrincipal2.getName())).thenReturn(new PrincipalImpl(validPrincipal2.getName()));

        MgrProvider provider = mock(MgrProvider.class);
        when(provider.getPrincipalManager()).thenReturn(principalMgr);
        when(provider.getRoot()).thenReturn(root);
        when(provider.getSecurityProvider()).thenReturn(securityProvider);
        when(provider.getNamePathMapper()).thenReturn(getNamePathMapper());

        PrincipalBasedAccessControlManager acm = new PrincipalBasedAccessControlManager(provider, getFilterProvider());
        AccessControlPolicy[] effective = acm.getEffectivePolicies(getNamePathMapper().getJcrPath(TEST_OAK_PATH));
        assertEquals(0, effective.length);
    }

    @Test
    public void testEffectivePolicyByNullPath() throws Exception {
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies((String) null);
        assertEquals(1, effective.length);
        assertTrue(effective[0] instanceof ImmutablePrincipalPolicy);
        assertEquals(validPrincipal, ((ImmutablePrincipalPolicy)effective[0]).getPrincipal());

        List<JackrabbitAccessControlEntry> entries = ((ImmutablePrincipalPolicy)effective[0]).getEntries();
        assertEquals(1, entries.size());

        assertTrue(entries.get(0) instanceof PrincipalAccessControlList.Entry);
        assertNull(((PrincipalAccessControlList.Entry)entries.get(0)).getEffectivePath());
        assertEquals(validPrincipal, entries.get(0).getPrincipal());
        assertArrayEquals(privilegesFromNames(JCR_NAMESPACE_MANAGEMENT), entries.get(0).getPrivileges());
    }
}

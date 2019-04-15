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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlPolicy;
import java.security.Principal;
import java.util.UUID;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_LOCK_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_VERSION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TransientPrincipalTest extends AbstractPrincipalBasedTest {

    private String uid = "testSystemUser" + UUID.randomUUID();
    private Principal principal;
    private PrincipalBasedAccessControlManager acMgr;

    @Before
    public void before() throws Exception {
        super.before();

        principal = getUserManager(root).createSystemUser(uid, INTERMEDIATE_PATH).getPrincipal();
        acMgr = new PrincipalBasedAccessControlManager(getMgrProvider(root), getFilterProvider());
    }

    @After
    public void after() throws Exception {
        try {
            root.refresh();
            Authorizable a = getUserManager(root).getAuthorizable(uid);
            if (a != null) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME, ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT));
    }

    private PrincipalPolicyImpl getApplicable() throws Exception {
        JackrabbitAccessControlPolicy[] applicable = acMgr.getApplicablePolicies(principal);
        return (PrincipalPolicyImpl) applicable[0];
    }

    @Test
    public void testApplicablePolicies() throws Exception {
        JackrabbitAccessControlPolicy[] applicable = acMgr.getApplicablePolicies(principal);
        assertEquals(1, applicable.length);
        assertTrue(applicable[0] instanceof PrincipalPolicyImpl);
        assertEquals(principal.getName(), ((PrincipalPolicyImpl) applicable[0]).getPrincipal().getName());
    }

    @Test
    public void testTransientGetSetRemovePolicy() throws Exception {
        JackrabbitAccessControlPolicy[] policies = acMgr.getPolicies(principal);
        assertEquals(0, policies.length);

        PrincipalPolicyImpl policy = getApplicable();
        acMgr.setPolicy(policy.getPath(), policy);

        policies = acMgr.getPolicies(principal);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof PrincipalPolicyImpl);

        policy = (PrincipalPolicyImpl) policies[0];
        assertTrue(policy.isEmpty());

        policy.addEntry(testContentJcrPath, privilegesFromNames(JCR_VERSION_MANAGEMENT));
        acMgr.setPolicy(policy.getPath(), policy);

        policies = acMgr.getPolicies(principal);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof PrincipalPolicyImpl);

        policy = (PrincipalPolicyImpl) policies[0];
        assertEquals(1, policy.size());

        acMgr.removePolicy(policy.getPath(), policy);
        policies = acMgr.getPolicies(principal);
        assertEquals(0, policies.length);
    }

    @Test
    public void testGetSetRemovePolicy() throws Exception {
        JackrabbitAccessControlPolicy[] policies = acMgr.getPolicies(principal);
        assertEquals(0, policies.length);

        PrincipalPolicyImpl policy = getApplicable();
        acMgr.setPolicy(policy.getPath(), policy);
        root.commit();

        policies = acMgr.getPolicies(principal);
        assertEquals(1, policies.length);

        acMgr.removePolicy(policy.getPath(), policy);
        root.commit();

        policies = acMgr.getPolicies(principal);
        assertEquals(0, policies.length);
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(ImmutableSet.of(principal));
        assertEquals(0, effective.length);

        PrincipalPolicyImpl policy = getApplicable();
        policy.addEntry(testJcrPath, privilegesFromNames(JCR_WRITE));
        acMgr.setPolicy(policy.getPath(), policy);

        effective = acMgr.getEffectivePolicies(ImmutableSet.of(principal));
        assertEquals(0, effective.length);
    }

    @Test
    public void testGetEffectivePoliciesByPath() throws Exception {
        setupContentTrees(TEST_OAK_PATH);

        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(testJcrPath);
        assertEquals(0, effective.length);

        PrincipalPolicyImpl policy = getApplicable();
        policy.addEntry(testJcrPath, privilegesFromNames(JCR_WRITE));
        acMgr.setPolicy(policy.getPath(), policy);

        effective = acMgr.getEffectivePolicies(testJcrPath);
        assertEquals(0, effective.length);
    }

    @Test
    public void testGetPrivileges() throws Exception {
        setupContentTrees(TEST_OAK_PATH);
        assertEquals(0, acMgr.getPrivileges(testJcrPath, ImmutableSet.of(principal)).length);

        // transient modifications => not respected by permission evaluation
        PrincipalPolicyImpl policy = getApplicable();
        policy.addEntry(testJcrPath, privilegesFromNames(JCR_LOCK_MANAGEMENT));
        acMgr.setPolicy(policy.getPath(), policy);

        assertEquals(0, acMgr.getPrivileges(testJcrPath, ImmutableSet.of(principal)).length);
    }

    @Test
    public void testGetPermssionProvider() throws Exception {
        PrincipalBasedAuthorizationConfiguration pbac = getPrincipalBasedAuthorizationConfiguration();
        // with remapped namespaces
        PermissionProvider pp = pbac.getPermissionProvider(root, root.getContentSession().getWorkspaceName(), ImmutableSet.of(principal));
        assertSame(EmptyPermissionProvider.getInstance(), pp);

        // with default ns-mapping as used to create permission provider
        Principal transientWithDefaultNs = getConfig(UserConfiguration.class).getUserManager(root, NamePathMapper.DEFAULT).getAuthorizable(uid).getPrincipal();
        pp = pbac.getPermissionProvider(root, root.getContentSession().getWorkspaceName(), ImmutableSet.of(transientWithDefaultNs));
        // since permission provider is created with a read-only root the transient principal node does not exist and
        // no evaluation will take place
        assertSame(EmptyPermissionProvider.getInstance(), pp);
    }
}
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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CugConfigurationTest extends AbstractSecurityTest {

    private static CugConfiguration createConfiguration(ConfigurationParameters params) {
        SecurityProvider sp = new CugSecurityProvider(ConfigurationParameters.EMPTY);
        CugConfiguration cugConfiguration = new CugConfiguration();
        cugConfiguration.setSecurityProvider(sp);
        cugConfiguration.activate(params);
        return cugConfiguration;
    }

    @Test
    public void testEmptyConstructor() {
        assertEquals(ConfigurationParameters.EMPTY, new CugConfiguration().getParameters());
    }

    @Test
    public void testGetName() {
        assertEquals(AuthorizationConfiguration.NAME, new CugConfiguration().getName());
    }

    @Test
    public void testGetRestrictionProvider() {
        assertSame(RestrictionProvider.EMPTY, new CugConfiguration().getRestrictionProvider());
    }

    @Test
    public void testGetCommitHooks() {
        List<? extends CommitHook> l = new CugConfiguration().getCommitHooks("wspName");
        assertEquals(1, l.size());
        assertTrue(l.iterator().next() instanceof NestedCugHook);
    }

    @Test
    public void testGetValidators() {
        List<? extends ValidatorProvider> l = new CugConfiguration().getValidators("wspName", ImmutableSet.of(), new MoveTracker());
        assertEquals(1, l.size());
        assertTrue(l.iterator().next() instanceof CugValidatorProvider);
    }

    @Test
    public void testGetProtectedItemImporters() {
        List<ProtectedItemImporter> l = new CugConfiguration().getProtectedItemImporters();
        assertEquals(1, l.size());
        assertTrue(l.iterator().next() instanceof CugImporter);
    }

    @Test
    public void testGetContext() {
        assertSame(CugContext.INSTANCE, new CugConfiguration().getContext());
    }

    @Test
    public void testGetPermissionProviderDisabled() {
        CugConfiguration cc = createConfiguration(ConfigurationParameters.of(CugConstants.PARAM_CUG_ENABLED, false));

        PermissionProvider pp = cc.getPermissionProvider(root, root.getContentSession().getWorkspaceName(), ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()));
        assertSame(EmptyPermissionProvider.getInstance(), pp);
    }

    @Test
    public void testGetPermissionProviderDisabled2() {
        ConfigurationParameters params = ConfigurationParameters.of(
                CugConstants.PARAM_CUG_ENABLED, false,
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, "/content");
        CugConfiguration cc = createConfiguration(params);
        PermissionProvider pp = cc.getPermissionProvider(root, "default", ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()));
        assertSame(EmptyPermissionProvider.getInstance(), pp);
    }

    @Test
    public void testGetPermissionProviderDisabled3() {
        CugConfiguration cc = createConfiguration(ConfigurationParameters.EMPTY);

        PermissionProvider pp = cc.getPermissionProvider(root, "default", ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()));
        assertSame(EmptyPermissionProvider.getInstance(), pp);
    }

    @Test
    public void testGetPermissionProviderNoSupportedPaths() {
        // enabled but no supported paths specified
        CugConfiguration cc = createConfiguration(ConfigurationParameters.of(CugConstants.PARAM_CUG_ENABLED, true));

        PermissionProvider pp = cc.getPermissionProvider(root, "default", ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()));
        assertSame(EmptyPermissionProvider.getInstance(), pp);
    }

    @Test
    public void testGetPermissionProviderSupportedPaths() {
        ConfigurationParameters params = ConfigurationParameters.of(
                CugConstants.PARAM_CUG_ENABLED, true,
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, "/content");
        CugConfiguration cc = createConfiguration(params);

        PermissionProvider pp = cc.getPermissionProvider(root, "default", ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()));
        assertTrue(pp instanceof CugPermissionProvider);
    }

    @Test
    public void testGetAccessControlManagerDisabled() {
        CugConfiguration cc = createConfiguration(ConfigurationParameters.of(CugConstants.PARAM_CUG_ENABLED, false));

        AccessControlManager acMgr = cc.getAccessControlManager(root, NamePathMapper.DEFAULT);
        assertTrue(acMgr instanceof CugAccessControlManager);
    }

    @Test
    public void testGetAccessControlManagerNoSupportedPaths() {
        CugConfiguration cc = createConfiguration(ConfigurationParameters.of(CugConstants.PARAM_CUG_ENABLED, true));

        AccessControlManager acMgr = cc.getAccessControlManager(root, NamePathMapper.DEFAULT);
        assertTrue(acMgr instanceof CugAccessControlManager);
    }

    @Test
    public void testGetAccessControlManagerSupportedPaths() {
        ConfigurationParameters params = ConfigurationParameters.of(
                CugConstants.PARAM_CUG_ENABLED, true,
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, "/content");
        CugConfiguration cc = createConfiguration(params);

        AccessControlManager acMgr = cc.getAccessControlManager(root, NamePathMapper.DEFAULT);
        assertTrue(acMgr instanceof CugAccessControlManager);
    }

    @Test
    public void testExcludedPrincipals() {
        ConfigurationParameters params = ConfigurationParameters.of(
                CugConstants.PARAM_CUG_ENABLED, true,
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, "/content");
        CugConfiguration cc = createConfiguration(params);

        List<Principal> excluded = ImmutableList.of(
                SystemPrincipal.INSTANCE,
                new AdminPrincipal() {
                    @Override
                    public String getName() {
                        return "admin";
                    }
                },
                new SystemUserPrincipal() {
                    @Override
                    public String getName() {
                        return "systemUser";
                    }
                });

        for (Principal p : excluded) {
            Set<Principal> principals = ImmutableSet.of(p, EveryonePrincipal.getInstance());
            PermissionProvider pp = cc.getPermissionProvider(root, "default", principals);

            assertSame(EmptyPermissionProvider.getInstance(), pp);
        }
    }

    @Test
    public void testActivate() throws Exception {
        CugConfiguration cugConfiguration = new CugConfiguration(getSecurityProvider());
        cugConfiguration.activate(ImmutableMap.of(
                CugConstants.PARAM_CUG_ENABLED, false,
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, new String[] {"/content", "/anotherContent"}
        ));
        assertSupportedPaths(cugConfiguration, "/content", "/anotherContent");
    }

    @Test
    public void testModified() throws Exception {
        CugConfiguration cugConfiguration = new CugConfiguration(getSecurityProvider());
        cugConfiguration.modified(ImmutableMap.of(
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, new String[]{"/changed"}
        ));
        assertSupportedPaths(cugConfiguration, "/changed");
    }

    private static void assertSupportedPaths(@Nonnull CugConfiguration configuration, @Nonnull String... paths) throws Exception {
        Set<String> expected = ImmutableSet.copyOf(paths);
        assertEquals(expected, configuration.getParameters().getConfigValue(CugConstants.PARAM_CUG_SUPPORTED_PATHS, ImmutableSet.of()));
    }
}
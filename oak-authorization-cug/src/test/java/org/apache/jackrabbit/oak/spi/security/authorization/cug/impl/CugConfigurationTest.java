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

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CugConfigurationTest extends AbstractCugTest {

    private static CugConfiguration createConfiguration(ConfigurationParameters params) {
        SecurityProvider sp = CugSecurityProvider.newTestSecurityProvider(ConfigurationParameters.of(AuthorizationConfiguration.NAME, params));
        return CugSecurityProvider.getCugConfiguration(sp);
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
        List<? extends ValidatorProvider> l = new CugConfiguration().getValidators("wspName", Set.of(), new MoveTracker());
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

        PermissionProvider pp = cc.getPermissionProvider(root, root.getContentSession().getWorkspaceName(), Set.of(EveryonePrincipal.getInstance()));
        assertSame(EmptyPermissionProvider.getInstance(), pp);
    }

    @Test
    public void testGetPermissionProviderDisabled2() {
        ConfigurationParameters params = ConfigurationParameters.of(
                CugConstants.PARAM_CUG_ENABLED, false,
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, "/content");
        CugConfiguration cc = createConfiguration(params);
        PermissionProvider pp = cc.getPermissionProvider(root, "default", Set.of(EveryonePrincipal.getInstance()));
        assertSame(EmptyPermissionProvider.getInstance(), pp);
    }

    @Test
    public void testGetPermissionProviderDisabled3() {
        CugConfiguration cc = createConfiguration(ConfigurationParameters.EMPTY);

        PermissionProvider pp = cc.getPermissionProvider(root, "default", Set.of(EveryonePrincipal.getInstance()));
        assertSame(EmptyPermissionProvider.getInstance(), pp);
    }

    @Test
    public void testGetPermissionProviderNoSupportedPaths() {
        // enabled but no supported paths specified
        CugConfiguration cc = createConfiguration(ConfigurationParameters.of(CugConstants.PARAM_CUG_ENABLED, true));

        PermissionProvider pp = cc.getPermissionProvider(root, "default", Set.of(EveryonePrincipal.getInstance()));
        assertSame(EmptyPermissionProvider.getInstance(), pp);
    }

    @Test
    public void testGetPermissionProviderSupportedPaths() {
        ConfigurationParameters params = ConfigurationParameters.of(
                CugConstants.PARAM_CUG_ENABLED, true,
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, "/content");
        CugConfiguration cc = createConfiguration(params);

        PermissionProvider pp = cc.getPermissionProvider(root, "default", Set.of(EveryonePrincipal.getInstance()));
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
                (AdminPrincipal) () -> "admin",
                (SystemUserPrincipal) () -> "systemUser");

        for (Principal p : excluded) {
            Set<Principal> principals = Set.of(p, EveryonePrincipal.getInstance());
            PermissionProvider pp = cc.getPermissionProvider(root, "default", principals);

            assertSame(EmptyPermissionProvider.getInstance(), pp);
        }
    }

    @Test
    public void testActivate() {
        CugConfiguration cugConfiguration = createConfiguration(ConfigurationParameters.EMPTY);
        cugConfiguration.activate(Map.of(
                CugConstants.PARAM_CUG_ENABLED, false,
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, new String[] {"/content", "/anotherContent"}
        ));
        assertSupportedPaths(cugConfiguration, "/content", "/anotherContent");
    }

    @Test
    public void testModified() {
        CugConfiguration cugConfiguration = createConfiguration(ConfigurationParameters.EMPTY);
        cugConfiguration.modified(Map.of(
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, new String[]{"/changed"}
        ));
        assertSupportedPaths(cugConfiguration, "/changed");
    }

    private static void assertSupportedPaths(@NotNull CugConfiguration configuration, @NotNull String... paths) {
        Set<String> expected = Set.of(paths);
        assertEquals(expected, configuration.getParameters().getConfigValue(CugConstants.PARAM_CUG_SUPPORTED_PATHS, Set.of()));
    }

    @Test
    public void testUnbindMountInfoProvider() throws Exception {
        CugConfiguration cugConfiguration = createConfiguration(ConfigurationParameters.EMPTY);
        cugConfiguration.unbindMountInfoProvider(mock(MountInfoProvider.class));

        Field f = cugConfiguration.getClass().getDeclaredField("mountInfoProvider");
        f.setAccessible(true);
        assertNull(f.get(cugConfiguration));
    }

    @Test
    public void testUnbindCugExclude() throws Exception {
        CugConfiguration cugConfiguration = createConfiguration(ConfigurationParameters.EMPTY);
        cugConfiguration.unbindExclude(mock(CugExclude.class));

        Field f = cugConfiguration.getClass().getDeclaredField("exclude");
        f.setAccessible(true);
        assertNull(f.get(cugConfiguration));
    }

    @Test
    public void testRepositoryInitializerAlreadyInitialized() {
        AuthorizationConfiguration ac = getConfig(AuthorizationConfiguration.class);
        assertTrue(ac instanceof CompositeAuthorizationConfiguration);

        AuthorizationConfiguration cugConfig = null;
        for (AuthorizationConfiguration config : ((CompositeAuthorizationConfiguration) ac).getConfigurations()) {
            if (config instanceof CugConfiguration) {
                cugConfig = config;
                break;
            }
        }
        assertNotNull(cugConfig);
        RepositoryInitializer ri = cugConfig.getRepositoryInitializer();
        NodeBuilder rootBuilder = spy(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)).builder());
        ri.initialize(rootBuilder);

        verify(rootBuilder, times(1)).getNodeState();
    }
}

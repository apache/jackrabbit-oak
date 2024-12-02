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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ProtectionConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.monitor.ExternalIdentityMonitorImpl;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.stats.Monitor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MapUtil;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import javax.jcr.ValueFactory;
import java.lang.reflect.Field;
import java.security.Principal;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExternalPrincipalConfigurationTest extends AbstractExternalAuthTest {

    private void registerDynamicSyncHandler() {
        registerSyncHandler(true, false);
    }

    private void registerSyncHandler(boolean dynamicMembership, boolean dynamicGroups) {
        Map<String, Object> config = new HashMap<>();
        config.put(DefaultSyncConfigImpl.PARAM_NAME, DefaultSyncConfig.DEFAULT_NAME);
        if (dynamicMembership) {
            config.put(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true);
        } 
        if (dynamicGroups) {
            config.put(DefaultSyncConfigImpl.PARAM_GROUP_DYNAMIC_GROUPS, true);
        }
        registerSyncHandler(Collections.unmodifiableMap(config), idp.getName());
    }

    private void assertIsEnabled(ExternalPrincipalConfiguration externalPrincipalConfiguration, boolean expected) {
        PrincipalProvider pp = externalPrincipalConfiguration.getPrincipalProvider(root, getNamePathMapper());
        assertEquals(expected, pp instanceof ExternalGroupPrincipalProvider);
    }

    @Test
    public void testGetPrincipalManager() {
        assertNotNull(externalPrincipalConfiguration.getPrincipalManager(root, NamePathMapper.DEFAULT));
    }

    @Test
    public void testGetPrincipalManagerEnabled() {
        registerDynamicSyncHandler();
        assertNotNull(externalPrincipalConfiguration.getPrincipalManager(root, NamePathMapper.DEFAULT));
    }

    @Test
    public void testGetPrincipalProvider() {
        PrincipalProvider pp = externalPrincipalConfiguration.getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertNotNull(pp);
        assertFalse(pp instanceof ExternalGroupPrincipalProvider);

    }

    @Test
    public void testGetPrincipalProviderEnabled() {
        registerDynamicSyncHandler();
        PrincipalProvider pp = externalPrincipalConfiguration.getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertNotNull(pp);
        assertTrue(pp instanceof ExternalGroupPrincipalProvider);
    }

    @Test
    public void testGetName() {
        assertEquals(PrincipalConfiguration.NAME, externalPrincipalConfiguration.getName());

        registerDynamicSyncHandler();
        assertEquals(PrincipalConfiguration.NAME, externalPrincipalConfiguration.getName());
    }

    @Test
    public void testGetContext() {
        assertSame(Context.DEFAULT, externalPrincipalConfiguration.getContext());

        registerDynamicSyncHandler();
        assertSame(Context.DEFAULT, externalPrincipalConfiguration.getContext());
    }

    @Test
    public void testGetWorkspaceInitializer() {
        assertSame(WorkspaceInitializer.DEFAULT, externalPrincipalConfiguration.getWorkspaceInitializer());

        registerDynamicSyncHandler();
        assertSame(WorkspaceInitializer.DEFAULT, externalPrincipalConfiguration.getWorkspaceInitializer());
    }

    @Test
    public void testGetRepositoryInitializer() {
        assertTrue(externalPrincipalConfiguration.getRepositoryInitializer() instanceof ExternalIdentityRepositoryInitializer);

        registerDynamicSyncHandler();
        assertTrue(externalPrincipalConfiguration.getRepositoryInitializer() instanceof ExternalIdentityRepositoryInitializer);
    }

    @Test
    public void testGetValidators() {
        ContentSession cs = root.getContentSession();
        List<? extends ValidatorProvider> validatorProviders = externalPrincipalConfiguration.getValidators(cs.getWorkspaceName(), cs.getAuthInfo().getPrincipals(), new MoveTracker());

        assertFalse(validatorProviders.isEmpty());
        assertEquals(1, validatorProviders.size());
        assertTrue(validatorProviders.get(0) instanceof ExternalIdentityValidatorProvider);

        validatorProviders = externalPrincipalConfiguration.getValidators(cs.getWorkspaceName(), cs.getAuthInfo().getPrincipals(), new MoveTracker());
        assertFalse(validatorProviders.isEmpty());
        assertEquals(1, validatorProviders.size());
        assertTrue(validatorProviders.get(0) instanceof ExternalIdentityValidatorProvider);

        registerDynamicSyncHandler();

        validatorProviders = externalPrincipalConfiguration.getValidators(cs.getWorkspaceName(), cs.getAuthInfo().getPrincipals(), new MoveTracker());
        assertFalse(validatorProviders.isEmpty());
        assertEquals(1, validatorProviders.size());
        assertTrue(validatorProviders.get(0) instanceof ExternalIdentityValidatorProvider);
    }

    @Test
    public void testGetValidatorsOmitIdProtection() {
        externalPrincipalConfiguration.setParameters(ConfigurationParameters.of(ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDS, false));
        ContentSession cs = root.getContentSession();

        List<? extends ValidatorProvider> validatorProviders = externalPrincipalConfiguration.getValidators(cs.getWorkspaceName(), cs.getAuthInfo().getPrincipals(), new MoveTracker());
        assertFalse(validatorProviders.isEmpty());
        assertEquals(1, validatorProviders.size());
        assertTrue(validatorProviders.get(0) instanceof ExternalIdentityValidatorProvider);

        registerDynamicSyncHandler();

        validatorProviders = externalPrincipalConfiguration.getValidators(cs.getWorkspaceName(), cs.getAuthInfo().getPrincipals(), new MoveTracker());
        assertFalse(validatorProviders.isEmpty());
        assertEquals(1, validatorProviders.size());
        assertTrue(validatorProviders.get(0) instanceof ExternalIdentityValidatorProvider);
    }
    
    @Test
    public void testGetValidatorsIdentityProtectionWarn() {
        externalPrincipalConfiguration.setParameters(ConfigurationParameters.of(ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDENTITIES, ExternalIdentityConstants.VALUE_PROTECT_EXTERNAL_IDENTITIES_WARN));

        ContentSession cs = root.getContentSession();
        String workspaceName = cs.getWorkspaceName();
        
        // access validators for regular session
        List<? extends ValidatorProvider> validatorProviders = externalPrincipalConfiguration.getValidators(workspaceName, cs.getAuthInfo().getPrincipals(), new MoveTracker());
        assertEquals(2, validatorProviders.size());
        assertTrue(validatorProviders.get(1) instanceof ExternalUserValidatorProvider);
        
        // access validators for system-session
        validatorProviders = externalPrincipalConfiguration.getValidators(workspaceName, SystemSubject.INSTANCE.getPrincipals(), new MoveTracker());
        assertEquals(1, validatorProviders.size());
        assertFalse(validatorProviders.get(0) instanceof ExternalUserValidatorProvider);
    }

    @Test
    public void testGetValidatorsIdentityProtectionProtect() {
        String principalName = UserConstants.DEFAULT_ADMIN_ID;
        externalPrincipalConfiguration.setParameters(ConfigurationParameters.of(
                ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDENTITIES, ExternalIdentityConstants.VALUE_PROTECT_EXTERNAL_IDENTITIES_PROTECTED,
                ExternalIdentityConstants.PARAM_SYSTEM_PRINCIPAL_NAMES, new String[] {principalName}));

        ContentSession cs = root.getContentSession();
        String workspaceName = cs.getWorkspaceName();

        // access validators for regular session (no system user principal)
        List<? extends ValidatorProvider> validatorProviders = externalPrincipalConfiguration.getValidators(workspaceName, cs.getAuthInfo().getPrincipals(), new MoveTracker());
        assertEquals(2, validatorProviders.size());
        assertTrue(validatorProviders.get(1) instanceof ExternalUserValidatorProvider);

        // access validators for system-user-principal
        Set<Principal> systemPrincipals = Collections.singleton((SystemUserPrincipal) () -> principalName); 
        validatorProviders = externalPrincipalConfiguration.getValidators(workspaceName, systemPrincipals, new MoveTracker());
        assertEquals(1, validatorProviders.size());
        assertFalse(validatorProviders.get(0) instanceof ExternalUserValidatorProvider);
    }

    @Test
    public void testGetValidatorsDynamicGroupsEnabledWithoutDynamicMembership() {
        ContentSession cs = root.getContentSession();
        String workspaceName = cs.getWorkspaceName();

        // dynamic groups only effective if dynamic-membership is enabled as well
        registerSyncHandler(false, true);

        List<? extends ValidatorProvider> validatorProviders = externalPrincipalConfiguration.getValidators(workspaceName, cs.getAuthInfo().getPrincipals(), new MoveTracker());
        assertEquals(1, validatorProviders.size());
        assertTrue(validatorProviders.get(0) instanceof ExternalIdentityValidatorProvider);
    }

    @Test
    public void testGetValidatorsDynamicGroupsEnabled() {
        ContentSession cs = root.getContentSession();
        String workspaceName = cs.getWorkspaceName();
        
        // upon registering a sync-handler with dynamic-membership the dynamic-group-validator must be present as well
        registerSyncHandler(true, true);

        List<? extends ValidatorProvider> validatorProviders = externalPrincipalConfiguration.getValidators(workspaceName, cs.getAuthInfo().getPrincipals(), new MoveTracker());
        assertEquals(2, validatorProviders.size());
        assertTrue(validatorProviders.get(0) instanceof ExternalIdentityValidatorProvider);
        assertTrue(validatorProviders.get(1) instanceof DynamicGroupValidatorProvider);

        externalPrincipalConfiguration.setParameters(ConfigurationParameters.of(
                ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDENTITIES, ExternalIdentityConstants.VALUE_PROTECT_EXTERNAL_IDENTITIES_PROTECTED));
        
        validatorProviders = externalPrincipalConfiguration.getValidators(workspaceName, cs.getAuthInfo().getPrincipals(), new MoveTracker());
        assertEquals(3, validatorProviders.size());
        assertTrue(validatorProviders.get(1) instanceof DynamicGroupValidatorProvider);
    }
    
    @Test
    public void testGetValidatorsMissingActivate() throws Exception {
        ConfigurationParameters config = ConfigurationParameters.of(ExternalIdentityConstants.PARAM_PROTECT_EXTERNAL_IDENTITIES, ExternalIdentityConstants.VALUE_PROTECT_EXTERNAL_IDENTITIES_WARN);
        
        SecurityProvider sp = mock(SecurityProvider.class);
        when(sp.getParameters(PrincipalConfiguration.NAME)).thenReturn(config);
        when(sp.getParameters(UserConfiguration.NAME)).thenReturn(ConfigurationParameters.EMPTY);
        
        ExternalPrincipalConfiguration epc = new ExternalPrincipalConfiguration(sp);
        epc.setRootProvider(getRootProvider());
        epc.setTreeProvider(getTreeProvider());
        
        List<? extends ValidatorProvider> vps = epc.getValidators(root.getContentSession().getWorkspaceName(), Collections.singleton(EveryonePrincipal.getInstance()), new MoveTracker());
        assertEquals(2, vps.size());
        
        Optional<? extends ValidatorProvider> optional = vps.stream().filter((Predicate<ValidatorProvider>) validatorProvider1 -> validatorProvider1 instanceof ExternalUserValidatorProvider).findFirst();
        if (optional.isPresent()) {
            ExternalUserValidatorProvider validatorProvider = (ExternalUserValidatorProvider) optional.get();
            assertNotNull(validatorProvider);
            assertDefaultProtectionConfig(validatorProvider);
        } else {
            fail("ExternalUserValidatorProvider expected to be present");
        }
    }
    
    private static void assertDefaultProtectionConfig(@NotNull ExternalUserValidatorProvider vp) throws Exception {
        Field f = ExternalUserValidatorProvider.class.getDeclaredField("protectionConfig");
        f.setAccessible(true);
        assertSame(ProtectionConfig.DEFAULT, f.get(vp));
    }
    
    @Test
    public void testGetProtectedItemImporters() {
        List<? extends ProtectedItemImporter> importers = externalPrincipalConfiguration.getProtectedItemImporters();

        assertFalse(importers.isEmpty());
        assertEquals(1, importers.size());
        assertTrue(importers.get(0) instanceof ExternalIdentityImporter);

        registerDynamicSyncHandler();

        importers = externalPrincipalConfiguration.getProtectedItemImporters();
        assertFalse(importers.isEmpty());
        assertEquals(1, importers.size());
        assertTrue(importers.get(0) instanceof ExternalIdentityImporter);
    }

    @Test
    public void testGetMonitors() {
        Iterable<Monitor<?>> monitors = externalPrincipalConfiguration.getMonitors(StatisticsProvider.NOOP);
        assertEquals(1, Iterables.size(monitors));
        assertTrue(monitors.iterator().next() instanceof ExternalIdentityMonitorImpl);
    }

    @Test
    public void testDeactivateWithNullTrackers() {
        ExternalPrincipalConfiguration epc = new ExternalPrincipalConfiguration(getSecurityProvider());
        BundleContext bundleContext = context.bundleContext();
        
        assertNull(bundleContext.getServiceReference(SyncConfigTracker.class.getName()));
        
        MockOsgi.deactivate(epc, bundleContext, Collections.emptyMap());

        assertNull(bundleContext.getServiceReference(SyncConfigTracker.class.getName()));
    }

    @Test
    public void testAddingSyncHandler() {
        Map<String, Object> enableProps =  Map.of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true);
        Map<String, Object> disableProps =  Map.of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, false);

        SyncHandler sh = new DefaultSyncHandler();
        context.registerService(SyncHandler.class, sh, Map.of());
        assertIsEnabled(externalPrincipalConfiguration, false);

        context.registerService(SyncHandler.class, sh, disableProps);
        assertIsEnabled(externalPrincipalConfiguration, false);

        context.registerService(SyncHandler.class, sh, enableProps);
        assertIsEnabled(externalPrincipalConfiguration, true);

        context.registerService(DefaultSyncHandler.class, new DefaultSyncHandler(), enableProps);
        assertIsEnabled(externalPrincipalConfiguration, true);
    }

    @Test
    public void testAddingCustomSyncHandler() {
        Map<String, Object> enableProps =  Map.of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true);

        SyncHandler sh = new TestSyncHandler();
        context.registerService(SyncHandler.class, sh, Map.of());
        assertIsEnabled(externalPrincipalConfiguration, false);

        context.registerService(SyncHandler.class, sh, enableProps);
        assertIsEnabled(externalPrincipalConfiguration, true);
    }

    @Test
    public void testRemoveSyncHandler() {
        Dictionary<String, Object> enableProps = MapUtil.toDictionary(Map.of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        Dictionary<String, Object> disableProps =  MapUtil.toDictionary(Map.of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, false));

        DefaultSyncHandler sh = new DefaultSyncHandler();
        BundleContext bundleContext = context.bundleContext();

        ServiceRegistration registration1 = bundleContext.registerService(SyncHandler.class.getName(), sh, enableProps);
        ServiceRegistration registration2 = bundleContext.registerService(SyncHandler.class.getName(), sh, enableProps);
        ServiceRegistration registration3 = bundleContext.registerService(SyncHandler.class.getName(), sh, disableProps);

        assertIsEnabled(externalPrincipalConfiguration, true);

        registration2.unregister();
        assertIsEnabled(externalPrincipalConfiguration, true);

        registration1.unregister();
        assertIsEnabled(externalPrincipalConfiguration, false);

        registration3.unregister();
        assertIsEnabled(externalPrincipalConfiguration, false);
    }

    private static final class TestSyncHandler implements SyncHandler {

        @NotNull
        @Override
        public String getName() {
            return "name";
        }

        @NotNull
        @Override
        public SyncContext createContext(@NotNull ExternalIdentityProvider idp, @NotNull UserManager userManager, @NotNull ValueFactory valueFactory) {
            return new DefaultSyncContext(new DefaultSyncConfig(), idp, userManager, valueFactory);
        }

        @Override
        public @Nullable SyncedIdentity findIdentity(@NotNull UserManager userManager, @NotNull String id) {
            return null;
        }

        @Override
        public boolean requiresSync(@NotNull SyncedIdentity identity) {
            return false;
        }

        @NotNull
        @Override
        public Iterator<SyncedIdentity> listIdentities(@NotNull UserManager userManager) {
            return Collections.emptyIterator();
        }
    }
}

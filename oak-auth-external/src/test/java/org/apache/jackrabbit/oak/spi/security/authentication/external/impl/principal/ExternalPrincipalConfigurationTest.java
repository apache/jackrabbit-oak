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

import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.jcr.ValueFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModuleFactory;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.monitor.ExternalIdentityMonitorImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.stats.Monitor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ExternalPrincipalConfigurationTest extends AbstractExternalAuthTest {

    private void enable() {
        context.registerService(SyncHandler.class, new DefaultSyncHandler(), ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true));
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
        enable();
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
        enable();
        PrincipalProvider pp = externalPrincipalConfiguration.getPrincipalProvider(root, NamePathMapper.DEFAULT);
        assertNotNull(pp);
        assertTrue(pp instanceof ExternalGroupPrincipalProvider);
    }

    @Test
    public void testGetName() {
        assertEquals(PrincipalConfiguration.NAME, externalPrincipalConfiguration.getName());

        enable();
        assertEquals(PrincipalConfiguration.NAME, externalPrincipalConfiguration.getName());
    }

    @Test
    public void testGetContext() {
        assertSame(Context.DEFAULT, externalPrincipalConfiguration.getContext());

        enable();
        assertSame(Context.DEFAULT, externalPrincipalConfiguration.getContext());
    }

    @Test
    public void testGetWorkspaceInitializer() {
        assertSame(WorkspaceInitializer.DEFAULT, externalPrincipalConfiguration.getWorkspaceInitializer());

        enable();
        assertSame(WorkspaceInitializer.DEFAULT, externalPrincipalConfiguration.getWorkspaceInitializer());
    }

    @Test
    public void testGetRepositoryInitializer() {
        assertTrue(externalPrincipalConfiguration.getRepositoryInitializer() instanceof ExternalIdentityRepositoryInitializer);

        enable();
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

        enable();

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

        enable();

        validatorProviders = externalPrincipalConfiguration.getValidators(cs.getWorkspaceName(), cs.getAuthInfo().getPrincipals(), new MoveTracker());
        assertFalse(validatorProviders.isEmpty());
        assertEquals(1, validatorProviders.size());
        assertTrue(validatorProviders.get(0) instanceof ExternalIdentityValidatorProvider);
    }

    @Test
    public void testGetProtectedItemImporters() {
        List<? extends ProtectedItemImporter> importers = externalPrincipalConfiguration.getProtectedItemImporters();

        assertFalse(importers.isEmpty());
        assertEquals(1, importers.size());
        assertTrue(importers.get(0) instanceof ExternalIdentityImporter);

        enable();

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
        MockOsgi.deactivate(epc, context.bundleContext(), Collections.emptyMap());
    }

    @Test
    public void testAddingSyncHandler() {
        Map<String, Object> enableProps =  ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true);
        Map<String, Object> disableProps =  ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, false);

        SyncHandler sh = new DefaultSyncHandler();
        context.registerService(SyncHandler.class, sh, ImmutableMap.<String, Object>of());
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
        Map<String, Object> enableProps =  ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true);

        SyncHandler sh = new TestSyncHandler();
        context.registerService(SyncHandler.class, sh, ImmutableMap.<String, Object>of());
        assertIsEnabled(externalPrincipalConfiguration, false);

        context.registerService(SyncHandler.class, sh, enableProps);
        assertIsEnabled(externalPrincipalConfiguration, true);
    }

    @Ignore("TODO: mock doesn't reflect property-changes on the registration.")
    @Test
    public void testModifySyncHandler() {
        Dictionary<String, Object> enableProps =  new Hashtable<>(ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        Dictionary<String, Object> disableProps =  new Hashtable<>(ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, false));

        DefaultSyncHandler sh = new DefaultSyncHandler();
        BundleContext bundleContext = context.bundleContext();

        ServiceRegistration registration = bundleContext.registerService(DefaultSyncHandler.class.getName(), sh, disableProps);
        assertIsEnabled(externalPrincipalConfiguration, false);

        MockOsgi.modified(sh, bundleContext, enableProps);
        assertIsEnabled(externalPrincipalConfiguration, true);

        MockOsgi.modified(sh, bundleContext, disableProps);
        assertIsEnabled(externalPrincipalConfiguration, false);
    }

    @Test
    public void testRemoveSyncHandler() {
        Dictionary<String, Object> enableProps =  new Hashtable<>(ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        Dictionary<String, Object> disableProps =  new Hashtable<>(ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, false));

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

    @Test
    public void testAddingIncompleteSyncHandlerMapping() {
        SyncHandlerMapping mapping = mock(SyncHandlerMapping.class);

        context.registerService(SyncHandlerMapping.class, mapping);

        context.registerService(SyncHandlerMapping.class, mapping, ImmutableMap.of(ExternalLoginModuleFactory.PARAM_IDP_NAME, "idpName"));

        context.registerService(SyncHandlerMapping.class, mapping, ImmutableMap.of(ExternalLoginModuleFactory.PARAM_SYNC_HANDLER_NAME, "syncHandlerName"));

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
        public SyncedIdentity findIdentity(@NotNull UserManager userManager, @NotNull String id) {
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

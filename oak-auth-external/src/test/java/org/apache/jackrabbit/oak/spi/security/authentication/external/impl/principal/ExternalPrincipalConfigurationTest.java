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

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
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
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Ignore;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ExternalPrincipalConfigurationTest extends AbstractExternalAuthTest {

    private void enable() {
        context.registerService(SyncHandler.class, new DefaultSyncHandler(), ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true));
    }

    private void assertIsEnabled(ExternalPrincipalConfiguration externalPrincipalConfiguration, boolean expected) throws Exception {
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
    public void testGetPrincipalProvider() throws Exception {
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
    public void testGetValidatorsOmitIdProtection() throws Exception {
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
    public void testAddingSyncHandler() throws Exception {
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
    public void testAddingCustomSyncHandler() throws Exception {
        Map<String, Object> enableProps =  ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true);

        SyncHandler sh = new TestSyncHandler();
        context.registerService(SyncHandler.class, sh, ImmutableMap.<String, Object>of());
        assertIsEnabled(externalPrincipalConfiguration, false);

        context.registerService(SyncHandler.class, sh, enableProps);
        assertIsEnabled(externalPrincipalConfiguration, true);
    }

    @Ignore("TODO: mock doesn't reflect property-changes on the registration.")
    @Test
    public void testModifySyncHandler() throws Exception {
        Dictionary<String, Object> enableProps =  new Hashtable(ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        Dictionary<String, Object> disableProps =  new Hashtable(ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, false));

        DefaultSyncHandler sh = new DefaultSyncHandler();
        BundleContext bundleContext = context.bundleContext();

        ServiceRegistration registration = bundleContext.registerService(DefaultSyncHandler.class.getName(), sh, disableProps);
        assertIsEnabled(externalPrincipalConfiguration, false);

        registration.setProperties(enableProps);
        assertIsEnabled(externalPrincipalConfiguration, true);

        registration.setProperties(disableProps);
        assertIsEnabled(externalPrincipalConfiguration, false);
    }

    @Test
    public void testRemoveSyncHandler() throws Exception {
        Dictionary<String, Object> enableProps =  new Hashtable(ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        Dictionary<String, Object> disableProps =  new Hashtable(ImmutableMap.<String, Object>of(DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP, false));

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

        @Nonnull
        @Override
        public String getName() {
            return "name";
        }

        @Nonnull
        @Override
        public SyncContext createContext(@Nonnull ExternalIdentityProvider idp, @Nonnull UserManager userManager, @Nonnull ValueFactory valueFactory) throws SyncException {
            return new DefaultSyncContext(new DefaultSyncConfig(), idp, userManager, valueFactory);
        }

        @Override
        public SyncedIdentity findIdentity(@Nonnull UserManager userManager, @Nonnull String id) throws RepositoryException {
            return null;
        }

        @Override
        public boolean requiresSync(@Nonnull SyncedIdentity identity) {
            return false;
        }

        @Nonnull
        @Override
        public Iterator<SyncedIdentity> listIdentities(@Nonnull UserManager userManager) throws RepositoryException {
            return Iterators.emptyIterator();
        }
    }
}
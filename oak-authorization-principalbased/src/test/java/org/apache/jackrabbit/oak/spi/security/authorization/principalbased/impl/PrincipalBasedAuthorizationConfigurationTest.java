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

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregationFilter;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.security.AccessControlManager;
import java.lang.reflect.Field;
import java.security.Principal;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.CompositeConfiguration.PARAM_RANKING;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_ENTRY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.PARAM_ENABLE_AGGREGATION_FILTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrincipalBasedAuthorizationConfigurationTest extends AbstractPrincipalBasedTest {

    @Test
    public void testEmptyConstructor() {
        assertEquals(ConfigurationParameters.EMPTY, new PrincipalBasedAuthorizationConfiguration().getParameters());
    }

    @Test
    public void testGetName() {
        assertEquals(AuthorizationConfiguration.NAME, new PrincipalBasedAuthorizationConfiguration().getName());
    }

    @Test
    public void testGetCommitHooks() {
        assertTrue(new PrincipalBasedAuthorizationConfiguration().getCommitHooks("wspName").isEmpty());
    }

    @Test
    public void testGetValidators() {
        List<? extends ValidatorProvider> l = new PrincipalBasedAuthorizationConfiguration().getValidators("wspName", Set.of(), new MoveTracker());
        assertEquals(1, l.size());
        assertTrue(l.get(0) instanceof PrincipalPolicyValidatorProvider);
    }

    @Test
    public void testGetProtectedItemImporters() {
        List<ProtectedItemImporter> l = new PrincipalBasedAuthorizationConfiguration().getProtectedItemImporters();
        assertEquals(1, l.size());
        assertTrue(l.get(0) instanceof PrincipalPolicyImporter);
    }

    @Test
    public void testGetRepositoryInitializer() {
        RepositoryInitializer ri = new PrincipalBasedAuthorizationConfiguration().getRepositoryInitializer();
        assertNotSame(RepositoryInitializer.DEFAULT, ri);
    }

    @Test
    public void testInitialized() throws Exception {
        NodeTypeManager nodeTypeManager = ReadOnlyNodeTypeManager.getInstance(root, NamePathMapper.DEFAULT);
        for (String ntName : new String[] {NT_REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_ENTRY, NT_REP_RESTRICTIONS}) {
            assertTrue(nodeTypeManager.hasNodeType(ntName));
        }
    }

    @Test
    public void testGetRepositoryInitializerInitialized() {
        Root r = getRootProvider().createReadOnlyRoot(root);
        CompositeConfiguration<AuthorizationConfiguration> cc = (CompositeConfiguration) getSecurityProvider().getConfiguration(AuthorizationConfiguration.class);
        PrincipalBasedAuthorizationConfiguration pbac = null;
        for (AuthorizationConfiguration ac : cc.getConfigurations())  {
            if (ac instanceof PrincipalBasedAuthorizationConfiguration) {
                pbac = (PrincipalBasedAuthorizationConfiguration) ac;
                break;
            }
        }

        assertNotNull(pbac);
        RepositoryInitializer ri = pbac.getRepositoryInitializer();
        ri.initialize(new ReadOnlyBuilder(getTreeProvider().asNodeState(r.getTree("/"))));
    }

    @Test
    public void testGetContext() {
        assertSame(ContextImpl.INSTANCE, new PrincipalBasedAuthorizationConfiguration().getContext());
    }

    @Test
    public void testGetPermissionProviderUnsupportedPrincipals() throws Exception {
        FilterProvider fp = when(mock(FilterProvider.class).getFilter(any(SecurityProvider.class), any(Root.class), any(NamePathMapper.class))).thenReturn(mock(Filter.class)).getMock();

        PrincipalBasedAuthorizationConfiguration pbac = new PrincipalBasedAuthorizationConfiguration();
        pbac.bindFilterProvider(fp);
        pbac.setSecurityProvider(securityProvider);
        pbac.setRootProvider(getRootProvider());

        Set<Principal> principals = Set.of(EveryonePrincipal.getInstance(), getTestUser().getPrincipal());
        PermissionProvider pp = pbac.getPermissionProvider(root, "wspName", principals);
        assertSame(EmptyPermissionProvider.getInstance(), pp);

        principals = Set.of(getTestSystemUser().getPrincipal());
        pp = pbac.getPermissionProvider(root, "wspName", principals);
        assertSame(EmptyPermissionProvider.getInstance(), pp);
    }

    @Test
    public void testGetPermissionProvider() throws Exception {
        Filter filter = mock(Filter.class);
        when(filter.canHandle(any(Set.class))).thenReturn(Boolean.TRUE);
        when(filter.getOakPath(any(Principal.class))).thenReturn("/some/path");
        FilterProvider fp = when(mock(FilterProvider.class).getFilter(any(SecurityProvider.class), any(Root.class), any(NamePathMapper.class))).thenReturn(filter).getMock();

        PrincipalBasedAuthorizationConfiguration pbac = new PrincipalBasedAuthorizationConfiguration();
        pbac.bindFilterProvider(fp);
        pbac.setSecurityProvider(getSecurityProvider());
        pbac.setRootProvider(getRootProvider());

        Set<Principal> principals = Set.of(getTestUser().getPrincipal());
        PermissionProvider pp = pbac.getPermissionProvider(root, "wspName", principals);
        assertTrue(pp instanceof PrincipalBasedPermissionProvider);
    }

    @Test
    public void testGetPermissionProvider2() throws Exception {
        PrincipalBasedAuthorizationConfiguration pbac = new PrincipalBasedAuthorizationConfiguration();
        pbac.bindFilterProvider(getFilterProvider());
        pbac.setSecurityProvider(getSecurityProvider());
        pbac.setRootProvider(getRootProvider());

        Set<Principal> principals = Set.of(getTestSystemUser().getPrincipal());
        PermissionProvider pp = pbac.getPermissionProvider(root, "wspName", principals);
        assertTrue(pp instanceof PrincipalBasedPermissionProvider);
    }

    @Test
    public void testGetAccessControlManager() {
        PrincipalBasedAuthorizationConfiguration pbac = new PrincipalBasedAuthorizationConfiguration();
        pbac.setSecurityProvider(getSecurityProvider());
        pbac.bindFilterProvider(mock(FilterProvider.class));

        AccessControlManager acMgr = pbac.getAccessControlManager(root, NamePathMapper.DEFAULT);
        assertTrue(acMgr instanceof PrincipalBasedAccessControlManager);
    }

    @Test
    public void testGetRestrictionProvider() {
        PrincipalBasedAuthorizationConfiguration pbac = new PrincipalBasedAuthorizationConfiguration();
        assertSame(RestrictionProvider.EMPTY, pbac.getRestrictionProvider());

        pbac.setParameters(getSecurityProvider().getParameters(AuthorizationConfiguration.NAME));
        RestrictionProvider rp = pbac.getRestrictionProvider();

        assert(rp instanceof RestrictionProviderImpl);
    }

    @Test
    public void testActivate() {
        PrincipalBasedAuthorizationConfiguration pbac = getPrincipalBasedAuthorizationConfiguration();

        BundleContext ctx = mock(BundleContext.class);
        PrincipalBasedAuthorizationConfiguration.Configuration config = mock(PrincipalBasedAuthorizationConfiguration.Configuration.class);
        when(config.configurationRanking()).thenReturn(50);
        when(config.enableAggregationFilter()).thenReturn(true);
        pbac.activate(ctx, config);

        ConfigurationParameters params = pbac.getParameters();
        assertEquals(50, params.get(PARAM_RANKING));
        assertEquals(Boolean.TRUE, params.get(PARAM_ENABLE_AGGREGATION_FILTER));

        verify(ctx, times(1)).registerService(anyString(), any(AggregationFilter.class), any(Hashtable.class));
    }

    @Test
    public void testModified() {
        PrincipalBasedAuthorizationConfiguration pbac = getPrincipalBasedAuthorizationConfiguration();

        ServiceRegistration registrationMock = mock(ServiceRegistration.class);
        BundleContext ctx = when(mock(BundleContext.class).registerService(anyString(), any(AggregationFilter.class), any(Hashtable.class))).thenReturn(registrationMock).getMock();
        PrincipalBasedAuthorizationConfiguration.Configuration config = mock(PrincipalBasedAuthorizationConfiguration.Configuration.class);
        when(config.configurationRanking()).thenReturn(50);
        when(config.enableAggregationFilter()).thenReturn(true);
        pbac.activate(ctx, config);

        when(config.configurationRanking()).thenReturn(85);
        when(config.enableAggregationFilter()).thenReturn(true);
        pbac.modified(ctx, config);

        ConfigurationParameters params = pbac.getParameters();
        assertEquals(85, params.get(PARAM_RANKING));
        assertEquals(Boolean.TRUE, params.get(PARAM_ENABLE_AGGREGATION_FILTER));

        verify(ctx, times(1)).registerService(anyString(), any(AggregationFilter.class), any(Hashtable.class));
    }

    @Test
    public void testModified2() {
        PrincipalBasedAuthorizationConfiguration pbac = getPrincipalBasedAuthorizationConfiguration();

        ServiceRegistration registrationMock = mock(ServiceRegistration.class);
        BundleContext ctx = when(mock(BundleContext.class).registerService(anyString(), any(AggregationFilter.class), any(Hashtable.class))).thenReturn(registrationMock).getMock();
        PrincipalBasedAuthorizationConfiguration.Configuration config = mock(PrincipalBasedAuthorizationConfiguration.Configuration.class);
        when(config.configurationRanking()).thenReturn(50);
        when(config.enableAggregationFilter()).thenReturn(true);
        pbac.activate(ctx, config);

        when(config.configurationRanking()).thenReturn(85);
        when(config.enableAggregationFilter()).thenReturn(false);
        pbac.modified(ctx, config);

        ConfigurationParameters params = pbac.getParameters();
        assertEquals(85, params.get(PARAM_RANKING));
        assertEquals(Boolean.FALSE, params.get(PARAM_ENABLE_AGGREGATION_FILTER));

        verify(registrationMock, times(1)).unregister();
    }

    @Test
    public void testDeactivate() {
        PrincipalBasedAuthorizationConfiguration pbac = getPrincipalBasedAuthorizationConfiguration();

        ServiceRegistration registrationMock = mock(ServiceRegistration.class);
        BundleContext ctx = when(mock(BundleContext.class).registerService(anyString(), any(AggregationFilter.class), any(Hashtable.class))).thenReturn(registrationMock).getMock();
        PrincipalBasedAuthorizationConfiguration.Configuration config = mock(PrincipalBasedAuthorizationConfiguration.Configuration.class);
        when(config.configurationRanking()).thenReturn(50);
        when(config.enableAggregationFilter()).thenReturn(true);
        pbac.activate(ctx, config);

        pbac.deactivate(ctx, config);
        verify(registrationMock, times(1)).unregister();
    }

    @Test
    public void testBindUnbindFilterProvider() throws Exception {
        Field fp = PrincipalBasedAuthorizationConfiguration.class.getDeclaredField("filterProvider");
        fp.setAccessible(true);

        PrincipalBasedAuthorizationConfiguration pbac = new PrincipalBasedAuthorizationConfiguration();
        pbac.bindFilterProvider(mock(FilterProvider.class));

        assertNotNull(fp.get(pbac));

        pbac.unbindFilterProvider(mock(FilterProvider.class));
        assertNull(fp.get(pbac));
    }

    @Test
    public void testBindUnbindMountInfoProvider() throws Exception {
        Field f = PrincipalBasedAuthorizationConfiguration.class.getDeclaredField("mountInfoProvider");
        f.setAccessible(true);

        PrincipalBasedAuthorizationConfiguration pbac = new PrincipalBasedAuthorizationConfiguration();
        pbac.bindMountInfoProvider(mock(MountInfoProvider.class));

        assertNotNull(f.get(pbac));

        pbac.unbindMountInfoProvider(mock(MountInfoProvider.class));
        assertNull(f.get(pbac));
    }
}
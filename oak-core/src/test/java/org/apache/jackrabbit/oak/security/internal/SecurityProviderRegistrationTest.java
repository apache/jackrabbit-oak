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
 * See the License for the specific language governing permissions andF
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.security.internal;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authentication.monitor.LoginModuleMonitorImpl;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.security.authorization.restriction.WhiteboardRestrictionProvider;
import org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.RandomAuthorizableNodeName;
import org.apache.jackrabbit.oak.security.user.UserAuthenticationFactoryImpl;
import org.apache.jackrabbit.oak.security.user.whiteboard.WhiteboardAuthorizableActionProvider;
import org.apache.jackrabbit.oak.security.user.whiteboard.WhiteboardAuthorizableNodeName;
import org.apache.jackrabbit.oak.security.user.whiteboard.WhiteboardUserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.RegistrationConstants;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMBean;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMonitor;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleStatsCollector;
import org.apache.jackrabbit.oak.spi.security.authentication.token.CompositeTokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ReadPolicy;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregationFilter;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.CompositePrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider;
import org.apache.jackrabbit.oak.stats.Monitor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

import javax.jcr.security.AccessControlPolicy;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.osgi.framework.Constants.SERVICE_PID;
import static org.osgi.framework.Constants.SERVICE_RANKING;

public class SecurityProviderRegistrationTest extends AbstractSecurityTest {

    private static final Map<String, Object> PROPS = Map.of(SERVICE_PID, "pid");

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final SecurityProviderRegistration registration = new SecurityProviderRegistration();

    private static void assertContext(@NotNull Context context, int expectedSize, @NotNull Tree tree, boolean isDefined) throws Exception {
        Class<?> c = context.getClass();
        assertTrue(c.getName().endsWith("CompositeContext"));

        Field f = c.getDeclaredField("delegatees");
        f.setAccessible(true);

        if (expectedSize == 0) {
            assertNull(f.get(context));
        } else {
            assertEquals(expectedSize, ((Context[]) f.get(context)).length);
        }

        assertEquals(isDefined, context.definesContextRoot(tree));
        assertEquals(isDefined, context.definesTree(tree));
        assertEquals(isDefined, context.definesProperty(tree, PropertyStates.createProperty("abc", "abc")));
        assertEquals(isDefined, context.definesLocation(TreeLocation.create(tree)));
    }

    private static <T extends SecurityConfiguration> T mockConfiguration(Class<T> cl) {
        T sc = mock(cl);
        Context ctx = mock(Context.class, withSettings().defaultAnswer(invocationOnMock -> Boolean.TRUE));
        when(sc.getContext()).thenReturn(ctx);
        when(sc.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
        return sc;
    }

    private static SecurityProviderRegistration.Configuration configWithRequiredServiceIds(@NotNull String... ids) {
        return new SecurityProviderRegistration.Configuration() {
            @Override
            public Class<? extends Annotation> annotationType() { return SecurityProviderRegistration.Configuration.class; }

            @Override
            public String[] requiredServicePids() { return ids; }

            @Override
            public String authorizationCompositionType() { return "AND"; }
        };
    }

    @Test
    public void testActivateWithRequiredId() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceId"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNull(service);

        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("serviceId").getMock();
        registration.bindAuthorizableNodeName(sr, mock(AuthorizableNodeName.class));

        service = context.getService(SecurityProvider.class);
        assertNotNull(service);
    }

    @Test
    public void testActivate() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceA", "serviceB"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNull(service);

        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("serviceA").getMock();

        registration.bindRestrictionProvider(sr, mockRp);

        service = context.getService(SecurityProvider.class);
        assertNull(service);

        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), Map.of(SERVICE_PID, "serviceB"));
        service = context.getService(SecurityProvider.class);
        assertNotNull(service);
    }

    @Test
    public void testActivateAddsPrecondition() throws Exception {
        Field f = registration.getClass().getDeclaredField("preconditions");
        f.setAccessible(true);

        assertTrue(((Preconditions) f.get(registration)).areSatisfied());

        registration.activate(context.bundleContext(), configWithRequiredServiceIds("requiredService"));

        assertFalse(((Preconditions) f.get(registration)).areSatisfied());
    }

    @Test
    public void testActivateWithoutPreconditions() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds());

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNotNull(service);
        assertEquals(6, Iterables.size(Iterables.filter(service.getConfigurations(), x -> x != null)));
    }


    @Test
    public void testActivateWhileRegistering() throws Exception {
        Field f = registration.getClass().getDeclaredField("registering");
        f.setAccessible(true);
        f.set(registration, true);

        registration.activate(context.bundleContext(), configWithRequiredServiceIds());
        assertNull(context.getService(SecurityProvider.class));
    }

    @Test
    public void testModified() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("rpId", "authorizationId"));

        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), Map.of(SERVICE_PID, "authorizationId"));

        assertNull(context.getService(SecurityProvider.class));

        // modify requiredServiceIds by removing the rpId from the mandatory services
        // => should re-register the security provider
        registration.modified(configWithRequiredServiceIds("authorizationId"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNotNull(service);

        RestrictionProvider rp = service.getConfiguration(AuthorizationConfiguration.class).getRestrictionProvider();
        assertTrue(rp instanceof WhiteboardRestrictionProvider);
    }

    @Test
    public void testModifiedPreconditionStillSatisfied() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("rpId", "authorizationId"));

        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("rpId").getMock();

        registration.bindRestrictionProvider(sr, mockRp);
        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), Map.of(SERVICE_PID, "authorizationId"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNotNull(service);

        registration.modified(configWithRequiredServiceIds("authorizationId"));

        SecurityProvider service2 = context.getService(SecurityProvider.class);
        assertSame(service, service2);
    }

    @Test
    public void testDeactivate() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("nodeName"));

        AuthorizableNodeName mock = mock(AuthorizableNodeName.class);
        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("nodeName").getMock();

        registration.bindAuthorizableNodeName(sr, mock);

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNotNull(service);

        registration.deactivate();

        // provider must have been unregistered
        assertNull(context.getService(SecurityProvider.class));
    }

    @Test
    public void testDeactivateWithoutPreconditions() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds());
        UserAuthenticationFactory mock = mock(UserAuthenticationFactory.class);
        ServiceReference serviceReference = when(mock(ServiceReference.class).getProperty(OAK_SECURITY_NAME)).thenReturn("my.custom.uaf").getMock();
        registration.bindUserAuthenticationFactory(serviceReference, mock);

        assertNotNull(context.getService(SecurityProvider.class));

        registration.deactivate();

        // securityprovider must have been unregistered
        assertNull(context.getService(SecurityProvider.class));
    }

    @Test
    public void testDeactivateClearsPreconditions() throws Exception {
        Field f = registration.getClass().getDeclaredField("preconditions");
        f.setAccessible(true);

        registration.activate(context.bundleContext(), configWithRequiredServiceIds("nodeName"));

        assertFalse(((Preconditions) f.get(registration)).areSatisfied());

        AuthorizableNodeName mock = mock(AuthorizableNodeName.class);
        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("nodeName").getMock();

        registration.bindAuthorizableNodeName(sr, mock);

        assertTrue(((Preconditions) f.get(registration)).areSatisfied());

        registration.deactivate();

        assertTrue(((Preconditions) f.get(registration)).areSatisfied());
    }

    @Test
    public void testDeactivateUnregistersMBeans() {
        // activate
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceA"));
        
        // validate that registration is not performed
        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNull(service);
        
        // register AuthenticationConfiguration to trigger MBean registration 
        AuthenticationConfigurationImpl mockAc = mock(AuthenticationConfigurationImpl.class);
        when(mockAc.getMonitors(any(StatisticsProvider.class))).thenReturn(Collections.singleton(new LoginModuleMonitorImpl(StatisticsProvider.NOOP)));
        registration.bindAuthenticationConfiguration(mockAc);

        // register required service
        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("serviceA").getMock();

        registration.bindRestrictionProvider(sr, mockRp);

        // assert that SecurityProvider is registered
        service = context.getService(SecurityProvider.class);
        assertNotNull(service);
        
        // assert that MBean is registered
        assertThat("LoginModuleMBean is not registered", context.getService(LoginModuleMBean.class), notNullValue());
        
        // manually deactivate, simulate AuthenticationConfiguration going away due to new config for it coming in
        registration.deactivate();
        
        // assert that service is now unregistered
        service = context.getService(SecurityProvider.class);
        assertNull(service);
        
        // assert that MBean is no longer registered
        assertThat("LoginModuleMBean is still registered", context.getService(LoginModuleMBean.class), nullValue());
    }
    
    @Test
    public void testBindOptionalCandidate() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceId"));

        Field f = registration.getClass().getDeclaredField("preconditions");
        f.setAccessible(true);

        TokenConfiguration tc = mockConfiguration(TokenConfiguration.class);
        registration.bindTokenConfiguration(tc, Map.of(SERVICE_PID, "otherServiceId"));

        Preconditions preconditions = (Preconditions) f.get(registration);
        assertFalse(preconditions.areSatisfied());

        assertNull(context.getService(SecurityProvider.class));
    }

    @Test
    public void testBindOptionalCandidateAfterRegistration() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceId"));

        registration.bindTokenConfiguration(mockConfiguration(TokenConfiguration.class), Map.of(SERVICE_PID, "serviceId"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNotNull(service);

        // binding another (optional configuration) must not result in re-registration of the service
        registration.bindPrincipalConfiguration(mockConfiguration(PrincipalConfiguration.class), Map.of(SERVICE_PID, "optionalService"));

        SecurityProvider service2 = context.getService(SecurityProvider.class);
        assertSame(service, service2);
    }

    @Test
    public void testBindMandatoryCandidate() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceId"));

        Field f = registration.getClass().getDeclaredField("preconditions");
        f.setAccessible(true);

        TokenConfiguration tc = mockConfiguration(TokenConfiguration.class);
        registration.bindTokenConfiguration(tc, Map.of(SERVICE_PID, "serviceId"));

        Preconditions preconditions = (Preconditions) f.get(registration);
        assertTrue(preconditions.areSatisfied());

        assertNotNull(context.getService(SecurityProvider.class));
    }

    @Test
    public void testUnbindMandatoryCandidate() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("actionProvider"));

        registration.bindUserConfiguration(mockConfiguration(UserConfiguration.class));

        AuthorizableActionProvider ap = mock(AuthorizableActionProvider.class);
        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("actionProvider").getMock();

        registration.bindAuthorizableActionProvider(sr, ap);

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNotNull(service);

        registration.unbindAuthorizableActionProvider(sr, ap);
        service = context.getService(SecurityProvider.class);
        assertNull(service);
    }

    @Test
    public void testUnbindMandatoryCandidateOnPreconditions() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("nodeName"));

        Field f = registration.getClass().getDeclaredField("preconditions");
        f.setAccessible(true);

        AuthorizableNodeName mock = mock(AuthorizableNodeName.class);
        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("nodeName").getMock();

        registration.bindAuthorizableNodeName(sr, mock);
        registration.unbindAuthorizableNodeName(sr, mock);

        Preconditions preconditions = (Preconditions) f.get(registration);
        assertFalse(preconditions.areSatisfied());
    }

    @Test
    public void testUnbindOptionalCandidateAfterRegistration() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceId"));

        UserAuthenticationFactory uaf = mock(UserAuthenticationFactory.class);
        ServiceReference serviceReference = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("notMandatory").getMock();

        registration.bindUserAuthenticationFactory(serviceReference, uaf);

        assertNull(context.getService(SecurityProvider.class));

        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("serviceId").getMock();
        registration.bindAuthorizableActionProvider(sr, mock(AuthorizableActionProvider.class));

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNotNull(service);

        // unbinding an optional configuration must not result in unrregistration of the service
        registration.unbindUserAuthenticationFactory(serviceReference, uaf);

        SecurityProvider service2 = context.getService(SecurityProvider.class);
        assertSame(service, service2);
    }

    @Test
    public void testBindUnbindAuthenticationConfiguration() throws Exception {
        Field f = registration.getClass().getDeclaredField("authenticationConfiguration");
        f.setAccessible(true);

        assertNull(f.get(registration));

        AuthenticationConfiguration ac = mockConfiguration(AuthenticationConfiguration.class);
        registration.bindAuthenticationConfiguration(ac);

        assertSame(ac, f.get(registration));

        registration.unbindAuthenticationConfiguration(ac);
        assertNull(f.get(registration));
    }

    @Test
    public void testBindAnotherAuthenticationConfiguration() throws Exception {
        Field f = registration.getClass().getDeclaredField("authenticationConfiguration");
        f.setAccessible(true);

        AuthenticationConfiguration ac = mockConfiguration(AuthenticationConfiguration.class);
        registration.bindAuthenticationConfiguration(ac);
        assertSame(ac, f.get(registration));

        AuthenticationConfiguration ac2 = mockConfiguration(AuthenticationConfiguration.class);
        registration.bindAuthenticationConfiguration(ac2);
        assertSame(ac2, f.get(registration));
    }

    @Test
    public void testBindAuthenticationConfigWithLoginModuleStatsCollector() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("authorizationId"));

        AuthenticationConfiguration ac = mock(AuthenticationConfiguration.class, withSettings().extraInterfaces(LoginModuleStatsCollector.class));
        when(ac.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
        when(ac.getContext()).thenReturn(mock(Context.class));

        registration.bindAuthenticationConfiguration(ac);
        // trigger maybeRegister
        registration.bindAuthorizationConfiguration(mockConfiguration(AuthorizationConfiguration.class), ConfigurationParameters.of(OAK_SECURITY_NAME, "authorizationId"));

        verify(((LoginModuleStatsCollector) ac), never()).setLoginModuleMonitor(any(LoginModuleMonitor.class));
        verify(ac, times(1)).getMonitors(any(StatisticsProvider.class));
    }

    @Test
    public void testBindUnbindPrivilegeConfiguration() throws Exception {
        Field f = registration.getClass().getDeclaredField("privilegeConfiguration");
        f.setAccessible(true);

        assertNull(f.get(registration));

        PrivilegeConfiguration pc = mockConfiguration(PrivilegeConfiguration.class);
        registration.bindPrivilegeConfiguration(pc);

        assertSame(pc, f.get(registration));

        registration.unbindPrivilegeConfiguration(pc);
        assertNull(f.get(registration));
    }

    @Test
    public void testBindUnbindUserConfiguration() throws Exception {
        Field f = registration.getClass().getDeclaredField("userConfiguration");
        f.setAccessible(true);

        assertNull(f.get(registration));

        UserConfiguration uc = mockConfiguration(UserConfiguration.class);
        registration.bindUserConfiguration(uc);

        assertSame(uc, f.get(registration));

        registration.unbindUserConfiguration(uc);
        assertNull(f.get(registration));
    }

    @Test
    public void testBindUnbindTokenConfiguration() throws Exception {
        Field f = registration.getClass().getDeclaredField("tokenConfiguration");
        f.setAccessible(true);

        assertTrue(f.get(registration) instanceof CompositeTokenConfiguration);

        TokenConfiguration tc = mockConfiguration(TokenConfiguration.class);
        registration.bindTokenConfiguration(tc, PROPS);

        CompositeTokenConfiguration composite = (CompositeTokenConfiguration) f.get(registration);
        assertEquals(1, composite.getConfigurations().size());
        assertTrue(composite.getConfigurations().contains(tc));

        registration.unbindTokenConfiguration(tc, PROPS);
        composite = (CompositeTokenConfiguration) f.get(registration);
        assertTrue(composite.getConfigurations().isEmpty());
    }

    @Test
    public void testAuthorizationRanking() throws Exception {
        Field f = registration.getClass().getDeclaredField("authorizationConfiguration");
        f.setAccessible(true);

        AuthorizationConfiguration testAc = mockConfiguration(AuthorizationConfiguration.class);
        registration.bindAuthorizationConfiguration(testAc, ConfigurationParameters.EMPTY);

        AuthorizationConfigurationImpl ac = new AuthorizationConfigurationImpl();
        ac.setParameters(ConfigurationParameters.of(CompositeConfiguration.PARAM_RANKING, 500));
        registration.bindAuthorizationConfiguration(ac, PROPS);

        AuthorizationConfiguration testAc2 = mockConfiguration(AuthorizationConfiguration.class);
        Map<String, Object> props = Map.of(Constants.SERVICE_RANKING, 100);
        registration.bindAuthorizationConfiguration(testAc2, props);

        CompositeAuthorizationConfiguration cac = (CompositeAuthorizationConfiguration) f.get(registration);

        List<AuthorizationConfiguration> list = cac.getConfigurations();
        assertEquals(3, list.size());

        assertSame(ac, list.get(0));
        assertSame(testAc2, list.get(1));
        assertSame(testAc, list.get(2));
    }

    @Test
    public void testAuthorizationContext() throws Exception {
        Tree t = root.getTree("/");

        Field f = registration.getClass().getDeclaredField("authorizationConfiguration");
        f.setAccessible(true);

        AuthorizationConfiguration ac = new AuthorizationConfigurationImpl();
        registration.bindAuthorizationConfiguration(ac, PROPS);
        CompositeAuthorizationConfiguration cac = (CompositeAuthorizationConfiguration) f.get(registration);
        Context ctx = cac.getContext();
        assertContext(ctx, 1, t, false);

        AuthorizationConfiguration ac1 = mockConfiguration(AuthorizationConfiguration.class);
        registration.bindAuthorizationConfiguration(ac1, PROPS);
        cac = (CompositeAuthorizationConfiguration) f.get(registration);
        ctx = cac.getContext();
        assertContext(ctx, 2, t, true);

        AuthorizationConfiguration ac2 = mockConfiguration(AuthorizationConfiguration.class);
        registration.bindAuthorizationConfiguration(ac2, PROPS);
        cac = (CompositeAuthorizationConfiguration) f.get(registration);
        ctx = cac.getContext();
        assertContext(ctx, 3, t, true);

        // unbind again:

        registration.unbindAuthorizationConfiguration(ac1, PROPS);
        cac = (CompositeAuthorizationConfiguration) f.get(registration);
        ctx = cac.getContext();
        assertContext(ctx, 2, t, true);

        registration.unbindAuthorizationConfiguration(ac, PROPS);
        cac = (CompositeAuthorizationConfiguration) f.get(registration);
        ctx = cac.getContext();
        assertContext(ctx, 1, t, true);

        registration.unbindAuthorizationConfiguration(ac2, PROPS);
        cac = (CompositeAuthorizationConfiguration) f.get(registration);
        ctx = cac.getContext();
        assertContext(ctx, 0, t, false);
    }

    @Test
    public void testPrincipalContext() throws Exception {
        Tree t = root.getTree("/");

        Field f = registration.getClass().getDeclaredField("principalConfiguration");
        f.setAccessible(true);

        PrincipalConfiguration pc = new PrincipalConfigurationImpl();
        registration.bindPrincipalConfiguration(pc, PROPS);
        CompositePrincipalConfiguration cpc = (CompositePrincipalConfiguration) f.get(registration);
        Context ctx = cpc.getContext();
        // expected size = 0 because PrincipalConfigurationImpl comes with the default ctx
        assertContext(ctx, 0, t, false);

        PrincipalConfiguration pc1 = mockConfiguration(PrincipalConfiguration.class);
        registration.bindPrincipalConfiguration(pc1, PROPS);
        cpc = (CompositePrincipalConfiguration) f.get(registration);
        ctx = cpc.getContext();
        // expected size 1 because the PrincipalConfigurationImpl comes with the default ctx
        assertContext(ctx, 1, t, true);

        PrincipalConfiguration pc2 = mockConfiguration(PrincipalConfiguration.class);
        registration.bindPrincipalConfiguration(pc2, PROPS);
        cpc = (CompositePrincipalConfiguration) f.get(registration);
        ctx = cpc.getContext();
        assertContext(ctx, 2, t, true);

        // unbind again:

        registration.unbindPrincipalConfiguration(pc, PROPS);
        cpc = (CompositePrincipalConfiguration) f.get(registration);
        ctx = cpc.getContext();
        assertContext(ctx, 2, t, true);

        registration.unbindPrincipalConfiguration(pc1, PROPS);
        cpc = (CompositePrincipalConfiguration) f.get(registration);
        ctx = cpc.getContext();
        assertContext(ctx, 1, t, true);

        registration.unbindPrincipalConfiguration(pc2, PROPS);
        cpc = (CompositePrincipalConfiguration) f.get(registration);
        ctx = cpc.getContext();
        assertContext(ctx, 0, t, false);
    }

    @Test
    public void testBindRestrictionProviderWithoutAuthorizationConfig() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceId"));

        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("serviceId").getMock();

        registration.bindRestrictionProvider(sr, mockRp);

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNotNull(service);

        AuthorizationConfiguration ac = service.getConfiguration(AuthorizationConfiguration.class);
        assertTrue(ac instanceof CompositeAuthorizationConfiguration);

        // empty composite configuration => empty rp
        RestrictionProvider rp = ac.getRestrictionProvider();
        assertSame(RestrictionProvider.EMPTY, rp);
    }

    @Test
    public void testBindRestrictionProviderWithAuthorizationConfig() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("rpId", "authorizationId"));

        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("rpId").getMock();
        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        registration.bindRestrictionProvider(sr, mockRp);
        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), Map.of(SERVICE_PID, "authorizationId"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        RestrictionProvider rp = service.getConfiguration(AuthorizationConfiguration.class).getRestrictionProvider();
        assertTrue(rp instanceof WhiteboardRestrictionProvider);
    }

    @Test
    public void testBindWithMissingPID() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("rpId"));

        ServiceReference sr = mock(ServiceReference.class);
        RestrictionProvider mockRp = mock(RestrictionProvider.class);

        registration.bindRestrictionProvider(sr, mockRp);
        assertNull(context.getService(SecurityProvider.class));
    }

    @Test
    public void testUnbindWithMissingPID() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("rpId"));

        ServiceReference sr = when(mock(ServiceReference.class).getProperty(SERVICE_PID)).thenReturn("rpId").getMock();
        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        registration.bindRestrictionProvider(sr, mockRp);
        assertNotNull(context.getService(SecurityProvider.class));

        when(sr.getProperty(SERVICE_PID)).thenReturn(null);
        registration.unbindRestrictionProvider(sr, mockRp);

        assertNotNull(context.getService(SecurityProvider.class));
    }

    @Test
    public void testBindConfigurationWithMissingPID() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("authorizationId"));

        AuthorizationConfiguration mockConfiguration = mockConfiguration(AuthorizationConfiguration.class);
        registration.bindAuthorizationConfiguration(mockConfiguration, ConfigurationParameters.EMPTY);

        assertNull(context.getService(SecurityProvider.class));
    }

    @Test
    public void testUnbindConfigurationWithMissingPID() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("authorizationId"));

        AuthorizationConfiguration mockConfiguration = mockConfiguration(AuthorizationConfiguration.class);
        registration.bindAuthorizationConfiguration(mockConfiguration, ConfigurationParameters.of(OAK_SECURITY_NAME, "authorizationId"));

        assertNotNull(context.getService(SecurityProvider.class));

        registration.unbindAuthorizationConfiguration(mockConfiguration, ConfigurationParameters.EMPTY);

        assertNotNull(context.getService(SecurityProvider.class));
    }

    @Test
    public void testActivateWithRequiredOakSecurityName() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceId"));

        assertNull(context.getService(SecurityProvider.class));

        ServiceReference sr = when(mock(ServiceReference.class).getProperty(OAK_SECURITY_NAME)).thenReturn("serviceId").getMock();
        RestrictionProvider rp = mock(RestrictionProvider.class);
        registration.bindRestrictionProvider(sr, rp);

        assertNotNull(context.getService(SecurityProvider.class));

        registration.unbindRestrictionProvider(sr, rp);
        assertNull(context.getService(SecurityProvider.class));
    }
    
    @Test
    public void testActivateWithMixedServicePiAnddOakServiceName() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("rpId", "authorizationId"));

        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        ServiceRegistration sr = context.bundleContext().registerService(RestrictionProvider.class.getName(), mockRp, new Hashtable(Map.of(SERVICE_PID, "rpId")));

        registration.bindRestrictionProvider(sr.getReference(), mockRp);
        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), Map.of(RegistrationConstants.OAK_SECURITY_NAME, "authorizationId"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        RestrictionProvider rp = service.getConfiguration(AuthorizationConfiguration.class).getRestrictionProvider();
        assertTrue(rp instanceof WhiteboardRestrictionProvider);
    }

    @Test
    public void testWhileboardRestrictionProvider() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("rpId", "authorizationId"));

        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        ServiceRegistration rpSr = context.bundleContext().registerService(RestrictionProvider.class.getName(), mockRp, new Hashtable(Map.of(SERVICE_PID, "rpId")));
        registration.bindRestrictionProvider(rpSr.getReference(), mockRp);
        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), Map.of(OAK_SECURITY_NAME, "authorizationId"));

        SecurityProvider service = context.getService(SecurityProvider.class);

        RestrictionProvider rp = service.getConfiguration(AuthorizationConfiguration.class).getRestrictionProvider();
        assertTrue(rp instanceof WhiteboardRestrictionProvider);
        rp.getSupportedRestrictions(null);
        verify(mockRp, times(1)).getSupportedRestrictions(null);
    }

    @Test
    public void testWhileboardAuthorizableActionProvider() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("apId"));

        AuthorizableActionProvider mockAp = mock(AuthorizableActionProvider.class);
        ServiceRegistration rpSr = context.bundleContext().registerService(AuthorizableActionProvider.class.getName(), mockAp, new Hashtable(Map.of(SERVICE_PID, "apId")));
        registration.bindAuthorizableActionProvider(rpSr.getReference(), mockAp);

        SecurityProvider service = context.getService(SecurityProvider.class);

        AuthorizableActionProvider ap = service.getConfiguration(UserConfiguration.class).getParameters().getConfigValue(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, null, AuthorizableActionProvider.class);
        assertTrue(ap instanceof WhiteboardAuthorizableActionProvider);
        ap.getAuthorizableActions(service);
        verify(mockAp, times(1)).getAuthorizableActions(service);
    }

    @Test
    public void testWhileboardAuthorizableNodeName() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("anId"));

        AuthorizableNodeName mockAn = mock(AuthorizableNodeName.class);
        ServiceRegistration rpSr = context.bundleContext().registerService(AuthorizableNodeName.class.getName(), mockAn, new Hashtable(Map.of(SERVICE_PID, "anId")));
        registration.bindAuthorizableNodeName(rpSr.getReference(), mockAn);

        SecurityProvider service = context.getService(SecurityProvider.class);

        AuthorizableNodeName an = service.getConfiguration(UserConfiguration.class).getParameters().getConfigValue(UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, null, AuthorizableNodeName.class);
        assertTrue(an instanceof WhiteboardAuthorizableNodeName);
        an.generateNodeName("id");
        verify(mockAn, times(1)).generateNodeName("id");
    }

    @Test
    public void testWhileboardUserAuthenticationFactory() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("uafId"));

        UserAuthenticationFactory mockUaf = mock(UserAuthenticationFactory.class);
        ServiceRegistration rpSr = context.bundleContext().registerService(UserAuthenticationFactory.class.getName(), mockUaf, new Hashtable(Map.of(SERVICE_PID, "uafId")));
        registration.bindUserAuthenticationFactory(rpSr.getReference(), mockUaf);

        SecurityProvider service = context.getService(SecurityProvider.class);

        UserAuthenticationFactory uaf = service.getConfiguration(UserConfiguration.class).getParameters().getConfigValue(UserConstants.PARAM_USER_AUTHENTICATION_FACTORY, null, UserAuthenticationFactory.class);
        assertTrue(uaf instanceof WhiteboardUserAuthenticationFactory);
        uaf.getAuthentication(getUserConfiguration(), root, "id");
        verify(mockUaf, times(1)).getAuthentication(getUserConfiguration(), root, "id");
    }

    @Test
    public void testMultipleUserAuthenticationFactoriesRespectsRanking() throws Exception {
        testMultipleServiceWithRanking("userAuthenticationFactories", new UserAuthenticationFactoryImpl(), new UserAuthenticationFactoryImpl(), new UserAuthenticationFactoryImpl());
    }

    @Test
    public void testMultipleAuthorizableNodeNamesRespectsRanking() throws Exception {
        testMultipleServiceWithRanking("authorizableNodeNames", new RandomAuthorizableNodeName(), new RandomAuthorizableNodeName(), new RandomAuthorizableNodeName());
    }

    @Test
    public void testMultipleAuthorizableActionProvidersRespectsRanking() throws Exception {
        testMultipleServiceWithRanking("authorizableActionProviders", new DefaultAuthorizableActionProvider(), new DefaultAuthorizableActionProvider(), new DefaultAuthorizableActionProvider());
    }

    @Test
    public void testMultipleRestrictionProvidersRespectsRanking() throws Exception {
        testMultipleServiceWithRanking("restrictionProviders", new RestrictionProviderImpl(), new RestrictionProviderImpl(), new RestrictionProviderImpl());
    }

    private void testMultipleServiceWithRanking(@NotNull String fieldName, @NotNull Object service1, @NotNull Object service2, @NotNull Object service3) throws Exception {
        context.registerService(SecurityProviderRegistration.class, registration, Map.of("requiredServicePids", new String[] {"s1", "s2", "s3"}));

        context.registerInjectActivateService(service1, Map.of(RegistrationConstants.OAK_SECURITY_NAME, "s1", Constants.SERVICE_RANKING, 50));
        context.registerInjectActivateService(service2, Map.of(RegistrationConstants.OAK_SECURITY_NAME, "s2"));
        context.registerInjectActivateService(service3, Map.of(RegistrationConstants.OAK_SECURITY_NAME, "s3", Constants.SERVICE_RANKING, 1));

        Field f = registration.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);

        SortedMap m = (SortedMap) f.get(registration);
        assertEquals(3, m.size());
        Collection c = m.values();
        assertTrue(Iterables.elementsEqual(ImmutableList.of(service2, service3, service1), c));
    }

    @Test
    public void testBindUnbindRootProvider() throws Exception {
        Field f = registration.getClass().getDeclaredField("rootProvider");
        f.setAccessible(true);

        assertNull(f.get(registration));

        RootProvider rp = mock(RootProvider.class);
        registration.bindRootProvider(rp);

        assertSame(rp, f.get(registration));

        registration.unbindRootProvider(rp);
        assertNull(f.get(registration));
    }

    @Test
    public void testBindUnbindTreeProvider() throws Exception {
        Field f = registration.getClass().getDeclaredField("treeProvider");
        f.setAccessible(true);

        assertNull(f.get(registration));

        TreeProvider tp = mock(TreeProvider.class);
        registration.bindTreeProvider(tp);

        assertSame(tp, f.get(registration));

        registration.unbindTreeProvider(tp);
        assertNull(f.get(registration));
    }

    @Test
    public void testBindAggregationFilter() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("filterId", "a1", "a2"));

        AggregationFilter filter = mock(AggregationFilter.class, withSettings().defaultAnswer(invocationOnMock -> Boolean.TRUE));
        ServiceRegistration sr = context.bundleContext().registerService(AggregationFilter.class.getName(), filter, new Hashtable(Map.of(SERVICE_PID, "filterId")));
        registration.bindAggregationFilter(sr.getReference(), filter);

        AggregatedPermissionProvider pp = mock(AggregatedPermissionProvider.class);
        JackrabbitAccessControlManager acMgr = mock(JackrabbitAccessControlManager.class);
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        when(acMgr.getEffectivePolicies(anyString())).thenReturn(new AccessControlPolicy[] {policy});
        when(acMgr.getEffectivePolicies(any(Set.class))).thenReturn(new AccessControlPolicy[] {policy});

        AuthorizationConfiguration ac1 = mock(AuthorizationConfiguration.class);
        AuthorizationConfiguration ac2 = mock(AuthorizationConfiguration.class);
        for (AuthorizationConfiguration ac : new AuthorizationConfiguration[]{ac1, ac2}) {
            when(ac.getPermissionProvider(any(Root.class), anyString(), any(Set.class))).thenReturn(pp);
            when(ac.getAccessControlManager(any(Root.class), any(NamePathMapper.class))).thenReturn(acMgr);
            when(ac.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
            when(ac.getContext()).thenReturn(Context.DEFAULT);
        }

        registration.bindAuthorizationConfiguration(ac1, new Hashtable(Map.of(SERVICE_PID, "a1")));
        registration.bindAuthorizationConfiguration(ac2, new Hashtable(Map.of(SERVICE_PID, "a2")));

        SecurityProvider service = context.getService(SecurityProvider.class);

        AuthorizationConfiguration ac = service.getConfiguration(AuthorizationConfiguration.class);
        assertTrue(ac instanceof CompositeAuthorizationConfiguration);

        PermissionProvider permissionProvider = ac.getPermissionProvider(root, adminSession.getWorkspaceName(), Set.of());
        assertSame(pp, permissionProvider);
        verify(filter, times(1)).stop(pp, Set.of());

        JackrabbitAccessControlManager am = (JackrabbitAccessControlManager) ac.getAccessControlManager(root, getNamePathMapper());

        assertEquals(1, am.getEffectivePolicies(PathUtils.ROOT_PATH).length);
        verify(filter, times(1)).stop(acMgr, PathUtils.ROOT_PATH);

        assertEquals(1, am.getEffectivePolicies(Set.of(EveryonePrincipal.getInstance())).length);
        verify(filter, times(1)).stop(acMgr, Set.of(EveryonePrincipal.getInstance()));
    }

    @Test
    public void testUnbindAggregationFilter() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("a1", "a2", "f1"));

        AggregationFilter filter = mock(AggregationFilter.class, withSettings().defaultAnswer(invocationOnMock -> Boolean.TRUE));
        ServiceRegistration sr = context.bundleContext().registerService(AggregationFilter.class.getName(), filter, new Hashtable(Map.of(SERVICE_PID, "f1")));
        registration.bindAggregationFilter(sr.getReference(), filter);

        AggregatedPermissionProvider pp = mock(AggregatedPermissionProvider.class);
        AuthorizationConfiguration ac1 = mock(AuthorizationConfiguration.class);
        AuthorizationConfiguration ac2 = mock(AuthorizationConfiguration.class);
        for (AuthorizationConfiguration ac : new AuthorizationConfiguration[]{ac1, ac2}) {
            when(ac.getPermissionProvider(any(Root.class), anyString(), any(Set.class))).thenReturn(pp);
            when(ac.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
            when(ac.getContext()).thenReturn(Context.DEFAULT);
        }

        registration.bindAuthorizationConfiguration(ac1, new Hashtable(Map.of(SERVICE_PID, "a1")));
        registration.bindAuthorizationConfiguration(ac2, new Hashtable(Map.of(SERVICE_PID, "a2")));

        AuthorizationConfiguration ac = context.getService(SecurityProvider.class).getConfiguration(AuthorizationConfiguration.class);
        assertTrue(ac instanceof CompositeAuthorizationConfiguration);

        PermissionProvider permissionProvider = ac.getPermissionProvider(root, adminSession.getWorkspaceName(), Set.of());
        assertSame(pp, permissionProvider);
        verify(filter, times(1)).stop(pp, Set.of());

        registration.unbindAggregationFilter(sr.getReference(), filter);
        assertNull(context.getService(SecurityProvider.class));

        registration.modified(configWithRequiredServiceIds("a1", "a2"));

        context.getService(SecurityProvider.class).getConfiguration(AuthorizationConfiguration.class).getPermissionProvider(root, adminSession.getWorkspaceName(), Set.of());
        // since unbind was called on filter -> no additional calls
        verify(filter, times(1)).stop(pp, Set.of());
    }

    @Test
    public void testMultipleEvaluationFilterFalse() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("f1", "f2", "ac1", "ac2"));

        AggregationFilter filter1 = mock(AggregationFilter.class, withSettings().defaultAnswer(invocationOnMock -> Boolean.FALSE));
        ServiceRegistration sr1 = context.bundleContext().registerService(AggregationFilter.class.getName(), filter1, new Hashtable(Map.of(SERVICE_PID, "f1", SERVICE_RANKING, 100)));
        registration.bindAggregationFilter(sr1.getReference(), filter1);

        AggregationFilter filter2 = mock(AggregationFilter.class, withSettings().defaultAnswer(invocationOnMock -> Boolean.FALSE));
        ServiceRegistration sr2 = context.bundleContext().registerService(AggregationFilter.class.getName(), filter2, new Hashtable(Map.of(SERVICE_PID, "f2", SERVICE_RANKING, 200)));
        registration.bindAggregationFilter(sr2.getReference(), filter2);

        AggregatedPermissionProvider pp = mock(AggregatedPermissionProvider.class);
        JackrabbitAccessControlManager acMgr = mock(JackrabbitAccessControlManager.class);
        // make sure different policies are returned for subsequent calls of the aggregated configurations
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        when(acMgr.getEffectivePolicies(anyString())).thenReturn(new AccessControlPolicy[] {policy}).thenReturn(new AccessControlPolicy[] {ReadPolicy.INSTANCE});
        when(acMgr.getEffectivePolicies(any(Set.class))).thenReturn(new AccessControlPolicy[] {policy}).thenReturn(new AccessControlPolicy[] {ReadPolicy.INSTANCE});

        AuthorizationConfiguration ac1 = mock(AuthorizationConfiguration.class);
        AuthorizationConfiguration ac2 = mock(AuthorizationConfiguration.class);
        for (AuthorizationConfiguration ac : new AuthorizationConfiguration[]{ac1, ac2}) {
            when(ac.getPermissionProvider(any(Root.class), anyString(), any(Set.class))).thenReturn(pp);
            when(ac.getAccessControlManager(any(Root.class), any(NamePathMapper.class))).thenReturn(acMgr);
            when(ac.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
            when(ac.getContext()).thenReturn(Context.DEFAULT);
        }

        registration.bindAuthorizationConfiguration(ac1, new Hashtable(Map.of(SERVICE_PID, "ac1")));
        registration.bindAuthorizationConfiguration(ac2, new Hashtable(Map.of(SERVICE_PID, "ac2")));

        AuthorizationConfiguration config = context.getService(SecurityProvider.class).getConfiguration(AuthorizationConfiguration.class);
        PermissionProvider permissionProvider = config.getPermissionProvider(root, adminSession.getWorkspaceName(), Set.of());

        verify(filter1, times(2)).stop(pp, Set.of());
        verify(filter2, times(2)).stop(pp, Set.of());


        JackrabbitAccessControlManager am = (JackrabbitAccessControlManager) config.getAccessControlManager(root, getNamePathMapper());

        assertEquals(2, am.getEffectivePolicies(PathUtils.ROOT_PATH).length);
        verify(filter1, times(2)).stop(acMgr, PathUtils.ROOT_PATH);
        verify(filter2, times(2)).stop(acMgr, PathUtils.ROOT_PATH);

        assertEquals(2, am.getEffectivePolicies(Set.of(EveryonePrincipal.getInstance())).length);
        verify(filter1, times(2)).stop(acMgr, Set.of(EveryonePrincipal.getInstance()));
        verify(filter2, times(2)).stop(acMgr, Set.of(EveryonePrincipal.getInstance()));
    }

    @Test
    public void testMultipleEvaluationFilterTrue() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("f1", "f2", "ac1", "ac2"));

        AggregationFilter filter1 = mock(AggregationFilter.class, withSettings().defaultAnswer(invocationOnMock -> Boolean.TRUE));
        ServiceRegistration sr1 = context.bundleContext().registerService(AggregationFilter.class.getName(), filter1, new Hashtable(Map.of(SERVICE_PID, "f1", SERVICE_RANKING, 200)));
        registration.bindAggregationFilter(sr1.getReference(), filter1);

        AggregationFilter filter2 = mock(AggregationFilter.class, withSettings().defaultAnswer(invocationOnMock -> Boolean.TRUE));
        ServiceRegistration sr2 = context.bundleContext().registerService(AggregationFilter.class.getName(), filter2, new Hashtable(Map.of(SERVICE_PID, "f2", SERVICE_RANKING, 100)));
        registration.bindAggregationFilter(sr2.getReference(), filter2);

        AggregatedPermissionProvider pp = mock(AggregatedPermissionProvider.class);
        JackrabbitAccessControlManager acMgr = mock(JackrabbitAccessControlManager.class);
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        when(acMgr.getEffectivePolicies(anyString())).thenReturn(new AccessControlPolicy[] {policy});
        when(acMgr.getEffectivePolicies(any(Set.class))).thenReturn(new AccessControlPolicy[] {policy});

        AuthorizationConfiguration ac1 = mock(AuthorizationConfiguration.class);
        AuthorizationConfiguration ac2 = mock(AuthorizationConfiguration.class);
        for (AuthorizationConfiguration ac : new AuthorizationConfiguration[]{ac1, ac2}) {
            when(ac.getPermissionProvider(any(Root.class), anyString(), any(Set.class))).thenReturn(pp);
            when(ac.getAccessControlManager(any(Root.class), any(NamePathMapper.class))).thenReturn(acMgr);
            when(ac.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
            when(ac.getContext()).thenReturn(Context.DEFAULT);
        }

        registration.bindAuthorizationConfiguration(ac1, new Hashtable(Map.of(SERVICE_PID, "ac1")));
        registration.bindAuthorizationConfiguration(ac2, new Hashtable(Map.of(SERVICE_PID, "ac2")));

        AuthorizationConfiguration config = context.getService(SecurityProvider.class).getConfiguration(AuthorizationConfiguration.class);
        PermissionProvider permissionProvider = config.getPermissionProvider(root, adminSession.getWorkspaceName(), Set.of());

        verify(filter1, never()).stop(pp, Set.of());
        verify(filter2, times(1)).stop(pp, Set.of());

        JackrabbitAccessControlManager am = (JackrabbitAccessControlManager) config.getAccessControlManager(root, getNamePathMapper());

        assertEquals(1, am.getEffectivePolicies(PathUtils.ROOT_PATH).length);
        verify(filter1, never()).stop(acMgr, PathUtils.ROOT_PATH);
        verify(filter2, times(1)).stop(acMgr, PathUtils.ROOT_PATH);

        assertEquals(1, am.getEffectivePolicies(Set.of(EveryonePrincipal.getInstance())).length);
        verify(filter1, never()).stop(acMgr, Set.of(EveryonePrincipal.getInstance()));
        verify(filter2, times(1)).stop(acMgr, Set.of(EveryonePrincipal.getInstance()));
    }

    @Test
    public void testRegisterWithMonitors() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("customAuthorizationConfig"));
        assertNull(context.getService(SecurityProvider.class));

        AuthorizationConfiguration mockConfiguration = mockConfiguration(AuthorizationConfiguration.class);
        when(mockConfiguration.getMonitors(any(StatisticsProvider.class))).thenReturn(ImmutableList.of(new TestMonitor()));

        registration.bindAuthorizationConfiguration(mockConfiguration, Collections.singletonMap(SERVICE_PID, "customAuthorizationConfig"));
        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNotNull(service);

        verify(mockConfiguration, times(1)).getMonitors(any(StatisticsProvider.class));
    }

    private static final class TestMonitor implements Monitor<TestMonitor> {

        @Override
        public @NotNull Class<TestMonitor> getMonitorClass() {
            return TestMonitor.class;
        }

        @Override
        public @NotNull Map<Object, Object> getMonitorProperties() {
            return Collections.emptyMap();
        }
    }
}

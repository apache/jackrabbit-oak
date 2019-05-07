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
package org.apache.jackrabbit.oak.security.internal;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
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
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMonitor;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleStatsCollector;
import org.apache.jackrabbit.oak.spi.security.authentication.token.CompositeTokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.CompositePrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.osgi.framework.Constants.SERVICE_PID;

public class SecurityProviderRegistrationTest extends AbstractSecurityTest {

    private static final Map<String, Object> PROPS = ImmutableMap.<String, Object>of(SERVICE_PID, "pid");

    @Rule
    public final OsgiContext context = new OsgiContext();

    private SecurityProviderRegistration registration = new SecurityProviderRegistration();

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

        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), ImmutableMap.of(SERVICE_PID, "serviceB"));
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
        assertEquals(6, Iterables.size(Iterables.filter(service.getConfigurations(), Predicates.notNull())));
    }

    @Test
    public void testModified() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("rpId", "authorizationId"));

        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), ImmutableMap.of(SERVICE_PID, "authorizationId"));

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
        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), ImmutableMap.of(SERVICE_PID, "authorizationId"));

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
    public void testBindOptionalCandidate() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceId"));

        Field f = registration.getClass().getDeclaredField("preconditions");
        f.setAccessible(true);

        TokenConfiguration tc = mockConfiguration(TokenConfiguration.class);
        registration.bindTokenConfiguration(tc, ImmutableMap.of(SERVICE_PID, "otherServiceId"));

        Preconditions preconditions = (Preconditions) f.get(registration);
        assertFalse(preconditions.areSatisfied());

        assertNull(context.getService(SecurityProvider.class));
    }

    @Test
    public void testBindOptionalCandidateAfterRegistration() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceId"));

        registration.bindTokenConfiguration(mockConfiguration(TokenConfiguration.class), ImmutableMap.of(SERVICE_PID, "serviceId"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNotNull(service);

        // binding another (optional configuration) must not result in re-registration of the service
        registration.bindPrincipalConfiguration(mockConfiguration(PrincipalConfiguration.class), ImmutableMap.of(SERVICE_PID, "optionalService"));

        SecurityProvider service2 = context.getService(SecurityProvider.class);
        assertSame(service, service2);
    }

    @Test
    public void testBindMandatoryCandidate() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceId"));

        Field f = registration.getClass().getDeclaredField("preconditions");
        f.setAccessible(true);

        TokenConfiguration tc = mockConfiguration(TokenConfiguration.class);
        registration.bindTokenConfiguration(tc, ImmutableMap.of(SERVICE_PID, "serviceId"));

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

        verify(((LoginModuleStatsCollector) ac), times(1)).setLoginModuleMonitor(any(LoginModuleMonitor.class));
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
        Map<String, Object> props = ImmutableMap.<String, Object>of(Constants.SERVICE_RANKING, new Integer(100));
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
        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), ImmutableMap.of(SERVICE_PID, "authorizationId"));

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
        ServiceRegistration sr = context.bundleContext().registerService(RestrictionProvider.class.getName(), mockRp, new Hashtable(ImmutableMap.of(SERVICE_PID, "rpId")));

        registration.bindRestrictionProvider(sr.getReference(), mockRp);
        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), ImmutableMap.of(RegistrationConstants.OAK_SECURITY_NAME, "authorizationId"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        RestrictionProvider rp = service.getConfiguration(AuthorizationConfiguration.class).getRestrictionProvider();
        assertTrue(rp instanceof WhiteboardRestrictionProvider);
    }

    @Test
    public void testWhileboardRestrictionProvider() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("rpId", "authorizationId"));

        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        ServiceRegistration rpSr = context.bundleContext().registerService(RestrictionProvider.class.getName(), mockRp, new Hashtable(ImmutableMap.of(SERVICE_PID, "rpId")));
        registration.bindRestrictionProvider(rpSr.getReference(), mockRp);
        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), ImmutableMap.of(OAK_SECURITY_NAME, "authorizationId"));

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
        ServiceRegistration rpSr = context.bundleContext().registerService(AuthorizableActionProvider.class.getName(), mockAp, new Hashtable(ImmutableMap.of(SERVICE_PID, "apId")));
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
        ServiceRegistration rpSr = context.bundleContext().registerService(AuthorizableNodeName.class.getName(), mockAn, new Hashtable(ImmutableMap.of(SERVICE_PID, "anId")));
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
        ServiceRegistration rpSr = context.bundleContext().registerService(UserAuthenticationFactory.class.getName(), mockUaf, new Hashtable(ImmutableMap.of(SERVICE_PID, "uafId")));
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
        context.registerService(SecurityProviderRegistration.class, registration, ImmutableMap.of("requiredServicePids", new String[] {"s1", "s2", "s3"}));

        context.registerInjectActivateService(service1, ImmutableMap.of(RegistrationConstants.OAK_SECURITY_NAME, "s1", Constants.SERVICE_RANKING, 50));
        context.registerInjectActivateService(service2, ImmutableMap.of(RegistrationConstants.OAK_SECURITY_NAME, "s2"));
        context.registerInjectActivateService(service3, ImmutableMap.of(RegistrationConstants.OAK_SECURITY_NAME, "s3", Constants.SERVICE_RANKING, 1));

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
}

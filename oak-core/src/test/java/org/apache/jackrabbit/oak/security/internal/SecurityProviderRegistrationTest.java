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

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.security.authorization.restriction.WhiteboardRestrictionProvider;
import org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.UserAuthenticationFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.RegistrationConstants;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
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
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
import org.osgi.service.component.annotations.Component;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.osgi.framework.Constants.SERVICE_PID;

public class SecurityProviderRegistrationTest extends AbstractSecurityTest {

    private static final Map<String, Object> PROPS = ImmutableMap.<String, Object>of("prop", "val");

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
        when(sc.getContext()).thenReturn(new ContextImpl());
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

        registration.bindAuthorizableNodeName(mock(AuthorizableNodeName.class), ImmutableMap.of(SERVICE_PID, "serviceId"));

        service = context.getService(SecurityProvider.class);
        assertNotNull(service);
    }

    @Test
    public void testActivate() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceA", "serviceB"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNull(service);

        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        registration.bindRestrictionProvider(mockRp, ImmutableMap.of(SERVICE_PID, "serviceA"));

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
        registration.bindRestrictionProvider(mockRp, ImmutableMap.of(SERVICE_PID, "rpId"));
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
        registration.bindAuthorizableNodeName(mock, ImmutableMap.of(SERVICE_PID, "nodeName"));

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
        registration.bindAuthorizableNodeName(mock, ImmutableMap.of(SERVICE_PID, "nodeName"));

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
        registration.bindAuthorizableActionProvider(ap, ImmutableMap.of(SERVICE_PID, "actionProvider"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNotNull(service);

        registration.unbindAuthorizableActionProvider(ap, ImmutableMap.of(SERVICE_PID, "actionProvider"));
        service = context.getService(SecurityProvider.class);
        assertNull(service);
    }

    @Test
    public void testUnbindMandatoryCandidateOnPreconditions() throws Exception {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("nodeName"));

        Field f = registration.getClass().getDeclaredField("preconditions");
        f.setAccessible(true);

        AuthorizableNodeName mock = mock(AuthorizableNodeName.class);
        registration.bindAuthorizableNodeName(mock, ImmutableMap.of(SERVICE_PID, "nodeName"));
        registration.unbindAuthorizableNodeName(mock, ImmutableMap.of(SERVICE_PID, "nodeName"));

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

        registration.bindAuthorizableActionProvider(mock(AuthorizableActionProvider.class), ImmutableMap.of(SERVICE_PID, "serviceId"));

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
        registration.bindRestrictionProvider(mockRp, ImmutableMap.of(SERVICE_PID, "serviceId"));

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

        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        registration.bindRestrictionProvider(mockRp, ImmutableMap.of(SERVICE_PID, "rpId"));
        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), ImmutableMap.of(SERVICE_PID, "authorizationId"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        RestrictionProvider rp = service.getConfiguration(AuthorizationConfiguration.class).getRestrictionProvider();
        assertTrue(rp instanceof WhiteboardRestrictionProvider);
    }
    
    @Test
    public void testActivateWithRequiredOakSecurityName() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("serviceId"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        assertNull(service);

        registration.bindAuthorizableNodeName(mock(AuthorizableNodeName.class), ImmutableMap.of(RegistrationConstants.OAK_SECURITY_NAME, "serviceId"));

        service = context.getService(SecurityProvider.class);
        assertNotNull(service);
    }
    
    @Test
    public void testActivateWithMixedServicePiAnddOakServiceName() {
        registration.activate(context.bundleContext(), configWithRequiredServiceIds("rpId", "authorizationId"));
        
        RestrictionProvider mockRp = mock(RestrictionProvider.class);
        registration.bindRestrictionProvider(mockRp, ImmutableMap.of(SERVICE_PID, "rpId"));
        registration.bindAuthorizationConfiguration(new AuthorizationConfigurationImpl(), ImmutableMap.of(RegistrationConstants.OAK_SECURITY_NAME, "authorizationId"));

        SecurityProvider service = context.getService(SecurityProvider.class);
        RestrictionProvider rp = service.getConfiguration(AuthorizationConfiguration.class).getRestrictionProvider();
        assertTrue(rp instanceof WhiteboardRestrictionProvider);
    }

    @Test
    public void testMultipleUserAuthenticationFactoriesRespectsRanking() throws Exception {
        context.registerService(SecurityProviderRegistration.class, registration, ImmutableMap.of("requiredServicePids", new String[] {"uaf1", "uaf2", "uaf3"}));

        UserAuthenticationFactory uaf1 = new UserAuthenticationFactoryImpl();
        UserAuthenticationFactory uaf2 = new UserAuthenticationFactoryImpl();
        UserAuthenticationFactory uaf3 = new UserAuthenticationFactoryImpl();

        context.registerInjectActivateService(uaf1, ImmutableMap.of(RegistrationConstants.OAK_SECURITY_NAME, "uaf1", Constants.SERVICE_RANKING, 50));
        context.registerInjectActivateService(uaf2, ImmutableMap.of(RegistrationConstants.OAK_SECURITY_NAME, "uaf2"));
        context.registerInjectActivateService(uaf3, ImmutableMap.of(RegistrationConstants.OAK_SECURITY_NAME, "uaf3", Constants.SERVICE_RANKING, 1));

        Field f = registration.getClass().getDeclaredField("userAuthenticationFactories");
        f.setAccessible(true);

        SortedMap<ServiceReference, UserAuthenticationFactory> m = (SortedMap<ServiceReference, UserAuthenticationFactory>) f.get(registration);
        assertEquals(3, m.size());
        Collection<UserAuthenticationFactory> c = m.values();
        assertTrue(Iterables.elementsEqual(ImmutableList.of(uaf2, uaf3, uaf1), c));
    }

    private static class ContextImpl implements Context {

        @Override
        public boolean definesProperty(@NotNull Tree parent, @NotNull PropertyState property) {
            return true;
        }

        @Override
        public boolean definesContextRoot(@NotNull Tree tree) {
            return true;
        }

        @Override
        public boolean definesTree(@NotNull Tree tree) {
            return true;
        }

        @Override
        public boolean definesLocation(@NotNull TreeLocation location) {
            return true;
        }

        @Override
        public boolean definesInternal(@NotNull Tree tree) {
            return true;
        }
    }
}

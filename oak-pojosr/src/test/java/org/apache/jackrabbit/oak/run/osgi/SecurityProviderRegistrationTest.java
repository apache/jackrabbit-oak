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
package org.apache.jackrabbit.oak.run.osgi;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.action.AccessControlAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationAdmin;

public class SecurityProviderRegistrationTest extends AbstractRepositoryFactoryTest {

    private PojoServiceRegistry registry;

    @Before
    public void initializeRegistry() {
        registry = repositoryFactory.initializeServiceRegistry(config);
    }

    @Override
    protected PojoServiceRegistry getRegistry() {
        return registry;
    }
    /**
     * Test that, without any additional configuration, a SecurityProvider service is registered by default.
     */
    @Test
    public void testDefaultSetup() {
        assertNotNull(getSecurityProviderServiceReferences());
    }

    /**
     * A SecurityProvider shouldn't start without a required AuthorizationConfiguration service.
     */
    @Test
    public void testRequiredAuthorizationConfigurationNotAvailable() {
        AuthorizationConfiguration m = Mockito.mock(AuthorizationConfiguration.class);
        Mockito.when(m.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
        Mockito.when(m.getContext()).thenReturn(Context.DEFAULT);

        testRequiredService(AuthorizationConfiguration.class, m);
    }

    /**
     * A SecurityProvider shouldn't start without a required PrincipalConfiguration service.
     */
    @Test
    public void testRequiredPrincipalConfigurationNotAvailable() {
        PrincipalConfiguration m = Mockito.mock(PrincipalConfiguration.class);
        Mockito.when(m.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
        Mockito.when(m.getContext()).thenReturn(Context.DEFAULT);

        testRequiredService(PrincipalConfiguration.class, m);
    }

    /**
     * A SecurityProvider shouldn't start without a required TokenConfiguration service.
     */
    @Test
    public void testRequiredTokenConfigurationNotAvailable() {
        TokenConfiguration m = Mockito.mock(TokenConfiguration.class);
        Mockito.when(m.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
        Mockito.when(m.getContext()).thenReturn(Context.DEFAULT);

        testRequiredService(TokenConfiguration.class, m);
    }

    /**
     * A SecurityProvider shouldn't start without a required AuthorizableNodeName service.
     */
    @Test
    public void testRequiredAuthorizableNodeNameNotAvailable() {
        testRequiredService(AuthorizableNodeName.class, Mockito.mock(AuthorizableNodeName.class));
    }

    /**
     * A SecurityProvider shouldn't start without a required AuthorizableActionProvider service.
     */
    @Test
    public void testRequiredAuthorizableActionProviderNotAvailable() {
        testRequiredService(AuthorizableActionProvider.class, Mockito.mock(AuthorizableActionProvider.class));
    }

    /**
     * A SecurityProvider shouldn't start without a required RestrictionProvider service.
     */
    @Test
    public void testRequiredRestrictionProviderNotAvailable() {
        testRequiredService(RestrictionProvider.class, Mockito.mock(RestrictionProvider.class));
    }

    /**
     * A SecurityProvider shouldn't start without a required UserAuthenticationFactory service.
     */
    @Test
    public void testRequiredUserAuthenticationFactoryNotAvailable() {
        testRequiredService(UserAuthenticationFactory.class, Mockito.mock(UserAuthenticationFactory.class));
    }

    /**
     * A SecurityProvider should be registered only if every every prerequisite is satisfied.
     */
    @Test
    public void testMultipleRequiredServices() {

        // Set up the SecurityProvider to require 4 services
        awaitServiceEvent(() ->
                        setRequiredServicePids("test.RequiredAuthorizationConfiguration",
                                "test.RequiredPrincipalConfiguration",
                                "test.RequiredTokenConfiguration",
                                "test.RestrictionProvider"),
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)", ServiceEvent.UNREGISTERING);

        assertNull(getSecurityProviderServiceReferences());

        // Start the services and verify that only at the end the
        // SecurityProvider registers itself
        AuthorizationConfiguration ac = Mockito.mock(AuthorizationConfiguration.class);
        Mockito.when(ac.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
        Mockito.when(ac.getContext()).thenReturn(Context.DEFAULT);

        LinkedHashMap<String, String> map = new LinkedHashMap<>(1);
        map.put("service.pid", "test.RequiredAuthorizationConfiguration");
        registry.registerService(AuthorizationConfiguration.class.getName(), ac, dict(map));
        assertNull(getSecurityProviderServiceReferences());

        PrincipalConfiguration pc = Mockito.mock(PrincipalConfiguration.class);
        Mockito.when(pc.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
        Mockito.when(pc.getContext()).thenReturn(Context.DEFAULT);

        LinkedHashMap<String, String> map1 = new LinkedHashMap<>(1);
        map1.put("service.pid", "test.RequiredPrincipalConfiguration");
        registry.registerService(PrincipalConfiguration.class.getName(), pc, dict(map1));
        assertNull(getSecurityProviderServiceReferences());

        TokenConfiguration tc = Mockito.mock(TokenConfiguration.class);
        Mockito.when(tc.getParameters()).thenReturn(ConfigurationParameters.EMPTY);
        Mockito.when(tc.getContext()).thenReturn(Context.DEFAULT);

        LinkedHashMap<String, String> map2 = new LinkedHashMap<>(1);
        map2.put("service.pid", "test.RequiredTokenConfiguration");
        registry.registerService(TokenConfiguration.class.getName(), tc, dict(map2));
        assertNull(getSecurityProviderServiceReferences());

        LinkedHashMap<String, String> map3 = new LinkedHashMap<>(1);
        map3.put("service.pid", "test.RestrictionProvider");
        registry.registerService(RestrictionProvider.class.getName(),
                Mockito.mock(RestrictionProvider.class),
                dict(map3));
        assertNotNull(getSecurityProviderServiceReferences());
    }

    @Test
    public void testSecurityConfigurations() {
        SecurityProvider securityProvider = (SecurityProvider) registry.getService((ServiceReference<?>) registry.getServiceReference(
                SecurityProvider.class.getName()));
        assertAuthorizationConfig(securityProvider);
        assertUserConfig(securityProvider);

        //Keep a dummy reference to UserConfiguration such that SCR does not deactivate it
        //once SecurityProviderRegistration gets deactivated. Otherwise if SecurityProviderRegistration is
        //the only component that refers it then upon its deactivation UserConfiguration would also be
        //deactivate and its internal state would be reset
        UserConfiguration userConfiguration = getServiceWithWait(UserConfiguration.class);

        final String authComponentName = "org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl";

        // 1. Disable AuthenticationConfiguration such that SecurityProvider is unregistered
        awaitServiceEvent(() -> disableComponent(authComponentName),
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.UNREGISTERING,
                5000,
                TimeUnit.MILLISECONDS);

        assertNull(getSecurityProviderServiceReferences());

        // 2. Modify the config for AuthorizableActionProvider. It's expected that this config change is picked up
        awaitServiceEvent(() -> {
                    Map<String, Map<String, Object>> map = new LinkedHashMap<>(1);
                    map.put("org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider",
                            Map.of("groupPrivilegeNames", "jcr:read"));
                    setConfiguration(map);
                },
                AbstractRepositoryFactoryTest.classNameFilter(
                        "org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider"),
                ServiceEvent.MODIFIED | ServiceEvent.REGISTERED);

        // 3. Enable component again such that SecurityProvider gets reactivated
        awaitServiceEvent(() -> enableComponent(authComponentName),
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.REGISTERED);

        securityProvider = getServiceWithWait(SecurityProvider.class);
        assertAuthorizationConfig(securityProvider);
        assertUserConfig(securityProvider, "jcr:read");
    }

    @Test
    public void testSecurityConfigurations2() {
        SecurityProvider securityProvider = (SecurityProvider) registry.getService(registry.getServiceReference(SecurityProvider.class.getName()));
        assertAuthorizationConfig(securityProvider);
        assertUserConfig(securityProvider);

        //Keep a dummy reference to UserConfiguration such that SCR does not deactivate it
        //once SecurityProviderRegistration gets deactivated. Otherwise if SecurityProviderRegistration is
        //the only component that refers it then upon its deactivation UserConfiguration would also be
        //deactivate and its internal state would be reset
        UserConfiguration userConfiguration = getServiceWithWait(UserConfiguration.class);

        //1. Modify the config for AuthorizableActionProvider. It's expected that this config change is picked up
        final String servicePid = "org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider";
        awaitServiceEvent(() -> setConfiguration(Map.of(servicePid, Map.of("groupPrivilegeNames", "jcr:read"))),
                "(service.pid=" + servicePid + ")", ServiceEvent.MODIFIED | ServiceEvent.REGISTERED);

        securityProvider = getServiceWithWait(SecurityProvider.class);
        assertAuthorizationConfig(securityProvider);
        assertUserConfig(securityProvider, "jcr:read");

        final String authComponentName = "org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl";

        // 2. Disable AuthenticationConfiguration such that SecurityProvider is unregistered
        awaitServiceEvent(() -> disableComponent(authComponentName),
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.UNREGISTERING);

        assertNull(getSecurityProviderServiceReferences());

        // 3. Enable component again such that SecurityProvider gets reactivated
        awaitServiceEvent(() -> enableComponent(authComponentName),
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.REGISTERED);

        securityProvider = getServiceWithWait(SecurityProvider.class);
        assertAuthorizationConfig(securityProvider);
        assertUserConfig(securityProvider, "jcr:read");
    }

    private <T> void testRequiredService(final Class<T> serviceClass, final T service) {

        // Adding a new precondition on a missing service PID forces the
        // SecurityProvider to unregister.

        awaitServiceEvent(() -> setRequiredServicePids("test.Required" + serviceClass.getSimpleName()),
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.UNREGISTERING);

        assertNull(getSecurityProviderServiceReferences());

        // If a service is registered, and if the PID of the service matches the
        // precondition, the SecurityProvider is registered again.

        AtomicReference<ServiceRegistration<?>> registration = new AtomicReference<>();
        awaitServiceEvent(() -> {
            Map<String, Object> map = new LinkedHashMap<>(1);
            map.put("service.pid", "test.Required" + serviceClass.getSimpleName());
            registration.set(registry.registerService(serviceClass.getName(), service, dict(map)));
        }, "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)", ServiceEvent.REGISTERED);
        assertNotNull(getSecurityProviderServiceReferences());

        // If the service is unregistered, but the precondition is still in
        // place, the SecurityProvider unregisters again.

        awaitServiceEvent(() -> registration.get().unregister(),
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.UNREGISTERING);
        assertNull(getSecurityProviderServiceReferences());

        // Removing the precondition allows the SecurityProvider to register.
        awaitServiceEvent(this::setRequiredServicePids,
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.REGISTERED);
        assertNotNull(getSecurityProviderServiceReferences());
    }

    private ServiceReference<?>[] getSecurityProviderServiceReferences() {
        try {
            return registry.getServiceReferences(SecurityProvider.class.getName(), "(type=default)");
        } catch (InvalidSyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void setRequiredServicePids(String... pids) {
        setConfiguration(
                Map.of("org.apache.jackrabbit.oak.security.internal.SecurityProviderRegistration",
                        Map.of("requiredServicePids", pids)));
    }

    private void setConfiguration(Map<String, Map<String, Object>> configuration) {
        try {
            getConfigurationInstaller().installConfigs(configuration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ConfigInstaller getConfigurationInstaller() {
        return new ConfigInstaller(getConfigurationAdmin(), registry.getBundleContext());
    }

    private ConfigurationAdmin getConfigurationAdmin() {
        return (ConfigurationAdmin) registry.getService(registry.getServiceReference(ConfigurationAdmin.class.getName()));
    }

    private static <K, V> Dictionary<K, V> dict(Map<K, V> map) {
        return new Hashtable<>(map);
    }

    private static void assertAuthorizationConfig(SecurityProvider securityProvider, String... adminPrincipals) {
        AuthorizationConfiguration ac = securityProvider.getConfiguration(AuthorizationConfiguration.class);

        assertTrue(ac.getParameters().containsKey("restrictionProvider"));
        assertNotNull(ac.getRestrictionProvider());
        assertEquals(ac.getParameters().get("restrictionProvider"), ac.getRestrictionProvider());

        String[] found = ac.getParameters().getConfigValue("administrativePrincipals", new String[0]);
        assertArrayEquals(adminPrincipals, found);
    }

    private static void assertUserConfig(SecurityProvider securityProvider, String... groupPrivilegeNames) {
        UserConfiguration uc = securityProvider.getConfiguration(UserConfiguration.class);

        assertTrue(uc.getParameters().containsKey("authorizableActionProvider"));
        AuthorizableActionProvider ap = (AuthorizableActionProvider) uc.getParameters()
                .get("authorizableActionProvider");
        assertNotNull(ap);

        List<AuthorizableAction> actionList = (List<AuthorizableAction>) ap.getAuthorizableActions(securityProvider);
        assertEquals(1, actionList.size());
        AuthorizableAction action = actionList.get(0);
        assertTrue(action instanceof AccessControlAction);

        // Changed implementation to use reflection to access private field
        // before it was using Groovy advantages to access groupPrivilegeNames internal field
        try {
            Field privateField = AccessControlAction.class.getDeclaredField("groupPrivilegeNames");
            privateField.setAccessible(true);
            String[] privilegeNames = (String[]) privateField.get(action);
            assertNotNull(privilegeNames);
            assertArrayEquals(groupPrivilegeNames, privilegeNames);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }


}

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
package org.apache.jackrabbit.oak.run.osgi

import org.apache.felix.connect.launch.PojoServiceRegistry
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters
import org.apache.jackrabbit.oak.spi.security.Context
import org.apache.jackrabbit.oak.spi.security.SecurityProvider
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration
import org.apache.jackrabbit.oak.spi.security.user.action.AccessControlAction
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider
import org.junit.Before
import org.junit.Test
import org.osgi.framework.ServiceEvent
import org.osgi.framework.ServiceReference
import org.osgi.service.cm.ConfigurationAdmin


import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when
import static org.osgi.service.component.runtime.ServiceComponentRuntime.*

class SecurityProviderRegistrationTest extends AbstractRepositoryFactoryTest {

    private PojoServiceRegistry registry;

    @Before
    public void initializeRegistry() {
        registry = repositoryFactory.initializeServiceRegistry(config)
    }

    @Override
    protected PojoServiceRegistry getRegistry() {
        return registry
    }
    /**
     * Test that, without any additional configuration, a SecurityProvider
     * service is registered by default.
     */
    @Test
    public void testDefaultSetup() {
        assert securityProviderServiceReferences != null
    }

    /**
     * A SecurityProvider shouldn't start without a required
     * AuthorizationConfiguration service.
     */
    @Test
    public void testRequiredAuthorizationConfigurationNotAvailable() {
        def m = mock(AuthorizationConfiguration)
        when(m.getParameters()).thenReturn(ConfigurationParameters.EMPTY)
        when(m.getContext()).thenReturn(Context.DEFAULT)

        testRequiredService(AuthorizationConfiguration, m)
    }

    /**
     * A SecurityProvider shouldn't start without a required
     * PrincipalConfiguration service.
     */
    @Test
    public void testRequiredPrincipalConfigurationNotAvailable() {
        def m = mock(PrincipalConfiguration)
        when(m.getParameters()).thenReturn(ConfigurationParameters.EMPTY)
        when(m.getContext()).thenReturn(Context.DEFAULT)

        testRequiredService(PrincipalConfiguration, m)
    }

    /**
     * A SecurityProvider shouldn't start without a required TokenConfiguration
     * service.
     */
    @Test
    public void testRequiredTokenConfigurationNotAvailable() {
        def m = mock(TokenConfiguration)
        when(m.getParameters()).thenReturn(ConfigurationParameters.EMPTY)
        when(m.getContext()).thenReturn(Context.DEFAULT)

        testRequiredService(TokenConfiguration, m)
    }

    /**
     * A SecurityProvider shouldn't start without a required
     * AuthorizableNodeName service.
     */
    @Test
    public void testRequiredAuthorizableNodeNameNotAvailable() {
        testRequiredService(AuthorizableNodeName, mock(AuthorizableNodeName))
    }

    /**
     * A SecurityProvider shouldn't start without a required
     * AuthorizableActionProvider service.
     */
    @Test
    public void testRequiredAuthorizableActionProviderNotAvailable() {
        testRequiredService(AuthorizableActionProvider, mock(AuthorizableActionProvider))
    }

    /**
     * A SecurityProvider shouldn't start without a required RestrictionProvider
     * service.
     */
    @Test
    public void testRequiredRestrictionProviderNotAvailable() {
        testRequiredService(RestrictionProvider, mock(RestrictionProvider))
    }

    /**
     * A SecurityProvider shouldn't start without a required
     * UserAuthenticationFactory service.
     */
    @Test
    public void testRequiredUserAuthenticationFactoryNotAvailable() {
        testRequiredService(UserAuthenticationFactory, mock(UserAuthenticationFactory))
    }

    /**
     * A SecurityProvider should be registered only if every every prerequisite
     * is satisfied.
     */
    @Test
    public void testMultipleRequiredServices() {

        // Set up the SecurityProvider to require 4 services
        awaitServiceEvent({
                    setRequiredServicePids(
                            "test.RequiredAuthorizationConfiguration",
                            "test.RequiredPrincipalConfiguration",
                            "test.RequiredTokenConfiguration",
                            "test.RestrictionProvider")
                },
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.UNREGISTERING
        )
        assert securityProviderServiceReferences == null

        // Start the services and verify that only at the end the
        // SecurityProvider registers itself
        def ac = mock(AuthorizationConfiguration)
        when(ac.getParameters()).thenReturn(ConfigurationParameters.EMPTY)
        when(ac.getContext()).thenReturn(Context.DEFAULT)

        registry.registerService(AuthorizationConfiguration.class.name, ac, dict("service.pid": "test.RequiredAuthorizationConfiguration"))
        assert securityProviderServiceReferences == null

        def pc = mock(PrincipalConfiguration)
        when(pc.getParameters()).thenReturn(ConfigurationParameters.EMPTY)
        when(pc.getContext()).thenReturn(Context.DEFAULT)

        registry.registerService(PrincipalConfiguration.class.name, pc, dict("service.pid": "test.RequiredPrincipalConfiguration"))
        assert securityProviderServiceReferences == null

        def tc = mock(TokenConfiguration)
        when(tc.getParameters()).thenReturn(ConfigurationParameters.EMPTY)
        when(tc.getContext()).thenReturn(Context.DEFAULT)

        registry.registerService(TokenConfiguration.class.name, tc, dict("service.pid": "test.RequiredTokenConfiguration"))
        assert securityProviderServiceReferences == null

        registry.registerService(RestrictionProvider.class.name, mock(RestrictionProvider), dict("service.pid": "test.RestrictionProvider"))
        assert securityProviderServiceReferences != null
    }

    @Test
    public void testSecurityConfigurations() {
        SecurityProvider securityProvider = registry.getService(registry.getServiceReference(SecurityProvider.class.name))
        assertAuthorizationConfig(securityProvider)
        assertUserConfig(securityProvider)

        //Keep a dummy reference to UserConfiguration such that SCR does not deactivate it
        //once SecurityProviderRegistration gets deactivated. Otherwise if SecurityProviderRegistration is
        //the only component that refers it then upon its deactivation UserConfiguration would also be
        //deactivate and its internal state would be reset
        UserConfiguration userConfiguration = getServiceWithWait(UserConfiguration.class)

        def authComponentName = 'org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl'

        // 1. Disable AuthenticationConfiguration such that SecurityProvider is unregistered
        awaitServiceEvent({
                    disableComponent(authComponentName)
                },
                '(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)',
                ServiceEvent.UNREGISTERING,
                5000
        )

        assert securityProviderServiceReferences == null

        // 2. Modify the config for AuthorizableActionProvider. It's expected that this config change is picked up
        awaitServiceEvent({
                    setConfiguration([
                            "org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider": [
                                    "groupPrivilegeNames":"jcr:read"
                            ]
                    ])
                },
                classNameFilter('org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider'),
                ServiceEvent.MODIFIED | ServiceEvent.REGISTERED
        )

        // 3. Enable component again such that SecurityProvider gets reactivated
        awaitServiceEvent({
                   enableComponent(authComponentName)
                },
                '(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)',
                ServiceEvent.REGISTERED
        )

        securityProvider = getServiceWithWait(SecurityProvider.class)
        assertAuthorizationConfig(securityProvider)
        assertUserConfig(securityProvider, "jcr:read")
    }

    @Test
    public void testSecurityConfigurations2() {
        SecurityProvider securityProvider = registry.getService(registry.getServiceReference(SecurityProvider.class.name))
        assertAuthorizationConfig(securityProvider)
        assertUserConfig(securityProvider)

        //Keep a dummy reference to UserConfiguration such that SCR does not deactivate it
        //once SecurityProviderRegistration gets deactivated. Otherwise if SecurityProviderRegistration is
        //the only component that refers it then upon its deactivation UserConfiguration would also be
        //deactivate and its internal state would be reset
        UserConfiguration userConfiguration = getServiceWithWait(UserConfiguration.class)

        //1. Modify the config for AuthorizableActionProvider. It's expected that this config change is picked up
        def servicePid = "org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider"
        awaitServiceEvent({
            setConfiguration([
                        (servicePid): [
                                "groupPrivilegeNames":"jcr:read"
                        ]
                ]
            )},
            "(service.pid=${servicePid})",
            ServiceEvent.MODIFIED | ServiceEvent.REGISTERED
        )

        securityProvider = getServiceWithWait(SecurityProvider.class)
        assertAuthorizationConfig(securityProvider)
        assertUserConfig(securityProvider, "jcr:read")

        String authComponentName = 'org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl'

        // 2. Disable AuthenticationConfiguration such that SecurityProvider is unregistered
        awaitServiceEvent({
                    disableComponent(authComponentName)
                },
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.UNREGISTERING
        )

        assert securityProviderServiceReferences == null

        // 3. Enable component again such that SecurityProvider gets reactivated
        awaitServiceEvent({
                    enableComponent(authComponentName)
                },
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.REGISTERED
        )

        securityProvider = getServiceWithWait(SecurityProvider.class)
        assertAuthorizationConfig(securityProvider)
        assertUserConfig(securityProvider, "jcr:read")
    }

    private <T> void testRequiredService(Class<T> serviceClass, T service) {

        // Adding a new precondition on a missing service PID forces the
        // SecurityProvider to unregister.

        awaitServiceEvent({
                    setRequiredServicePids("test.Required" + serviceClass.simpleName)
                },
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.UNREGISTERING
        )
        assert securityProviderServiceReferences == null

        // If a service is registered, and if the PID of the service matches the
        // precondition, the SecurityProvider is registered again.

        def registration = registry.registerService(serviceClass.name, service, dict("service.pid": "test.Required" + serviceClass.simpleName))
        assert securityProviderServiceReferences != null

        // If the service is unregistered, but the precondition is still in
        // place, the SecurityProvider unregisters again.

        registration.unregister()
        assert securityProviderServiceReferences == null

        // Removing the precondition allows the SecurityProvider to register.
        awaitServiceEvent({
                    setRequiredServicePids()
                },
                "(objectClass=org.apache.jackrabbit.oak.spi.security.SecurityProvider)",
                ServiceEvent.REGISTERED
        )
        assert securityProviderServiceReferences != null
    }

    private ServiceReference<?>[] getSecurityProviderServiceReferences() {
        return registry.getServiceReferences(SecurityProvider.class.name, "(type=default)")
    }

    private void setRequiredServicePids(String... pids) {
        setConfiguration([
                "org.apache.jackrabbit.oak.security.internal.SecurityProviderRegistration": [
                        "requiredServicePids": pids
                ]
        ])
    }

    private void setConfiguration(Map<String, Map<String, Object>> configuration) {
        getConfigurationInstaller().installConfigs(configuration)
    }

    private ConfigInstaller getConfigurationInstaller() {
        return new ConfigInstaller(getConfigurationAdmin(), registry.bundleContext)
    }

    private ConfigurationAdmin getConfigurationAdmin() {
        return registry.getService(registry.getServiceReference(ConfigurationAdmin.class.name)) as ConfigurationAdmin
    }

    private static <K, V> Dictionary<K, V> dict(Map<K, V> map) {
        return new Hashtable<K, V>(map);
    }

    private static void assertAuthorizationConfig(SecurityProvider securityProvider, String... adminPrincipals) {
        AuthorizationConfiguration ac = securityProvider.getConfiguration(AuthorizationConfiguration.class)

        assert ac.getParameters().containsKey("restrictionProvider")
        assert ac.getRestrictionProvider() != null
        assert ac.getParameters().get("restrictionProvider") == ac.getRestrictionProvider()

        String[] found = ac.getParameters().getConfigValue("administrativePrincipals", new String[0])
        assert Arrays.equals(adminPrincipals, found)
    }

    private static void assertUserConfig(SecurityProvider securityProvider, String... groupPrivilegeNames) {
        UserConfiguration uc = securityProvider.getConfiguration(UserConfiguration.class)

        assert uc.getParameters().containsKey("authorizableActionProvider")
        AuthorizableActionProvider ap = uc.getParameters().get("authorizableActionProvider")
        assert ap != null;

        List<AuthorizableAction> actionList = ap.getAuthorizableActions(securityProvider)
        assert actionList.size() == 1
        AuthorizableAction action = actionList.get(0)
        assert Arrays.equals(groupPrivilegeNames, ((AccessControlAction) action).groupPrivilegeNames)
    }

}

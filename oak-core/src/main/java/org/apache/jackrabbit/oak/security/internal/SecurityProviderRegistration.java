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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyUnbounded;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.References;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.CompositeTokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.CompositePrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardRestrictionProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newCopyOnWriteArrayList;
import static com.google.common.collect.Maps.newHashMap;

@Component(
        immediate = true,
        metatype = true,
        label = "Apache Jackrabbit Oak SecurityProvider",
        description = "The default SecurityProvider embedded in Apache Jackrabbit Oak"
)
@Properties({
        @Property(
                name = "requiredServicePids",
                label = "Required service PIDs",
                description = "The SecurityProvider will not register itself " +
                        "unless the services identified by these PIDs are " +
                        "registered first. Only the PIDs of implementations of " +
                        "the following interfaces are checked: " +
                        "PrincipalConfiguration, TokenConfiguration, " +
                        "AuthorizableNodeName, AuthorizableActionProvider, and " +
                        "RestrictionProvider.",
                value = {
                        "org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl",
                        "org.apache.jackrabbit.oak.security.authentication.token.TokenConfigurationImpl",
                        "org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider",
                        "org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl"
                },
                unbounded = PropertyUnbounded.ARRAY
        )
})
@References({
        @Reference(
                name = "principalConfiguration",
                referenceInterface = PrincipalConfiguration.class,
                cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
                policy = ReferencePolicy.DYNAMIC
        ),
        @Reference(
                name = "tokenConfiguration",
                referenceInterface = TokenConfiguration.class,
                cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
                policy = ReferencePolicy.DYNAMIC
        ),
        @Reference(
                name = "authorizableNodeName",
                referenceInterface = AuthorizableNodeName.class,
                cardinality = ReferenceCardinality.OPTIONAL_UNARY,
                policy = ReferencePolicy.DYNAMIC
        ),
        @Reference(
                name = "authorizableActionProvider",
                referenceInterface = AuthorizableActionProvider.class,
                cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
                policy = ReferencePolicy.DYNAMIC
        ),
        @Reference(
                name = "restrictionProvider",
                referenceInterface = RestrictionProvider.class,
                cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
                policy = ReferencePolicy.DYNAMIC
        )
})
@SuppressWarnings("unused")
public class SecurityProviderRegistration {

    private static final Logger log = LoggerFactory.getLogger(SecurityProviderRegistration.class);

    @Reference
    private AuthorizationConfiguration authorizationConfiguration;

    @Reference
    private AuthenticationConfiguration authenticationConfiguration;

    @Reference
    private PrivilegeConfiguration privilegeConfiguration;

    @Reference
    private UserConfiguration userConfiguration;

    private BundleContext context;

    private ServiceRegistration registration;

    private boolean registering;

    private final Preconditions preconditions = new Preconditions();

    private final List<PrincipalConfiguration> principalConfigurations = newCopyOnWriteArrayList();

    private final List<TokenConfiguration> tokenConfigurations = newCopyOnWriteArrayList();

    private final List<AuthorizableActionProvider> authorizableActionProviders = newCopyOnWriteArrayList();

    private final List<RestrictionProvider> restrictionProviders = newCopyOnWriteArrayList();

    private volatile AuthorizableNodeName authorizableNodeName = AuthorizableNodeName.DEFAULT;

    @Activate
    public void activate(BundleContext context, Map<String, Object> configuration) {
        String[] requiredServicePids = getRequiredServicePids(configuration);

        synchronized (this) {
            for (String pid : requiredServicePids) {
                preconditions.addPrecondition(pid);
            }

            this.context = context;
        }

        maybeRegister();
    }

    @Modified
    public void modified(Map<String, Object> configuration) {
        String[] requiredServicePids = getRequiredServicePids(configuration);

        synchronized (this) {
            preconditions.clearPreconditions();

            for (String pid : requiredServicePids) {
                preconditions.addPrecondition(pid);
            }
        }

        maybeUnregister();
        maybeRegister();
    }

    @Deactivate
    public void deactivate() {
        ServiceRegistration registration;

        synchronized (this) {
            registration = this.registration;

            this.registration = null;
            this.registering = false;
            this.context = null;

            this.preconditions.clearPreconditions();
        }

        if (registration != null) {
            registration.unregister();
        }
    }

    public void bindAuthorizationConfiguration(AuthorizationConfiguration authorizationConfiguration) {
        this.authorizationConfiguration = authorizationConfiguration;
    }

    public void unbindAuthorizationConfiguration(AuthorizationConfiguration authorizationConfiguration) {
        this.authorizationConfiguration = null;
    }

    public void bindAuthenticationConfiguration(AuthenticationConfiguration authenticationConfiguration) {
        this.authenticationConfiguration = authenticationConfiguration;
    }

    public void unbindAuthenticationConfiguration(AuthenticationConfiguration authenticationConfiguration) {
        this.authenticationConfiguration = null;
    }

    public void bindPrivilegeConfiguration(PrivilegeConfiguration privilegeConfiguration) {
        this.privilegeConfiguration = privilegeConfiguration;
    }

    public void unbindPrivilegeConfiguration(PrivilegeConfiguration privilegeConfiguration) {
        this.privilegeConfiguration = null;
    }

    public void bindUserConfiguration(UserConfiguration userConfiguration) {
        this.userConfiguration = userConfiguration;
    }

    public void unbindUserConfiguration(UserConfiguration userConfiguration) {
        this.userConfiguration = null;
    }

    public void bindPrincipalConfiguration(PrincipalConfiguration principalConfiguration, Map<String, Object> properties) {
        synchronized (this) {
            principalConfigurations.add(principalConfiguration);
            addCandidate(properties);
        }

        maybeRegister();
    }

    public void unbindPrincipalConfiguration(PrincipalConfiguration principalConfiguration, Map<String, Object> properties) {
        synchronized (this) {
            principalConfigurations.remove(principalConfiguration);
            removeCandidate(properties);
        }

        maybeUnregister();
    }

    public void bindTokenConfiguration(TokenConfiguration tokenConfiguration, Map<String, Object> properties) {
        synchronized (this) {
            tokenConfigurations.add(tokenConfiguration);
            addCandidate(properties);
        }

        maybeRegister();
    }

    public void unbindTokenConfiguration(TokenConfiguration tokenConfiguration, Map<String, Object> properties) {
        synchronized (this) {
            tokenConfigurations.remove(tokenConfiguration);
            removeCandidate(properties);
        }

        maybeUnregister();
    }

    public void bindAuthorizableNodeName(AuthorizableNodeName authorizableNodeName, Map<String, Object> properties) {
        synchronized (this) {
            this.authorizableNodeName = authorizableNodeName;
            addCandidate(properties);
        }

        maybeRegister();
    }

    public void unbindAuthorizableNodeName(AuthorizableNodeName authorizableNodeName, Map<String, Object> properties) {
        synchronized (this) {
            if (this.authorizableNodeName == authorizableNodeName) {
                this.authorizableNodeName = AuthorizableNodeName.DEFAULT;
            }

            removeCandidate(properties);
        }

        maybeUnregister();
    }

    public void bindAuthorizableActionProvider(AuthorizableActionProvider authorizableActionProvider, Map<String, Object> properties) {
        synchronized (this) {
            authorizableActionProviders.add(authorizableActionProvider);
            addCandidate(properties);
        }

        maybeRegister();
    }

    public void unbindAuthorizableActionProvider(AuthorizableActionProvider authorizableActionProvider, Map<String, Object> properties) {
        synchronized (this) {
            authorizableActionProviders.remove(authorizableActionProvider);
            removeCandidate(properties);
        }

        maybeUnregister();
    }

    public void bindRestrictionProvider(RestrictionProvider restrictionProvider, Map<String, Object> properties) {
        synchronized (this) {
            restrictionProviders.add(restrictionProvider);
            addCandidate(properties);
        }

        maybeRegister();
    }

    public void unbindRestrictionProvider(RestrictionProvider restrictionProvider, Map<String, Object> properties) {
        synchronized (this) {
            restrictionProviders.remove(restrictionProvider);
            removeCandidate(properties);
        }

        maybeUnregister();
    }

    private void maybeRegister() {
        BundleContext context;

        log.info("Trying to register a SecurityProvider...");

        synchronized (this) {

            // The component is not activated, yet. We have no means of registering
            // the SecurityProvider. This method will be called again after
            // activation completes.

            if (this.context == null) {
                log.info("Aborting: no BundleContext is available");
                return;
            }

            // The preconditions are not satisifed. This may happen when this
            // component is activated but not enough mandatory services are bound
            // to it.

            if (!preconditions.areSatisfied()) {
                log.info("Aborting: preconditions are not satisfied: {}", preconditions);
                return;
            }

            // The SecurityProvider is already registered. This may happen when a
            // new dependency is added to this component, but the requirements are
            // already satisfied.

            if (registration != null) {
                log.info("Aborting: a SecurityProvider is already registered");
                return;
            }

            // If the component is in the process of registering an instance of
            // SecurityProvider, return. This check is necessary because we don't
            // want to call createSecurityProvider() more than once. That method,
            // in fact, changes the state of the bound dependencies (it sets a
            // back-reference from the security configurations to the new
            // SecurityProvider). We want those dependencies to change state only
            // when we are sure that we will register the SecurityProvider we
            // are creating.

            if (registering) {
                log.info("Aborting: a SecurityProvider is already being registered");
                return;
            }

            // Mark the start of a registration process.

            registering = true;

            // Save the BundleContext for local usage.

            context = this.context;
        }

        // Register the SecurityProvider.

        Dictionary<String, Object> properties = new Hashtable<String, Object>();

        properties.put("type", "default");

        ServiceRegistration registration = context.registerService(
                SecurityProvider.class.getName(),
                createSecurityProvider(context),
                properties
        );

        synchronized (this) {
            this.registration = registration;
            this.registering = false;
        }

        log.info("SecurityProvider instance registered");
    }

    private void maybeUnregister() {
        ServiceRegistration registration;

        log.info("Trying to unregister the SecurityProvider...");

        synchronized (this) {

            // If there is nothing to register, we obviously have nothing to do.

            if (this.registration == null) {
                log.info("Aborting: no SecurityProvider is registered");
                return;
            }

            // The preconditions are not satisfied. This may happen when a
            // dependency is unbound from the current component.

            if (preconditions.areSatisfied()) {
                log.info("Aborting: preconditions are satisfied");
                return;
            }

            // Save the ServiceRegistration for local use.

            registration = this.registration;
            this.registration = null;
        }

        registration.unregister();

        log.info("SecurityProvider instance unregistered");
    }

    private SecurityProvider createSecurityProvider(BundleContext context) {
        InternalSecurityProvider securityProvider = new InternalSecurityProvider();

        // Static, mandatory references

        securityProvider.setAuthenticationConfiguration(initializeConfiguration(securityProvider, authenticationConfiguration));
        securityProvider.setAuthorizationConfiguration(initializeConfiguration(securityProvider, authorizationConfiguration));
        securityProvider.setUserConfiguration(initializeConfiguration(securityProvider, userConfiguration));
        securityProvider.setPrivilegeConfiguration(initializeConfiguration(securityProvider, privilegeConfiguration));

        // Multiple, dynamic references

        securityProvider.setPrincipalConfiguration(createCompositePrincipalConfiguration(securityProvider));
        securityProvider.setTokenConfiguration(createCompositeTokenConfiguration(securityProvider));

        // Whiteboard

        securityProvider.setWhiteboard(new OsgiWhiteboard(context));

        return securityProvider;
    }

    private PrincipalConfiguration createCompositePrincipalConfiguration(SecurityProvider securityProvider) {
        return new CompositePrincipalConfiguration(securityProvider) {

            @Override
            protected List<PrincipalConfiguration> getConfigurations() {
                ArrayList<PrincipalConfiguration> configurations = newArrayList(principalConfigurations);

                for (PrincipalConfiguration configuration : configurations) {
                    initializeConfiguration(getSecurityProvider(), configuration);
                }

                return configurations;
            }

        };
    }

    private TokenConfiguration createCompositeTokenConfiguration(SecurityProvider securityProvider) {
        return new CompositeTokenConfiguration(securityProvider) {

            @Override
            protected List<TokenConfiguration> getConfigurations() {
                List<TokenConfiguration> configurations = newArrayList(tokenConfigurations);

                for (TokenConfiguration configuration : configurations) {
                    initializeConfiguration(getSecurityProvider(), configuration);
                }

                return configurations;
            }

        };
    }

    private AuthorizationConfiguration initializeConfiguration(SecurityProvider securityProvider, AuthorizationConfiguration authorizationConfiguration) {
        Map<String, Object> parameters = newHashMap();

        parameters.put(AccessControlConstants.PARAM_RESTRICTION_PROVIDER, createCompositeRestrictionProvider());

        return initializeConfiguration(securityProvider, authorizationConfiguration, ConfigurationParameters.of(parameters));
    }

    private UserConfiguration initializeConfiguration(SecurityProvider securityProvider, UserConfiguration userConfiguration) {
        Map<String, Object> parameters = newHashMap();

        parameters.put(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, createCompositeAuthorizableActionProvider());
        parameters.put(UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, authorizableNodeName);

        return initializeConfiguration(securityProvider, userConfiguration, ConfigurationParameters.of(parameters));
    }

    private <T extends SecurityConfiguration> T initializeConfiguration(SecurityProvider securityProvider, T configuration) {
        return initializeConfiguration(securityProvider, configuration, ConfigurationParameters.EMPTY);
    }

    private <T extends SecurityConfiguration> T initializeConfiguration(SecurityProvider securityProvider, T configuration, ConfigurationParameters parameters) {
        if (configuration instanceof ConfigurationBase) {
            ConfigurationBase base = (ConfigurationBase) configuration;
            base.setSecurityProvider(securityProvider);
            base.setParameters(ConfigurationParameters.of(parameters, base.getParameters()));
        }

        return configuration;
    }

    private RestrictionProvider createCompositeRestrictionProvider() {
        return new WhiteboardRestrictionProvider() {

            @Override
            protected List<RestrictionProvider> getServices() {
                return newArrayList(restrictionProviders);
            }

        };
    }

    private AuthorizableActionProvider createCompositeAuthorizableActionProvider() {
        return new WhiteboardAuthorizableActionProvider() {

            @Override
            protected List<AuthorizableActionProvider> getServices() {
                return newArrayList(authorizableActionProviders);
            }

        };
    }

    private void addCandidate(Map<String, Object> properties) {
        String pid = getServicePid(properties);

        if (pid == null) {
            return;
        }

        preconditions.addCandidate(pid);
    }

    private void removeCandidate(Map<String, Object> properties) {
        String pid = getServicePid(properties);

        if (pid == null) {
            return;
        }

        preconditions.removeCandidate(pid);
    }

    private String getServicePid(Map<String, Object> properties) {
        return PropertiesUtil.toString(properties.get(Constants.SERVICE_PID), null);
    }

    private String[] getRequiredServicePids(Map<String, Object> configuration) {
        return PropertiesUtil.toStringArray(configuration.get("requiredServicePids"), new String[]{});
    }

}

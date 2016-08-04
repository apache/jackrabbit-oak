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

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

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
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
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
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardRestrictionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUserAuthenticationFactory;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newCopyOnWriteArrayList;

@Component(
        immediate = true,
        metatype = true,
        label = "Apache Jackrabbit Oak SecurityProvider",
        description = "The default SecurityProvider embedded in Apache Jackrabbit Oak"
)
@Properties({
        @Property(
                name = "requiredServicePids",
                label = "Required Service PIDs",
                description = "The SecurityProvider will not register itself " +
                        "unless the services identified by these PIDs are " +
                        "registered first. Only the PIDs of implementations of " +
                        "the following interfaces are checked: " +
                        "AuthorizationConfiguration, PrincipalConfiguration, " +
                        "TokenConfiguration, AuthorizableActionProvider, " +
                        "RestrictionProvider and UserAuthenticationFactory.",
                value = {
                        "org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl",
                        "org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl",
                        "org.apache.jackrabbit.oak.security.authentication.token.TokenConfigurationImpl",
                        "org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider",
                        "org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl",
                        "org.apache.jackrabbit.oak.security.user.UserAuthenticationFactoryImpl"
                },
                unbounded = PropertyUnbounded.ARRAY
        )
})
@References({
        @Reference(
                name = "authorizationConfiguration",
                referenceInterface = AuthorizationConfiguration.class,
                cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
                policy = ReferencePolicy.DYNAMIC
        ),
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
                cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
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
        ),
        @Reference(
                name = "userAuthenticationFactory",
                referenceInterface = UserAuthenticationFactory.class,
                cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
                policy = ReferencePolicy.DYNAMIC
        )
})
@SuppressWarnings("unused")
public class SecurityProviderRegistration {

    private static final Logger log = LoggerFactory.getLogger(SecurityProviderRegistration.class);

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

    private final CompositeAuthorizationConfiguration authorizationConfiguration = new CompositeAuthorizationConfiguration();
    private final CompositePrincipalConfiguration principalConfiguration = new CompositePrincipalConfiguration();
    private final CompositeTokenConfiguration tokenConfiguration = new CompositeTokenConfiguration();

    private final List<AuthorizableNodeName> authorizableNodeNames = newCopyOnWriteArrayList();
    private final List<AuthorizableActionProvider> authorizableActionProviders = newCopyOnWriteArrayList();
    private final List<RestrictionProvider> restrictionProviders = newCopyOnWriteArrayList();
    private final List<UserAuthenticationFactory> userAuthenticationFactories = newCopyOnWriteArrayList();

    //----------------------------------------------------< SCR integration >---

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

    //--------------------------------------< unary security configurations >---

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

    //-----------------------------------< multiple security configurations >---

    public void bindAuthorizationConfiguration(AuthorizationConfiguration configuration, Map<String, Object> properties) {
        bindConfiguration(authorizationConfiguration, configuration, properties);
    }

    public void unbindAuthorizationConfiguration(AuthorizationConfiguration configuration, Map<String, Object> properties) {
        unbindConfiguration(authorizationConfiguration, configuration, properties);
    }

    public void bindPrincipalConfiguration(PrincipalConfiguration configuration, Map<String, Object> properties) {
        bindConfiguration(principalConfiguration, configuration, properties);
    }

    public void unbindPrincipalConfiguration(PrincipalConfiguration configuration, Map<String, Object> properties) {
        unbindConfiguration(principalConfiguration, configuration, properties);
    }

    public void bindTokenConfiguration(TokenConfiguration configuration, Map<String, Object> properties) {
        bindConfiguration(tokenConfiguration, configuration, properties);
    }

    public void unbindTokenConfiguration(TokenConfiguration configuration, Map<String, Object> properties) {
        unbindConfiguration(tokenConfiguration, configuration, properties);
    }

    private void bindConfiguration(@Nonnull CompositeConfiguration composite, @Nonnull SecurityConfiguration configuration, Map<String, Object> properties) {
        synchronized (this) {
            composite.addConfiguration(configuration, ConfigurationParameters.of(properties));
            addCandidate(properties);
        }
        maybeRegister();
    }

    private void unbindConfiguration(@Nonnull CompositeConfiguration composite, @Nonnull SecurityConfiguration configuration, Map<String, Object> properties) {
        synchronized (this) {
            composite.removeConfiguration(configuration);
            removeCandidate(properties);
        }
        maybeUnregister();
    }

    //------------------------------------------------------------< add ons >---

    public void bindAuthorizableNodeName(AuthorizableNodeName authorizableNodeName, Map<String, Object> properties) {
        synchronized (this) {
            authorizableNodeNames.add(authorizableNodeName);
            addCandidate(properties);
        }

        maybeRegister();
    }

    public void unbindAuthorizableNodeName(AuthorizableNodeName authorizableNodeName, Map<String, Object> properties) {
        synchronized (this) {
            authorizableNodeNames.remove(authorizableNodeName);
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

    public void bindUserAuthenticationFactory(UserAuthenticationFactory userAuthenticationFactory, Map<String, Object> properties) {
        synchronized (this) {
            userAuthenticationFactories.add(userAuthenticationFactory);
            addCandidate(properties);
        }

        maybeRegister();
    }

    public void unbindUserAuthenticationFactory(UserAuthenticationFactory userAuthenticationFactory, Map<String, Object> properties) {
        synchronized (this) {
            userAuthenticationFactories.remove(userAuthenticationFactory);
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

    private SecurityProvider createSecurityProvider(@Nonnull BundleContext context) {
        InternalSecurityProvider securityProvider = new InternalSecurityProvider();

        // Static, mandatory references

        securityProvider.setAuthenticationConfiguration(ConfigurationInitializer.initializeConfiguration(securityProvider, authenticationConfiguration));
        securityProvider.setPrivilegeConfiguration(ConfigurationInitializer.initializeConfiguration(securityProvider, privilegeConfiguration));

        ConfigurationParameters userParams = ConfigurationParameters.of(
                ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, createWhiteboardAuthorizableActionProvider()),
                ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, createWhiteboardAuthorizableNodeName()),
                ConfigurationParameters.of(UserConstants.PARAM_USER_AUTHENTICATION_FACTORY, createWhiteboardUserAuthenticationFactory()));
        securityProvider.setUserConfiguration(ConfigurationInitializer.initializeConfiguration(securityProvider, userConfiguration, userParams));

        // Multiple, dynamic references

        ConfigurationParameters restrictionParams = ConfigurationParameters.of(AccessControlConstants.PARAM_RESTRICTION_PROVIDER, createWhiteboardRestrictionProvider());
        ConfigurationInitializer.initializeConfigurations(securityProvider, authorizationConfiguration, restrictionParams);
        securityProvider.setAuthorizationConfiguration(authorizationConfiguration);

        ConfigurationInitializer.initializeConfigurations(securityProvider, principalConfiguration, ConfigurationParameters.EMPTY);
        securityProvider.setPrincipalConfiguration(principalConfiguration);

        ConfigurationInitializer.initializeConfigurations(securityProvider, tokenConfiguration, ConfigurationParameters.EMPTY);
        securityProvider.setTokenConfiguration(tokenConfiguration);

        // Whiteboard

        securityProvider.setWhiteboard(new OsgiWhiteboard(context));

        return securityProvider;
    }

    private RestrictionProvider createWhiteboardRestrictionProvider() {
        return new WhiteboardRestrictionProvider() {

            @Override
            protected List<RestrictionProvider> getServices() {
                return newArrayList(restrictionProviders);
            }

        };
    }

    private AuthorizableActionProvider createWhiteboardAuthorizableActionProvider() {
        return new WhiteboardAuthorizableActionProvider() {

            @Override
            protected List<AuthorizableActionProvider> getServices() {
                return newArrayList(authorizableActionProviders);
            }

        };
    }

    private AuthorizableNodeName createWhiteboardAuthorizableNodeName() {
        return new WhiteboardAuthorizableNodeName() {

            @Override
            protected List<AuthorizableNodeName> getServices() {
                return newArrayList(authorizableNodeNames);
            }

        };
    }

    private UserAuthenticationFactory createWhiteboardUserAuthenticationFactory() {
        return new WhiteboardUserAuthenticationFactory(UserConfigurationImpl.getDefaultAuthenticationFactory()) {

            @Override
            protected List<UserAuthenticationFactory> getServices() {
                return newArrayList(userAuthenticationFactories);
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

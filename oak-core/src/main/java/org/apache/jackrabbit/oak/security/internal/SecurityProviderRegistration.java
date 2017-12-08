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

import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.authorization.restriction.WhiteboardRestrictionProvider;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.whiteboard.WhiteboardAuthorizableActionProvider;
import org.apache.jackrabbit.oak.security.user.whiteboard.WhiteboardAuthorizableNodeName;
import org.apache.jackrabbit.oak.security.user.whiteboard.WhiteboardUserAuthenticationFactory;
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
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newCopyOnWriteArrayList;
import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

@Component(immediate=true)
@Designate(ocd = SecurityProviderRegistration.Configuration.class)
@SuppressWarnings("unused")
public class SecurityProviderRegistration {

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak SecurityProvider",
            description = "The default SecurityProvider embedded in Apache Jackrabbit Oak"
    )
    @interface Configuration {
        @AttributeDefinition(
                name = "Required Services",
                description = "The SecurityProvider will not register itself " +
                        "unless the services identified by the following service pids " +
                        "or the oak.security.name properties are registered first. The class name is " +
                        "identified by checking the service.pid property. If that property " +
                        "does not exist, the oak.security.name property is used as a fallback." +
                        "Only implementations of the following interfaces are checked :" +
                        "AuthorizationConfiguration, PrincipalConfiguration, " +
                        "TokenConfiguration, AuthorizableActionProvider, " +
                        "RestrictionProvider and UserAuthenticationFactory."
        )
        String[] requiredServicePids() default {
                "org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl",
                "org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl",
                "org.apache.jackrabbit.oak.security.authentication.token.TokenConfigurationImpl",
                "org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider",
                "org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl",
                "org.apache.jackrabbit.oak.security.user.UserAuthenticationFactoryImpl"
        };
        
        @AttributeDefinition(
                name = "Authorization Composition Type",
                description = "The Composite Authorization model uses this flag to determine what type of logic "
                        + "to apply to the existing providers (default value is AND).",
                options = {
                        @Option(label = "AND", value = "AND"),
                        @Option(label = "OR", value = "OR")
                }
        )
        String authorizationCompositionType() default "AND";

    }

    private static final Logger log = LoggerFactory.getLogger(SecurityProviderRegistration.class);

    private AuthenticationConfiguration authenticationConfiguration;

    private PrivilegeConfiguration privilegeConfiguration;

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

    private RootProvider rootProvider;
    private TreeProvider treeProvider;

    //----------------------------------------------------< SCR integration >---

    @Activate
    public void activate(BundleContext context, Configuration configuration) {
        String[] requiredServicePids = configuration.requiredServicePids();

        synchronized (this) {
            for (String pid : requiredServicePids) {
                preconditions.addPrecondition(pid);
            }

            this.context = context;
        }
        this.authorizationConfiguration.withCompositionType(configuration.authorizationCompositionType());

        maybeRegister();
    }

    @Modified
    public void modified(Configuration configuration) {
        String[] requiredServicePids = configuration.requiredServicePids();

        synchronized (this) {
            preconditions.clearPreconditions();

            for (String pid : requiredServicePids) {
                preconditions.addPrecondition(pid);
            }
        }
        this.authorizationConfiguration.withCompositionType(configuration.authorizationCompositionType());

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

    @Reference(name = "authenticationConfiguration")
    public void bindAuthenticationConfiguration(AuthenticationConfiguration authenticationConfiguration) {
        this.authenticationConfiguration = authenticationConfiguration;
    }

    public void unbindAuthenticationConfiguration(AuthenticationConfiguration authenticationConfiguration) {
        this.authenticationConfiguration = null;
    }

    @Reference(name = "privilegeConfiguration")
    public void bindPrivilegeConfiguration(PrivilegeConfiguration privilegeConfiguration) {
        this.privilegeConfiguration = privilegeConfiguration;
    }

    public void unbindPrivilegeConfiguration(PrivilegeConfiguration privilegeConfiguration) {
        this.privilegeConfiguration = null;
    }

    @Reference(name = "userConfiguration")
    public void bindUserConfiguration(UserConfiguration userConfiguration) {
        this.userConfiguration = userConfiguration;
    }

    public void unbindUserConfiguration(UserConfiguration userConfiguration) {
        this.userConfiguration = null;
    }

    //-------------------------------------------< unary tree/root provider >---

    @Reference(name = "rootProvider")
    public void bindRootProvider(RootProvider rootProvider) {
        this.rootProvider = rootProvider;
    }

    public void unbindRootProvider(RootProvider rootProvider) {
        this.rootProvider = null;
    }

    @Reference(name = "treeProvider")
    public void bindTreeProvider(TreeProvider treeProvider) {
        this.treeProvider = treeProvider;
    }

    public void unbindTreeProvider(TreeProvider treeProvider) {
        this.treeProvider = null;
    }

    //-----------------------------------< multiple security configurations >---

    @Reference(
            name = "authorizationConfiguration",
            service = AuthorizationConfiguration.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
    public void bindAuthorizationConfiguration(AuthorizationConfiguration configuration, Map<String, Object> properties) {
        bindConfiguration(authorizationConfiguration, configuration, properties);
    }

    public void unbindAuthorizationConfiguration(AuthorizationConfiguration configuration, Map<String, Object> properties) {
        unbindConfiguration(authorizationConfiguration, configuration, properties);
    }

    @Reference(
            name = "principalConfiguration",
            service = PrincipalConfiguration.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
    public void bindPrincipalConfiguration(PrincipalConfiguration configuration, Map<String, Object> properties) {
        bindConfiguration(principalConfiguration, configuration, properties);
    }

    public void unbindPrincipalConfiguration(PrincipalConfiguration configuration, Map<String, Object> properties) {
        unbindConfiguration(principalConfiguration, configuration, properties);
    }

    @Reference(
            name = "tokenConfiguration",
            service = TokenConfiguration.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
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
    @Reference(
            name = "authorizableNodeName",
            service = AuthorizableNodeName.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
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

    @Reference(
            name = "authorizableActionProvider",
            service = AuthorizableActionProvider.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
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

    @Reference(
            name = "restrictionProvider",
            service = RestrictionProvider.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
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

    @Reference(
            name = "userAuthenticationFactory",
            service = UserAuthenticationFactory.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
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

            // The preconditions are still satisfied. This may happen when a
            // dependency is unbound while not being listed as required service.

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

        securityProvider.setAuthenticationConfiguration(ConfigurationInitializer.initializeConfiguration(authenticationConfiguration, securityProvider, rootProvider, treeProvider));
        securityProvider.setPrivilegeConfiguration(ConfigurationInitializer.initializeConfiguration(privilegeConfiguration, securityProvider, rootProvider, treeProvider));

        ConfigurationParameters userParams = ConfigurationParameters.of(
                ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, createWhiteboardAuthorizableActionProvider()),
                ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, createWhiteboardAuthorizableNodeName()),
                ConfigurationParameters.of(UserConstants.PARAM_USER_AUTHENTICATION_FACTORY, createWhiteboardUserAuthenticationFactory()));
        securityProvider.setUserConfiguration(ConfigurationInitializer.initializeConfiguration(userConfiguration, securityProvider, userParams, rootProvider, treeProvider));

        // Multiple, dynamic references

        ConfigurationParameters restrictionParams = ConfigurationParameters.of(AccessControlConstants.PARAM_RESTRICTION_PROVIDER, createWhiteboardRestrictionProvider());
        ConfigurationInitializer.initializeConfigurations(authorizationConfiguration, securityProvider, restrictionParams, rootProvider, treeProvider);
        securityProvider.setAuthorizationConfiguration(authorizationConfiguration);

        ConfigurationInitializer.initializeConfigurations(principalConfiguration, securityProvider, ConfigurationParameters.EMPTY, rootProvider, treeProvider);
        securityProvider.setPrincipalConfiguration(principalConfiguration);

        ConfigurationInitializer.initializeConfigurations(tokenConfiguration, securityProvider, ConfigurationParameters.EMPTY, rootProvider, treeProvider);
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
        String pidOrName = getServicePidOrComponentName(properties);

        if (pidOrName == null) {
            return;
        }

        preconditions.addCandidate(pidOrName);
    }

    private void removeCandidate(Map<String, Object> properties) {
        String pidOrName = getServicePidOrComponentName(properties);

        if (pidOrName == null) {
            return;
        }

        preconditions.removeCandidate(pidOrName);
    }

    private static String getServicePidOrComponentName(Map<String, Object> properties) {
        String servicePid = PropertiesUtil.toString(properties.get(Constants.SERVICE_PID), null);
        if ( servicePid != null ) {
            return servicePid;
        }
        return PropertiesUtil.toString(properties.get(OAK_SECURITY_NAME), null);
    }
}

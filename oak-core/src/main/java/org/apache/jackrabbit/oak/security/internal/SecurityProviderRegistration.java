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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
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
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMBean;
import org.apache.jackrabbit.oak.spi.security.authentication.token.CompositeTokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregationFilter;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.CompositePrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.Monitor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceReference;
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

import javax.jcr.security.AccessControlManager;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.spi.security.ConfigurationParameters.EMPTY;
import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

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

    private final SortedMap<ServiceReference, AuthorizableNodeName> authorizableNodeNames = Collections.synchronizedSortedMap(new TreeMap<>());
    private final SortedMap<ServiceReference, AuthorizableActionProvider> authorizableActionProviders = Collections.synchronizedSortedMap(new TreeMap<>());
    private final SortedMap<ServiceReference, RestrictionProvider> restrictionProviders = Collections.synchronizedSortedMap(new TreeMap<>());
    private final SortedMap<ServiceReference, UserAuthenticationFactory> userAuthenticationFactories = Collections.synchronizedSortedMap(new TreeMap<>());
    private final SortedMap<ServiceReference, AggregationFilter> aggregationFilters = Collections.synchronizedSortedMap(new TreeMap<>());

    private RootProvider rootProvider;
    private TreeProvider treeProvider;

    @Reference
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;
    private Closer closer;

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
        closeQuietly(closer);
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

    private <T extends SecurityConfiguration> void bindConfiguration(@NotNull CompositeConfiguration<T> composite, @NotNull T configuration, Map<String, Object> properties) {
        synchronized (this) {
            composite.addConfiguration(configuration, ConfigurationParameters.of(properties));
            addCandidate(properties);
        }
        maybeRegister();
    }

    private <T extends SecurityConfiguration> void unbindConfiguration(@NotNull CompositeConfiguration<T> composite, @NotNull T configuration, Map<String, Object> properties) {
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
    public void bindAuthorizableNodeName(@NotNull ServiceReference serviceReference, @NotNull AuthorizableNodeName authorizableNodeName) {
        synchronized (this) {
            authorizableNodeNames.put(serviceReference, authorizableNodeName);
            addCandidate(serviceReference);
        }

        maybeRegister();
    }

    public void unbindAuthorizableNodeName(@NotNull ServiceReference serviceReference, @NotNull AuthorizableNodeName authorizableNodeName) {
        synchronized (this) {
            authorizableNodeNames.remove(serviceReference);
            removeCandidate(serviceReference);
        }

        maybeUnregister();
    }

    @Reference(
            name = "authorizableActionProvider",
            service = AuthorizableActionProvider.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
    public void bindAuthorizableActionProvider(@NotNull ServiceReference serviceReference, @NotNull AuthorizableActionProvider authorizableActionProvider) {
        synchronized (this) {
            authorizableActionProviders.put(serviceReference, authorizableActionProvider);
            addCandidate(serviceReference);
        }

        maybeRegister();
    }

    public void unbindAuthorizableActionProvider(@NotNull ServiceReference serviceReference, @NotNull AuthorizableActionProvider authorizableActionProvider) {
        synchronized (this) {
            authorizableActionProviders.remove(serviceReference);
            removeCandidate(serviceReference);
        }

        maybeUnregister();
    }

    @Reference(
            name = "restrictionProvider",
            service = RestrictionProvider.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
    public void bindRestrictionProvider(@NotNull ServiceReference serviceReference, @NotNull RestrictionProvider restrictionProvider) {
        synchronized (this) {
            restrictionProviders.put(serviceReference, restrictionProvider);
            addCandidate(serviceReference);
        }

        maybeRegister();
    }

    public void unbindRestrictionProvider(@NotNull ServiceReference serviceReference, @NotNull RestrictionProvider restrictionProvider) {
        synchronized (this) {
            restrictionProviders.remove(serviceReference);
            removeCandidate(serviceReference);
        }

        maybeUnregister();
    }

    @Reference(
            name = "userAuthenticationFactory",
            service = UserAuthenticationFactory.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC
    )
    public void bindUserAuthenticationFactory(@NotNull ServiceReference serviceReference, @NotNull UserAuthenticationFactory userAuthenticationFactory) {
        synchronized (this) {
            userAuthenticationFactories.put(serviceReference, userAuthenticationFactory);
            addCandidate(serviceReference);
        }

        maybeRegister();
    }

    public void unbindUserAuthenticationFactory(@NotNull ServiceReference serviceReference, @NotNull UserAuthenticationFactory userAuthenticationFactory) {
        synchronized (this) {
            userAuthenticationFactories.remove(serviceReference);
            removeCandidate(serviceReference);
        }

        maybeUnregister();
    }

    @Reference(
            name = "aggregationFilters", service = AggregationFilter.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC)
    public void bindAggregationFilter(@NotNull ServiceReference serviceReference, @NotNull AggregationFilter aggregationFilter) {
        synchronized (this) {
            aggregationFilters.put(serviceReference, aggregationFilter);
            addCandidate(serviceReference);
        }
        maybeRegister();
    }

    public void unbindAggregationFilter(@NotNull ServiceReference serviceReference, @NotNull AggregationFilter aggregationFilter) {
        synchronized (this) {
            aggregationFilters.remove(serviceReference);
            removeCandidate(serviceReference);
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

        Dictionary<String, Object> properties = new Hashtable<>();

        properties.put("type", "default");

        Whiteboard whiteboard = new OsgiWhiteboard(context);
        SecurityProvider securityProvider = createSecurityProvider(whiteboard);
        ServiceRegistration registration = context.registerService(
                SecurityProvider.class.getName(),
                securityProvider,
                properties
        );

        synchronized (this) {
            this.registration = registration;
            this.registering = false;
        }

        closer = Closer.create();
        Iterable<Iterable<Monitor<?>>> monitors = Iterables.transform(securityProvider.getConfigurations(), sc -> sc.getMonitors(statisticsProvider));
        for (Monitor monitor : Iterables.concat(monitors)) {
            Registration reg = whiteboard.register(monitor.getMonitorClass(), monitor, monitor.getMonitorProperties());
            closer.register(reg::unregister);

            if (monitor instanceof LoginModuleMBean) {
                Registration mbean = registerMBean(whiteboard, LoginModuleMBean.class, (LoginModuleMBean) monitor, LoginModuleMBean.TYPE, LoginModuleMBean.NAME);
                closer.register(mbean::unregister);
            }
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
        closeQuietly(closer);

        log.info("SecurityProvider instance unregistered");
    }

    private SecurityProvider createSecurityProvider(@NotNull Whiteboard whiteboard) {
        ConfigurationParameters userParams = ConfigurationParameters.of(
              ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, createWhiteboardAuthorizableActionProvider()),
              ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, createWhiteboardAuthorizableNodeName()),
              ConfigurationParameters.of(UserConstants.PARAM_USER_AUTHENTICATION_FACTORY, createWhiteboardUserAuthenticationFactory()));

        ConfigurationParameters authorizationParams = ConfigurationParameters
                .of(AccessControlConstants.PARAM_RESTRICTION_PROVIDER, createWhiteboardRestrictionProvider());
        authorizationConfiguration.withAggregationFilter(createAggregationFilter());

        return SecurityProviderBuilder.newBuilder().withRootProvider(rootProvider).withTreeProvider(treeProvider)
                .with(authenticationConfiguration, EMPTY, privilegeConfiguration, EMPTY, userConfiguration, userParams,
                        authorizationConfiguration, authorizationParams, principalConfiguration, EMPTY,
                        tokenConfiguration, EMPTY)
                .withWhiteboard(whiteboard).build();
    }

    private RestrictionProvider createWhiteboardRestrictionProvider() {
        return new WhiteboardRestrictionProvider() {

            @Override
            protected List<RestrictionProvider> getServices() {
                Collection<RestrictionProvider> values = restrictionProviders.values();
                synchronized (restrictionProviders) {
                    return new ArrayList<>(values);
                }
            }

        };
    }

    private AuthorizableActionProvider createWhiteboardAuthorizableActionProvider() {
        return new WhiteboardAuthorizableActionProvider() {

            @Override
            protected List<AuthorizableActionProvider> getServices() {
                Collection<AuthorizableActionProvider> values = authorizableActionProviders.values();
                synchronized (authorizableActionProviders) {
                    return new ArrayList<>(values);
                }
            }

        };
    }

    private AuthorizableNodeName createWhiteboardAuthorizableNodeName() {
        return new WhiteboardAuthorizableNodeName() {

            @Override
            protected List<AuthorizableNodeName> getServices() {
                Collection<AuthorizableNodeName> values = authorizableNodeNames.values();
                synchronized (authorizableNodeNames) {
                    return new ArrayList<>(values);
                }
            }

        };
    }

    private UserAuthenticationFactory createWhiteboardUserAuthenticationFactory() {
        return new WhiteboardUserAuthenticationFactory(UserConfigurationImpl.getDefaultAuthenticationFactory()) {

            @Override
            protected List<UserAuthenticationFactory> getServices() {
                Collection<UserAuthenticationFactory> values = userAuthenticationFactories.values();
                synchronized (userAuthenticationFactories) {
                    return new ArrayList<>(values);
                }
            }

        };
    }

    private AggregationFilter createAggregationFilter() {
        List<AggregationFilter> filters;
        synchronized (aggregationFilters) {
            filters = new ArrayList<>(aggregationFilters.values());
        }
        switch (filters.size()) {
            case 0: return AggregationFilter.DEFAULT;
            case 1: return filters.get(0);
            default:
                return new AggregationFilter() {
                    @Override
                    public boolean stop(@NotNull AggregatedPermissionProvider permissionProvider, @NotNull Set<Principal> principals) {
                        for (AggregationFilter f : filters) {
                            if (f.stop(permissionProvider, principals)) {
                                return true;
                            }
                        }
                        return false;
                    }

                    @Override
                    public boolean stop(@NotNull JackrabbitAccessControlManager accessControlManager, @NotNull Set<Principal> principals) {
                        for (AggregationFilter f : filters) {
                            if (f.stop(accessControlManager, principals)) {
                                return true;
                            }
                        }
                        return false;
                    }

                    @Override
                    public boolean stop(@NotNull AccessControlManager accessControlManager, @Nullable String absPath) {
                        for (AggregationFilter f : filters) {
                            if (f.stop(accessControlManager, absPath)) {
                                return true;
                            }
                        }
                        return false;
                    }
                };
        }
    }

    private void addCandidate(Map<String, Object> properties) {
        String pidOrName = getServicePidOrComponentName(properties);

        if (pidOrName == null) {
            return;
        }

        preconditions.addCandidate(pidOrName);
    }

    private void addCandidate(@NotNull ServiceReference serviceReference) {
        String pidOrName = getServicePidOrComponentName(serviceReference);

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

    private void removeCandidate(@NotNull ServiceReference serviceReference) {
        String pidOrName = getServicePidOrComponentName(serviceReference);

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

    private static String getServicePidOrComponentName(@NotNull ServiceReference serviceReference) {
        String servicePid = PropertiesUtil.toString(serviceReference.getProperty(Constants.SERVICE_PID), null);
        if ( servicePid != null ) {
            return servicePid;
        }
        return PropertiesUtil.toString(serviceReference.getProperty(OAK_SECURITY_NAME), null);
    }
}

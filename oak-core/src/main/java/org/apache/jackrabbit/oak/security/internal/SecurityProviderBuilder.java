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

import static org.apache.jackrabbit.oak.security.internal.ConfigurationInitializer.initializeConfiguration;
import static org.apache.jackrabbit.oak.security.internal.ConfigurationInitializer.initializeConfigurations;
import static org.apache.jackrabbit.oak.spi.security.ConfigurationParameters.EMPTY;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.impl.RootProviderService;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeProviderService;
import org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authentication.token.TokenConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
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
import org.apache.jackrabbit.oak.spi.security.user.action.DefaultAuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

public final class SecurityProviderBuilder {

    private Whiteboard whiteboard;
    private RootProvider rootProvider;
    private TreeProvider treeProvider;

    private ConfigurationParameters authenticationParams = EMPTY;
    private AuthenticationConfiguration authenticationConfiguration;

    private ConfigurationParameters privilegeParams = EMPTY;
    private PrivilegeConfiguration privilegeConfiguration;

    private ConfigurationParameters userParams = EMPTY;
    private UserConfiguration userConfiguration;

    private ConfigurationParameters authorizationParams = EMPTY;
    private CompositeAuthorizationConfiguration authorizationConfiguration;

    private ConfigurationParameters principalParams = EMPTY;
    private CompositePrincipalConfiguration principalConfiguration;

    private ConfigurationParameters tokenParams = EMPTY;
    private CompositeTokenConfiguration tokenConfiguration;

    @Nonnull
    public static SecurityProviderBuilder newBuilder() {
        return new SecurityProviderBuilder();
    }

    private SecurityProviderBuilder() {
    }

    public SecurityProviderBuilder with(@Nonnull ConfigurationParameters configuration) {
        authenticationParams = configuration.getConfigValue(AuthenticationConfiguration.NAME, EMPTY);
        privilegeParams = configuration.getConfigValue(PrivilegeConfiguration.NAME, EMPTY);

        if (configuration.contains(UserConfiguration.NAME)) {
            userParams = configuration.getConfigValue(UserConfiguration.NAME, EMPTY);
        } else {
            AuthorizableActionProvider authorizableActionProvider = new DefaultAuthorizableActionProvider();
            AuthorizableNodeName authorizableNodeName = AuthorizableNodeName.DEFAULT;
            UserAuthenticationFactory userAuthenticationFactory = UserConfigurationImpl
                    .getDefaultAuthenticationFactory();

            userParams = ConfigurationParameters.of(
                    ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER,
                            authorizableActionProvider),
                    ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, authorizableNodeName),
                    ConfigurationParameters.of(UserConstants.PARAM_USER_AUTHENTICATION_FACTORY,
                            userAuthenticationFactory));
        }
        if (configuration.contains(AuthorizationConfiguration.NAME)) {
            authorizationParams = configuration.getConfigValue(AuthorizationConfiguration.NAME, EMPTY);
        } else {
            RestrictionProvider restrictionProvider = new RestrictionProviderImpl();
            authorizationParams = ConfigurationParameters.of(AccessControlConstants.PARAM_RESTRICTION_PROVIDER,
                    restrictionProvider);
        }
        principalParams = configuration.getConfigValue(PrincipalConfiguration.NAME, EMPTY);
        tokenParams = configuration.getConfigValue(TokenConfiguration.NAME, EMPTY);
        return this;
    }

    public SecurityProviderBuilder with(@Nonnull AuthenticationConfiguration authenticationConfiguration,
            @Nonnull ConfigurationParameters authenticationParams,
            @Nonnull PrivilegeConfiguration privilegeConfiguration, @Nonnull ConfigurationParameters privilegeParams,
            @Nonnull UserConfiguration userConfiguration, @Nonnull ConfigurationParameters userParams,
            @Nonnull CompositeAuthorizationConfiguration authorizationConfiguration,
            @Nonnull ConfigurationParameters authorizationParams,
            @Nonnull CompositePrincipalConfiguration principalConfiguration,
            @Nonnull ConfigurationParameters principalParams, @Nonnull CompositeTokenConfiguration tokenConfiguration,
            @Nonnull ConfigurationParameters tokenParams) {

        this.authenticationConfiguration = authenticationConfiguration;
        this.authenticationParams = authenticationParams;

        this.privilegeConfiguration = privilegeConfiguration;
        this.privilegeParams = privilegeParams;

        this.userConfiguration = userConfiguration;
        this.userParams = userParams;

        this.authorizationConfiguration = authorizationConfiguration;
        this.authorizationParams = authorizationParams;

        this.principalConfiguration = principalConfiguration;
        this.principalParams = principalParams;

        this.tokenConfiguration = tokenConfiguration;
        this.tokenParams = tokenParams;

        return this;
    }

    public SecurityProvider build() {
        InternalSecurityProvider securityProvider = new InternalSecurityProvider();

        if (rootProvider == null) {
            rootProvider = new RootProviderService();
        }
        if (treeProvider == null) {
            treeProvider = new TreeProviderService();
        }

        // authentication
        if (authenticationConfiguration == null) {
            authenticationConfiguration = new AuthenticationConfigurationImpl();
        }
        securityProvider.setAuthenticationConfiguration(initializeConfiguration(authenticationConfiguration,
                securityProvider, authenticationParams, rootProvider, treeProvider));

        // privilege
        if (privilegeConfiguration == null) {
            privilegeConfiguration = new PrivilegeConfigurationImpl();
        }
        securityProvider.setPrivilegeConfiguration(initializeConfiguration(privilegeConfiguration, securityProvider,
                privilegeParams, rootProvider, treeProvider));

        // user
        if (userConfiguration == null) {
            userConfiguration = new UserConfigurationImpl();
        }
        securityProvider.setUserConfiguration(
                initializeConfiguration(userConfiguration, securityProvider, userParams, rootProvider, treeProvider));

        // authorization
        if (authorizationConfiguration == null) {
            authorizationConfiguration = new CompositeAuthorizationConfiguration();
            authorizationConfiguration.setDefaultConfig(initializeConfiguration(new AuthorizationConfigurationImpl(),
                    securityProvider, rootProvider, treeProvider));
        }
        initializeConfigurations(authorizationConfiguration, securityProvider, authorizationParams, rootProvider,
                treeProvider);
        securityProvider.setAuthorizationConfiguration(authorizationConfiguration);

        // principal
        if (principalConfiguration == null) {
            principalConfiguration = new CompositePrincipalConfiguration();
            principalConfiguration.setDefaultConfig(initializeConfiguration(new PrincipalConfigurationImpl(),
                    securityProvider, rootProvider, treeProvider));
        }
        initializeConfigurations(principalConfiguration, securityProvider, principalParams, rootProvider, treeProvider);
        securityProvider.setPrincipalConfiguration(principalConfiguration);

        // token
        if (tokenConfiguration == null) {
            tokenConfiguration = new CompositeTokenConfiguration();
            tokenConfiguration.setDefaultConfig(initializeConfiguration(new TokenConfigurationImpl(), securityProvider,
                    rootProvider, treeProvider));
        }
        initializeConfigurations(tokenConfiguration, securityProvider, tokenParams, rootProvider, treeProvider);
        securityProvider.setTokenConfiguration(tokenConfiguration);

        // whiteboard
        if (whiteboard != null) {
            securityProvider.setWhiteboard(whiteboard);
        }

        return securityProvider;
    }

    public SecurityProviderBuilder withWhiteboard(@Nonnull Whiteboard whiteboard) {
        this.whiteboard = whiteboard;
        return this;
    }

    public SecurityProviderBuilder withRootProvider(@Nonnull RootProvider rootProvider) {
        this.rootProvider = rootProvider;
        return this;
    }

    public SecurityProviderBuilder withTreeProvider(@Nonnull TreeProvider treeProvider) {
        this.treeProvider = treeProvider;
        return this;
    }

}

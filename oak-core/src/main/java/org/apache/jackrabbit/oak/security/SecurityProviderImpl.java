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
package org.apache.jackrabbit.oak.security;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.impl.RootProviderService;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeProviderService;
import org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authentication.token.TokenConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.authorization.restriction.WhiteboardRestrictionProvider;
import org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.whiteboard.WhiteboardAuthorizableActionProvider;
import org.apache.jackrabbit.oak.security.user.whiteboard.WhiteboardAuthorizableNodeName;
import org.apache.jackrabbit.oak.security.user.whiteboard.WhiteboardUserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.CompositeTokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.CompositePrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAware;
import org.osgi.framework.BundleContext;

import static com.google.common.base.Preconditions.checkNotNull;

@Deprecated
public class SecurityProviderImpl implements SecurityProvider, WhiteboardAware {

    private volatile AuthenticationConfiguration authenticationConfiguration;

    private volatile PrivilegeConfiguration privilegeConfiguration;

    private volatile UserConfiguration userConfiguration;

    private final CompositeAuthorizationConfiguration authorizationConfiguration = new CompositeAuthorizationConfiguration(this);

    private final CompositePrincipalConfiguration principalConfiguration = new CompositePrincipalConfiguration(this);

    private final CompositeTokenConfiguration tokenConfiguration = new CompositeTokenConfiguration(this);

    private final WhiteboardAuthorizableNodeName authorizableNodeName = new WhiteboardAuthorizableNodeName();
    private final WhiteboardAuthorizableActionProvider authorizableActionProvider = new WhiteboardAuthorizableActionProvider();
    private final WhiteboardRestrictionProvider restrictionProvider = new WhiteboardRestrictionProvider();
    private final WhiteboardUserAuthenticationFactory userAuthenticationFactory = new WhiteboardUserAuthenticationFactory(UserConfigurationImpl.getDefaultAuthenticationFactory());

    private ConfigurationParameters configuration;

    private Whiteboard whiteboard;

    private final RootProvider rootProvider = new RootProviderService();
    private final TreeProvider treeProvider = new TreeProviderService();

    /**
     * Default constructor used in OSGi environments.
     */
    public SecurityProviderImpl() {
        this(ConfigurationParameters.EMPTY);
    }

    /**
     * Create a new {@code SecurityProvider} instance with the given configuration
     * parameters.
     *
     * @param configuration security configuration
     */
    public SecurityProviderImpl(@Nonnull ConfigurationParameters configuration) {
        checkNotNull(configuration);
        this.configuration = configuration;

        authenticationConfiguration = initDefaultConfiguration(new AuthenticationConfigurationImpl(this));

        userConfiguration = initDefaultConfiguration(new UserConfigurationImpl(this));
        privilegeConfiguration = initDefaultConfiguration(new PrivilegeConfigurationImpl());

        initCompositeConfiguration(authorizationConfiguration, new AuthorizationConfigurationImpl(this));
        initCompositeConfiguration(principalConfiguration, new PrincipalConfigurationImpl(this));
        initCompositeConfiguration(tokenConfiguration, new TokenConfigurationImpl(this));
    }

    @Override
    public void setWhiteboard(@Nonnull Whiteboard whiteboard) {
        this.whiteboard = whiteboard;
    }

    @Override
    public Whiteboard getWhiteboard() {
        return whiteboard;
    }

    @Nonnull
    @Override
    public ConfigurationParameters getParameters(@Nullable String name) {
        if (name == null) {
            return configuration;
        }
        ConfigurationParameters params = configuration.getConfigValue(name, ConfigurationParameters.EMPTY);
        for (SecurityConfiguration sc : getConfigurations()) {
            if (sc != null && sc.getName().equals(name)) {
                return ConfigurationParameters.of(params, sc.getParameters());
            }
        }
        return params;
    }

    @Nonnull
    @Override
    public Iterable<? extends SecurityConfiguration> getConfigurations() {
        Set<SecurityConfiguration> scs = new HashSet<SecurityConfiguration>();
        scs.add(authenticationConfiguration);
        scs.add(authorizationConfiguration);
        scs.add(userConfiguration);
        scs.add(principalConfiguration);
        scs.add(privilegeConfiguration);
        scs.add(tokenConfiguration);
        return scs;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public <T> T getConfiguration(@Nonnull Class<T> configClass) {
        if (AuthenticationConfiguration.class == configClass) {
            return (T) authenticationConfiguration;
        } else if (AuthorizationConfiguration.class == configClass) {
            return (T) authorizationConfiguration;
        } else if (UserConfiguration.class == configClass) {
            return (T) userConfiguration;
        } else if (PrincipalConfiguration.class == configClass) {
            return (T) principalConfiguration;
        } else if (PrivilegeConfiguration.class == configClass) {
            return (T) privilegeConfiguration;
        } else if (TokenConfiguration.class == configClass) {
            return (T) tokenConfiguration;
        } else {
            throw new IllegalArgumentException("Unsupported security configuration class " + configClass);
        }
    }

    protected void activate(BundleContext context) {
        whiteboard = new OsgiWhiteboard(context);
        authorizableActionProvider.start(whiteboard);
        authorizableNodeName.start(whiteboard);
        restrictionProvider.start(whiteboard);
        userAuthenticationFactory.start(whiteboard);

        initializeConfigurations();
    }

    protected void deactivate() {
        authorizableActionProvider.stop();
        authorizableNodeName.stop();
        restrictionProvider.stop();
        userAuthenticationFactory.stop();
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void bindPrincipalConfiguration(@Nonnull PrincipalConfiguration reference) {
        principalConfiguration.addConfiguration(initConfiguration(reference));
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindPrincipalConfiguration(@Nonnull PrincipalConfiguration reference) {
        principalConfiguration.removeConfiguration(reference);
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void bindTokenConfiguration(@Nonnull TokenConfiguration reference) {
        tokenConfiguration.addConfiguration(initConfiguration(reference));
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindTokenConfiguration(@Nonnull TokenConfiguration reference) {
        tokenConfiguration.removeConfiguration(reference);
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void bindAuthorizationConfiguration(@Nonnull AuthorizationConfiguration reference) {
        authorizationConfiguration.addConfiguration(initConfiguration(reference));
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindAuthorizationConfiguration(@Nonnull AuthorizationConfiguration reference) {
        authorizationConfiguration.removeConfiguration(reference);
    }

    //------------------------------------------------------------< private >---
    private void initializeConfigurations() {
        initConfiguration(authorizationConfiguration, ConfigurationParameters.of(
                        AccessControlConstants.PARAM_RESTRICTION_PROVIDER, restrictionProvider)
        );

        Map<String, Object> userMap = ImmutableMap.<String,Object>of(
                UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, authorizableActionProvider,
                UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, authorizableNodeName,
                UserConstants.PARAM_USER_AUTHENTICATION_FACTORY, userAuthenticationFactory);
        initConfiguration(userConfiguration, ConfigurationParameters.of(userMap));

        initConfiguration(authenticationConfiguration);
        initConfiguration(privilegeConfiguration);
    }

    private <T extends SecurityConfiguration> T initConfiguration(@Nonnull T config) {
        return initConfiguration(config, ConfigurationParameters.EMPTY);
    }

    private <T extends SecurityConfiguration> T initConfiguration(@Nonnull T config, @Nonnull ConfigurationParameters params) {
        if (config instanceof ConfigurationBase) {
            ConfigurationBase cfg = (ConfigurationBase) config;
            cfg.setSecurityProvider(this);
            cfg.setParameters(ConfigurationParameters.of(params, cfg.getParameters()));
            cfg.setRootProvider(rootProvider);
            cfg.setTreeProvider(treeProvider);
        }
        return config;
    }

    private CompositeConfiguration initCompositeConfiguration(@Nonnull CompositeConfiguration composite, @Nonnull SecurityConfiguration defaultConfig) {
        composite.setRootProvider(rootProvider);
        composite.setTreeProvider(treeProvider);
        composite.setDefaultConfig(initDefaultConfiguration(defaultConfig));
        return composite;
    }

    private <T extends SecurityConfiguration> T initDefaultConfiguration(@Nonnull T config) {
        if (config instanceof ConfigurationBase) {
            ConfigurationBase cfg = (ConfigurationBase) config;
            cfg.setRootProvider(rootProvider);
            cfg.setTreeProvider(treeProvider);
        }
        return config;
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void bindAuthenticationConfiguration(AuthenticationConfiguration authenticationConfiguration) {
        this.authenticationConfiguration = authenticationConfiguration;
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindAuthenticationConfiguration(AuthenticationConfiguration authenticationConfiguration) {
        this.authenticationConfiguration = null;
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void bindPrivilegeConfiguration(PrivilegeConfiguration privilegeConfiguration) {
        this.privilegeConfiguration = privilegeConfiguration;
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindPrivilegeConfiguration(PrivilegeConfiguration privilegeConfiguration) {
        this.privilegeConfiguration = null;
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void bindUserConfiguration(UserConfiguration userConfiguration) {
        this.userConfiguration = userConfiguration;
    }

    @SuppressWarnings("UnusedDeclaration")
    protected void unbindUserConfiguration(UserConfiguration userConfiguration) {
        this.userConfiguration = null;
    }

}

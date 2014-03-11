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

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authentication.token.TokenConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
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
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAware;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardRestrictionProvider;
import org.osgi.framework.BundleContext;

import com.google.common.collect.ImmutableMap;

import static com.google.common.base.Preconditions.checkNotNull;

@Component
@Service(value = {SecurityProvider.class})
public class SecurityProviderImpl implements SecurityProvider, WhiteboardAware {

    @Reference
    private volatile AuthorizationConfiguration authorizationConfiguration;

    @Reference
    private volatile AuthenticationConfiguration authenticationConfiguration;

    @Reference
    private volatile PrivilegeConfiguration privilegeConfiguration;

    @Reference
    private volatile UserConfiguration userConfiguration;

    @Reference(referenceInterface = PrincipalConfiguration.class,
            name = "principalConfiguration",
            bind = "bindPrincipalConfiguration",
            unbind = "unbindPrincipalConfiguration",
            cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE)
    private final CompositePrincipalConfiguration compositePrincipalConfiguration = new CompositePrincipalConfiguration(this);

    @Reference(referenceInterface = TokenConfiguration.class,
            name = "tokenConfiguration",
            bind = "bindTokenConfiguration",
            unbind = "unbindTokenConfiguration",
            cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE)
    private final CompositeTokenConfiguration compositeTokenConfiguration = new CompositeTokenConfiguration(this);

    @Reference(referenceInterface = AuthorizableNodeName.class,
            name = "authorizableNodeName",
            bind = "bindAuthorizableNodeName",
            cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policyOption = ReferencePolicyOption.GREEDY)
    private final NameGenerator nameGenerator = new NameGenerator();

    private final WhiteboardAuthorizableActionProvider authorizableActionProvider = new WhiteboardAuthorizableActionProvider();
    private final WhiteboardRestrictionProvider restrictionProvider = new WhiteboardRestrictionProvider();

    private ConfigurationParameters configuration;

    private Whiteboard whiteboard;

    /**
     * Default constructor used in OSGi environments.
     */
    public SecurityProviderImpl() {
        this(ConfigurationParameters.EMPTY);
    }

    /**
     * Constructor used for non OSGi environments.
     * @param configuration security configuration
     */
    public SecurityProviderImpl(@Nonnull ConfigurationParameters configuration) {
        checkNotNull(configuration);
        this.configuration = configuration;

        authenticationConfiguration = new AuthenticationConfigurationImpl(this);
        authorizationConfiguration = new AuthorizationConfigurationImpl(this);
        userConfiguration = new UserConfigurationImpl(this);
        compositePrincipalConfiguration.addConfiguration(new PrincipalConfigurationImpl(this));
        privilegeConfiguration = new PrivilegeConfigurationImpl();
        compositeTokenConfiguration.addConfiguration(new TokenConfigurationImpl(this));
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
        scs.add(compositePrincipalConfiguration);
        scs.add(privilegeConfiguration);
        scs.add(compositeTokenConfiguration);
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
            return (T) compositePrincipalConfiguration;
        } else if (PrivilegeConfiguration.class == configClass) {
            return (T) privilegeConfiguration;
        } else if (TokenConfiguration.class == configClass) {
            return (T) compositeTokenConfiguration;
        } else {
            throw new IllegalArgumentException("Unsupported security configuration class " + configClass);
        }
    }

    @Activate
    protected void activate(BundleContext context) throws Exception {
        whiteboard = new OsgiWhiteboard(context);
        authorizableActionProvider.start(whiteboard);
        restrictionProvider.start(whiteboard);

        initializeConfigurations();
    }

    @Deactivate
    protected void deactivate() throws Exception {
        authorizableActionProvider.stop();
        restrictionProvider.stop();
    }

    private void initializeConfigurations() {
        Map<String, WhiteboardRestrictionProvider> authorizMap = ImmutableMap.of(
                AccessControlConstants.PARAM_RESTRICTION_PROVIDER, restrictionProvider
        );
        // also add authorization config specific default parameters for OSGi environments
        // todo: the config class should track the 'restrictionProvider' itself.
        initConfiguration(authorizationConfiguration, ConfigurationParameters.of(authorizMap));

        initConfiguration(authenticationConfiguration, ConfigurationParameters.EMPTY);

        // also initialize user config specific default parameters for OSGi environments
        // todo: the config class should track the 'providers' itself.
        Map<String, Object> userMap = ImmutableMap.of(
                UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, authorizableActionProvider,
                UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, nameGenerator);
        initConfiguration(userConfiguration, ConfigurationParameters.of(userMap));

        initConfiguration(privilegeConfiguration, ConfigurationParameters.EMPTY);
    }

    protected void bindPrincipalConfiguration(@Nonnull PrincipalConfiguration reference) {
        compositePrincipalConfiguration.addConfiguration(initConfiguration(reference, ConfigurationParameters.EMPTY));
    }

    protected void unbindPrincipalConfiguration(@Nonnull PrincipalConfiguration reference) {
        compositePrincipalConfiguration.removeConfiguration(reference);
    }

    protected void bindTokenConfiguration(@Nonnull TokenConfiguration reference) {
        compositeTokenConfiguration.addConfiguration(initConfiguration(reference, ConfigurationParameters.EMPTY));
    }

    protected void unbindTokenConfiguration(@Nonnull TokenConfiguration reference) {
        compositeTokenConfiguration.removeConfiguration(reference);
    }

    protected void bindAuthorizableNodeName(@Nonnull AuthorizableNodeName reference) {
        nameGenerator.dlg = reference;
    }

    private <T extends SecurityConfiguration> T initConfiguration(@Nonnull T config, @Nonnull ConfigurationParameters params) {
        if (config instanceof ConfigurationBase) {
            ConfigurationBase cfg = (ConfigurationBase) config;
            cfg.setSecurityProvider(this);
            cfg.setParameters(ConfigurationParameters.of(params, cfg.getParameters()));
        }
        return config;
    }

    private final class NameGenerator implements AuthorizableNodeName {

        private volatile AuthorizableNodeName dlg = AuthorizableNodeName.DEFAULT;

        @Nonnull
        @Override
        public String generateNodeName(@Nonnull String authorizableId) {
            return dlg.generateNodeName(authorizableId);
        }
    }
}

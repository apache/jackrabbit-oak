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

import java.util.HashMap;
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
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIDPManagerImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
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
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardRestrictionProvider;
import org.osgi.framework.ServiceReference;
import org.osgi.service.component.ComponentContext;

import com.google.common.collect.ImmutableMap;

@Component(immediate = true)
@Service
public class SecurityProviderImpl implements SecurityProvider {

    @Reference(bind = "bindAuthorizationConfiguration",
            cardinality = ReferenceCardinality.MANDATORY_UNARY, // FIXME OAK-1268
            policyOption = ReferencePolicyOption.GREEDY)
    private AuthorizationConfiguration authorizationConfiguration;

    @Reference(bind = "bindAuthenticationConfiguration",
            cardinality = ReferenceCardinality.MANDATORY_UNARY,
            policyOption = ReferencePolicyOption.GREEDY)
    private AuthenticationConfiguration authenticationConfiguration;

    @Reference(bind = "bindPrivilegeConfiguration",
            cardinality = ReferenceCardinality.MANDATORY_UNARY,
            policyOption = ReferencePolicyOption.GREEDY)
    private PrivilegeConfiguration privilegeConfiguration;

    @Reference(bind = "bindUserConfiguration",
            cardinality = ReferenceCardinality.MANDATORY_UNARY,
            policyOption = ReferencePolicyOption.GREEDY)
    private UserConfiguration userConfiguration;

    @Reference(referenceInterface = PrincipalConfiguration.class,
            bind = "bindPrincipalConfiguration",
            unbind = "unbindPrincipalConfiguration",
            cardinality = ReferenceCardinality.MANDATORY_MULTIPLE,
            policyOption = ReferencePolicyOption.GREEDY)
    private PrincipalConfiguration principalConfiguration = new CompositePrincipalConfiguration(this);

    @Reference(referenceInterface = TokenConfiguration.class,
            bind = "bindTokenConfiguration",
            unbind = "unbindTokenConfiguration",
            cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
            policyOption = ReferencePolicyOption.GREEDY)
    private TokenConfiguration tokenConfiguration = new CompositeTokenConfiguration(this);

    @Reference(referenceInterface = AuthorizableNodeName.class,
            bind = "bindAuthorizableNodeName",
            cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policyOption = ReferencePolicyOption.GREEDY)
    private NameGenerator authorizableNodeName = new NameGenerator();

    private final WhiteboardAuthorizableActionProvider authorizableActionProvider = new WhiteboardAuthorizableActionProvider();
    private final WhiteboardRestrictionProvider restrictionProvider = new WhiteboardRestrictionProvider();

    private ConfigurationParameters configuration;

    @Reference
    private ExternalIdentityProviderManager identityProviderManager;

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
    public SecurityProviderImpl(ConfigurationParameters configuration) {
        this.configuration = configuration;

        authenticationConfiguration = new AuthenticationConfigurationImpl(this);
        authorizationConfiguration = new AuthorizationConfigurationImpl(this);
        userConfiguration = new UserConfigurationImpl(this);
        principalConfiguration = new PrincipalConfigurationImpl(this);
        privilegeConfiguration = new PrivilegeConfigurationImpl();
        tokenConfiguration = new TokenConfigurationImpl(this);
        identityProviderManager = new ExternalIDPManagerImpl();
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
        } else if (ExternalIdentityProviderManager.class == configClass) {
            return (T) identityProviderManager;
        } else {
            throw new IllegalArgumentException("Unsupported security configuration class " + configClass);
        }
    }

    @Activate
    protected void activate(ComponentContext context) throws Exception {
        Whiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
        authorizableActionProvider.start(whiteboard);
        restrictionProvider.start(whiteboard);
    }

    @Deactivate
    protected void deactivate() throws Exception {
        authorizableActionProvider.stop();
        restrictionProvider.stop();
    }

    protected void bindAuthorizationConfiguration(@Nonnull ServiceReference reference) {
        // also initialize authorization config specific default parameters or OSGi environments
        Map<String, WhiteboardRestrictionProvider> authorizMap = ImmutableMap.of(
                AccessControlConstants.PARAM_RESTRICTION_PROVIDER, restrictionProvider
        );
        Map<String, Object> newConfig = new HashMap<String, Object>(configuration);
        newConfig.put(AuthorizationConfiguration.NAME, ConfigurationParameters.of(authorizMap));
        configuration = ConfigurationParameters.of(newConfig);

        authorizationConfiguration = (AuthorizationConfiguration) initConfiguration(reference);
    }

    protected void bindAuthenticationConfiguration(@Nonnull ServiceReference reference) {
        authenticationConfiguration = (AuthenticationConfiguration) initConfiguration(reference);
    }

    protected void bindUserConfiguration(@Nonnull ServiceReference reference) {
        // also initialize user config specific default parameters or OSGi environments
        Map<String, Object> userMap = ImmutableMap.of(
                UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, authorizableActionProvider,
                UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, authorizableNodeName);

        Map<String, Object> newConfig = new HashMap<String, Object>(configuration);
        newConfig.put(UserConfiguration.NAME, ConfigurationParameters.of(userMap));
        configuration = ConfigurationParameters.of(newConfig);

        userConfiguration = (UserConfiguration) initConfiguration(reference);
    }

    protected void bindPrivilegeConfiguration(@Nonnull ServiceReference reference) {
        privilegeConfiguration = (PrivilegeConfiguration) initConfiguration(reference);
    }

    protected void bindPrincipalConfiguration(@Nonnull ServiceReference reference) {
        // replace composite configuration if needed
        if (!(principalConfiguration instanceof CompositePrincipalConfiguration)) {
            principalConfiguration = new CompositePrincipalConfiguration(this);
        }
        ((CompositePrincipalConfiguration) principalConfiguration).addConfiguration(
                (PrincipalConfiguration) initConfiguration(reference));
    }

    protected void unbindPrincipalConfiguration(@Nonnull ServiceReference reference) {
        Object pc = reference.getBundle().getBundleContext().getService(reference);
        if (pc instanceof PrincipalConfiguration) {
            if (principalConfiguration instanceof CompositePrincipalConfiguration) {
                ((CompositePrincipalConfiguration) principalConfiguration).removeConfiguration((PrincipalConfiguration) pc);
            }
        }
    }

    protected void bindTokenConfiguration(@Nonnull ServiceReference reference) {
        // replace composite configuration if needed
        if (!(tokenConfiguration instanceof CompositeTokenConfiguration)) {
            tokenConfiguration = new CompositeTokenConfiguration(this);
        }
        ((CompositeTokenConfiguration) tokenConfiguration).addConfiguration(
                (TokenConfiguration) initConfiguration(reference));
    }

    protected void unbindTokenConfiguration(@Nonnull ServiceReference reference) {
        Object tc = reference.getBundle().getBundleContext().getService(reference);
        if (tc instanceof TokenConfiguration) {
            if (tokenConfiguration instanceof CompositeTokenConfiguration) {
                ((CompositeTokenConfiguration) tokenConfiguration).removeConfiguration((TokenConfiguration) tc);
            }
        }
    }

    protected void bindAuthorizableNodeName(@Nonnull ServiceReference reference) {
        Object ann = reference.getBundle().getBundleContext().getService(reference);
        if (ann instanceof AuthorizableNodeName) {
            authorizableNodeName.dlg = (AuthorizableNodeName) ann;
        }
    }

    private Object initConfiguration(@Nonnull ServiceReference reference) {
        Object service = reference.getBundle().getBundleContext().getService(reference);
        if (service instanceof ConfigurationBase) {
            ((ConfigurationBase) service).setSecurityProvider(this);
        }
        return service;
    }

    private final class NameGenerator implements AuthorizableNodeName {

        private AuthorizableNodeName dlg = AuthorizableNodeName.DEFAULT;

        @Nonnull
        @Override
        public String generateNodeName(@Nonnull String authorizableId) {
            return dlg.generateNodeName(authorizableId);
        }
    }

}

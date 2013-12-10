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
package org.apache.jackrabbit.oak.osgi;

import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.CompositeTokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.CompositePrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.osgi.framework.ServiceReference;

/**
 * OsgiSecurityProvider... TODO
 */
public class OsgiSecurityProvider extends AbstractServiceTracker<SecurityConfiguration> implements SecurityProvider {

    @Reference(bind = "bindAuthorizationConfiguration")
    private AuthorizationConfiguration authorizationConfiguration;

    @Reference(bind = "bindAuthenticationConfiguration")
    private AuthenticationConfiguration authenticationConfiguration;

    @Reference(bind = "bindPrivilegeConfiguration")
    private PrivilegeConfiguration privilegeConfiguration;

    @Reference(bind = "bindUserConfiguration")
    private UserConfiguration userConfiguration;

    private CompositePrincipalConfiguration principalConfiguration = new CompositePrincipalConfiguration(this);
    private CompositeTokenConfiguration tokenConfiguration = new CompositeTokenConfiguration(this);

    private ConfigurationParameters config;

    public OsgiSecurityProvider(@Nonnull ConfigurationParameters config) {
        super(SecurityConfiguration.class);
        this.config = config;
    }

    //---------------------------------------------------< SecurityProvider >---
    @Nonnull
    @Override
    public ConfigurationParameters getParameters(@Nullable String name) {
        if (name == null) {
            return config;
        }
        ConfigurationParameters params = config.getConfigValue(name, ConfigurationParameters.EMPTY);
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

    //-------------------------------------------< ServiceTrackerCustomizer >---
    @Override
    public Object addingService(ServiceReference reference) {
        Object service = super.addingService(reference);
        if (service instanceof TokenConfiguration) {
            tokenConfiguration.addConfiguration((TokenConfiguration) service);
        } else if (service instanceof PrincipalConfiguration) {
            principalConfiguration.addConfiguration((PrincipalConfiguration) service);
        }
        return service;
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        super.removedService(reference, service);
        if (service instanceof TokenConfiguration) {
            tokenConfiguration.removeConfiguration((TokenConfiguration) service);
        } else if (service instanceof PrincipalConfiguration) {
            principalConfiguration.removeConfiguration((PrincipalConfiguration) service);
        }
    }

    //--------------------------------------------------------------------------
    protected void bindAuthorizationConfiguration(@Nonnull ServiceReference reference) {
        authorizationConfiguration = (AuthorizationConfiguration) initConfiguration(reference);
    }

    protected void bindAuthenticationConfiguration(@Nonnull ServiceReference reference) {
        authenticationConfiguration = (AuthenticationConfiguration) initConfiguration(reference);
    }

    protected void bindUserConfiguration(@Nonnull ServiceReference reference) {
        userConfiguration = (UserConfiguration) initConfiguration(reference);
    }

    protected void bindPrivilegeConfiguration(@Nonnull ServiceReference reference) {
        privilegeConfiguration = (PrivilegeConfiguration) initConfiguration(reference);
    }

    private Object initConfiguration(@Nonnull ServiceReference reference) {
        Object service = reference.getBundle().getBundleContext().getService(reference);
        if (service instanceof ConfigurationBase) {
            ((ConfigurationBase) service).setSecurityProvider(this);
        }
        return service;
    }
}

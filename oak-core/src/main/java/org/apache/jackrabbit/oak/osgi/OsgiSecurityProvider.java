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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.token.CompositeTokenProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.osgi.framework.ServiceReference;

/**
 * OsgiSecurityProvider... TODO
 */
public class OsgiSecurityProvider extends AbstractServiceTracker<SecurityConfiguration> implements SecurityProvider {

    private CompositeTokenConfiguration tokenConfiguration = new CompositeTokenConfiguration();
    private Map<String, SecurityConfiguration> serviceMap = Maps.newHashMap();
    private ConfigurationParameters config;

    public OsgiSecurityProvider(@Nonnull ConfigurationParameters config) {
        super(SecurityConfiguration.class);
        this.config = config;
        serviceMap.put(TokenConfiguration.NAME, tokenConfiguration);
    }

    //-------------------------------------------< ServiceTrackerCustomizer >---
    @Override
    public Object addingService(ServiceReference reference) {
        Object service = super.addingService(reference);
        if (service instanceof SecurityConfiguration) {
            SecurityConfiguration sc = (SecurityConfiguration) service;

            if (sc instanceof TokenConfiguration) {
                tokenConfiguration.services.add((TokenConfiguration) sc);
            } else {
                synchronized (this) {
                    serviceMap.put(sc.getName(), sc);
                }
            }

            if (service instanceof ConfigurationBase) {
                ((ConfigurationBase) service).setSecurityProvider(this);
            }
        }
        return service;
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        super.removedService(reference, service);
        if (service instanceof SecurityConfiguration) {
            SecurityConfiguration sc = (SecurityConfiguration) service;
            if (sc instanceof TokenConfiguration) {
                tokenConfiguration.services.remove(sc);
            } else {
                synchronized (this) {
                    serviceMap.remove(((SecurityConfiguration) service).getName());
                }
            }
        }
    }


    //---------------------------------------------------< SecurityProvider >---
    @Nonnull
    @Override
    public ConfigurationParameters getParameters(@Nullable String name) {
        if (name == null) {
            return config;
        }
        ConfigurationParameters params = config.getConfigValue(name, ConfigurationParameters.EMPTY);
        SecurityConfiguration sc = serviceMap.get(name);
        if (sc != null) {
            return ConfigurationParameters.of(params, sc.getParameters());
        } else {
            return params;
        }
    }

    @Nonnull
    @Override
    public Iterable<? extends SecurityConfiguration> getConfigurations() {
        return serviceMap.values();
    }

    @Nonnull
    @Override
    public <T> T getConfiguration(@Nonnull Class<T> configClass) {
        for (SecurityConfiguration sc : serviceMap.values()) {
            if (configClass.isAssignableFrom(sc.getClass())) {
                return (T) sc;
            }
        }
        throw new IllegalStateException("Unsupported configuration class " + configClass.getName());
    }

    private final class CompositeTokenConfiguration extends ConfigurationBase implements TokenConfiguration {

        private List<TokenConfiguration> services = new ArrayList<TokenConfiguration>();

        @Nonnull
        @Override
        public TokenProvider getTokenProvider(Root root) {
            List<TokenProvider> providers = new ArrayList<TokenProvider>();
            for (TokenConfiguration tc : services) {
                providers.add(tc.getTokenProvider(root));
            }
            return CompositeTokenProvider.newInstance(providers);
        }

        @Nonnull
        @Override
        public String getName() {
            return TokenConfiguration.NAME;
        }

        @Nonnull
        @Override
        public ConfigurationParameters getParameters() {
            ConfigurationParameters[] params = new ConfigurationParameters[services.size()];
            for (int i = 0; i < services.size(); i++) {
                params[i] = services.get(i).getParameters();
            }
            return ConfigurationParameters.of(params);
        }
    }
}
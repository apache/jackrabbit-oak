/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.apache.jackrabbit.oak.osgi.OsgiUtil;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Configuration for the {@link DocumentNodeStoreService}. Access is provided
 * via {@link Configuration}, while internally the implementation considers
 * entries in the following sequence:
 * <ul>
 *     <li>Framework/system properties, potentially with mapped names. See
 *          {@link #frameworkPropertyNameFor(String)}.</li>
 *     <li>OSGi configuration for {@link DocumentNodeStoreService} with
 *          {@link Configuration#PID} if the property is set via the
 *          OSGi Configuration Admin.</li>
 *     <li>OSGi configuration with {@link Configuration#PRESET_PID}. The
 *          default value for a configuration entry will be provided if the
 *          OSGi Configuration Admin does not have an entry as a preset.</li>
 * </ul>
 */
final class DocumentNodeStoreServiceConfiguration {

    /**
     * Default framework property name prefix.
     */
    private static final String DEFAULT_FWK_PREFIX = "oak.documentstore.";

    /**
     * Name of framework property to configure Mongo Connection URI
     */
    private static final String FWK_PROP_URI = "oak.mongo.uri";

    /**
     * Name of framework property to configure Mongo Database name
     * to use
     */
    private static final String FWK_PROP_DB = "oak.mongo.db";

    /**
     * Name of framework property to configure socket keep-alive for MongoDB
     */
    private static final String FWK_PROP_SO_KEEP_ALIVE = "oak.mongo.socketKeepAlive";

    /**
     * Name of the framework property to configure the update limit.
     */
    private static final String FWK_PROP_UPDATE_LIMIT = "update.limit";

    private static final String PROP_DB = "db";
    private static final String PROP_URI = "mongouri";
    private static final String PROP_HOME = "repository.home";
    static final String PROP_SO_KEEP_ALIVE = "socketKeepAlive";
    static final String PROP_UPDATE_LIMIT = "updateLimit";

    /**
     * Special mapping of property names to framework properties. All other
     * property names are mapped to framework properties by prefixing them with
     * {@link #DEFAULT_FWK_PREFIX}.
     */
    private static final Map<String, String> FWK_PROP_MAPPING = ImmutableMap.of(
            PROP_DB, FWK_PROP_DB,
            PROP_URI, FWK_PROP_URI,
            PROP_HOME, PROP_HOME,
            PROP_SO_KEEP_ALIVE, FWK_PROP_SO_KEEP_ALIVE,
            PROP_UPDATE_LIMIT, FWK_PROP_UPDATE_LIMIT
    );

    private DocumentNodeStoreServiceConfiguration() {
    }

    static Configuration create(ComponentContext context,
                                ConfigurationAdmin configurationAdmin,
                                Configuration preset,
                                Configuration configuration)
            throws IOException {
        return (Configuration) Proxy.newProxyInstance(
                DocumentNodeStoreServiceConfiguration.class.getClassLoader(),
                new Class[]{Configuration.class},
                new ConfigurationHandler(context, configurationAdmin, preset, configuration)
        );
    }

    private static String frameworkPropertyNameFor(String propertyName) {
        String fwkPropName = FWK_PROP_MAPPING.get(propertyName);
        if (fwkPropName == null) {
            fwkPropName = DEFAULT_FWK_PREFIX + propertyName;
        }
        return fwkPropName;
    }

    private static final class ConfigurationHandler implements InvocationHandler {

        private final ComponentContext context;

        /**
         * The preset configuration.
         */
        private final Configuration preset;

        /**
         * The configuration taking precedence over the preset.
         */
        private final Configuration configuration;

        private final Set<String> configurationKeys;

        ConfigurationHandler(ComponentContext context,
                             ConfigurationAdmin configurationAdmin,
                             Configuration preset,
                             Configuration configuration) throws IOException {
            this.context = checkNotNull(context);
            this.preset = checkNotNull(preset);
            this.configuration = checkNotNull(configuration);
            this.configurationKeys = getConfigurationKeys(checkNotNull(configurationAdmin));
        }

        private static Set<String> getConfigurationKeys(ConfigurationAdmin configurationAdmin)
                throws IOException {
            Set<String> keys = new HashSet<>();
            org.osgi.service.cm.Configuration c = configurationAdmin.getConfiguration(Configuration.PID);
            for (Object k : Collections.list(c.getProperties().keys())) {
                keys.add(k.toString());
            }
            return keys;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            String name = method.getName().replaceAll("_", ".");
            Configuration c;
            if (configurationKeys.contains(name)) {
                c = configuration;
            } else {
                c = preset;
            }
            Object value = method.invoke(c);
            // check if this is overridden by a framework property
            String frameworkProp = OsgiUtil.lookup(
                    context.getBundleContext(), frameworkPropertyNameFor(name));
            if (frameworkProp != null) {
                value = tryCoerce(frameworkProp, method.getReturnType(), value);
            }
            return value;
        }

        private Object tryCoerce(String value, Class<?> type, Object defaultValue) {
            Object obj;
            if (type == Boolean.class || type == boolean.class) {
                obj = Boolean.parseBoolean(value);
            } else if (type == Integer.class || type == int.class) {
                try {
                    obj = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    obj = defaultValue;
                }
            } else if (type == Long.class || type == long.class) {
                try {
                    obj = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    obj = defaultValue;
                }
            } else if (type == String.class) {
                obj = value;
            } else {
                obj = defaultValue;
            }
            return obj;
        }
    }
}

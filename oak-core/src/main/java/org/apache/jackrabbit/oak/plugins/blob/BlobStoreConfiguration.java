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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Strings;
import org.osgi.framework.BundleContext;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Defines the configuration needed by a BlobStore.
 */
public class BlobStoreConfiguration {

    public static final String PRIMARY_DATA_STORE = "primary";

    public static final String ARCHIVE_DATA_STORE = "archive";

    public static final String PROP_DATA_STORE = "dataStoreProvider";

    public static final String PROP_BLOB_STORE_PROVIDER = "blobStoreProvider";

    public static final String DEFAULT_BLOB_STORE_PROVIDER = "";

    private Map<String, String> configMap;

    private Set<String> propKeys;

    /**
     * Instantiates a new data store configuration.
     */
    private BlobStoreConfiguration() {
        configMap = Maps.newHashMap();
        propKeys = Sets.newHashSet();

        // get default props
        Properties props = new Properties();
        try {
            props.load(this.getClass().getResourceAsStream("blobstore.properties"));
        } catch (IOException ignore) {
        }

        // populate keys from the default set
        Map<String, String> defaultMap = Maps.fromProperties(props);
        propKeys.addAll(defaultMap.keySet());

        // Remove empty default properties from the map
        getConfigMap().putAll(
                Maps.filterValues(defaultMap, new Predicate<String>() {
                    @Override
                    public boolean apply(@Nullable String input) {
                        if ((input == null) || input.trim().length() == 0) {
                            return false;
                        }
                        return true;
                    }
                }));
    }

    /**
     * Creates a new configuration object with default values.
     * 
     * @return the data store configuration
     */
    public static BlobStoreConfiguration newInstance() {
        return new BlobStoreConfiguration();
    }

    /**
     * Load configuration from the system props.
     * 
     * @return the configuration
     */
    public BlobStoreConfiguration loadFromSystemProps() {
        // remove all jvm set properties to trim the map
        getConfigMap().putAll(Maps.filterKeys(Maps.fromProperties(System.getProperties()), new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                if (input.startsWith("java.") || input.startsWith("sun.") || input.startsWith("user.")
                        || input.startsWith("file.") || input.startsWith("line.") || input.startsWith("os.")
                        || input.startsWith("awt.") || input.startsWith("path.")) {
                    return false;
                } else {
                    return true;
                }
            }
        }));
        return this;
    }

    /**
     * Load configuration from a map.
     * 
     * @param map
     *            the map
     * @return the configuration
     */
    @SuppressWarnings("unchecked")
    public BlobStoreConfiguration loadFromMap(Map<String, ?> cfgMap) {
        getConfigMap().putAll((Map<? extends String, ? extends String>) cfgMap);
        loadFromSystemProps();

        return this;
    }

    /**
     * Load configuration from a BundleContext or the map provided.
     * 
     * @param map
     *            the map
     * @param context
     *            the context
     * @return the configuration
     */
    public BlobStoreConfiguration loadFromContextOrMap(Map<String, ?> map, BundleContext context) {
        loadFromMap(map);

        for (String key : getPropKeys()) {
            if (context.getProperty(key) != null) {
                configMap.put(key, context.getProperty(key));
            }
        }
        return this;
    }

    public String getProperty(String key) {
        return getConfigMap().get(key);
    }

    public void addProperty(String key, String val) {
        getConfigMap().put(key, val);
    }

    public Map<String, String> getConfigMap() {
        return configMap;
    }

    public void setConfigMap(Map<String, String> configMap) {
        this.configMap = configMap;
    }

    public Set<String> getPropKeys() {
        return propKeys;
    }

    public void setPropKeys(Set<String> propKeys) {
        this.propKeys = propKeys;
    }
}

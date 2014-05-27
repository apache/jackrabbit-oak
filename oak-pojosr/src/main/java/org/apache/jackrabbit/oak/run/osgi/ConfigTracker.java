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

package org.apache.jackrabbit.oak.run.osgi;

import java.io.File;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.felix.utils.collections.DictionaryAsMap;
import org.apache.felix.utils.properties.InterpolationHelper;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Installs config obtained from JSON Config file or passed as part of
 * startup
 */
class ConfigTracker extends ServiceTracker {
    private static final String MARKER_NAME = "oak.configinstall.name";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private ConfigurationAdmin cm;
    private final Map config;
    private final BundleContext bundleContext;

    public ConfigTracker(Map config, BundleContext bundleContext) {
        super(bundleContext, ConfigurationAdmin.class.getName(), null);
        this.config = config;
        this.bundleContext = bundleContext;
        open();
    }

    @Override
    public Object addingService(ServiceReference reference) {
        cm = (ConfigurationAdmin) super.addingService(reference);
        try {
            synchronizeConfigs();
        } catch (Exception e) {
            log.error("Error occurred while installing configs", e);
            throw new RuntimeException(e);
        }
        return cm;
    }

    /**
     * Synchronizes the configs. All config added by this class is also kept in sync with re runs
     * i.e. if a config was added in first run and say later removed then that would also be removed
     * from the ConfigurationAdmin
     */
    private void synchronizeConfigs() throws Exception {
        Set<String> existingPids = determineExistingConfigs();
        Set<String> processedPids = Sets.newHashSet();

        Map<String, Map<String, Object>> configs = Maps.newHashMap();

        Map<String, Map<String, Object>> configFromFile =
                parseJSONConfig((String) config.get(OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE));
        configs.putAll(configFromFile);

        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> runtimeConfig =
                (Map<String, Map<String, Object>>) config.get(OakOSGiRepositoryFactory.REPOSITORY_CONFIG);
        if (runtimeConfig != null) {
            configs.putAll(runtimeConfig);
        }

        installConfigs(configs, processedPids);
        Set<String> pidsToBeRemoved = Sets.difference(existingPids, processedPids);
        removeConfigs(pidsToBeRemoved);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Object>> parseJSONConfig(String jsonFilePath) throws IOException {
        Map<String, Map<String, Object>> configs = Maps.newHashMap();
        if (jsonFilePath == null) {
            return configs;
        }

        List<String> files = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(jsonFilePath);
        for (String filePath : files) {
            File jsonFile = new File(filePath);
            if (!jsonFile.exists()) {
                log.warn("No file found at path {}. Ignoring the file entry", jsonFile.getAbsolutePath());
                continue;
            }

            String content = Files.toString(jsonFile, Charsets.UTF_8);
            JSONObject json = (JSONObject) JSONValue.parse(content);
            configs.putAll(json);
        }

        return configs;
    }

    private void removeConfigs(Set<String> pidsToBeRemoved) throws Exception {
        for (String pidString : pidsToBeRemoved) {
            String[] pid = parsePid(pidString);
            Configuration config = getConfiguration(pidString, pid[0], pid[1]);
            config.delete();
        }

        if (!pidsToBeRemoved.isEmpty()) {
            log.info("Configuration belonging to following pids have been removed ", pidsToBeRemoved);
        }
    }

    private void installConfigs(Map<String, Map<String, Object>> osgiConfig, Set<String> processedPids)
            throws Exception {
        for (Map.Entry<String, Map<String, Object>> pidEntry : osgiConfig.entrySet()) {
            final String pidString = pidEntry.getKey();

            final Hashtable<String, Object> current = new Hashtable<String, Object>();
            current.putAll(pidEntry.getValue());
            performSubstitution(current);

            String[] pid = parsePid(pidString);
            Configuration config = getConfiguration(pidString, pid[0], pid[1]);

            @SuppressWarnings("unchecked") Dictionary<String, Object> props = config.getProperties();
            Hashtable<String, Object> old = props != null ?
                    new Hashtable<String, Object>(new DictionaryAsMap<String, Object>(props)) : null;
            if (old != null) {
                old.remove(MARKER_NAME);
                old.remove(Constants.SERVICE_PID);
                old.remove(ConfigurationAdmin.SERVICE_FACTORYPID);
            }

            if (!current.equals(old)) {
                current.put(MARKER_NAME, pidString);
                if (config.getBundleLocation() != null) {
                    config.setBundleLocation(null);
                }
                if (old == null) {
                    log.info("Creating configuration from {}", pidString);
                } else {
                    log.info("Updating configuration from {}", pidString);
                }
                config.update(current);
                processedPids.add(pidString);
            }
        }
    }

    private void performSubstitution(Hashtable<String, Object> current) {
        Map<String, String> simpleConfig = Maps.newHashMap();

        for (Map.Entry<String, Object> e : current.entrySet()) {
            if (e.getValue() instanceof String) {
                simpleConfig.put(e.getKey(), (String) e.getValue());
            }
        }

        InterpolationHelper.performSubstitution(simpleConfig, bundleContext);

        for (Map.Entry<String, String> e : simpleConfig.entrySet()) {
            current.put(e.getKey(), e.getValue());
        }
    }

    /**
     * Determines the existing configs which are installed by ConfigInstaller
     *
     * @return set of pid strings
     */
    private Set<String> determineExistingConfigs() throws IOException, InvalidSyntaxException {
        Set<String> pids = Sets.newHashSet();
        String filter = "(" + MARKER_NAME + "=" + "*" + ")";
        Configuration[] configurations = cm.listConfigurations(filter);
        if (configurations != null) {
            for (Configuration cfg : configurations) {
                pids.add((String) cfg.getProperties().get(MARKER_NAME));
            }
        }
        return pids;
    }

    private Configuration getConfiguration(String pidString, String pid, String factoryPid)
            throws Exception {
        Configuration oldConfiguration = findExistingConfiguration(pidString);
        if (oldConfiguration != null) {
            return oldConfiguration;
        } else {
            Configuration newConfiguration;
            if (factoryPid != null) {
                newConfiguration = cm.createFactoryConfiguration(pid, null);
            } else {
                newConfiguration = cm.getConfiguration(pid, null);
            }
            return newConfiguration;
        }
    }

    private Configuration findExistingConfiguration(String pidString) throws Exception {
        String filter = "(" + MARKER_NAME + "=" + escapeFilterValue(pidString) + ")";
        Configuration[] configurations = cm.listConfigurations(filter);
        if (configurations != null && configurations.length > 0) {
            return configurations[0];
        } else {
            return null;
        }
    }

    private static String escapeFilterValue(String s) {
        return s.replaceAll("[(]", "\\\\(").
                replaceAll("[)]", "\\\\)").
                replaceAll("[=]", "\\\\=").
                replaceAll("[\\*]", "\\\\*");
    }

    private static String[] parsePid(String pid) {
        int n = pid.indexOf('-');
        if (n > 0) {
            String factoryPid = pid.substring(n + 1);
            pid = pid.substring(0, n);
            return new String[]
                    {
                            pid, factoryPid
                    };
        } else {
            return new String[]
                    {
                            pid, null
                    };
        }
    }
}

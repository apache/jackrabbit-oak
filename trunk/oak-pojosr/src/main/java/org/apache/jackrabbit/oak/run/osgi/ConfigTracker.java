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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Installs config obtained from JSON Config file or passed as part of
 * startup
 */
class ConfigTracker extends ServiceTracker<ConfigurationAdmin, ConfigurationAdmin> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Map config;
    private final BundleContext bundleContext;

    public ConfigTracker(Map config, BundleContext bundleContext) {
        super(bundleContext, ConfigurationAdmin.class.getName(), null);
        this.config = config;
        this.bundleContext = bundleContext;
        open();
    }

    @Override
    public ConfigurationAdmin addingService(ServiceReference<ConfigurationAdmin> reference) {
        ConfigurationAdmin cm = super.addingService(reference);
        try {
            synchronizeConfigs(new ConfigInstaller(cm, bundleContext));
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
    private void synchronizeConfigs(ConfigInstaller configInstaller) throws Exception {
        Set<String> existingPids = configInstaller.determineExistingConfigs();

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

        configInstaller.installConfigs(configs);
        //Find out the config *installed by ConfigInstaller* and are not present in
        //current config files. Such configs must be remove. Note it does not lead to
        //removal of configs added by using ConfigAdmin directly, say using WebConsole
        //ui
        Set<String> pidsToBeRemoved = Sets.difference(existingPids, configs.keySet());
        configInstaller.removeConfigs(pidsToBeRemoved);
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
}

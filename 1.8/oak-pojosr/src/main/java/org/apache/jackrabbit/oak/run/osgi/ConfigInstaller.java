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

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.felix.utils.properties.InterpolationHelper;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConfigInstaller {
    private static final String MARKER_NAME = "oak.configinstall.name";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ConfigurationAdmin cm;
    private final BundleContext bundleContext;

    public ConfigInstaller(ConfigurationAdmin cm, BundleContext bundleContext) {
        this.cm = cm;
        this.bundleContext = bundleContext;
    }

    public void installConfigs(Map<String, Map<String, Object>> osgiConfig)
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
            }
        }
    }

    public void removeConfigs(Set<String> pidsToBeRemoved) throws Exception {
        for (String pidString : pidsToBeRemoved) {
            String[] pid = parsePid(pidString);
            Configuration config = getConfiguration(pidString, pid[0], pid[1]);
            config.delete();
        }

        if (!pidsToBeRemoved.isEmpty()) {
            log.info("Configuration belonging to following pids have been removed {}", pidsToBeRemoved);
        }
    }

    /**
     * Determines the existing configs which are installed by ConfigInstaller
     *
     * @return set of pid strings
     */
    public Set<String> determineExistingConfigs() throws IOException, InvalidSyntaxException {
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

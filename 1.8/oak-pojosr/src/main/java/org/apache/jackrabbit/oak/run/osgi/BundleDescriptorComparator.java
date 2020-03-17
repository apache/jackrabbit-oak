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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.felix.connect.launch.BundleDescriptor;
import org.osgi.framework.Constants;

/**
 * Comparator to simulate start level support of OSgi framework by ordering the startup
 * sequence. This is required to ensure that required configured is provisioned with
 * ConfigAdmin before the SCR bundle starts
 */
class BundleDescriptorComparator implements Comparator<BundleDescriptor> {
    public static final Integer DEFAULT_START_LEVEL = 20;

    private final Map<String, Integer> startLevels;

    public BundleDescriptorComparator() {
        startLevels = defaultStartLevels();
    }

    public BundleDescriptorComparator(Map<String, Integer> startLevels) {
        this.startLevels = startLevels;
    }

    private Map<String, Integer> defaultStartLevels() {
        Map<String, Integer> defaultLevels = new HashMap<String, Integer>();

        defaultLevels.put("org.apache.sling.commons.logservice", 1);

        defaultLevels.put("org.apache.felix.configadmin", 2);
        defaultLevels.put("org.apache.felix.fileinstall", 2);

        defaultLevels.put("org.apache.felix.scr", 10);
        return Collections.unmodifiableMap(defaultLevels);
    }

    @Override
    public int compare(BundleDescriptor o1, BundleDescriptor o2) {
        return getStartLevel(o1).compareTo(getStartLevel(o2));
    }

    private Integer getStartLevel(BundleDescriptor bd) {
        String symbolicName = ((Map<String, String>) bd.getHeaders()).get(Constants.BUNDLE_SYMBOLICNAME);
        Integer level = startLevels.get(symbolicName);
        if (level == null) {
            level = DEFAULT_START_LEVEL;
        }
        return level;
    }
}

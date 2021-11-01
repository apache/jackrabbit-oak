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
package org.apache.jackrabbit.oak.composite;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyUnbounded;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.osgi.framework.BundleContext;

import static java.util.stream.Collectors.toList;

@Component(label = "Apache Jackrabbit Oak MountInfo Config", configurationFactory = true, metatype = true)
@Service(MountInfoConfig.class)
public class MountInfoConfig {

    private static final String[] PROP_MOUNTED_PATHS_DEFAULT = {};

    @Property(label = "Mounted paths",
        unbounded = PropertyUnbounded.ARRAY,
        description = "Paths which are part of private mount"
    )
    private static final String PROP_MOUNT_PATHS = "mountedPaths";
    private List<String> paths;

    static final String PROP_MOUNT_NAME_DEFAULT = "private";

    @Property(label = "Mount name",
        description = "Name of the mount",
        value = PROP_MOUNT_NAME_DEFAULT
    )
    private static final String PROP_MOUNT_NAME = "mountName";
    private String mountName;

    private static final String[] PROP_PATHS_SUPPORTING_FRAGMENTS_DEFAULT = {"/"};

    @Property(label = "Paths supporting fragments",
        unbounded = PropertyUnbounded.ARRAY,
        description = "oak:mount-* under this paths will be included to mounts",
        value = {"/"}
    )
    private static final String PROP_PATHS_SUPPORTING_FRAGMENTS = "pathsSupportingFragments";
    private List<String> pathsSupportingFragments;

    private static final boolean PROP_MOUNT_READONLY_DEFAULT = true;

    @Property(label = "Readonly",
        description = "If enabled then mount would be considered as readonly",
        boolValue = PROP_MOUNT_READONLY_DEFAULT
    )
    private static final String PROP_MOUNT_READONLY = "readOnlyMount";

    private boolean readOnly;

    public MountInfoConfig() {
    }

    MountInfoConfig(String mountName, List<String> paths, List<String> pathsSupportingFragments, boolean readOnly) {
        this.mountName = mountName;
        this.paths = trimAll(paths);
        this.pathsSupportingFragments = trimAll(pathsSupportingFragments);
        this.readOnly = readOnly;
    }

    @Activate
    private void activate(BundleContext bundleContext, Map<String, ?> config) {
        paths = trimAll(Arrays.asList(PropertiesUtil.toStringArray(config.get(PROP_MOUNT_PATHS), PROP_MOUNTED_PATHS_DEFAULT)));
        mountName = PropertiesUtil.toString(config.get(PROP_MOUNT_NAME), PROP_MOUNT_NAME_DEFAULT).trim();
        pathsSupportingFragments = trimAll(Arrays.asList(PropertiesUtil.toStringArray(config.get(PROP_PATHS_SUPPORTING_FRAGMENTS), PROP_PATHS_SUPPORTING_FRAGMENTS_DEFAULT)));
        readOnly = PropertiesUtil.toBoolean(config.get(PROP_MOUNT_READONLY), PROP_MOUNT_READONLY_DEFAULT);
    }

    public List<String> getPaths() {
        return paths;
    }

    public String getMountName() {
        return mountName;
    }

    public List<String> getPathsSupportingFragments() {
        return pathsSupportingFragments;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    private static List<String> trimAll(List<String> strings) {
        return strings.stream()
            .map(String::trim)
            .collect(toList());
    }
}

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyUnbounded;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(metatype = false, label = "Apache Jackrabbit Oak MountInfoProvider")
public class MountInfoProviderService {

    @Property(label = "Mounted paths",
            unbounded = PropertyUnbounded.ARRAY,
            description = "Paths which are part of private mount"
    )
    private static final String PROP_MOUNT_PATHS = "mountedPaths";

    static final String PROP_MOUNT_NAME_DEFAULT = "private";

    @Property(label = "Mount name",
            description = "Name of the mount",
            value = PROP_MOUNT_NAME_DEFAULT
    )
    private static final String PROP_MOUNT_NAME = "mountName";

    private static final boolean PROP_MOUNT_READONLY_DEFAULT = false;

    @Property(label = "Readonly",
            description = "If enabled then mount would be considered as readonly",
            boolValue = PROP_MOUNT_READONLY_DEFAULT
    )
    private static final String PROP_MOUNT_READONLY = "readOnlyMount";

    private static final String[] PROP_PATHS_SUPPORTING_FRAGMENTS_DEFAULT = new String[] {"/"};

    @Property(label = "Paths supporting fragments",
            unbounded = PropertyUnbounded.ARRAY,
            description = "oak:mount-* under this paths will be included to mounts",
            value = {"/"}
    )
    private static final String PROP_PATHS_SUPPORTING_FRAGMENTS = "pathsSupportingFragments";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private ServiceRegistration reg;

    @Activate
    private void activate(BundleContext bundleContext, Map<String, ?> config) {
        String[] paths = PropertiesUtil.toStringArray(config.get(PROP_MOUNT_PATHS));
        String mountName = PropertiesUtil.toString(config.get(PROP_MOUNT_NAME), PROP_MOUNT_NAME_DEFAULT);
        boolean readOnly = PropertiesUtil.toBoolean(config.get(PROP_MOUNT_READONLY), PROP_MOUNT_READONLY_DEFAULT);
        String[] pathsSupportingFragments = PropertiesUtil.toStringArray(config.get(PROP_PATHS_SUPPORTING_FRAGMENTS), PROP_PATHS_SUPPORTING_FRAGMENTS_DEFAULT);

        MountInfoProvider mip = Mounts.defaultMountInfoProvider();
        if (paths != null) {
            mip = Mounts.newBuilder()
                    .mount(mountName.trim(), readOnly, trim(pathsSupportingFragments), trim(paths))
                    .build();
            log.info("Enabling mount for {}", mip);
        } else {
            log.info("No mount config provided. Mounting would be disabled");
        }

        reg = bundleContext.registerService(MountInfoProvider.class.getName(), mip, null);
    }

    private static List<String> trim(String[] array) {
        List<String> result = new ArrayList<>(array.length);
        for (String s : array) {
            result.add(s.trim());
        }
        return result;
    }

    @Deactivate
    private void deactivate() {
        if (reg != null) {
            reg.unregister();
            reg = null;
        }
    }
}

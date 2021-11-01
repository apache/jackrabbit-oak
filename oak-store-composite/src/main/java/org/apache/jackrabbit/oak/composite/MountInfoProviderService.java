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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyUnbounded;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Component(label = "Apache Jackrabbit Oak MountInfoProviderService")
public class MountInfoProviderService {
    private final static Logger LOG = LoggerFactory.getLogger(MountInfoProviderService.class);

    @Reference(
        cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
        policy = ReferencePolicy.STATIC,
        bind = "bindMountInfoConfig",
        unbind = "unbindMountInfoConfig",
        referenceInterface = MountInfoConfig.class)
    private List<MountInfoConfig> mountInfoConfigs = new CopyOnWriteArrayList<>();

    private static final String[] PROP_EXPECTED_MOUNTS_DEFAULT = {};
    
    @Property(
        label = "Expected mounts",
        description = "List of all expected read-only mount names",
        unbounded = PropertyUnbounded.ARRAY
    )
    private static final String PROP_EXPECTED_MOUNTS = "expectedMounts";
    private List<String> expectedMounts;

    @Property(label = "Mounted paths",
        unbounded = PropertyUnbounded.ARRAY,
        description = "Paths which are part of private mount"
    )
    @Deprecated
    private static final String PROP_MOUNT_PATHS = "mountedPaths";

    @Deprecated
    private static final String PROP_MOUNT_NAME_DEFAULT = "private";

    @Property(label = "Mount name",
        description = "Name of the mount",
        value = PROP_MOUNT_NAME_DEFAULT
    )
    @Deprecated
    private static final String PROP_MOUNT_NAME = "mountName";

    @Deprecated
    private static final boolean PROP_MOUNT_READONLY_DEFAULT = false;

    @Property(label = "Readonly",
        description = "If enabled then mount would be considered as readonly",
        boolValue = PROP_MOUNT_READONLY_DEFAULT
    )
    @Deprecated
    private static final String PROP_MOUNT_READONLY = "readOnlyMount";

    @Deprecated
    private static final String[] PROP_PATHS_SUPPORTING_FRAGMENTS_DEFAULT = new String[] {"/"};

    @Property(label = "Paths supporting fragments",
        unbounded = PropertyUnbounded.ARRAY,
        description = "oak:mount-* under this paths will be included to mounts",
        value = {"/"}
    )
    @Deprecated
    private static final String PROP_PATHS_SUPPORTING_FRAGMENTS = "pathsSupportingFragments";


    private ServiceRegistration reg;
    private BundleContext context;

    @Activate
    private void activate(BundleContext bundleContext, Map<String, ?> config) {
        expectedMounts = Stream.of(PropertiesUtil.toStringArray(config.get(PROP_EXPECTED_MOUNTS), PROP_EXPECTED_MOUNTS_DEFAULT))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(toList());
        context = bundleContext;
        addMountInfoConfigFromProperties(config);
        registerMountInfoProvider();
    }

    /**
     * @deprecated only used for backward-compatibility. Now mount configurations should be provided with the {@code MountInfoConfig} class.
     */
    @Deprecated
    private void addMountInfoConfigFromProperties(Map<String, ?> config) {
        String[] paths = PropertiesUtil.toStringArray(config.get(PROP_MOUNT_PATHS));
        String mountName = PropertiesUtil.toString(config.get(PROP_MOUNT_NAME), PROP_MOUNT_NAME_DEFAULT);
        boolean readOnly = PropertiesUtil.toBoolean(config.get(PROP_MOUNT_READONLY), PROP_MOUNT_READONLY_DEFAULT);
        String[] pathsSupportingFragments = PropertiesUtil.toStringArray(config.get(PROP_PATHS_SUPPORTING_FRAGMENTS), PROP_PATHS_SUPPORTING_FRAGMENTS_DEFAULT);
        if (paths != null) {
            mountInfoConfigs.add(new MountInfoConfig(mountName, Arrays.asList(paths), Arrays.asList(pathsSupportingFragments), readOnly));
            expectedMounts.add(mountName);
        }
    }

    @SuppressWarnings("unused")
    protected void bindMountInfoConfig(MountInfoConfig config) {
        if (!config.getPaths().isEmpty()) { // Ignore empty configs
            mountInfoConfigs.add(config);
        }
        registerMountInfoProvider();
    }

    private void registerMountInfoProvider() {
        if (context == null || reg != null) {
            return;
        }
        if (!mountInfoConfigs.stream().allMatch(mountInfo -> expectedMounts.contains(mountInfo.getMountName()))) {
            LOG.info("Not all expected mounts are present yet (expected: {}, current: {}). Postponing configuration...",
                expectedMounts, mountInfoConfigs.stream().map(MountInfoConfig::getMountName).collect(joining(",", "[", "]")));
            return;
        }

        MountInfoProvider mip;

        boolean hasPaths = mountInfoConfigs.stream().anyMatch(conf -> !conf.getPaths().isEmpty());
        if (hasPaths) {
            Mounts.Builder builder = Mounts.newBuilder();

            for (MountInfoConfig mountInfoConfig : mountInfoConfigs) {
                if (!mountInfoConfig.getPaths().isEmpty()) {
                    builder.mount(
                        mountInfoConfig.getMountName(),
                        mountInfoConfig.isReadOnly(), // read-only
                        mountInfoConfig.getPathsSupportingFragments(),
                        mountInfoConfig.getPaths());

                    LOG.info("Enabling mount for {}", mountInfoConfig.getMountName());
                }
            }
            mip = builder.build();
        } else {
            mip = Mounts.defaultMountInfoProvider();
            LOG.info("No mount config provided. Mounting would be disabled");
        }

        reg = context.registerService(MountInfoProvider.class.getName(), mip, null);
    }

    @Deactivate
    private void deactivate() {
        if (reg != null) {
            reg.unregister();
            reg = null;
        }
    }

    @SuppressWarnings("unused")
    protected void unbindMountInfoConfig(MountInfoConfig config) {
        LOG.warn("Dynamic removal of MountInfoConfig is unsupported");
    }
}

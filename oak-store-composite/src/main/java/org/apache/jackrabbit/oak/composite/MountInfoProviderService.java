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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;

@Component
@Designate(ocd = MountInfoProviderService.Props.class)
public class MountInfoProviderService {
    private final static Logger LOG = LoggerFactory.getLogger(MountInfoProviderService.class);

    @ObjectClassDefinition(name = "MountInfoProviderService Properties")
    @interface Props {

        String DEFAULT_MOUNT_NAME = "private";

        @AttributeDefinition(
            name = "Expected mounts",
            description = "List of all required mount names"
        )
        String[] expectedMounts() default {};

        @AttributeDefinition(
            name = "Mounted paths",
            description = "Paths which are part of private mount"
        )
        @Deprecated
        String[] mountedPaths() default {};

        @AttributeDefinition(
            name = "Mount name",
            description = "Name of the mount"
        )
        @Deprecated
        String mountName() default DEFAULT_MOUNT_NAME;

        @AttributeDefinition(
            name = "Readonly",
            description = "If enabled then mount would be considered as readonly"
        )
        @Deprecated
        boolean readOnlyMount() default true;

        @AttributeDefinition(
            name = "Paths supporting fragments",
            description = "oak:mount-* under this paths will be included to mounts"
        )
        @Deprecated
        String[] pathsSupportingFragments() default {"/"};

    }

    private final List<MountInfoConfig> mountInfoConfigs = new ArrayList<>();

    private Set<String> expectedMounts;
    private ServiceRegistration reg;
    private BundleContext context;

    @Activate
    public void activate(BundleContext bundleContext, Props props) {
        String[] expectedMounts = props.expectedMounts();
        if (expectedMounts != null) {
            this.expectedMounts = Stream.of(expectedMounts)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(toSet());
        } else {
            this.expectedMounts = new HashSet<>();
        }
        context = bundleContext;
        addMountInfoConfigFromProperties(props);
        registerMountInfoProvider();
    }

    /**
     * @deprecated only used for backward-compatibility. Now mount configurations should be provided with the {@code MountInfoConfig} class.
     */
    @Deprecated
    private void addMountInfoConfigFromProperties(Props mountInfoProps) {
        String[] paths = mountInfoProps.mountedPaths();
        String mountName = mountInfoProps.mountName();
        boolean readOnly = mountInfoProps.readOnlyMount();
        String[] pathsSupportingFragments = mountInfoProps.pathsSupportingFragments();
        if (paths != null && paths.length > 0) {
            mountInfoConfigs.add(new MountInfoConfig(mountName, Arrays.asList(paths), Arrays.asList(pathsSupportingFragments), readOnly));
            expectedMounts.add(mountName);
        }
    }

    @Reference(cardinality = ReferenceCardinality.MULTIPLE, policyOption = ReferencePolicyOption.GREEDY)
    protected void bindMountInfoConfig(MountInfoConfig config) {
        if (!config.getPaths().isEmpty()) { // Ignore empty configs
            mountInfoConfigs.add(config);
        }
        // this is a static reference, i.e. called before activate(), the registration happens during activate()
    }

    private void registerMountInfoProvider() {
        if (context == null || reg != null) {
            return;
        }
        Set<String> providedMounts = mountInfoConfigs.stream().map(MountInfoConfig::getMountName).collect(toSet());
        if (!providedMounts.containsAll(expectedMounts)) {
            LOG.info("Not all expected mounts are present yet (expected: {}, current: {}). Postponing configuration...",
                expectedMounts, providedMounts);
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

                    LOG.info("Enabling mount for {} with paths: {}", mountInfoConfig.getMountName(), mountInfoConfig.getPaths());
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
}

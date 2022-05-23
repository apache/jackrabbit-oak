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
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import static java.util.stream.Collectors.toList;

@Component(service = MountInfoConfig.class)
@Designate(ocd = MountInfoConfig.Props.class, factory = true)
public class MountInfoConfig {

    @ObjectClassDefinition(name = "MountInfoConfig Properties")
    public @interface Props {

        String DEFAULT_MOUNT_NAME = "private";

        @AttributeDefinition(
            name = "Mounted paths",
            description = "Paths which are part of private mount"
        )
        String[] mountedPaths() default {};

        @AttributeDefinition(
            name = "Mount name",
            description = "Name of the mount"
        )
        String mountName() default DEFAULT_MOUNT_NAME;

        @AttributeDefinition(
            name = "Readonly",
            description = "If enabled then mount would be considered as readonly"
        )
        boolean readOnlyMount() default true;

        @AttributeDefinition(
            name = "Paths supporting fragments",
            description = "oak:mount-* under this paths will be included to mounts"
        )
        String[] pathsSupportingFragments() default {"/"};

    }
    
    private List<String> paths;
    private String mountName;
    private List<String> pathsSupportingFragments;
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
    void activate(BundleContext bundleContext, Props props) {
        paths = trimAll(Arrays.asList(props.mountedPaths()));
        mountName = props.mountName().trim();
        pathsSupportingFragments = trimAll(Arrays.asList(props.pathsSupportingFragments()));
        readOnly = props.readOnlyMount();
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

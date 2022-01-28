/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.composite;

import java.lang.annotation.Annotation;

public class MountInfoPropsBuilder {
    private String[] expectedMounts;
    private String[] mountPaths;
    private String mountName;
    private boolean readonly = true;
    private String[] pathsSupportingFragments;

    public MountInfoPropsBuilder withExpectedMounts(String... expectedMounts) {
        this.expectedMounts = expectedMounts;
        return this;
    }

    public MountInfoPropsBuilder withMountPaths(String... mountPaths) {
        this.mountPaths = mountPaths;
        return this;
    }

    public MountInfoPropsBuilder withMountName(String mountName) {
        this.mountName = mountName;
        return this;
    }

    public MountInfoPropsBuilder withReadonly(boolean readonly) {
        this.readonly = readonly;
        return this;
    }

    public MountInfoPropsBuilder withPathsSupportingFragments(String... pathsSupportingFragments) {
        this.pathsSupportingFragments = pathsSupportingFragments;
        return this;
    }

    public MountInfoConfig.Props buildMountInfoProps() {
        return new MountInfoConfig.Props() {
            @Override
            public String[] mountedPaths() {
                return mountPaths == null ? new String[0] : mountPaths;
            }

            @Override
            public String mountName() {
                return mountName == null ? MountInfoConfig.Props.DEFAULT_MOUNT_NAME : mountName;
            }

            @Override
            public boolean readOnlyMount() {
                return readonly;
            }

            @Override
            public String[] pathsSupportingFragments() {
                return pathsSupportingFragments == null ? new String[0] : pathsSupportingFragments;
            }

            @Override
            public Class<? extends Annotation> annotationType() {
                return MountInfoConfig.Props.class;
            }
        };
    }

    public MountInfoProviderService.Props buildProviderServiceProps() {
        return new MountInfoProviderService.Props() {
            @Override
            public String[] expectedMounts() {
                return expectedMounts == null ? new String[0] : expectedMounts;
            }

            @Override
            public String[] mountedPaths() {
                return mountPaths == null ? new String[0] : mountPaths;
            }

            @Override
            public String mountName() {
                return mountName == null ? MountInfoProviderService.Props.DEFAULT_MOUNT_NAME : mountName;
            }

            @Override
            public boolean readOnlyMount() {
                return readonly;
            }

            @Override
            public String[] pathsSupportingFragments() {
                return pathsSupportingFragments == null ? new String[0] : pathsSupportingFragments;
            }

            @Override
            public Class<? extends Annotation> annotationType() {
                return MountInfoProviderService.Props.class;
            }
        };
    }
}

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

package org.apache.jackrabbit.oak.spi.mount;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class Mounts {

    private Mounts() {
    }

    private static final MountInfoProvider DEFAULT_PROVIDER = new MountInfoProvider() {
        @Override
        public Mount getMountByPath(String path) {
            return DEFAULT_MOUNT;
        }

        @Override
        public Collection<Mount> getNonDefaultMounts() {
            return Collections.emptySet();
        }

        @Override
        public Mount getMountByName(String name) {
            return DEFAULT_MOUNT.getName().equals(name) ? DEFAULT_MOUNT : null;
        }

        @Override
        public boolean hasNonDefaultMounts() {
            return false;
        }

        @Override
        public Collection<Mount> getMountsPlacedUnder(String path) {
            return Collections.emptySet();
        }

        public Collection<Mount> getMountsPlacedDirectlyUnder(String path) {
            return Collections.emptySet();
        }

        @Override
        public Mount getDefaultMount() {
            return DEFAULT_MOUNT;
        }
    };

    /**
     * Default Mount info which indicates that no explicit mount is created for
     * given path
     */
    private static Mount DEFAULT_MOUNT = new DefaultMount();

    static final class DefaultMount implements Mount {

        private final Collection<Mount> mounts;

        DefaultMount() {
            this(Collections.<Mount> emptySet());
        }

        DefaultMount(Collection<Mount> mounts) {
            this.mounts = mounts;
        }

        @Override
        public String getName() {
            return "";
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public boolean isDefault() {
            return true;
        }

        @Override
        public String getPathFragmentName() {
            return "";
        }

        @Override
        public boolean isSupportFragment(String path) {
            return false;
        }

        @Override
        public boolean isMounted(String path) {
            for (Mount m : mounts) {
                if (m.isMounted(path)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean isUnder(String path) {
            for (Mount m : mounts) {
                if (m.isMounted(path)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean isDirectlyUnder(String path) {
            for (Mount m : mounts) {
                if (m.isDirectlyUnder(path)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static MountInfoProvider defaultMountInfoProvider() {
        return DEFAULT_PROVIDER;
    }

    public static Mount defaultMount() {
        return DEFAULT_MOUNT;
    }

    public static Mount defaultMount(Collection<Mount> mounts) {
        return new DefaultMount(mounts);
    }

    public static Builder newBuilder(){
        return new Builder();
    }

    public static final class Builder {
        private final List<Mount> mounts = Lists.newArrayListWithCapacity(1);

        private Builder() {
        }

        public Builder mount(String name, String... paths) {
            mounts.add(new MountInfo(name, false, singletonList("/"), asList(paths)));
            return this;
        }

        public Builder readOnlyMount(String name, String... paths) {
            mounts.add(new MountInfo(name, true, singletonList("/"), asList(paths)));
            return this;
        }

        public Builder mount(String name, boolean readOnly, List<String> pathsSupportingFragments, List<String> paths) {
            mounts.add(new MountInfo(name, readOnly, pathsSupportingFragments, paths));
            return this;
        }

        public MountInfoProvider build() {
            return new SimpleMountInfoProvider(mounts);
        }
    }
}

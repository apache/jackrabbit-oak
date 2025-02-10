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

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A simple and inefficient implementation to manage mount points
 */
final class SimpleMountInfoProvider implements MountInfoProvider {

    private final Map<String, Mount> nameToMount;
    private final Mount defMount;
    private final boolean hasMounts;
    private final List<Mount> mounts;

    SimpleMountInfoProvider(List<Mount> mountInfos) {
        this.mounts = List.copyOf(mountInfos);
        this.nameToMount = mountInfos.stream().collect(Collectors.toMap(Mount::getName, mi -> mi));
        this.hasMounts = !this.mounts.isEmpty();
        this.defMount = new Mounts.DefaultMount(mounts);
        //TODO add validation of mountpoints
    }

    @Override
    public @NotNull Mount getMountByPath(String path) {
        for (Mount m : mounts) {
            if (m.isMounted(path)) {
                return m;
            }
        }
        return defMount;
    }

    @Override
    public @NotNull Collection<Mount> getNonDefaultMounts() {
        return mounts;
    }

    @Override
    public Mount getMountByName(String name) {
        return nameToMount.get(name);
    }

    @Override
    public boolean hasNonDefaultMounts() {
        return hasMounts;
    }

    @Override
    public @NotNull Collection<Mount> getMountsPlacedUnder(String path) {
        Collection<Mount> mounts = new ArrayList<>(1);
        for (Mount mount : this.mounts) {
            if (mount.isUnder(path)) {
                mounts.add(mount);
            }
        }
        return mounts;
    }

    @Override
    public @NotNull Collection<Mount> getMountsPlacedDirectlyUnder(String path) {
        Collection<Mount> mounts = new ArrayList<>(1);
        for (Mount mount : this.mounts) {
            if (mount.isDirectlyUnder(path)) {
                mounts.add(mount);
            }
        }
        return mounts;
    }

    @Override
    public @NotNull Mount getDefaultMount() {
        return defMount;
    }
}

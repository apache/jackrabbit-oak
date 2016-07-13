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

package org.apache.jackrabbit.oak.plugins.multiplex;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;

import static java.util.Arrays.asList;

/**
 * A simple and inefficient implementation to manage mount points
 */
public class SimpleMountInfoProvider implements MountInfoProvider {

    private final Map<String, Mount> mounts;
    private final Mount defMount;
    private final boolean hasMounts;

    public SimpleMountInfoProvider(List<Mount> mountInfos) {
        this.mounts = getMounts(mountInfos);
        this.hasMounts = !this.mounts.isEmpty();
        this.defMount = defaultMount(this.mounts);
        //TODO add validation of mountpoints
    }

    @Override
    public Mount getMountByPath(String path) {
        for (Mount m : mounts.values()){
            if (m.isMounted(path)){
                return m;
            }
        }
        return defMount;
    }

    @Override
    public Collection<Mount> getNonDefaultMounts() {
        return mounts.values();
    }

    @Override
    public Mount getMountByName(String name) {
        return mounts.get(name);
    }

    @Override
    public boolean hasNonDefaultMounts() {
        return hasMounts;
    }

    @Override
    public Collection<Mount> getMountsPlacedUnder(String path) {
        Collection<Mount> mounts = Lists.newArrayList();
        for ( Mount mount : this.mounts.values()) {
            if ( mount.isUnder(path) ) {
                mounts.add(mount);
            }
        }
        return mounts;
    }

    @Override
    public Mount getDefaultMount() {
        return defMount;
    }

    //~----------------------------------------< builder >

    public static Builder newBuilder(){
        return new Builder();
    }

    public static final class Builder {
        private final List<Mount> mounts = Lists.newArrayListWithCapacity(1);

        public Builder mount(String name, String... paths) {
            mounts.add(new MountInfo(name, false, false, asList(paths)));
            return this;
        }

        public Builder readOnlyMount(String name, String... paths) {
            mounts.add(new MountInfo(name, true, false, asList(paths)));
            return this;
        }

        public SimpleMountInfoProvider build() {
            return new SimpleMountInfoProvider(mounts);
        }
    }

    //~----------------------------------------< private >

    private static Map<String, Mount> getMounts(List<Mount> mountInfos) {
        Map<String, Mount> mounts = Maps.newHashMap();
        for (Mount mi : mountInfos) {
            mounts.put(mi.getName(), mi);
        }
        return ImmutableMap.copyOf(mounts);
    }

    private static Mount defaultMount(Map<String, Mount> mounts) {
        return Mounts.defaultMount(mounts.values());
    }

}

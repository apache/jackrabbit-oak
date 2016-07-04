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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;

import static java.util.Arrays.asList;

/**
 * A simple and inefficient implementation to manage mountpoints
 */
public class SimpleMountInfoProvider implements MountInfoProvider {
    private final List<MountInfo> mountInfos;
    private final Map<String, Mount> mounts;

    public SimpleMountInfoProvider(List<MountInfo> mountInfos){
        this.mountInfos = ImmutableList.copyOf(mountInfos);
        this.mounts = getMounts(mountInfos);
        //TODO add validation of mountpoints
    }

    @Override
    public Mount getMountInfo(String path) {
        for (MountInfo md : mountInfos){
            if (md.isMounted(path)){
                return md.getMount();
            }
        }
        return Mount.DEFAULT;
    }

    @Override
    public Collection<Mount> getNonDefaultMounts() {
        return mounts.values();
    }

    @Override
    public Mount getMount(String name) {
        return mounts.get(name);
    }

    //~----------------------------------------< builder >

    public static Builder newBuilder(){
        return new Builder();
    }

    public static final class Builder {
        private final List<MountInfo> mounts = Lists.newArrayListWithCapacity(1);

        public Builder mount(String name, String... paths) {
            mounts.add(new MountInfo(new Mount(name), asList(paths)));
            return this;
        }

        public Builder readOnlyMount(String name, String... paths) {
            mounts.add(new MountInfo(new Mount(name, true), asList(paths)));
            return this;
        }

        public SimpleMountInfoProvider build() {
            return new SimpleMountInfoProvider(mounts);
        }
    }

    //~----------------------------------------< private >

    private static Map<String, Mount> getMounts(List<MountInfo> mountInfos) {
        Map<String, Mount> mounts = Maps.newHashMap();
        for (MountInfo mi : mountInfos){
            mounts.put(mi.getMount().getName(), mi.getMount());
        }
        return ImmutableMap.copyOf(mounts);
    }

}

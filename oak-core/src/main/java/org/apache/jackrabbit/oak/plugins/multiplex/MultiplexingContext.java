/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.multiplex;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

class MultiplexingContext {

    private final MountInfoProvider mip;

    private final MountedNodeStore globalStore;

    private final List<MountedNodeStore> nonDefaultStores;

    private final Map<Mount, MountedNodeStore> nodeStoresByMount;

    MultiplexingContext(MountInfoProvider mip, NodeStore globalStore, List<MountedNodeStore> nonDefaultStores) {
        this.mip = mip;
        this.globalStore = new MountedNodeStore(mip.getDefaultMount(), globalStore);
        this.nonDefaultStores = nonDefaultStores;
        this.nodeStoresByMount = copyOf(uniqueIndex(getAllMountedNodeStores(), new Function<MountedNodeStore, Mount>() {
            @Override
            public Mount apply(MountedNodeStore input) {
                return input.getMount();
            }
        }));
    }

    MountedNodeStore getGlobalStore() {
        return globalStore;
    }

    List<MountedNodeStore> getNonDefaultStores() {
        return nonDefaultStores;
    }

    MountedNodeStore getOwningStore(String path) {
        Mount mount = mip.getMountByPath(path);
        if (nodeStoresByMount.containsKey(mount)) {
            return nodeStoresByMount.get(mount);
        } else {
            throw new IllegalArgumentException("Unable to find an owning store for path " + path);
        }
    }

    List<MountedNodeStore> getContributingStoresForNodes(String path, final Map<MountedNodeStore, NodeState> nodeStates) {
        return getContributingStores(path, new Function<MountedNodeStore, Iterable<String>>() {
            @Override
            public Iterable<String> apply(MountedNodeStore input) {
                return nodeStates.get(input).getChildNodeNames();
            }
        });
    }

    List<MountedNodeStore> getContributingStoresForBuilders(String path, final Map<MountedNodeStore, NodeBuilder> nodeBuilders) {
        return getContributingStores(path, new Function<MountedNodeStore, Iterable<String>>() {
            @Override
            public Iterable<String> apply(MountedNodeStore input) {
                return nodeBuilders.get(input).getChildNodeNames();
            }
        });
    }

    private List<MountedNodeStore> getContributingStores(String path, Function<MountedNodeStore, Iterable<String>> childrenProvider) {
        Mount owningMount = mip.getMountByPath(path);
        if (!owningMount.isDefault() && nodeStoresByMount.containsKey(owningMount)) {
            MountedNodeStore nodeStore = nodeStoresByMount.get(owningMount);
            if (nodeStore != globalStore) {
                return singletonList(nodeStore);
            }
        }

        // scenario 2 - multiple mounts participate
        List<MountedNodeStore> mountedStores = newArrayList();
        mountedStores.add(globalStore);

        // we need mounts placed exactly one level beneath this path
        Collection<Mount> mounts = mip.getMountsPlacedDirectlyUnder(path);

        // query the mounts next
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            if (mounts.contains(mountedNodeStore.getMount())) {
                mountedStores.add(mountedNodeStore);
            } else {
                if (mountedNodeStore.hasChildren(childrenProvider.apply(mountedNodeStore))) {
                    mountedStores.add(mountedNodeStore);
                }
            }
        }

        return mountedStores;
    }

    Iterable<MountedNodeStore> getAllMountedNodeStores() {
        return concat(singleton(globalStore), nonDefaultStores);
    }

    Blob createBlob(InputStream inputStream) throws IOException {
        return globalStore.getNodeStore().createBlob(inputStream);
    }

    int getStoresCount() {
        return nonDefaultStores.size() + 1;
    }

    Predicate<String> belongsToStore(final MountedNodeStore mountedNodeStore, final String parentPath) {
        return new Predicate<String>() {
            @Override
            public boolean apply(String childName) {
                return getOwningStore(PathUtils.concat(parentPath, childName)) == mountedNodeStore;
            }
        };
    }
}

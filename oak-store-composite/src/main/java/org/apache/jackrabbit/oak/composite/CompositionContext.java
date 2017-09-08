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
package org.apache.jackrabbit.oak.composite;

import com.google.common.collect.ImmutableSet;
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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singletonList;

class CompositionContext {

    private final MountInfoProvider mip;

    private final MountedNodeStore globalStore;

    private final List<MountedNodeStore> nonDefaultStores;

    private final Map<Mount, MountedNodeStore> nodeStoresByMount;

    private final Set<MountedNodeStore> allStores;

    private final StringCache pathCache;

    CompositionContext(MountInfoProvider mip, NodeStore globalStore, List<MountedNodeStore> nonDefaultStores) {
        this.pathCache = new StringCache();
        this.mip = mip;
        this.globalStore = new MountedNodeStore(mip.getDefaultMount(), globalStore);
        this.nonDefaultStores = nonDefaultStores;

        ImmutableSet.Builder<MountedNodeStore> b = ImmutableSet.builder();
        b.add(this.globalStore);
        b.addAll(this.nonDefaultStores);
        allStores = b.build();

        this.nodeStoresByMount = allStores.stream().collect(Collectors.toMap(MountedNodeStore::getMount, Function.identity()));

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

    List<MountedNodeStore> getContributingStoresForNodes(String path, final NodeMap<NodeState> nodeStates) {
        return getContributingStores(path, mns -> nodeStates.get(mns).getChildNodeNames());
    }

    List<MountedNodeStore> getContributingStoresForBuilders(String path, final NodeMap<NodeBuilder> nodeBuilders) {
        return getContributingStores(path, mns -> nodeBuilders.get(mns).getChildNodeNames());
    }

    boolean shouldBeComposite(final String path) {
        boolean supportMounts = false;
        if (mip.getNonDefaultMounts().stream().anyMatch(m -> m.isSupportFragmentUnder(path))) {
            supportMounts = true;
        } else if (!mip.getMountsPlacedUnder(path).isEmpty()) {
            supportMounts = true;
        }
        return supportMounts && mip.getMountByPath(path).isDefault();
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
            final Mount mount = mountedNodeStore.getMount();
            if (mounts.contains(mount)) {
                mountedStores.add(mountedNodeStore);
            } else if (hasChildrenContainingPathFragmentName(mountedNodeStore, path, childrenProvider)) {
                mountedStores.add(mountedNodeStore);
            }
        }

        return mountedStores;
    }

    private boolean hasChildrenContainingPathFragmentName(MountedNodeStore mns, String parentPath, Function<MountedNodeStore, Iterable<String>> childrenProvider) {
        final Mount mount = mns.getMount();
        if (!mount.isSupportFragment(parentPath)) {
            return false;
        }

        return StreamSupport.stream(childrenProvider.apply(mns).spliterator(), false)
                .anyMatch(i -> i.contains(mount.getPathFragmentName()));
    }

    Set<MountedNodeStore> getAllMountedNodeStores() {
        return allStores;
    }

    Blob createBlob(InputStream inputStream) throws IOException {
        return globalStore.getNodeStore().createBlob(inputStream);
    }

    boolean belongsToStore(final MountedNodeStore mountedNodeStore, final String parentPath, final String childName) {
        return getOwningStore(PathUtils.concat(parentPath, childName)) == mountedNodeStore;
    }

    CompositeNodeState createRootNodeState(Map<MountedNodeStore, NodeState> rootStates) {
        for (Map.Entry<MountedNodeStore, NodeState> e : rootStates.entrySet()) {
            MountedNodeStore mns = e.getKey();
            NodeState nodeState = e.getValue();
            if (nodeState instanceof CompositeNodeState) {
                throw new IllegalArgumentException("Nesting composite node states is not supported");
            }
            if (nodeState == null) {
                throw new NullPointerException("Passed null as a nodestate for " + mns.getMount().getName());
            }
        }
        for (MountedNodeStore mns : nonDefaultStores) {
            if (!rootStates.containsKey(mns)) {
                throw new IllegalArgumentException("Can't find node state for " + mns.getMount().getName());
            }
        }
        if (!rootStates.containsKey(globalStore)) {
            throw new IllegalArgumentException("Can't find node state for the global store");
        }
        if (rootStates.size() != nonDefaultStores.size() + 1) {
            throw new IllegalArgumentException("Too many root states passed: " + rootStates.size());
        }
        return new CompositeNodeState("/", NodeMap.create(rootStates), this);
    }

    StringCache getPathCache() {
        return pathCache;
    }

}

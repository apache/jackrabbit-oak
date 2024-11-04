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

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.composite.checks.NodeStoreChecks;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.PrefetchNodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.ImmutableMap.copyOf;

import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static org.apache.jackrabbit.guava.common.collect.Maps.filterKeys;

import static java.lang.System.currentTimeMillis;
import static org.apache.jackrabbit.oak.composite.ModifiedPathDiff.getModifiedPaths;

/**
 * A {@link NodeStore} implementation that combines other {@link NodeStore} instances
 * mounted under paths defined by {@link Mount}.
 *
 * <p>The main objective of this implementation is to proxy operations working on
 * at most single read-write store with any number of read-only stores. While the
 * composition would technically work at the NodeStore level there are several
 * less-than-obvious issues which prevent it:
 * <ol>
 *   <li>Thread safety of the write operation can be quite costly, and will come on top
 *   of the thread safety measures already put in place by the composite node stores.</li>
 *   <li>Many JCR subsystems require global state, e.g. the versioning store. This global state
 *   can become corrupt if multiple mounts operate on it or if mounts are added and removed.</li>
 * </ol>
 * 
 * <p>As such, the only supported configuration is at most a single write-enabled store.
 *
 * <p>Because of the limitation described above, right now the only correct way to use
 * CompositeNodeStore is to create a normal repository, split it into parts
 * using oak-upgrade {@code --{include,exclude}-paths} and then configure this
 * node store implementation to composite split parts together.
 */
public class CompositeNodeStore implements NodeStore, PrefetchNodeStore, Observable {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeNodeStore.class);

    static final String CHECKPOINT_METADATA = "composite.checkpoint.";

    private static final String CHECKPOINT_METADATA_MOUNT = CHECKPOINT_METADATA + "mount.";

    final CompositionContext ctx;

    private final ChangeDispatcher dispatcher;

    // visible for testing only
    CompositeNodeStore(MountInfoProvider mip, NodeStore globalStore, List<MountedNodeStore> nonDefaultStore) {
        this(mip, globalStore, nonDefaultStore, CompositeNodeStoreMonitor.EMPTY_INSTANCE, CompositeNodeStoreMonitor.EMPTY_INSTANCE);
    }

    CompositeNodeStore(MountInfoProvider mip, NodeStore globalStore, List<MountedNodeStore> nonDefaultStore, CompositeNodeStoreMonitor nodeStateMonitor, CompositeNodeStoreMonitor nodeBuilderMonitor) {
        assertPartialMountsAreReadOnly(nonDefaultStore);

        this.ctx = new CompositionContext(mip, globalStore, nonDefaultStore, nodeStateMonitor, nodeBuilderMonitor);
        this.dispatcher = new ChangeDispatcher(getRoot());

        // setup observation proxy mechanism for underlying store for events not dispatched from within our
        // merge
        if (globalStore instanceof Observable) {
            Observable globalStoreObservable = (Observable) globalStore;
            globalStoreObservable.addObserver((root, info) -> dispatcher.contentChanged(ctx.createRootNodeState(root), info));
        }
    }

    private static void assertPartialMountsAreReadOnly(List<MountedNodeStore> nonDefaultStores) {
        List<String> readWriteMountNames = nonDefaultStores
                .stream()
                .map(MountedNodeStore::getMount)
                .filter(m -> !m.isReadOnly())
                .map(Mount::getName)
                .collect(Collectors.toList());

        checkArgument(readWriteMountNames.isEmpty(),
                "Following partial mounts are write-enabled: %s", readWriteMountNames);
    }

    @Override
    public NodeState getRoot() {
        // the composite root state exposes the node states as they are
        // at this certain point in time, so we eagerly retrieve them from all stores
        return ctx.createRootNodeState(ctx.getGlobalStore().getNodeStore().getRoot());
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        checkArgument(builder instanceof CompositeNodeBuilder);
        CompositeNodeBuilder nodeBuilder = (CompositeNodeBuilder) builder;
        if (!PathUtils.denotesRoot(nodeBuilder.getPath())) {
            throw new IllegalArgumentException();
        }

        assertNoChangesOnReadOnlyMounts(nodeBuilder);

        // merge the global builder and apply the commit hooks within
        MountedNodeStore globalStore = ctx.getGlobalStore();
        CommitHookEnhancer hookEnhancer = new CommitHookEnhancer(commitHook, ctx);
        NodeState globalResult = globalStore.getNodeStore().merge(nodeBuilder.getNodeBuilder(globalStore), hookEnhancer, info);
        return ctx.createRootNodeState(globalResult);
   }

    private void assertNoChangesOnReadOnlyMounts(CompositeNodeBuilder nodeBuilder) throws CommitFailedException {
        for (MountedNodeStore mountedNodeStore : ctx.getNonDefaultStores()) {
            NodeBuilder partialBuilder = nodeBuilder.getNodeBuilder(mountedNodeStore);
            assertNoChange(mountedNodeStore, partialBuilder);
        }
    }

    private void assertNoChange(MountedNodeStore mountedNodeStore, NodeBuilder partialBuilder) throws CommitFailedException {
        NodeState baseState = partialBuilder.getBaseState();
        NodeState nodeState = partialBuilder.getNodeState();
        if (!nodeState.equals(baseState)) {
            Set<String> changedPaths = getModifiedPaths(baseState, nodeState);
            if (!changedPaths.isEmpty()) {
                throw new CommitFailedException("CompositeStore", 31, "Unable to perform changes on read-only mount " + mountedNodeStore.getMount().getName() + ". Failing paths: " + changedPaths.toString());
            }
        }
    }

    @Override
    public NodeState rebase(NodeBuilder builder) {
        checkArgument(builder instanceof CompositeNodeBuilder);
        CompositeNodeBuilder nodeBuilder = (CompositeNodeBuilder) builder;
        MountedNodeStore globalStore = ctx.getGlobalStore();
        NodeState globalResult = globalStore.getNodeStore().rebase(nodeBuilder.getNodeBuilder(globalStore));
        return ctx.createRootNodeState(globalResult);
    }

    @Override
    public NodeState reset(NodeBuilder builder) {
        checkArgument(builder instanceof CompositeNodeBuilder);
        CompositeNodeBuilder nodeBuilder = (CompositeNodeBuilder) builder;
        MountedNodeStore globalStore = ctx.getGlobalStore();
        NodeState globalResult = globalStore.getNodeStore().reset(nodeBuilder.getNodeBuilder(globalStore));
        return ctx.createRootNodeState(globalResult);
    }

    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        // since there is no way to infer a path for a blob, we create all blobs in the root store
        return ctx.createBlob(inputStream);
    }

    @Override
    public Blob getBlob(String reference) {
        for (MountedNodeStore nodeStore : ctx.getAllMountedNodeStores()) {
            Blob found = nodeStore.getNodeStore().getBlob(reference);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    public Iterable<String> checkpoints() {
        final NodeStore globalNodeStore = ctx.getGlobalStore().getNodeStore();
        return filter(globalNodeStore.checkpoints(),
                checkpoint -> isCompositeCheckpoint(checkpoint));
    }

    private boolean isCompositeCheckpoint(String checkpoint) {
        Map<String, String> props = ctx.getGlobalStore().getNodeStore().checkpointInfo(checkpoint);
        if (props == null) {
            return false;
        }
        return props.containsKey(CHECKPOINT_METADATA + "created");
    }

    @Override
    public String checkpoint(long lifetime, Map<String, String> properties) {
        Map<String, String> globalProperties = new HashMap<>(properties);
        globalProperties.put(CHECKPOINT_METADATA + "created", Long.toString(currentTimeMillis()));
        globalProperties.put(CHECKPOINT_METADATA + "expires", Long.toString(currentTimeMillis() + lifetime));
        String newCheckpoint = ctx.getGlobalStore().getNodeStore().checkpoint(lifetime, globalProperties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created checkpoint {}. Debug info:\n{}", newCheckpoint, checkpointDebugInfo());
        }
        return newCheckpoint;
    }

    @Override
    public String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections. <String, String> emptyMap());
    }

    @Override
    public Map<String, String> checkpointInfo(String checkpoint) {
        if (!checkpointExists(ctx.getGlobalStore().getNodeStore(), checkpoint)) {
            LOG.debug("Checkpoint {} doesn't exist. Debug info:\n{}", checkpoint, checkpointDebugInfo(), new Exception("call stack"));
            return Collections.emptyMap();
        }
        return copyOf(filterKeys(ctx.getGlobalStore().getNodeStore().checkpointInfo(checkpoint),
                input -> !input.startsWith(CHECKPOINT_METADATA)));
    }

    Map<String, String> allCheckpointInfo(String checkpoint) {
        return ctx.getGlobalStore().getNodeStore().checkpointInfo(checkpoint);
    }

    @Override
    public NodeState retrieve(String checkpoint) {
        if (!checkpointExists(ctx.getGlobalStore().getNodeStore(), checkpoint)) {
            LOG.warn("Checkpoint {} doesn't exist on the global store. Debug info:\n{}", checkpoint, checkpointDebugInfo());
            return null;
        }
        Map<String, String> props = ctx.getGlobalStore().getNodeStore().checkpointInfo(checkpoint);
        Map<MountedNodeStore, NodeState> nodeStates = new HashMap<>();
        nodeStates.put(ctx.getGlobalStore(), ctx.getGlobalStore().getNodeStore().retrieve(checkpoint));
        for (MountedNodeStore nodeStore : ctx.getNonDefaultStores()) {
            NodeState nodeState;
            String partialCheckpoint = getPartialCheckpointName(nodeStore, checkpoint, props, true);
            if (partialCheckpoint == null) {
                nodeState = nodeStore.getNodeStore().getRoot();
            } else {
                nodeState = nodeStore.getNodeStore().retrieve(partialCheckpoint);
            }
            nodeStates.put(nodeStore, nodeState);
        }
        if (nodeStates.values().contains(null)) {
            LOG.warn("Checkpoint {} doesn't exist. Debug info:\n{}", checkpoint, checkpointDebugInfo(), new Exception());
            return null;
        }
        return ctx.createRootNodeState(nodeStates);
    }

    @Override
    public boolean release(String checkpoint) {
        Map<String, String> props;
        boolean result;
        if (checkpointExists(ctx.getGlobalStore().getNodeStore(), checkpoint)) {
            props = ctx.getGlobalStore().getNodeStore().checkpointInfo(checkpoint);
            result = ctx.getGlobalStore().getNodeStore().release(checkpoint);
        } else {
            props = Collections.emptyMap();
            result = true;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Released checkpoint {}. Result: {}. Debug info:\n{}", checkpoint, result, checkpointDebugInfo());
        }
        return result;
    }

    private String getPartialCheckpointName(MountedNodeStore nodeStore, String globalCheckpoint, Map<String, String> globalCheckpointProperties, boolean resolveByName) {
        Set<String> validCheckpointNames = ImmutableSet.copyOf(nodeStore.getNodeStore().checkpoints());
        String result = globalCheckpointProperties.get(CHECKPOINT_METADATA_MOUNT + nodeStore.getMount().getName());
        if (result != null && validCheckpointNames.contains(result)) {
            return result;
        }
        if (globalCheckpoint != null && validCheckpointNames.contains(globalCheckpoint)) {
            return globalCheckpoint;
        }

        if (resolveByName) {
            String nameProp = globalCheckpointProperties.get("name");
            if (nameProp == null) {
                return null;
            }
            for (String c : validCheckpointNames) {
                Map<String, String> partialCheckpointProperties = nodeStore.getNodeStore().checkpointInfo(c);
                if (nameProp.equals(partialCheckpointProperties.get("name"))) {
                    return c;
                }
            }
        }
        return null;
    }

    private static boolean checkpointExists(NodeStore nodeStore, String checkpoint) {
        return CollectionUtils.toStream(nodeStore.checkpoints()).anyMatch(x -> Objects.equals(x, checkpoint));
    }

    private String checkpointDebugInfo() {
        StringBuilder builder = new StringBuilder();
        for (MountedNodeStore mns : ctx.getAllMountedNodeStores()) {
            Mount mount = mns.getMount();
            NodeStore nodeStore = mns.getNodeStore();

            builder.append("Mount: ").append(mount.isDefault() ? "[default]" : mount.getName()).append('\n');
            builder.append("Checkpoints:").append('\n');
            for (String checkpoint : nodeStore.checkpoints()) {
                builder.append(" - ").append(checkpoint).append(": ").append(nodeStore.checkpointInfo(checkpoint)).append('\n');
            }
            builder.append("/:async node: ");
            NodeState asyncNode = nodeStore.getRoot().getChildNode(":async");
            if (asyncNode.exists()) {
                builder.append("{");
                Iterator<? extends PropertyState> i = asyncNode.getProperties().iterator();
                while (i.hasNext()) {
                    PropertyState p = i.next();
                    if (p.isArray()) {
                        builder.append(p.getName()).append(": [...]");
                    } else {
                        builder.append(p.toString());
                    }
                    if (i.hasNext()) {
                        builder.append(", ");
                    }
                }
                builder.append("}");
            } else {
                builder.append("N/A");
            }
            builder.append('\n');
        }
        return builder.toString();
    }

    @Override
    public Closeable addObserver(final Observer observer) {
        return dispatcher.addObserver(observer);
    }

    @Override
    public void prefetch(Collection<String> paths, NodeState rootState) {
        ctx.prefetch(paths, rootState);
    }

    public static class Builder {

        private final MountInfoProvider mip;

        private final NodeStore globalStore;

        private final List<MountedNodeStore> nonDefaultStores = new ArrayList<>();

        private CompositeNodeStoreMonitor nodeStateMonitor = CompositeNodeStoreMonitor.EMPTY_INSTANCE;

        private CompositeNodeStoreMonitor nodeBuilderMonitor = CompositeNodeStoreMonitor.EMPTY_INSTANCE;

        private NodeStoreChecks checks;

        public Builder(MountInfoProvider mip, NodeStore globalStore) {
            this.mip = requireNonNull(mip, "mountInfoProvider");
            this.globalStore = requireNonNull(globalStore, "globalStore");
        }

        public Builder with(NodeStoreChecks checks) {
            this.checks = checks;
            return this;
        }

        public Builder with(CompositeNodeStoreMonitor nodeStateMonitor, CompositeNodeStoreMonitor nodeBuilderMonitor) {
            this.nodeStateMonitor = nodeStateMonitor;
            this.nodeBuilderMonitor = nodeBuilderMonitor;
            return this;
        }

        public Builder addMount(String mountName, NodeStore store) {
            requireNonNull(store, "store");
            requireNonNull(mountName, "mountName");

            Mount mount = requireNonNull(mip.getMountByName(mountName), String.format("No mount with name '%s' found in %s", mountName, mip.getNonDefaultMounts()));
            nonDefaultStores.add(new MountedNodeStore(mount, store));
            return this;
        }

        public Builder addIgnoredReadOnlyWritePath(String path) {
            throw new UnsupportedOperationException();
        }

        public Builder setPartialReadOnly(boolean partialReadOnly) {
            // only read only partials are supported
            return this;
        }

        public void assertPartialMountsAreReadOnly() {
            List<String> readWriteMountNames = nonDefaultStores
                    .stream()
                    .map(MountedNodeStore::getMount)
                    .filter(m -> !m.isReadOnly())
                    .map(Mount::getName)
                    .collect(Collectors.toList());

            checkArgument(readWriteMountNames.isEmpty(),
                    "Following partial mounts are write-enabled: ", readWriteMountNames);
        }

        public CompositeNodeStore build() {
            checkMountsAreConsistentWithMounts();
            if (checks != null) {
                nonDefaultStores.forEach( s -> checks.check(globalStore, s));
            }
            if (globalStore instanceof Clusterable) {
                return new ClusterableCNS(mip, globalStore, nonDefaultStores, nodeStateMonitor, nodeBuilderMonitor);
            } else {
                return new CompositeNodeStore(mip, globalStore, nonDefaultStores, nodeStateMonitor, nodeBuilderMonitor);
            }
        }

        private void checkMountsAreConsistentWithMounts() {
            int buildMountCount = nonDefaultStores.size();
            int mipMountCount = mip.getNonDefaultMounts().size();
            checkArgument(buildMountCount == mipMountCount,
                    "Inconsistent mount configuration. Builder received %s mounts, but MountInfoProvider knows about %s.",
                    buildMountCount, mipMountCount);
        }
    }

    private static class ClusterableCNS
            extends CompositeNodeStore
            implements Clusterable {

        private final Clusterable clusterable;

        ClusterableCNS(MountInfoProvider mip,
                       NodeStore globalStore,
                       List<MountedNodeStore> nonDefaultStore,
                       CompositeNodeStoreMonitor nodeStateMonitor,
                       CompositeNodeStoreMonitor nodeBuilderMonitor) {
            super(mip, globalStore, nonDefaultStore, nodeStateMonitor, nodeBuilderMonitor);
            checkArgument(globalStore instanceof Clusterable,
                    "globalStore must implement Clusterable");
            this.clusterable = (Clusterable) globalStore;
        }

        @Override
        @NotNull
        public String getInstanceId() {
            return clusterable.getInstanceId();
        }

        @Override
        @Nullable
        public String getVisibilityToken() {
            return clusterable.getVisibilityToken();
        }

        @Override
        public boolean isVisible(@NotNull String visibilityToken,
                                 long maxWaitMillis)
                throws InterruptedException {
            return clusterable.isVisible(visibilityToken, maxWaitMillis);
        }
    }
}

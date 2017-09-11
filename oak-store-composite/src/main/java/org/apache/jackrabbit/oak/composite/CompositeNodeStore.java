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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.composite.checks.NodeStoreChecks;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.isNull;
import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.filter;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.System.currentTimeMillis;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAncestor;
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
public class CompositeNodeStore implements NodeStore, Observable {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeNodeStore.class);

    static final String CHECKPOINT_METADATA = "composite.checkpoint.";

    private static final String CHECKPOINT_METADATA_MOUNT = CHECKPOINT_METADATA + "mount.";

    private final TreeSet<String> ignoreReadOnlyWritePaths;

    final CompositionContext ctx;

    private final List<Observer> observers = new CopyOnWriteArrayList<>();

    private final Lock mergeLock;

    // visible for testing only
    CompositeNodeStore(MountInfoProvider mip, NodeStore globalStore, List<MountedNodeStore> nonDefaultStore) {
        this(mip, globalStore, nonDefaultStore, Collections.<String>emptyList());
    }

    CompositeNodeStore(MountInfoProvider mip, NodeStore globalStore, List<MountedNodeStore> nonDefaultStore, List<String> ignoreReadOnlyWritePaths) {
        this.ctx = new CompositionContext(mip, globalStore, nonDefaultStore);
        this.ignoreReadOnlyWritePaths = new TreeSet<>(ignoreReadOnlyWritePaths);
        this.mergeLock = new ReentrantLock();
    }

    @Override
    public NodeState getRoot() {
        // the composite root state exposes the node states as they are
        // at this certain point in time, so we eagerly retrieve them from all stores
        Map<MountedNodeStore, NodeState> nodeStates = newHashMap();
        for (MountedNodeStore nodeStore : ctx.getAllMountedNodeStores()) {
            nodeStates.put(nodeStore, nodeStore.getNodeStore().getRoot());
        }
        return ctx.createRootNodeState(nodeStates);
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        checkArgument(builder instanceof CompositeNodeBuilder);
        CompositeNodeBuilder nodeBuilder = (CompositeNodeBuilder) builder;
        if (!PathUtils.denotesRoot(nodeBuilder.getPath())) {
            throw new IllegalArgumentException();
        }

        assertNoChangesOnReadOnlyMounts(nodeBuilder);

        mergeLock.lock();
        try {
            // merge the global builder and apply the commit hooks within
            Map<MountedNodeStore, NodeState> resultStates = newHashMap();
            MountedNodeStore globalStore = ctx.getGlobalStore();
            CommitHookEnhancer hookEnhancer = new CommitHookEnhancer(commitHook, ctx, nodeBuilder);
            NodeState globalResult = globalStore.getNodeStore().merge(nodeBuilder.getNodeBuilder(globalStore), hookEnhancer, info);
            resultStates.put(globalStore, globalResult);

            if (!hookEnhancer.getUpdatedBuilder().isPresent()) {
                // it means that the commit hook wasn't invoked, because there were
                // no changes on the global store. we should invoke it anyway.
                hookEnhancer.processCommit(globalResult, globalResult, info);
            }
            CompositeNodeBuilder updatedBuilder = hookEnhancer.getUpdatedBuilder().get();

            // merge the partial builders
            for (MountedNodeStore mns : ctx.getNonDefaultStores()) {
                NodeBuilder partialBuilder = updatedBuilder.getNodeBuilder(mns);

                if (mns.getMount().isReadOnly()) {
                    assertNoChange(mns, partialBuilder);
                    resultStates.put(mns, mns.getNodeStore().getRoot());
                } else {
                    NodeState partialState = mns.getNodeStore().merge(partialBuilder, EmptyHook.INSTANCE, info);
                    resultStates.put(mns, partialState);
                }
            }

            CompositeNodeState newRoot = ctx.createRootNodeState(resultStates);
            for (Observer observer : observers) {
                observer.contentChanged(newRoot, info);
            }
            return newRoot;
        } finally {
            mergeLock.unlock();
        }
   }

    private void assertNoChangesOnReadOnlyMounts(CompositeNodeBuilder nodeBuilder) throws CommitFailedException {
        for (MountedNodeStore mountedNodeStore : ctx.getAllMountedNodeStores()) {
            if (!mountedNodeStore.getMount().isReadOnly()) {
                continue;
            }
            NodeBuilder partialBuilder = nodeBuilder.getNodeBuilder(mountedNodeStore);
            assertNoChange(mountedNodeStore, partialBuilder);
        }
    }

    private void assertNoChange(MountedNodeStore mountedNodeStore, NodeBuilder partialBuilder) throws CommitFailedException {
        NodeState baseState = partialBuilder.getBaseState();
        NodeState nodeState = partialBuilder.getNodeState();
        if (!nodeState.equals(baseState)) {
            Set<String> changedPaths = getModifiedPaths(baseState, nodeState);
            Set<String> ignoredChangedPaths = getIgnoredPaths(changedPaths);
            if (!ignoredChangedPaths.isEmpty()) {
                LOG.debug("Can't merge following read-only paths (they are configured to be ignored): {}.", ignoredChangedPaths);
            }
            Set<String> failingChangedPaths = difference(changedPaths, ignoredChangedPaths);
            if (!failingChangedPaths.isEmpty()) {
                throw new CommitFailedException("CompositeStore", 31, "Unable to perform changes on read-only mount " + mountedNodeStore.getMount().getName() + ". Failing paths: " + failingChangedPaths.toString());
            }
        }
    }

    @Override
    public NodeState rebase(NodeBuilder builder) {
        checkArgument(builder instanceof CompositeNodeBuilder);

        CompositeNodeBuilder nodeBuilder = (CompositeNodeBuilder) builder;
        Map<MountedNodeStore, NodeState> resultStates = newHashMap();
        for (MountedNodeStore mountedNodeStore : ctx.getAllMountedNodeStores()) {
            NodeStore nodeStore = mountedNodeStore.getNodeStore();
            NodeState result;
            if (mountedNodeStore.getMount().isReadOnly()) {
                result = nodeStore.getRoot();
            } else {
                NodeBuilder partialBuilder = nodeBuilder.getNodeBuilder(mountedNodeStore);
                result = nodeStore.rebase(partialBuilder);
            }
            resultStates.put(mountedNodeStore, result);
        }
        return ctx.createRootNodeState(resultStates);
    }

    @Override
    public NodeState reset(NodeBuilder builder) {
        checkArgument(builder instanceof CompositeNodeBuilder);

        CompositeNodeBuilder nodeBuilder = (CompositeNodeBuilder) builder;
        Map<MountedNodeStore, NodeState> resultStates = newHashMap();
        for (MountedNodeStore mountedNodeStore : ctx.getAllMountedNodeStores()) {
            NodeStore nodeStore = mountedNodeStore.getNodeStore();
            NodeState result;
            if (mountedNodeStore.getMount().isReadOnly()) {
                result = nodeStore.getRoot();
            } else {
                NodeBuilder partialBuilder = nodeBuilder.getNodeBuilder(mountedNodeStore);
                result = nodeStore.reset(partialBuilder);
            }
            resultStates.put(mountedNodeStore, result);
        }
        return ctx.createRootNodeState(resultStates);
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
        return filter(globalNodeStore.checkpoints(), new Predicate<String>() {
            @Override
            public boolean apply(String checkpoint) {
                return isCompositeCheckpoint(checkpoint);
            }
        });
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
        Map<String, String> globalProperties = newHashMap(properties);
        globalProperties.put(CHECKPOINT_METADATA + "created", Long.toString(currentTimeMillis()));
        globalProperties.put(CHECKPOINT_METADATA + "expires", Long.toString(currentTimeMillis() + lifetime));
        for (MountedNodeStore mns : ctx.getNonDefaultStores()) {
            if (mns.getMount().isReadOnly()) {
                continue;
            }
            String checkpoint = mns.getNodeStore().checkpoint(lifetime, properties);
            globalProperties.put(CHECKPOINT_METADATA_MOUNT + mns.getMount().getName(), checkpoint);
        }
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
            LOG.warn("Checkpoint {} doesn't exist. Debug info:\n{}", checkpoint, checkpointDebugInfo());
            return Collections.emptyMap();
        }
        return copyOf(filterKeys(ctx.getGlobalStore().getNodeStore().checkpointInfo(checkpoint), new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return !input.startsWith(CHECKPOINT_METADATA);
            }
        }));
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
        Map<MountedNodeStore, NodeState> nodeStates = newHashMap();
        nodeStates.put(ctx.getGlobalStore(), ctx.getGlobalStore().getNodeStore().retrieve(checkpoint));
        for (MountedNodeStore nodeStore : ctx.getNonDefaultStores()) {
            NodeState nodeState = null;
            String partialCheckpoint = getPartialCheckpointName(nodeStore, checkpoint, props, true);
            if (partialCheckpoint == null && nodeStore.getMount().isReadOnly()) {
                nodeState = nodeStore.getNodeStore().getRoot();
            } else if (partialCheckpoint != null) {
                nodeState = nodeStore.getNodeStore().retrieve(partialCheckpoint);
            }
            nodeStates.put(nodeStore, nodeState);
        }
        if (any(nodeStates.values(), isNull())) {
            LOG.warn("Checkpoint {} doesn't exist. Debug info:\n{}", checkpoint, checkpointDebugInfo());
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
        for (MountedNodeStore nodeStore : ctx.getNonDefaultStores()) {
            if (nodeStore.getMount().isReadOnly()) {
                continue;
            }
            boolean released = false;
            String partialCheckpoint = getPartialCheckpointName(nodeStore, checkpoint, props, false);
            if (partialCheckpoint != null) {
                released = nodeStore.getNodeStore().release(partialCheckpoint);
            }
            result &= released;
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
        return Iterables.any(nodeStore.checkpoints(), Predicates.equalTo(checkpoint));
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
        observer.contentChanged(getRoot(), CommitInfo.EMPTY_EXTERNAL);
        observers.add(observer);
        return new Closeable() {
            @Override
            public void close() throws IOException {
                observers.remove(observer);
            }
        };
    }

    private Set<String> getIgnoredPaths(Set<String> paths) {
        return newHashSet(filter(paths, new Predicate<String>() {
            @Override
            public boolean apply(String path) {
                String previousPath = ignoreReadOnlyWritePaths.floor(path);
                return previousPath != null && (previousPath.equals(path) || isAncestor(previousPath, path));
            }
        }));
    }

    public static class Builder {

        private final MountInfoProvider mip;

        private final NodeStore globalStore;

        private final List<MountedNodeStore> nonDefaultStores = Lists.newArrayList();

        private final List<String> ignoreReadOnlyWritePaths = Lists.newArrayList();

        private boolean partialReadOnly = true;

        private NodeStoreChecks checks;

        public Builder(MountInfoProvider mip, NodeStore globalStore) {
            this.mip = checkNotNull(mip, "mountInfoProvider");
            this.globalStore = checkNotNull(globalStore, "globalStore");
        }
        
        public Builder with(NodeStoreChecks checks) {
            this.checks = checks;
            return this;
        }

        public Builder addMount(String mountName, NodeStore store) {
            checkNotNull(store, "store");
            checkNotNull(mountName, "mountName");

            Mount mount = checkNotNull(mip.getMountByName(mountName), "No mount with name %s found in %s", mountName, mip);
            nonDefaultStores.add(new MountedNodeStore(mount, store));
            return this;
        }

        public Builder addIgnoredReadOnlyWritePath(String path) {
            ignoreReadOnlyWritePaths.add(path);
            return this;
        }

        public Builder setPartialReadOnly(boolean partialReadOnly) {
            this.partialReadOnly = partialReadOnly;
            return this;
        }

        public CompositeNodeStore build() {
            checkMountsAreConsistentWithMounts();
            if (partialReadOnly) {
                assertPartialMountsAreReadOnly();
            }
            if ( checks != null ) {
                nonDefaultStores.forEach( s -> checks.check(globalStore, s));
            }
            return new CompositeNodeStore(mip, globalStore, nonDefaultStores, ignoreReadOnlyWritePaths);
        }

        public void assertPartialMountsAreReadOnly() {
            List<String> readWriteMountNames = Lists.newArrayList();
            for (Mount mount : mip.getNonDefaultMounts()) {
                if (!mount.isReadOnly()) {
                    readWriteMountNames.add(mount.getName());
                }
            }
            checkArgument(readWriteMountNames.isEmpty(),
                    "Following partial mounts are write-enabled: ", readWriteMountNames);
        }

        private void checkMountsAreConsistentWithMounts() {
            int buildMountCount = nonDefaultStores.size();
            int mipMountCount = mip.getNonDefaultMounts().size();
            checkArgument(buildMountCount == mipMountCount,
                    "Inconsistent mount configuration. Builder received %s mounts, but MountInfoProvider knows about %s.",
                    buildMountCount, mipMountCount);
        }
    }
}

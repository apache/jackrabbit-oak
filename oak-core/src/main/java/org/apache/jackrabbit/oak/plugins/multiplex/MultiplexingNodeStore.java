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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
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
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static java.util.Collections.emptyMap;

/**
 * A {@link NodeStore} implementation that multiplexes other {@link NodeStore} instances
 * mounted under paths defined by {@link MountInfo}.
 */
public class MultiplexingNodeStore implements NodeStore, Observable {

    // TODO - define concurrency model
    //
    // This implementation operates on multiple mounted stores and is generally expected to be
    // thread safe. From a publication point of view this is achieved. It is up for debate
    // whether we need to make operations atomic, or rely on the internal consistency of the
    // mounted repositories. It's possible that there is some unfortunate interleaving of
    // operations which would ultimately require us to have some sort of global ordering.

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final char CHECKPOINT_MARKER = '|';

    private static final Splitter CHECKPOINT_SPLITTER = Splitter.on(CHECKPOINT_MARKER);

    private static final Joiner CHECKPOINT_JOINER = Joiner.on(CHECKPOINT_MARKER);

    final MultiplexingContext ctx;

    private final List<Observer> observers = new CopyOnWriteArrayList<>();

    private MultiplexingNodeStore(MountInfoProvider mip, NodeStore globalStore, List<MountedNodeStore> nonDefaultStore) {
        this.ctx = new MultiplexingContext(mip, globalStore, nonDefaultStore);
    }

    @Override
    public NodeState getRoot() {
        // the multiplexed root state exposes the node states as they are
        // at this certain point in time, so we eagerly retrieve them from all stores
        Map<MountedNodeStore, NodeState> nodeStates = newHashMap();
        for (MountedNodeStore nodeStore : ctx.getAllMountedNodeStores()) {
            nodeStates.put(nodeStore, nodeStore.getNodeStore().getRoot());
        }
        return createRootNodeState(nodeStates);
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        checkArgument(builder instanceof MultiplexingNodeBuilder);

        MultiplexingNodeBuilder nodeBuilder = (MultiplexingNodeBuilder) builder;
        NodeState processed = commitHook.processCommit(getRoot(), rebase(nodeBuilder), info);
        processed.compareAgainstBaseState(builder.getNodeState(), new ApplyDiff(nodeBuilder));

        Map<MountedNodeStore, NodeState> resultStates = newHashMap();
        for (MountedNodeStore mountedNodeStore : ctx.getAllMountedNodeStores()) {
            NodeStore nodeStore = mountedNodeStore.getNodeStore();
            NodeBuilder partialBuilder = nodeBuilder.getBuilders().get(mountedNodeStore);
            NodeState result = nodeStore.merge(partialBuilder, EmptyHook.INSTANCE, info);
            resultStates.put(mountedNodeStore, result);
        }
        MultiplexingNodeState newRoot = createRootNodeState(resultStates);

        for (Observer observer : observers) {
            observer.contentChanged(newRoot, info);
        }
        return newRoot;
   }

    @Override
    public NodeState rebase(NodeBuilder builder) {
        checkArgument(builder instanceof MultiplexingNodeBuilder);

        MultiplexingNodeBuilder nodeBuilder = (MultiplexingNodeBuilder) builder;
        Map<MountedNodeStore, NodeState> resultStates = newHashMap();
        for (MountedNodeStore mountedNodeStore : ctx.getAllMountedNodeStores()) {
            NodeStore nodeStore = mountedNodeStore.getNodeStore();
            NodeBuilder partialBuilder = nodeBuilder.getBuilders().get(mountedNodeStore);
            NodeState result = nodeStore.rebase(partialBuilder);
            resultStates.put(mountedNodeStore, result);
        }
        return createRootNodeState(resultStates);
    }

    @Override
    public NodeState reset(NodeBuilder builder) {
        checkArgument(builder instanceof MultiplexingNodeBuilder);

        MultiplexingNodeBuilder nodeBuilder = (MultiplexingNodeBuilder) builder;
        Map<MountedNodeStore, NodeState> resultStates = newHashMap();
        for (MountedNodeStore mountedNodeStore : ctx.getAllMountedNodeStores()) {
            NodeStore nodeStore = mountedNodeStore.getNodeStore();
            NodeBuilder partialBuilder = nodeBuilder.getBuilders().get(mountedNodeStore);
            NodeState result = nodeStore.reset(partialBuilder);
            resultStates.put(mountedNodeStore, result);
        }
        return createRootNodeState(resultStates);
    }

    private MultiplexingNodeState createRootNodeState(Map<MountedNodeStore, NodeState> rootStates) {
        return new MultiplexingNodeState("/", rootStates, ctx);
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

    @Override
    public String checkpoint(long lifetime, Map<String, String> properties) {
        // This implementation does a best-effort attempt to join all the returned checkpoints
        // In case of failure it bails out
        List<String> checkpoints = Lists.newArrayList();
        for (MountedNodeStore mountedNodeStore : ctx.getAllMountedNodeStores()) {
            addCheckpoint(mountedNodeStore, lifetime, properties, checkpoints);
        }
        return CHECKPOINT_JOINER.join(checkpoints);
    }

    private void addCheckpoint(MountedNodeStore store, long lifetime, Map<String, String> properties, List<String> accumulator) {
        NodeStore nodeStore = store.getNodeStore();
        String checkpoint = nodeStore.checkpoint(lifetime, properties);
        checkArgument(checkpoint.indexOf(CHECKPOINT_MARKER) == -1,
                "Checkpoint %s created by NodeStore %s mounted at %s contains the invalid entry %s. Unable to add checkpoint.",
                checkpoint, nodeStore, store.getMount().getName(), CHECKPOINT_MARKER);
        accumulator.add(checkpoint);
    }

    @Override
    public String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections. <String, String> emptyMap());
    }

    @Override
    public Map<String, String> checkpointInfo(String checkpoint) {
        List<String> checkpoints = splitCheckpoints(checkpoint);
        if (checkpoints == null) {
            return emptyMap();
        }
        // since checkpoints are by design kept in sync between the stores
        // it's enough to query one. The root one is the most convenient
        return ctx.getGlobalStore().getNodeStore().checkpointInfo(checkpoints.get(0));
    }

    @Override
    public NodeState retrieve(String checkpoint) {
        List<String> checkpointList = splitCheckpoints(checkpoint);
        if (checkpointList == null) {
            return null;
        }
        Iterator<String> checkpoints = checkpointList.iterator();

        Map<MountedNodeStore, NodeState> nodeStates = newHashMap();
        for (MountedNodeStore nodeStore : ctx.getAllMountedNodeStores()) {
            NodeState rootState = nodeStore.getNodeStore().retrieve(checkpoints.next());
            if (rootState == null) {
                return null;
            }
            nodeStates.put(nodeStore, rootState);
        }

        return new MultiplexingNodeState("/", nodeStates, ctx);
    }

    @Override
    public boolean release(String checkpoint) {
        List<String> checkpointList = splitCheckpoints(checkpoint);
        if (checkpointList == null) {
            return false;
        }
        Iterator<String> checkpoints = checkpointList.iterator();

        boolean result = true;
        for (MountedNodeStore nodeStore : ctx.getAllMountedNodeStores()) {
            result &= nodeStore.getNodeStore().release(checkpoints.next());
        }

        return result;
    }

    private List<String> splitCheckpoints(String checkpoint) {
        List<String> checkpoints = CHECKPOINT_SPLITTER.splitToList(checkpoint);
        if (checkpoints.size() == ctx.getStoresCount()) {
            return checkpoints;
        } else {
            log.debug("Illegal checkpoint string: [{}] for {} node stores", checkpoint, ctx.getStoresCount());
            return null;
        }
    }

    @Override
    public Closeable addObserver(final Observer observer) {
        observer.contentChanged(getRoot(), null);
        observers.add(observer);
        return new Closeable() {
            @Override
            public void close() throws IOException {
                observers.remove(observer);
            }
        };
    }

    public static class Builder {

        private final MountInfoProvider mip;

        private final NodeStore globalStore;

        private final List<MountedNodeStore> nonDefaultStores = Lists.newArrayList();

        public Builder(MountInfoProvider mip, NodeStore globalStore) {
            this.mip = checkNotNull(mip, "mountInfoProvider");
            this.globalStore = checkNotNull(globalStore, "globalStore");
        }

        public Builder addMount(String mountName, NodeStore store) {
            checkNotNull(store, "store");
            checkNotNull(mountName, "mountName");

            Mount mount = checkNotNull(mip.getMountByName(mountName), "No mount with name %s found in %s", mountName, mip);
            nonDefaultStores.add(new MountedNodeStore(mount, store));
            return this;
        }

        public MultiplexingNodeStore build() {
            int buildMountCount = nonDefaultStores.size();
            int mipMountCount = mip.getNonDefaultMounts().size();
            checkArgument(buildMountCount == mipMountCount,
                    "Inconsistent mount configuration. Builder received %s mounts, but MountInfoProvider knows about %s.",
                    buildMountCount, mipMountCount);
            return new MultiplexingNodeStore(mip, globalStore, nonDefaultStores);
        }
    }
}

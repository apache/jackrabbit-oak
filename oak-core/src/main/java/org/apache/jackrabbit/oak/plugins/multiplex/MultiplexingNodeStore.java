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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A <tt>NodeStore</tt> implementation that multiplexes multiple <tt>MemoryNodeStore</tt> instances
 *
 */
public class MultiplexingNodeStore implements NodeStore {
    
    private static final char CHECKPOINT_MARKER = '|';

    private static final Splitter CHECKPOINT_SPLITTER = Splitter.on(CHECKPOINT_MARKER);

    private static final Joiner CHECKPOINT_JOINER = Joiner.on(CHECKPOINT_MARKER);
    
    private final MountedNodeStore globalStore;
    private final List<MountedNodeStore> nonDefaultStores;

    private final MultiplexingContext ctx;
    
    private MultiplexingNodeStore(MountInfoProvider mip, NodeStore globalStore, List<MountedNodeStore> nonDefaultStore) {

        this.globalStore = new MountedNodeStore(mip.getDefaultMount(), globalStore);
        this.nonDefaultStores = ImmutableList.copyOf(nonDefaultStore);
        
        this.ctx = new MultiplexingContext(this, mip, this.globalStore, nonDefaultStores);
    }

    @Override
    public NodeState getRoot() {

        return new MultiplexingNodeState("/", globalStore.getNodeStore().getRoot(), ctx);
    }

    @Override
    public NodeState merge(NodeBuilder builder, CommitHook commitHook, CommitInfo info) throws CommitFailedException {
        
        checkArgument(builder instanceof MultiplexingNodeBuilder);
        
        MultiplexingNodeBuilder nodeBuilder = (MultiplexingNodeBuilder) builder;
        
        for ( Map.Entry<MountedNodeStore, NodeBuilder> affectedBuilderEntry : nodeBuilder.getAffectedBuilders().entrySet() ) {
            
            NodeStore nodeStore = affectedBuilderEntry.getKey().getNodeStore();
            NodeBuilder affectedBuilder = affectedBuilderEntry.getValue();
            
            nodeStore.merge(affectedBuilder, commitHook, info);
        }
        
        // TODO - is this correct or do we need a specific path?
        return getRoot();
   }

    @Override
    public NodeState rebase(NodeBuilder builder) {
        
        checkArgument(builder instanceof MultiplexingNodeBuilder);
        
        MultiplexingNodeBuilder nodeBuilder = (MultiplexingNodeBuilder) builder;
        
        for ( Map.Entry<MountedNodeStore, NodeBuilder> affectedBuilderEntry : nodeBuilder.getAffectedBuilders().entrySet() ) {
            
            NodeStore nodeStore = affectedBuilderEntry.getKey().getNodeStore();
            NodeBuilder affectedBuilder = affectedBuilderEntry.getValue();
            
            nodeStore.rebase(affectedBuilder);
        }
        
        // TODO - is this correct or do we need a specific path?
        return getRoot();
    }

    @Override
    public NodeState reset(NodeBuilder builder) {
        checkArgument(builder instanceof MultiplexingNodeBuilder);
        
        MultiplexingNodeBuilder nodeBuilder = (MultiplexingNodeBuilder) builder;
        
        for ( Map.Entry<MountedNodeStore, NodeBuilder> affectedBuilderEntry : nodeBuilder.getAffectedBuilders().entrySet() ) {
            
            NodeStore nodeStore = affectedBuilderEntry.getKey().getNodeStore();
            NodeBuilder affectedBuilder = affectedBuilderEntry.getValue();
            
            nodeStore.reset(affectedBuilder);
        }
        
        // TODO - is this correct or do we need a specific path?
        return getRoot();
    }

    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        
        // since there is no way to infer a path for a blob, we create all blobs in the root store
        return globalStore.getNodeStore().createBlob(inputStream);
    }

    @Override
    public Blob getBlob(String reference) {
        // blobs are searched in all stores
        Blob found = globalStore.getNodeStore().getBlob(reference);
        if ( found != null ) {
            return found;
        }
        
        for ( MountedNodeStore nodeStore : nonDefaultStores ) {
            found = nodeStore.getNodeStore().getBlob(reference);
            if ( found != null ) {
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
        addCheckpoint(globalStore, lifetime, properties, checkpoints);
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            addCheckpoint(mountedNodeStore, lifetime, properties, checkpoints);
        }
        
        return CHECKPOINT_JOINER.join(checkpoints);
    }
    
    private void addCheckpoint(MountedNodeStore store, long lifetime, Map<String, String> properties, List<String> accumulator) {
        
        NodeStore nodeStore = store.getNodeStore();
        String checkpoint = nodeStore.checkpoint(lifetime, properties);
        Preconditions.checkArgument(checkpoint.indexOf(CHECKPOINT_MARKER) == -1, 
                "Checkpoint %s created by NodeStore %s mounted at %s contains the invalid entry %s. Unable to add checkpoint.", 
                checkpoint, nodeStore, store.getMount().getName(), CHECKPOINT_MARKER );
        
        accumulator.add(checkpoint);
    }

    @Override
    public String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections. <String, String> emptyMap());
    }

    @Override
    public Map<String, String> checkpointInfo(String checkpoint) {
        
        // TODO - proper validation of checkpoints size compared to mounts
        Iterable<String> checkpoints = CHECKPOINT_SPLITTER.split(checkpoint);
        
        // since checkpoints are by design kept in sync between the stores
        // it's enough to query one. The root one is the most convenient
        return globalStore.getNodeStore().checkpointInfo(checkpoints.iterator().next());
    }

    @Override
    public NodeState retrieve(String checkpoint) {
        
        // TODO - proper validation of checkpoints size compared to mounts
        List<String> checkpoints = CHECKPOINT_SPLITTER.splitToList(checkpoint);
        
        // global store is always first
        return new MultiplexingNodeState("/", globalStore.getNodeStore().retrieve(checkpoints.get(0)), 
                ctx, checkpoints);
    }

    @Override
    public boolean release(String checkpoint) {
        
        boolean result = true;
        // TODO - proper validation of checkpoints size compared to mounts
        List<String> checkpoints = CHECKPOINT_SPLITTER.splitToList(checkpoint);

        result &= globalStore.getNodeStore().release(checkpoints.get(0));
        
        for ( int i = 0 ; i < nonDefaultStores.size(); i++ ) {
            result &= nonDefaultStores.get(i).getNodeStore().release(checkpoints.get(i + 1));
        }
        
        return result;
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

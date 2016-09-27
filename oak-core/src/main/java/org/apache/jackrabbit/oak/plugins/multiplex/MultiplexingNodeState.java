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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.compose;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.transformValues;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.state.ChildNodeEntry.GET_NAME;

import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class MultiplexingNodeState extends AbstractNodeState {

    // A note on content held by node stores which is outside the mount boundries
    //
    // As a matter of design, mounted stores will definitely hold information _above_ 
    // their mounted, path, e.g. a store mounted at /a/b/c will definitely have nodes
    // /a and /a/b, which will not be visible through the multiplexing node store.
    //
    // If a node store holds information _below_ a path which belongs to another 
    // repository, the multiplexing node store will not consider that information. 
    // 
    // For instance, with a node store mounted at /libs and the root store
    // having a node at /libs/food, both the /libs and /libs/foo nodes from
    // the root store will be ignored
    
    private final String path;

    private final MultiplexingContext ctx;

    private final List<String> checkpoints;

    private final Map<MountedNodeStore, NodeState> rootNodeStates;

    private NodeState wrappedNodeState;

    public MultiplexingNodeState(String path, MultiplexingContext ctx, List<String> checkpoints, Map<MountedNodeStore, NodeState> nodeStates) {
        this.path = path;
        this.ctx = ctx;
        this.checkpoints = checkpoints;
        this.rootNodeStates = nodeStates;

        if (!rootNodeStates.isEmpty()) {
            checkArgument(nodeStates.size() == ctx.getStoresCount(), "Got %s rootNodeStates but the context handles %s stores", nodeStates.size(), ctx.getStoresCount());
        }
    }

    @Override
    public boolean exists() {
        return getWrappedNodeState().exists();
    }
    
    // delegate all property access to wrapped node
    @Override
    public boolean hasProperty(String name) {
        return getWrappedNodeState().hasProperty(name);
    }

    @Override
    public PropertyState getProperty(String name) {
        return getWrappedNodeState().getProperty(name);
    }

    @Override
    public long getPropertyCount() {
        return getWrappedNodeState().getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return getWrappedNodeState().getProperties();
    }

    // child node operations
    
    @Override
    public boolean hasChildNode(String name) {
        String childPath = PathUtils.concat(path, name);
        MountedNodeStore mountedStore = ctx.getOwningStore(childPath);
        return getNodeState(mountedStore, path).hasChildNode(name);
    }

    @Override
    public NodeState getChildNode(String name) throws IllegalArgumentException {
        String childPath = PathUtils.concat(path, name);
        MountedNodeStore mountedStore = ctx.getOwningStore(childPath);
        return wrapChild(getNodeState(mountedStore, childPath), name);
    }
    
    @Override
    public long getChildNodeCount(long max) {
        long count = 0;

        Iterable<NodeState> allNodes = transform(ctx.getContributingStores(path, checkpoints), new Function<MountedNodeStore, NodeState>() {
            @Override
            public NodeState apply(MountedNodeStore mountedNodeStore) {
                return getNodeState(mountedNodeStore, path);
            }
        });

        for (NodeState parent : allNodes) {
            long mountCount = parent.getChildNodeCount(max);
            if (mountCount == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            count += mountCount;
        }
        
        return count;
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        Iterable<? extends ChildNodeEntry> nativeChildren = concat(transform(ctx.getContributingStores(path, checkpoints), new Function<MountedNodeStore, Iterable<? extends ChildNodeEntry>>() {
            @Override
            public Iterable<? extends ChildNodeEntry> apply(final MountedNodeStore mountedNodeStore) {
                return filter(getNodeByPath(rootNodeStates.get(mountedNodeStore), path).getChildNodeEntries(), compose(ctx.belongsToStore(mountedNodeStore, path), GET_NAME));
            }
        }));
        return transform(nativeChildren, new Function<ChildNodeEntry, ChildNodeEntry>() {
            @Override
            public ChildNodeEntry apply(ChildNodeEntry input) {
                return new MemoryChildNodeEntry(input.getName(), wrapChild(input.getNodeState(), input.getName()));
            }
        });
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base instanceof MultiplexingNodeState) {
            MultiplexingNodeState multiBase = (MultiplexingNodeState) base;
            NodeStateDiff wrappingDiff = new WrappingDiff(diff, multiBase);
            MountedNodeStore owningStore = ctx.getOwningStore(path);

            boolean full = getWrappedNodeState().compareAgainstBaseState(multiBase.getWrappedNodeState(), wrappingDiff);
            for (MountedNodeStore mns : ctx.getContributingStores(path, checkpoints)) {
                NodeStateDiff childrenDiffFilter = new ChildrenDiffFilter(wrappingDiff, mns);
                if (owningStore == mns) {
                    continue;
                }
                NodeState contributing = getNodeByPath(rootNodeStates.get(mns), path);
                NodeState contributingBase = getNodeByPath(multiBase.rootNodeStates.get(mns), path);
                full = full && contributing.compareAgainstBaseState(contributingBase, childrenDiffFilter);
            }
            return full;
        } else {
            return super.compareAgainstBaseState(base, diff);
        }
    }

    // write operations
    @Override
    public NodeBuilder builder() {
        Map<MountedNodeStore, NodeBuilder> rootBuilders = newHashMap(transformValues(rootNodeStates, new Function<NodeState, NodeBuilder>() {
            @Override
            public NodeBuilder apply(NodeState input) {
                return input.builder();
            }
        }));
        return new MultiplexingNodeBuilder(path, ctx, rootBuilders);
    }

    // helper methods
    private NodeState wrapChild(NodeState nodeState, String name) {
        if (nodeState == MISSING_NODE) {
            return nodeState;
        } else {
            MultiplexingNodeState mns = new MultiplexingNodeState(PathUtils.concat(path, name), ctx, checkpoints, rootNodeStates);
            mns.wrappedNodeState = nodeState;
            return mns;
        }
    }

    private NodeState getNodeState(MountedNodeStore mountedNodeStore, String nodePath) {
        NodeState root;
        if (checkpoints.isEmpty()) {
            root = rootNodeStates.get(mountedNodeStore);
        } else {
            NodeStore nodeStore = mountedNodeStore.getNodeStore();
            root = nodeStore.retrieve(ctx.getCheckpoint(nodeStore, checkpoints));
        }
        
        checkNotNull(root, "NodeState is null for mount named %s, nodePath %s", mountedNodeStore.getMount().getName(), nodePath);

        return getNodeByPath(root, nodePath);
    }

    static NodeState getNodeByPath(NodeState root, String path) {
        NodeState child = root;
        for (String element : PathUtils.elements(path)) {
            if (child.hasChildNode(element)) {
                child = child.getChildNode(element);
            } else {
                return MISSING_NODE;
            }
        }
        return child;
    }

    private NodeState getWrappedNodeState() {
        if (wrappedNodeState == null) {
            wrappedNodeState = getNodeState(ctx.getOwningStore(path), path);
        }
        return wrappedNodeState;
    }

    private class ChildrenDiffFilter implements NodeStateDiff {

        private final NodeStateDiff diff;

        private final MountedNodeStore mns;

        public ChildrenDiffFilter(NodeStateDiff diff, MountedNodeStore mns) {
            this.diff = diff;
            this.mns = mns;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            return true;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (belongsToNodeStore(name)) {
                return diff.childNodeAdded(name, after);
            } else {
                return true;
            }
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            if (belongsToNodeStore(name)) {
                return diff.childNodeChanged(name, before, after);
            } else {
                return true;
            }
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (belongsToNodeStore(name)) {
                return diff.childNodeDeleted(name, before);
            } else {
                return true;
            }
        }

        private boolean belongsToNodeStore(String name) {
            return ctx.getOwningStore(PathUtils.concat(path, name)) == mns;
        }
    }

    private class WrappingDiff implements NodeStateDiff {

        private final NodeStateDiff diff;

        private final MultiplexingNodeState base;

        public WrappingDiff(NodeStateDiff diff, MultiplexingNodeState base) {
            this.diff = diff;
            this.base = base;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            return diff.propertyAdded(after);
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            return diff.propertyChanged(before, after);
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            return diff.propertyDeleted(before);
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            return diff.childNodeAdded(name, wrapAfter(name, after));
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            return diff.childNodeChanged(name, wrapBefore(name, before), wrapAfter(name, after));
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            return diff.childNodeDeleted(name, wrapBefore(name, before));
        }

        private NodeState wrapBefore(String name, NodeState before) {
            return base.wrapChild(before, name);
        }

        private NodeState wrapAfter(String name, NodeState after) {
            return wrapChild(after, name);
        }
    }

}
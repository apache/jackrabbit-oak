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

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.compose;
import static com.google.common.collect.ImmutableMap.copyOf;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.transformValues;
import static java.lang.Long.MAX_VALUE;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.state.ChildNodeEntry.GET_NAME;

class MultiplexingNodeState extends AbstractNodeState {

    // A note on content held by node stores which is outside the mount boundaries
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

    static final String STOP_COUNTING_CHILDREN = new String(MultiplexingNodeState.class.getName() + ".stopCountingChildren");

    private final String path;

    private final MultiplexingContext ctx;

    private final MountedNodeStore owningStore;

    private final Map<MountedNodeStore, NodeState> nodeStates;

    MultiplexingNodeState(String path, Map<MountedNodeStore, NodeState> nodeStates, MultiplexingContext ctx) {
        checkArgument(nodeStates.size() == ctx.getStoresCount(), "Got %s node states but the context manages %s stores", nodeStates.size(), ctx.getStoresCount());
        this.path = path;
        this.ctx = ctx;
        this.nodeStates = copyOf(nodeStates);
        this.owningStore = ctx.getOwningStore(path);
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
        return nodeStates.get(mountedStore).hasChildNode(name);
    }

    @Override
    public NodeState getChildNode(String name) {
        String childPath = PathUtils.concat(path, name);
        MountedNodeStore mountedStore = ctx.getOwningStore(childPath);
        if (!nodeStates.get(mountedStore).hasChildNode(name)) {
            return MISSING_NODE;
        }

        Map<MountedNodeStore, NodeState> newNodeStates = newHashMap();
        for (MountedNodeStore mns : ctx.getAllMountedNodeStores()) {
            newNodeStates.put(mns, nodeStates.get(mns).getChildNode(name));
        }
        return new MultiplexingNodeState(childPath, newNodeStates, ctx);
    }

    @Override
    public long getChildNodeCount(final long max) {
        List<MountedNodeStore> contributingStores = ctx.getContributingStoresForNodes(path, nodeStates);
        if (contributingStores.isEmpty()) {
            return 0; // this shouldn't happen
        } else if (contributingStores.size() == 1) {
            return getWrappedNodeState().getChildNodeCount(max);
        } else {
            // Count the children in each contributing store.
            return accumulateChildSizes(concat(transform(contributingStores, new Function<MountedNodeStore, Iterable<String>>() {
                @Override
                public Iterable<String> apply(MountedNodeStore mns) {
                    NodeState contributing = nodeStates.get(mns);
                    if (contributing.getChildNodeCount(max) == MAX_VALUE) {
                        return singleton(STOP_COUNTING_CHILDREN);
                    } else {
                        return filter(contributing.getChildNodeNames(), ctx.belongsToStore(mns, path));
                    }
                }
            })), max);
        }
    }

    static long accumulateChildSizes(Iterable<String> nodeNames, long max) {
        long totalCount = 0;
        for (String name : nodeNames) {
            totalCount++;
            if (name == STOP_COUNTING_CHILDREN || totalCount >= max) {
                return MAX_VALUE;
            }
        }
        return totalCount;
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        Iterable<? extends ChildNodeEntry> nativeChildren = concat(transform(ctx.getContributingStoresForNodes(path, nodeStates), new Function<MountedNodeStore, Iterable<? extends ChildNodeEntry>>() {
            @Override
            public Iterable<? extends ChildNodeEntry> apply(final MountedNodeStore mountedNodeStore) {
                return filter(nodeStates.get(mountedNodeStore).getChildNodeEntries(), compose(ctx.belongsToStore(mountedNodeStore, path), GET_NAME));
            }
        }));
        return transform(nativeChildren, new Function<ChildNodeEntry, ChildNodeEntry>() {
            @Override
            public ChildNodeEntry apply(ChildNodeEntry input) {
                NodeState wrapped = getChildNode(input.getName());
                return new MemoryChildNodeEntry(input.getName(), wrapped);
            }
        });
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base instanceof MultiplexingNodeState) {
            MultiplexingNodeState multiBase = (MultiplexingNodeState) base;
            NodeStateDiff wrappingDiff = new WrappingDiff(diff, multiBase);
            boolean full = getWrappedNodeState().compareAgainstBaseState(multiBase.getWrappedNodeState(), new ChildrenDiffFilter(wrappingDiff, owningStore, true));
            for (MountedNodeStore mns : ctx.getContributingStoresForNodes(path, nodeStates)) {
                if (owningStore == mns) {
                    continue;
                }
                NodeStateDiff childrenDiffFilter = new ChildrenDiffFilter(wrappingDiff, mns, false);
                NodeState contributing = nodeStates.get(mns);
                NodeState contributingBase = multiBase.nodeStates.get(mns);
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
        Map<MountedNodeStore, NodeBuilder> nodeBuilders = copyOf(transformValues(nodeStates, new Function<NodeState, NodeBuilder>() {
            @Override
            public NodeBuilder apply(NodeState input) {
                return input.builder();
            }
        }));
        return new MultiplexingNodeBuilder(path, nodeBuilders, ctx);
    }

    private NodeState getWrappedNodeState() {
        return nodeStates.get(owningStore);
    }

    private class ChildrenDiffFilter implements NodeStateDiff {

        private final NodeStateDiff diff;

        private final MountedNodeStore mns;

        private final boolean includeProperties;

        public ChildrenDiffFilter(NodeStateDiff diff, MountedNodeStore mns, boolean includeProperties) {
            this.diff = diff;
            this.mns = mns;
            this.includeProperties = includeProperties;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (includeProperties) {
                return diff.propertyAdded(after);
            } else {
                return true;
            }
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (includeProperties) {
                return diff.propertyChanged(before, after);
            } else {
                return true;
            }
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (includeProperties) {
                return diff.propertyDeleted(before);
            } else {
                return true;
            }
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
            return diff.childNodeAdded(name, wrapAfter(name));
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            return diff.childNodeChanged(name, wrapBefore(name), wrapAfter(name));
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            return diff.childNodeDeleted(name, wrapBefore(name));
        }

        private NodeState wrapBefore(String name) {
            return base.getChildNode(name);
        }

        private NodeState wrapAfter(String name) {
            return MultiplexingNodeState.this.getChildNode(name);
        }
    }

}

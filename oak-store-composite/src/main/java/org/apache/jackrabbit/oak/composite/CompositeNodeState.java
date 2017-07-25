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
package org.apache.jackrabbit.oak.composite;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.compose;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.asMap;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Long.MAX_VALUE;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.composite.CompositeNodeBuilder.simpleConcat;
import static org.apache.jackrabbit.oak.spi.state.ChildNodeEntry.GET_NAME;

class CompositeNodeState extends AbstractNodeState {

    private static final Logger LOG = LoggerFactory.getLogger(CompositeNodeState.class);

    // A note on content held by node stores which is outside the mount boundaries
    //
    // As a matter of design, mounted stores will definitely hold information _above_
    // their mounted, path, e.g. a store mounted at /a/b/c will definitely have nodes
    // /a and /a/b, which will not be visible through the composite node store.
    //
    // If a node store holds information _below_ a path which belongs to another
    // repository, the composite node store will not consider that information.
    //
    // For instance, with a node store mounted at /libs and the root store
    // having a node at /libs/food, both the /libs and /libs/foo nodes from
    // the root store will be ignored

    static final String STOP_COUNTING_CHILDREN = new String(CompositeNodeState.class.getName() + ".stopCountingChildren");

    private final String path;

    private final CompositionContext ctx;

    private final MountedNodeStore owningStore;

    private final Map<MountedNodeStore, NodeState> nodeStates;

    CompositeNodeState(String path, Map<MountedNodeStore, NodeState> nodeStates, CompositionContext ctx) {
        checkArgument(nodeStates.size() == ctx.getStoresCount(), "Got %s node states but the context manages %s stores", nodeStates.size(), ctx.getStoresCount());
        this.path = path;
        this.ctx = ctx;
        this.nodeStates = new CopyOnReadIdentityMap<>(nodeStates);
        this.owningStore = ctx.getOwningStore(path);
    }

    NodeState getNodeState(MountedNodeStore mns) {
        NodeState nodeState = nodeStates.get(mns);
        if (nodeState != null) {
            return nodeState;
        }

        // this shouldn't happen, so we need to log some more debug info
        String mountName = mns.getMount().isDefault() ? "[default]" : mns.getMount().getName();
        LOG.warn("Can't find node state for path {} and mount {}. The node state map: {}", path, mountName, nodeStates);
        throw new IllegalStateException("Can't find the node state for mount " + mountName);
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
        String childPath = simpleConcat(path, name);
        MountedNodeStore mountedStore = ctx.getOwningStore(childPath);
        return getNodeState(mountedStore).hasChildNode(name);
    }

    @Override
    public NodeState getChildNode(final String name) {
        String childPath = simpleConcat(path, name);
        if (!ctx.shouldBeComposite(childPath)) {
            return getNodeState(ctx.getOwningStore(childPath)).getChildNode(name);
        }
        Map<MountedNodeStore, NodeState> newNodeStates = transformValues(safeGetMap(), new Function<NodeState, NodeState>() {
            @Override
            public NodeState apply(NodeState input) {
                return input.getChildNode(name);
            }
        });
        return new CompositeNodeState(childPath, newNodeStates, ctx);
    }

    @Override
    public long getChildNodeCount(final long max) {
        List<MountedNodeStore> contributingStores = ctx.getContributingStoresForNodes(path, safeGetMap());
        if (contributingStores.isEmpty()) {
            return 0; // this shouldn't happen
        } else if (contributingStores.size() == 1) {
            return getWrappedNodeState().getChildNodeCount(max);
        } else {
            // Count the children in each contributing store.
            return accumulateChildSizes(concat(transform(contributingStores, new Function<MountedNodeStore, Iterable<String>>() {
                @Override
                public Iterable<String> apply(MountedNodeStore mns) {
                    NodeState contributing = getNodeState(mns);
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
        Iterable<? extends ChildNodeEntry> nativeChildren = concat(transform(ctx.getContributingStoresForNodes(path, safeGetMap()), new Function<MountedNodeStore, Iterable<? extends ChildNodeEntry>>() {
            @Override
            public Iterable<? extends ChildNodeEntry> apply(final MountedNodeStore mountedNodeStore) {
                return filter(getNodeState(mountedNodeStore).getChildNodeEntries(), compose(ctx.belongsToStore(mountedNodeStore, path), GET_NAME));
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
        if (base instanceof CompositeNodeState) {
            CompositeNodeState multiBase = (CompositeNodeState) base;
            NodeStateDiff wrappingDiff = new WrappingDiff(diff, multiBase);
            boolean full = getWrappedNodeState().compareAgainstBaseState(multiBase.getWrappedNodeState(), new ChildrenDiffFilter(wrappingDiff, owningStore, true));
            for (MountedNodeStore mns : ctx.getContributingStoresForNodes(path, safeGetMap())) {
                if (owningStore == mns) {
                    continue;
                }
                NodeStateDiff childrenDiffFilter = new ChildrenDiffFilter(wrappingDiff, mns, false);
                NodeState contributing = getNodeState(mns);
                NodeState contributingBase = multiBase.getNodeState(mns);
                full = full && contributing.compareAgainstBaseState(contributingBase, childrenDiffFilter);
            }
            return full;
        } else {
            return super.compareAgainstBaseState(base, diff);
        }
    }

    // write operations
    @Override
    public CompositeNodeBuilder builder() {
        Map<MountedNodeStore, NodeBuilder> nodeBuilders = transformValues(safeGetMap(), new Function<NodeState, NodeBuilder>() {
            @Override
            public NodeBuilder apply(NodeState input) {
                return input.builder();
            }
        });
        return new CompositeNodeBuilder(path, nodeBuilders, ctx);
    }

    private NodeState getWrappedNodeState() {
        return getNodeState(owningStore);
    }

    private Map<MountedNodeStore, NodeState> safeGetMap() {
        return asMap(ctx.getAllMountedNodeStores(), this::getNodeState);
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

        private final CompositeNodeState base;

        public WrappingDiff(NodeStateDiff diff, CompositeNodeState base) {
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
            return CompositeNodeState.this.getChildNode(name);
        }
    }

}

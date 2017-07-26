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

import com.google.common.collect.FluentIterable;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.composite.util.Memoizer;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.lang.Long.MAX_VALUE;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.composite.CompositeNodeBuilder.simpleConcat;

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

    private final Function<MountedNodeStore, NodeState> nodeStates;

    CompositeNodeState(String path, Map<MountedNodeStore, NodeState> nodeStates, CompositionContext ctx) {
        this.path = path;
        this.ctx = ctx;
        this.nodeStates = wrapWithNullCheck(mns -> nodeStates.get(mns), LOG, path);
        this.owningStore = ctx.getOwningStore(path);
    }

    CompositeNodeState(String path, Function<MountedNodeStore, NodeState> nodeStates, CompositionContext ctx) {
        this.path = path;
        this.ctx = ctx;
        this.nodeStates = wrapWithNullCheck(Memoizer.memoize(nodeStates), LOG, path);
        this.owningStore = ctx.getOwningStore(path);
    }

    static <N> Function<MountedNodeStore, N> wrapWithNullCheck(Function<MountedNodeStore, N> f, Logger log, String path) {
        return mns -> {
            N nodeState = f.apply(mns);
            if (nodeState == null) {
                // this shouldn't happen, so we need to log some more debug info
                log.warn("Can't find node state for path {} and mount {}.", path, mns);
                throw new IllegalStateException("Can't find the node state for mount " + mns);
            }
            return nodeState;
        };
    }

    NodeState getNodeState(MountedNodeStore mns) {
        return nodeStates.apply(mns);
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
        return nodeStates.apply(mountedStore).hasChildNode(name);
    }

    @Override
    public NodeState getChildNode(final String name) {
        String childPath = simpleConcat(path, name);
        if (!ctx.shouldBeComposite(childPath)) {
            return nodeStates.apply(ctx.getOwningStore(childPath)).getChildNode(name);
        }
        Function<MountedNodeStore, NodeState> newNodeStates = nodeStates.andThen(n -> n.getChildNode(name));
        return new CompositeNodeState(childPath, newNodeStates, ctx);
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
            return accumulateChildSizes(FluentIterable.from(contributingStores)
                    .transformAndConcat(mns -> {
                        NodeState node = nodeStates.apply(mns);
                        if (node.getChildNodeCount(max) == MAX_VALUE) {
                            return singleton(STOP_COUNTING_CHILDREN);
                        } else {
                            return FluentIterable.from(node.getChildNodeNames()).filter(e -> belongsToStore(mns, e));
                        }
                    }), max);
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
        return FluentIterable.from(ctx.getContributingStoresForNodes(path, nodeStates))
                .transformAndConcat(mns -> FluentIterable
                        .from(nodeStates.apply(mns).getChildNodeNames())
                        .filter(n -> belongsToStore(mns, n)))
                .transform(n -> new MemoryChildNodeEntry(n, getChildNode(n)));
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (base instanceof CompositeNodeState) {
            CompositeNodeState multiBase = (CompositeNodeState) base;
            NodeStateDiff wrappingDiff = new WrappingDiff(diff, multiBase);
            boolean full = getWrappedNodeState().compareAgainstBaseState(multiBase.getWrappedNodeState(), new ChildrenDiffFilter(wrappingDiff, owningStore, true));
            for (MountedNodeStore mns : ctx.getContributingStoresForNodes(path, nodeStates)) {
                if (owningStore == mns) {
                    continue;
                }
                NodeStateDiff childrenDiffFilter = new ChildrenDiffFilter(wrappingDiff, mns, false);
                NodeState contributing = nodeStates.apply(mns);
                NodeState contributingBase = multiBase.nodeStates.apply(mns);
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
        return new CompositeNodeBuilder(path, nodeStates.andThen(NodeState::builder), ctx);
    }

    private NodeState getWrappedNodeState() {
        return nodeStates.apply(owningStore);
    }

    private boolean belongsToStore(MountedNodeStore mns, String childName) {
        return ctx.belongsToStore(mns, path, childName);
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

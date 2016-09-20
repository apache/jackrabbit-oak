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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
    private final NodeState wrapped;
    private final MultiplexingContext ctx;
    private final List<String> checkpoints;
    private final MultiplexingNodeBuilder nodeBuilder;
    private final Map<MountedNodeStore, NodeState> nodeStates;

    public MultiplexingNodeState(String path, NodeState wrapped, MultiplexingContext ctx, List<String> checkpoints) {
        
        this(path, wrapped, ctx, checkpoints, null, Collections.<MountedNodeStore, NodeState> emptyMap());
    }
    
    public MultiplexingNodeState(String path, NodeState wrapped, MultiplexingContext ctx, Map<MountedNodeStore, NodeState> nodeStates) {
        
        this(path, wrapped, ctx, Collections.<String> emptyList(), null, nodeStates);
    }
    
    public MultiplexingNodeState(String path, NodeState wrapped, MultiplexingContext ctx, MultiplexingNodeBuilder nodeBuilder) {
        
        this(path, wrapped, ctx, Collections.<String> emptyList(), nodeBuilder, Collections.<MountedNodeStore, NodeState> emptyMap());
        
    }

    private MultiplexingNodeState(String path, NodeState wrapped, MultiplexingContext ctx, List<String> checkpoints,
            MultiplexingNodeBuilder nodeBuilder, Map<MountedNodeStore, NodeState> nodeStates) {
        this.path = path;
        this.wrapped = wrapped;
        this.ctx = ctx;
        this.checkpoints = checkpoints;
        this.nodeBuilder = nodeBuilder;
        this.nodeStates = nodeStates;
        
        if ( nodeStates.size() != 0 ) {
            Preconditions.checkArgument(nodeStates.size() == ctx.getStoresCount(), "Got %s nodeStates but the context handles %s stores", 
                    nodeStates.size(), ctx.getStoresCount());
        }
    }

    @Override
    public boolean exists() {
        return wrapped.exists();
    }
    
    // delegate all property access to wrapped node
    @Override
    public boolean hasProperty(String name) {
        return wrapped.hasProperty(name);
    }

    @Override
    public PropertyState getProperty(String name) {
        return wrapped.getProperty(name);
    }

    @Override
    public long getPropertyCount() {
        return wrapped.getPropertyCount();
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return wrapped.getProperties();
    }

    // child node operations
    
    @Override
    public boolean hasChildNode(String name) {
        String childPath = PathUtils.concat(path, name);
        MountedNodeStore mountedStore = ctx.getOwningStore(childPath);
        if (nodeBuilder == null) {
            return getNodeState(mountedStore, path).hasChildNode(name);
        } else {
            NodeBuilder builder = nodeBuilder.getAffectedBuilders().get(mountedStore);
            return getNode(builder, path).hasChildNode(name);
        }
    }

    @Override
    public NodeState getChildNode(String name) throws IllegalArgumentException {
        String childPath = PathUtils.concat(path, name);
        
        MountedNodeStore mountedStore = ctx.getOwningStore(childPath);
        
        return wrap(getNodeState(mountedStore, childPath), childPath);
    }
    
    @Override
    public long getChildNodeCount(long max) {
        long count = 0;

        if (nodeBuilder == null) {
            for (NodeState parent : getNodesForPath(path)) {
                long mountCount = parent.getChildNodeCount(max);
                if (mountCount == Long.MAX_VALUE) {
                    return Long.MAX_VALUE;
                }
                count += mountCount;
            }
        } else {
            Map<MountedNodeStore, NodeBuilder> affected = nodeBuilder.getAffectedBuilders();
            for (MountedNodeStore mountedNodeStore : ctx.getContributingStores(path, checkpoints)) {
                NodeBuilder builder = getNode(affected.get(mountedNodeStore), path);
                long mountCount = builder.getChildNodeCount(max);
                if (mountCount == Long.MAX_VALUE) {
                    return Long.MAX_VALUE;
                }
                count += mountCount;
            }
        }
        
        return count;
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        List<ChildNodeEntry> entries = Lists.newArrayList();
        if (nodeBuilder == null) {
            for (NodeState parent : getNodesForPath(path)) {
                for (ChildNodeEntry entry : parent.getChildNodeEntries()) {
                    MultiplexingNodeState wrappedChild = wrap(entry.getNodeState(), PathUtils.concat(path, entry.getName()));
                    entries.add(new MultiplexingChildNodeEntry(entry.getName(), wrappedChild));
                }
            }
        } else {
            Map<MountedNodeStore, NodeBuilder> affected = nodeBuilder.getAffectedBuilders();
            for (MountedNodeStore mountedNodeStore : ctx.getContributingStores(path, checkpoints)) {
                NodeBuilder builder = getNode(affected.get(mountedNodeStore), path);
                for(String name : builder.getChildNodeNames()) {
                    MultiplexingNodeState wrappedChild = wrap(builder.getChildNode(name).getNodeState(), PathUtils.concat(path, name));
                    entries.add(new MultiplexingChildNodeEntry(name, wrappedChild));
                }
            }
        }
        return entries;
    }

    // write operations
    
    @Override
    public NodeBuilder builder() {
        if (nodeBuilder == null) {
            // register the initial builder as affected as it's already instantiated
            Map<MountedNodeStore, NodeBuilder> affectedBuilders = Maps.newHashMap();
            MountedNodeStore owningStore = ctx.getOwningStore(path);
            NodeBuilder builder = owningStore.getNodeStore().getRoot().builder();
            affectedBuilders.put(owningStore, builder);

            // make sure the wrapped builder references the path for this NodeState
            // so we can easily call methods which update properties or otherwise
            // don't require reaching out to multiple NodeStore instances
            for (String segment : PathUtils.elements(path))
                builder = builder.getChildNode(segment);

            // TODO - do we need to consider checkpoints when building the affected builders?
            for (MountedNodeStore mountedStore : nodeStates.keySet()) {
                // TODO - prevent overwriting the builder for the root store
                if (affectedBuilders.containsKey(mountedStore)) {
                    continue;
                }
                affectedBuilders.put(mountedStore, mountedStore.getNodeStore().getRoot().builder());
            }
            return new MultiplexingNodeBuilder(path, builder, ctx, affectedBuilders);
        } else {
            return new MultiplexingNodeBuilder(path, nodeBuilder, ctx, nodeBuilder.getAffectedBuilders());
        }
    }

    // helper methods
    
    private MultiplexingNodeState wrap(NodeState nodeState, String path) {
        return new MultiplexingNodeState(path, nodeState, ctx, checkpoints, nodeBuilder, nodeStates);
    }

    private NodeState getNodeState(MountedNodeStore mountedNodeStore, String nodePath) {
        NodeState root;
        if (nodeBuilder != null) {
            NodeBuilder rootBuilder = nodeBuilder.getAffectedBuilders().get(mountedNodeStore);
            return getNode(rootBuilder, nodePath).getNodeState();
        } else if (!checkpoints.isEmpty()) {
            NodeStore nodeStore = mountedNodeStore.getNodeStore();
            root = nodeStore.retrieve(ctx.getCheckpoint(nodeStore, checkpoints));
        } else {
            root = nodeStates.get(mountedNodeStore);            
        }
        
        checkNotNull(root, "NodeState is null for mount named %s, nodePath %s", mountedNodeStore.getMount().getName(), nodePath);

        return getNode(root, nodePath);
    }

    static NodeState getNode(NodeState root, String path) {
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

    static NodeBuilder getNode(NodeBuilder root, String path) {
        NodeBuilder child = root;
        for (String element : PathUtils.elements(path)) {
            if (child.hasChildNode(element)) {
                child = child.getChildNode(element);
            } else {
                return MISSING_NODE.builder();
            }
        }
        return child;
    }
    
    /**
     * Returns one or more nodes, from multiple NodeStores, which are located at the specific path
     * 
     * <p>This method is chiefly useful when looking for child nodes at a given path, since multiple
     * node stores may contribute.</p>
     *   
     * @param path the path 
     * @return one more multiple nodes
     */
    private Iterable<NodeState> getNodesForPath(String path) {
        
        // TODO - there is some performance optimisation to be done here
        // 
        // By lazily adding the elements in the collection on-access we can 
        // delay accessing various node stores. The gain would happen when
        // the collection would not be fully iterated.
        List<NodeState> nodes = Lists.newArrayList();
        
        // query the mounts next
        for (MountedNodeStore mountedNodeStore : ctx.getContributingStores(path, checkpoints)) {
                nodes.add(getNodeState(mountedNodeStore, path));
        }
        
        return nodes;
    }

}

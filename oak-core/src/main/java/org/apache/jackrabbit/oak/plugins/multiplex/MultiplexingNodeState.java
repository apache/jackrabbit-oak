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

    public MultiplexingNodeState(String path, NodeState wrapped, MultiplexingContext ctx, List<String> checkpoints) {
        
        this.path = path;
        this.wrapped = wrapped;
        this.ctx = ctx;
        this.checkpoints = checkpoints;
        this.nodeBuilder = null;
    }
    
    public MultiplexingNodeState(String path, NodeState wrapped, MultiplexingContext ctx) {
        
        this(path, wrapped, ctx, Collections.<String> emptyList());
    }
    
    public MultiplexingNodeState(String path, NodeState wrapped, MultiplexingContext ctx, MultiplexingNodeBuilder nodeBuilder) {
        
        this.path = path;
        this.wrapped = wrapped;
        this.ctx = ctx;
        this.checkpoints = Collections.emptyList();
        this.nodeBuilder = nodeBuilder;
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
        
        return getNodeState(mountedStore, path).hasChildNode(name);
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
        
        for ( NodeState parent : getNodesForPath(path) ) {
            long mountCount = parent.getChildNodeCount(max);
            if ( mountCount == Long.MAX_VALUE ) {
                return mountCount;
            }
            
            count += mountCount;
        }
        
        return count;
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        
        List<ChildNodeEntry> entries = Lists.newArrayList();

        for ( NodeState parent : getNodesForPath(path) ) {
            for ( ChildNodeEntry entry : parent.getChildNodeEntries() ) {
                MultiplexingNodeState wrappedChild = wrap(entry.getNodeState(), PathUtils.concat(path, entry.getName()));
                entries.add(new MultiplexingChildNodeEntry(entry.getName(), wrappedChild));
            }
        }
        
        return entries;
    }

    // write operations
    
    @Override
    public NodeBuilder builder() {
        
        // register the initial builder as affected as it's already instantiated
        Map<MountedNodeStore, NodeBuilder> affectedBuilders = Maps.newHashMap();
        MountedNodeStore owningStore = ctx.getOwningStore(path);
        NodeBuilder builder = owningStore.getNodeStore().getRoot().builder();
        affectedBuilders.put(owningStore, builder);
        
        // make sure the wrapped builder references the path for this NodeState
        // so we can easily call methods which update properties or otherwise
        // don't require reaching out to multiple NodeStore instances
        for ( String segment : PathUtils.elements(path))
            builder = builder.getChildNode(segment);
        
        return new MultiplexingNodeBuilder(path, builder, ctx, affectedBuilders);
    }

    // helper methods
    
    private MultiplexingNodeState wrap(NodeState nodeState, String path) {
        
        return new MultiplexingNodeState(path, nodeState, ctx, checkpoints);
    }

    private NodeState getNodeState(MountedNodeStore mountedNodeStore, String nodePath) {
        
        NodeStore nodeStore = mountedNodeStore.getNodeStore();
        
        NodeState root;
        if ( nodeBuilder != null && nodeBuilder.getAffectedBuilders().containsKey(mountedNodeStore) ) {
            root = nodeBuilder.getAffectedBuilders().get(mountedNodeStore).getNodeState();
        } else if ( checkpoints.isEmpty() ) {
            root = nodeStore.getRoot();
        } else {
            root = nodeStore.retrieve(ctx.getCheckpoint(nodeStore, checkpoints));
        }
        
        return getChildNode(root, nodePath);
    }

    private NodeState getChildNode(NodeState root, String path) {
        
        // TODO - do we need to call 'exists()' at any point?
        for ( String element : PathUtils.elements(path) ) {
            root = root.getChildNode(element);
        }
        return root;
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

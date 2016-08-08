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
package org.apache.jackrabbit.oak.plugins.memory.multiplex;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.collect.Lists;

public class MultiplexingMemoryNodeState extends AbstractNodeState {

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
    private final MountInfoProvider mip;
    private final MountedNodeStore globalStore;
    private final List<MountedNodeStore> nonDefaultStores;
    private final List<String> checkpoints;

    public MultiplexingMemoryNodeState(String path, NodeState wrapped, MountInfoProvider mip, MountedNodeStore globalStore, List<MountedNodeStore> nonDefaultStores, List<String> checkpoints) {
        
        this.path = path;
        this.wrapped = wrapped;
        this.mip = mip;
        this.globalStore = globalStore;
        this.nonDefaultStores = nonDefaultStores;
        this.checkpoints = checkpoints;
    }

    
    public MultiplexingMemoryNodeState(String path, NodeState wrapped, MountInfoProvider mip, MountedNodeStore globalStore, List<MountedNodeStore> nonDefaultStores) {
        
        this(path, wrapped, mip, globalStore, nonDefaultStores, Collections.<String> emptyList());
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
        
        Mount childMount = mip.getMountByPath(childPath);
        Mount ourMount = mip.getMountByPath(path);
        
        if ( childMount == ourMount ) {
            // same mount, no need to query other stores
            return wrapped.hasChildNode(name);
        }
        
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            if ( mountedNodeStore.getMount() == childMount ) {
                return getNodeState(mountedNodeStore, path).hasChildNode(name);
            }
        }
        return false;
    }

    @Override
    public NodeState getChildNode(String name) throws IllegalArgumentException {
        
        String childPath = PathUtils.concat(path, name);
        
        Mount childMount = mip.getMountByPath(childPath);
        Mount ourMount = mip.getMountByPath(path);
        
        if ( childMount == ourMount ) {
            // same mount, no need to query other stores
            return wrap(wrapped.getChildNode(name), childPath);
        }
        
        Mount mount = mip.getMountByPath(childPath);
        
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            if ( mountedNodeStore.getMount() == mount ) {
                return wrap(getNodeState(mountedNodeStore, childPath), childPath);
            }
        }
        
        // 'never' happens
        throw new IllegalArgumentException("Could not find a mounted node store for path " + childPath);
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
                MultiplexingMemoryNodeState wrappedChild = wrap(entry.getNodeState(), PathUtils.concat(path, entry.getName()));
                entries.add(new MemoryMultiplexingChildNodeEntry(entry.getName(), wrappedChild));
            }
        }
        
        return entries;
    }

    // write operations
    
    @Override
    public NodeBuilder builder() {
        return new MultiplexingNodeBuilder(path, wrapped.builder(), mip, globalStore, nonDefaultStores);
    }

    // helper methods
    
    private MultiplexingMemoryNodeState wrap(NodeState nodeState, String path) {
        
        return new MultiplexingMemoryNodeState(path, nodeState, mip, globalStore, nonDefaultStores, checkpoints);
    }

    private NodeState getNodeState(MountedNodeStore mountedNodeStore, String nodePath) {
        
        NodeStore nodeStore = mountedNodeStore.getNodeStore();
        
        NodeState root;
        if ( checkpoints.isEmpty() ) {
            root = nodeStore.getRoot();
        } else {
            root = nodeStore.retrieve(checkpoint(nodeStore));
        }
        
        return getChildNode(root, nodePath);
    }

    private String checkpoint(NodeStore nodeStore) {
        if ( nodeStore == globalStore.getNodeStore() ) {
            return checkpoints.get(0);
        }
        
        for ( int i = 1 ; i < checkpoints.size(); i++ ) {
            if (nonDefaultStores.get( i -1 ).getNodeStore() == nodeStore ) {
                return checkpoints.get(i);
            }
        }
        
        // 'never' happens
        throw new IllegalArgumentException("Could not find checkpoint for nodeStore " + nodeStore);
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
        
        // scenario 1 - owned exclusively by a non-root mount
        Mount owningMount = mip.getMountByPath(path);
        if ( owningMount != null && !owningMount.isDefault() ) {
            for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
                if ( mountedNodeStore.getMount() == owningMount ) {
                    return Collections.singletonList(getNodeState(mountedNodeStore, path));
                }
            }
        }

        // scenario 2 - multiple mounts participate
        
        List<NodeState> nodes = Lists.newArrayList();
        
        nodes.add(getNodeState(globalStore, path));

        // we need mounts placed exactly one level beneath this path
        Collection<Mount> mounts = mip.getMountsPlacedDirectlyUnder(path);
        
        // query the mounts next
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            if ( mounts.contains(mountedNodeStore.getMount()) ) {
                nodes.add(getNodeState(mountedNodeStore, path));
                
            }
        }
        
        return nodes;
    }

}

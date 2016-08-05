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

import com.google.common.collect.Lists;

public class MultiplexingMemoryNodeState extends AbstractNodeState {

    // TODO - decide whether we need to handle here extraneous content in mounted stores
    // 
    // As a matter of design, mounted stores will definitely hold information _below_ 
    // their mounted, path, e.g. a store mounted at /a/b/c will definitely have nodes
    // /a and /a/b, which will not be visible
    //
    // Complications can arise when mounts overlap, e.g. mounts at /c and /c/d/e. But the
    // simplest overlap is between the root mount and any other mount. Do we expect the
    // stores to only hold content which is not mounted? If we mount a repository at
    // /libs, do we expect to root mount to not have any content at or under /libs?
    
    private final String path;
    private final NodeState wrapped;
    private final MountInfoProvider mip;
    private final List<MountedNodeStore> nonDefaultStores;
    
    public MultiplexingMemoryNodeState(String path, NodeState wrapped, MountInfoProvider mip, List<MountedNodeStore> nonDefaultStores) {
        
        this.path = path;
        this.wrapped = wrapped;
        this.mip = mip;
        this.nonDefaultStores = nonDefaultStores;
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
            return wrapped.getChildNode(name);
        }
        
        Mount mount = mip.getMountByPath(childPath);
        
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            if ( mountedNodeStore.getMount() == mount ) {
                return getNodeState(mountedNodeStore, childPath);
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
                entries.add(entry);
            }
        }
        
        return entries;
    }

    // write operations
    
    @Override
    public NodeBuilder builder() {
        // TODO Auto-generated method stub
        return null;
    }

    // helper methods

    private NodeState getNodeState(MountedNodeStore mountedNodeStore, String nodePath) {
        NodeState match = mountedNodeStore.getNodeStore().getRoot();
        match = getChildNode(match, nodePath);
        return match;
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

        // we need mounts placed exactly one level beneath this path
        Collection<Mount> mounts = mip.getMountsPlacedUnder(path);
        
        // query the mounts next
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            if ( mounts.contains(mountedNodeStore.getMount()) ) {
                nodes.add(getNodeState(mountedNodeStore, path));
                
            }
        }
        
        return nodes;
    }

}

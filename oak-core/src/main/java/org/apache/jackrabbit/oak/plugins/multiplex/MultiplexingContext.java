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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.collect.Lists;

class MultiplexingContext {
    
    private final MultiplexingNodeStore multiplexingNodeStore;
    private final MountInfoProvider mip;
    private final MountedNodeStore globalStore;
    private final List<MountedNodeStore> nonDefaultStores;
    
    public MultiplexingContext(MultiplexingNodeStore multiplexingNodeStore, MountInfoProvider mip,
            MountedNodeStore globalStore, List<MountedNodeStore> nonDefaultStores) {
        this.multiplexingNodeStore = multiplexingNodeStore;
        this.mip = mip;
        this.globalStore = globalStore;
        this.nonDefaultStores = nonDefaultStores;
    }
    
    public MountedNodeStore getOwningStore(String path) {
        
        Mount mount = mip.getMountByPath(path);
        
        if ( globalStore.getMount() == mount ) {
            return globalStore;
        }
        
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            if ( mountedNodeStore.getMount() == mount ) {
                return mountedNodeStore;
            }
        }

        throw new IllegalArgumentException("Unable to find an owning store for path " + path);
    }
    
    public String getCheckpoint(NodeStore nodeStore, List<String> checkpoints) {
        
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
    
    public List<MountedNodeStore> getContributingStores(String path, List<String> checkpoints) {
        
        Mount owningMount = mip.getMountByPath(path);
        if ( owningMount != null && !owningMount.isDefault() ) {
            for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
                if ( mountedNodeStore.getMount() == owningMount ) {
                    return Collections.singletonList(mountedNodeStore);
                }
            }
        }

        // scenario 2 - multiple mounts participate
        
        List<MountedNodeStore> mountedStores = Lists.newArrayList();
        
        mountedStores.add(globalStore);

        // we need mounts placed exactly one level beneath this path
        Collection<Mount> mounts = mip.getMountsPlacedDirectlyUnder(path);
        
        // query the mounts next
        for (MountedNodeStore mountedNodeStore : nonDefaultStores) {
            
            if ( mounts.contains(mountedNodeStore.getMount()) ) {
                mountedStores.add(mountedNodeStore);
            } else {
                // TODO - suboptimal and also breaks encapsulation
                //
                // since we can't possibly know if a node matching the
                // 'oak:mount-*' pattern exists below a given path
                // we are forced to iterate for each node store 
                
                NodeStore mounted = mountedNodeStore.getNodeStore();
                NodeState mountedRoot = checkpoints.isEmpty() ? 
                        mounted.getRoot() : mounted.retrieve(getCheckpoint(mounted, checkpoints));
                        
                for ( String segment : PathUtils.elements(path) ) {
                    mountedRoot = mountedRoot.getChildNode(segment);
                }
                
                for ( String childNodeName : mountedRoot.getChildNodeNames() ) {
                    if ( childNodeName.startsWith(mountedNodeStore.getMount().getPathFragmentName())) {
                        mountedStores.add(mountedNodeStore);
                    }
                }
            }
        }
        
        return mountedStores;
    }
    
    // TODO - expose just the needed methods?
    public MultiplexingNodeStore getMultiplexingNodeStore() {
        return multiplexingNodeStore;
    }
    
    // exposed for internal consistency checks
    int getStoresCount() {
        return nonDefaultStores.size() + 1;
    }
    
}
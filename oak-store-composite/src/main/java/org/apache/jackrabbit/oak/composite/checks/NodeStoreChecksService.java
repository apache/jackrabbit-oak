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
package org.apache.jackrabbit.oak.composite.checks;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
@Service(NodeStoreChecks.class)
public class NodeStoreChecksService implements NodeStoreChecks {
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    @Reference(cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE, 
            bind = "bindChecker", 
            unbind = "unbindChecker",
            referenceInterface = MountedNodeStoreChecker.class)
    private List<MountedNodeStoreChecker<?>> checkers = new CopyOnWriteArrayList<>();
    
    @Reference
    private MountInfoProvider mip;

    // used by SCR
    public NodeStoreChecksService() {
        
    }
    
    // visible for testing
    public NodeStoreChecksService(MountInfoProvider mip, List<MountedNodeStoreChecker<?>> checkers) {
        this.checkers = checkers;
        this.mip = mip;
    }

    @Override
    public void check(NodeStore globalStore, MountedNodeStore mountedStore) {
        
        ErrorHolder errorHolder = new ErrorHolder();
        
        checkers.forEach( c -> {
            log.info("Checking NodeStore from mount {} with {}", mountedStore.getMount().getName(), c );
            
            check(mountedStore, errorHolder, globalStore, c);

            log.info("Check complete");
        });
        
        errorHolder.end();
    }
    
    private <T> void check(MountedNodeStore mountedStore, ErrorHolder errorHolder, NodeStore globalStore,
            MountedNodeStoreChecker<T> c) {
        
        T context = c.createContext(globalStore, mip);
        Tree mountRoot = TreeFactory.createReadOnlyTree(mountedStore.getNodeStore().getRoot());
        
        visit(mountRoot, mountedStore,  errorHolder, context, c);
    }

    private <T> void visit(Tree tree, MountedNodeStore mountedStore, ErrorHolder errorHolder, T context, MountedNodeStoreChecker<T> c) {
        
        
        // a mounted NodeStore may contain more paths than what it owns, but since these are not accessible
        // through the CompositeNodeStore skip them
        Mount mount = mountedStore.getMount();
        
        boolean under = mount.isUnder(tree.getPath());
        boolean mounted = mount.isMounted(tree.getPath());
        
        
        boolean keepGoing = true;
        if ( mounted ) {
            keepGoing = c.check(mountedStore, tree, errorHolder, context);
        }

        if ( ( mounted || under ) && keepGoing ) {
            tree.getChildren().forEach( child -> visit(child, mountedStore, errorHolder, context, c));
        }
    }    
    
    protected void bindChecker(MountedNodeStoreChecker<?> checker) {
        checkers.add(checker);
    }
    
    protected void unbindChecker(MountedNodeStoreChecker<?> checker) {
        checkers.remove(checker);
    }
}

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

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Checks that no <tt>versionable</tt> nodes are present in a non-default <tt>NodeStore</tt>
 */
@Component
@Service(MountedNodeStoreChecker.class)
public class VersionableNodesMountedNodeStoreChecker implements MountedNodeStoreChecker<VersionableNodesMountedNodeStoreChecker.Context> {

    @Override
    public Context createContext(NodeStore globalStore) {
        
        Root globalRoot = RootFactory.createReadOnlyRoot(globalStore.getRoot());
        ReadOnlyNodeTypeManager typeManager = ReadOnlyNodeTypeManager.getInstance(globalRoot, NamePathMapper.DEFAULT);

        return new Context(typeManager);
    }
    
    @Override
    public void check(MountedNodeStore mountedStore, Tree tree, ErrorHolder errorHolder, Context context) {
        
        if ( context.getTypeManager().isNodeType(tree, VersionConstants.MIX_VERSIONABLE) ) {
            errorHolder.report(mountedStore, tree.getPath(), "versionable node");
        }
    }
    
    static class Context {
        
        private final ReadOnlyNodeTypeManager typeManager;
        
        Context(ReadOnlyNodeTypeManager typeManager) {
            this.typeManager = typeManager;
        }
        
        public ReadOnlyNodeTypeManager getTypeManager() {
            return typeManager;
        }
    }

}

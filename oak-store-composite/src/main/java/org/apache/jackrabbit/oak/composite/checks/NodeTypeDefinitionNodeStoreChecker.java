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

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditor;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditor.ConstraintViolationCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;

/**
 * Checks that nodes present in a mount are consistent with the global node type definitions
 */
@Component
@Service(MountedNodeStoreChecker.class)
public class NodeTypeDefinitionNodeStoreChecker implements MountedNodeStoreChecker<NodeTypeDefinitionNodeStoreChecker.Context> {
    
    @Override
    public Context createContext(NodeStore globalStore, MountInfoProvider mip) {
        
        NodeState nodeTypes = globalStore.getRoot().getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES);
        
        return new Context(nodeTypes);
    }

    @Override
    public boolean check(MountedNodeStore mountedStore, Tree tree, ErrorHolder errorHolder, Context context) {
        
        ConstraintViolationCallback callback = new ConstraintViolationCallback() {
            @Override
            public void onConstraintViolation(String path, List<String> typeNames, int code, String message)
                    throws CommitFailedException {
                errorHolder.report(mountedStore, path, message);
            }
        };
        
        NodeState root = mountedStore.getNodeStore().getRoot();
        NodeBuilder rootBuilder = new ReadOnlyBuilder(root); // prevent accidental changes
        
        String primary = root.getName(JCR_PRIMARYTYPE);
        Iterable<String> mixins = root.getNames(JCR_MIXINTYPES);
        try {
            Set<String> checkNodeTypeNames = context.getAllNodeTypeNames();
            // apparently index definitions are not valid in a JCR sense but
            // the editor still tries to process them, so avoid that
            checkNodeTypeNames.remove(IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
            Editor editor = new VisibleEditor(TypeEditor.create(
                    callback, checkNodeTypeNames, context.nodeTypes,
                    primary, mixins, rootBuilder));
        
            // errors already propagated via the ConstraintViolationCallback so 
            // no need to look at the CommitException
            EditorDiff.process(editor, MISSING_NODE, root);
            
        } catch (CommitFailedException e) {
            errorHolder.report(mountedStore, "/", "Unexpected error : " + e.getMessage());
        }

        
        // the VisibleEditor does its own traversal so we only run one check
        return false;
    }

    static class Context {
        
        private final NodeState nodeTypes;

        private Context(NodeState nodeTypes) {
            this.nodeTypes = nodeTypes;
        }
        
        private Set<String> getAllNodeTypeNames() {
            
            Set<String> modifiedTypes = new HashSet<>();
            for ( ChildNodeEntry child : nodeTypes.getChildNodeEntries() ) {
                modifiedTypes.add(child.getNodeState().getName(JcrConstants.JCR_NODETYPENAME));
            }
            
            return modifiedTypes;
        }
        
    }

}

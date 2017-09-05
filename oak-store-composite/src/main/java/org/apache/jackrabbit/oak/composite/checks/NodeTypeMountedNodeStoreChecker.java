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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

@Component(configurationFactory=true, 
    policy = ConfigurationPolicy.REQUIRE)
@Service(MountedNodeStoreChecker.class)
public class NodeTypeMountedNodeStoreChecker implements 
        MountedNodeStoreChecker<NodeTypeMountedNodeStoreChecker.Context>  {
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    @Property(label = "The name of a node type that is invalid and will be rejected when found")
    private static final String INVALID_NODE_TYPE = "invalidNodeType";
    @Property(label = "The error label to use when rejecting an invalid node type")
    private static final String ERROR_LABEL = "errorLabel";
    
    @Property(label="Node types that will cause the check to succeeed, even in the invalid node type is also found.",
            cardinality = Integer.MAX_VALUE)
    private static final String EXCLUDED_NODE_TYPES = "excludedNodeTypes";

    private String invalidNodeType;
    private String errorLabel;
    private Set<String> excludedNodeTypes;
    
    // used by SCR
    public NodeTypeMountedNodeStoreChecker() {

    }
    
    // visible for testing
    public NodeTypeMountedNodeStoreChecker(String invalidNodeType, String errorLabel, String... excludedNodeTypes) {
        this.invalidNodeType = invalidNodeType;
        this.errorLabel = errorLabel;
        this.excludedNodeTypes = ImmutableSet.copyOf(excludedNodeTypes);
    }

    protected void activate(ComponentContext ctx) {
        invalidNodeType = checkNotNull(PropertiesUtil.toString(ctx.getProperties().get(INVALID_NODE_TYPE), null), INVALID_NODE_TYPE);
        errorLabel = checkNotNull(PropertiesUtil.toString(ctx.getProperties().get(ERROR_LABEL), null), ERROR_LABEL);
        excludedNodeTypes = ImmutableSet.copyOf(PropertiesUtil.toStringArray(ctx.getProperties().get(EXCLUDED_NODE_TYPES), new String[0]));
    }

    @Override
    public Context createContext(NodeStore globalStore, MountInfoProvider mip) {
        
        Root globalRoot = RootFactory.createReadOnlyRoot(globalStore.getRoot());
        ReadOnlyNodeTypeManager typeManager = ReadOnlyNodeTypeManager.getInstance(globalRoot, NamePathMapper.DEFAULT);
    
        return new Context(typeManager);
    }

    @Override
    public boolean check(MountedNodeStore mountedStore, Tree tree, ErrorHolder errorHolder, Context context) {
        
        if ( context.getTypeManager().isNodeType(tree, invalidNodeType) &&
                !isExcluded(mountedStore, tree, context) ) {
            errorHolder.report(mountedStore, tree.getPath(), errorLabel);
        }
        
        return true;
    }

    private boolean isExcluded(MountedNodeStore mountedStore, Tree tree, Context context) {

        for ( String excludedNodeType : excludedNodeTypes ) {
            if ( context.getTypeManager().isNodeType(tree, excludedNodeType ) ) {
                log.warn("Not failing check for tree at path {}, mount {} due to matching excluded node type {}", 
                        tree.getPath(), mountedStore.getMount().getName(), excludedNodeType);
                return true;
            }
        }
        return false;
    }
    
    @Override
    public String toString() {
        return getClass().getName()+ ": [ invalidNodeType: " + invalidNodeType + 
                ", excludedNodeTypes: " + excludedNodeTypes + " ]";
    }

    protected static class Context {

        private final ReadOnlyNodeTypeManager typeManager;

        Context(ReadOnlyNodeTypeManager typeManager) {
            this.typeManager = typeManager;
        }

        public ReadOnlyNodeTypeManager getTypeManager() {
            return typeManager;
        }
    }

}
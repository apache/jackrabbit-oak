/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.query;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_MIXIN_SUBTYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_SINGLE_VALUED_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_PRIMARY_SUBTYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_SUPERTYPES;

import java.util.Set;

import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A nodetype info provider that is based on node states.
 */
public class NodeStateNodeTypeInfoProvider implements NodeTypeInfoProvider {
    
    private final NodeState types;
    
    public NodeStateNodeTypeInfoProvider(NodeState baseState) {
        this.types = baseState.
                getChildNode(JCR_SYSTEM).
                getChildNode(JCR_NODE_TYPES);        
    }

    @Override
    public NodeTypeInfo getNodeTypeInfo(String nodeTypeName) {
        NodeState type = types.getChildNode(nodeTypeName);
        return new NodeStateNodeTypeInfo(type);
    }
    
    static class NodeStateNodeTypeInfo implements NodeTypeInfo {
        
        private final NodeState type;
        
        NodeStateNodeTypeInfo(NodeState type) {
            this.type = type;
        }
        
        @Override
        public boolean exists() {
            return type.exists();
        }

        @Override
        public String getNodeTypeName() {
            return  type.getName(JCR_NODETYPENAME);
        }

        @Override
        public Set<String> getSuperTypes() {
            return  newHashSet(type.getNames(REP_SUPERTYPES));
        }

        @Override
        public Set<String> getPrimarySubTypes() {
            return newHashSet(type
                    .getNames(REP_PRIMARY_SUBTYPES));
        }

        @Override
        public Set<String> getMixinSubTypes() {
            return newHashSet(type.getNames(REP_MIXIN_SUBTYPES));
        }

        @Override
        public boolean isMixin() {
            return type.getBoolean(JCR_ISMIXIN);
        }

        @Override
        public Iterable<String> getNamesSingleValuesProperties() {
            return type.getNames(REP_NAMED_SINGLE_VALUED_PROPERTIES);
        }

    }

}

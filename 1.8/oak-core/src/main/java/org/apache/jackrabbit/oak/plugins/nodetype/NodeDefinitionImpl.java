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
package org.apache.jackrabbit.oak.plugins.nodetype;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;

import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;

/**
 * <pre>
 * [nt:childNodeDefinition]
 *   ...
 * - jcr:requiredPrimaryTypes (NAME) = 'nt:base' protected mandatory multiple
 * - jcr:defaultPrimaryType (NAME) protected
 * - jcr:sameNameSiblings (BOOLEAN) protected mandatory
 * </pre>
 */
class NodeDefinitionImpl extends ItemDefinitionImpl implements NodeDefinition {

    NodeDefinitionImpl(Tree definition, NodeType type, NamePathMapper mapper) {
        super(definition, type, mapper);
    }

    //-----------------------------------------------------< NodeDefinition >---

    @Override
    public String[] getRequiredPrimaryTypeNames() {
        String[] names = getNames(JcrConstants.JCR_REQUIREDPRIMARYTYPES);
        if (names == null) {
            names = new String[] { JcrConstants.NT_BASE };
        }
        for (int i = 0; i < names.length; i++) {
            names[i] = mapper.getJcrName(names[i]);
        }
        return names;
    }

    @Override
    public NodeType[] getRequiredPrimaryTypes() {
        String[] oakNames = getNames(JcrConstants.JCR_REQUIREDPRIMARYTYPES);
        if (oakNames == null) {
            oakNames = new String[] { JcrConstants.NT_BASE };
        }

        NodeType[] types = new NodeType[oakNames.length];
        Tree root = definition.getParent();
        while (!JCR_NODE_TYPES.equals(root.getName())) {
            root = root.getParent();
        }
        for (int i = 0; i < oakNames.length; i++) {
            Tree type = root.getChild(oakNames[i]);
            checkState(type.exists());
            types[i] = new NodeTypeImpl(type, mapper);
        }
        return types;
    }

    @Override
    public String getDefaultPrimaryTypeName() {
        String oakName = getName(JcrConstants.JCR_DEFAULTPRIMARYTYPE);
        if (oakName != null) {
            return mapper.getJcrName(oakName);
        } else {
            return null;
        }
    }

    @Override
    public NodeType getDefaultPrimaryType() {
        String oakName = getName(JcrConstants.JCR_DEFAULTPRIMARYTYPE);
        if (oakName != null) {
            Tree types = definition.getParent();
            while (!JCR_NODE_TYPES.equals(types.getName())) {
                types = types.getParent();
            }
            Tree type = types.getChild(oakName);
            checkState(type.exists());
            return new NodeTypeImpl(type, mapper);
        } else {
            return null;
        }
    }

    @Override
    public boolean allowsSameNameSiblings() {
        return getBoolean(JcrConstants.JCR_SAMENAMESIBLINGS);
    }

}

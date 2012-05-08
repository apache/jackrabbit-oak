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
package org.apache.jackrabbit.oak.jcr.nodetype;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;

import org.apache.jackrabbit.oak.namepath.NameMapper;

class NodeDefinitionImpl extends ItemDefinitionImpl implements NodeDefinition {

    private final NodeTypeManager manager;

    private final NodeDefinitionDelegate dlg;

    protected NodeDefinitionImpl(NodeTypeManager manager, NodeType type, NameMapper mapper, NodeDefinitionDelegate delegate) {
        super(type, mapper, delegate);
        this.manager = manager;
        this.dlg = delegate;
    }

    @Override
    public NodeType[] getRequiredPrimaryTypes() {
        String[] names = getRequiredPrimaryTypeNames();
        NodeType[] types = new NodeType[names.length];
        for (int i = 0; i < names.length; i++) {
            try {
                types[i] = manager.getNodeType(names[i]);
            } catch (RepositoryException e) {
                throw new IllegalStateException("Inconsistent node definition: " + this, e);
            }
        }
        return types;
    }

    @Override
    public String[] getRequiredPrimaryTypeNames() {
        String[] requiredPrimaryTypeNames = dlg.getRequiredPrimaryTypeNames();
        String[] result = new String[requiredPrimaryTypeNames.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = mapper.getJcrName(requiredPrimaryTypeNames[i]);
        }
        return result;
    }

    @Override
    public NodeType getDefaultPrimaryType() {
        try {
            String defaultName = getDefaultPrimaryTypeName();
            return defaultName == null ? null : manager.getNodeType(getDefaultPrimaryTypeName());
        } catch (RepositoryException e) {
            throw new IllegalStateException("Inconsistent node definition: " + this, e);
        }
    }

    @Override
    public String getDefaultPrimaryTypeName() {
        String defaultPrimaryTypeName = dlg.getDefaultPrimaryTypeName();
        return defaultPrimaryTypeName == null ? null : mapper.getJcrName(defaultPrimaryTypeName);
    }

    @Override
    public boolean allowsSameNameSiblings() {
        return dlg.allowsSameNameSiblings();
    }

}

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

import java.util.ArrayList;
import java.util.List;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log =
            LoggerFactory.getLogger(NodeDefinitionImpl.class);

    private final NodeTypeManager manager;

    protected NodeDefinitionImpl(
            NodeTypeManager manager, NodeType type, NodeUtil node) {
        super(type, node);
        this.manager = manager;
    }

    //-----------------------------------------------------< NodeDefinition >---
    @Override
    public String[] getRequiredPrimaryTypeNames() {
        return node.getNames(JcrConstants.JCR_REQUIREDPRIMARYTYPES, JcrConstants.NT_BASE);
    }

    @Override
    public NodeType[] getRequiredPrimaryTypes() {
        String[] names = getRequiredPrimaryTypeNames();
        List<NodeType> types = new ArrayList<NodeType>(names.length);
        for (String name : names) {
            try {
                types.add(manager.getNodeType(name));
            }
            catch (RepositoryException e) {
                log.warn("Unable to access required primary type "
                        + name + " of node " + getName(), e);
            }
        }
        return types.toArray(new NodeType[types.size()]);
    }

    @Override
    public String getDefaultPrimaryTypeName() {
        return node.getName(JcrConstants.JCR_DEFAULTPRIMARYTYPE, null);
    }

    @Override
    public NodeType getDefaultPrimaryType() {
        String name = getDefaultPrimaryTypeName();
        if (name != null) {
            try {
                return manager.getNodeType(name);
            } catch (RepositoryException e) {
                log.warn("Unable to access default primary type "
                        + name + " of node " + getName(), e);
            }
        }
        return null;
    }

    @Override
    public boolean allowsSameNameSiblings() {
        return node.getBoolean(JcrConstants.JCR_SAMENAMESIBLINGS);
    }

}

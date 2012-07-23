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

import static java.util.Arrays.asList;

import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.annotation.CheckForNull;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

/**
 * Adapter class for turning an in-content node type definition
 * node ("nt:nodeTYpe") to a {@link NodeType} instance.
 */
class NodeTypeImpl extends TypeNode implements NodeType {

    private final NodeTypeManager manager;

    public NodeTypeImpl(NodeTypeManager manager, Node node) {
        super(node);
        this.manager = manager;
    }

    /** CND: <pre>- jcr:nodeTypeName (NAME) protected mandatory</pre> */
    @Override
    public String getName() {
        return getString(Property.JCR_NODE_TYPE_NAME);
    }

    /** CND: <pre>- jcr:supertypes (NAME) protected multiple</pre> */
    @Override
    public String[] getDeclaredSupertypeNames() {
        return getStrings(Property.JCR_SUPERTYPES, new String[0]);
    }

    /** CND: <pre>- jcr:isAbstract (BOOLEAN) protected mandatory</pre> */
    @Override
    public boolean isAbstract() {
        return getBoolean(Property.JCR_IS_ABSTRACT);
    }

    /** CND: <pre>- jcr:isMixin (BOOLEAN) protected mandatory</pre> */
    @Override
    public boolean isMixin() {
        return getBoolean(Property.JCR_IS_MIXIN);
    }

    /** CND: <pre>- jcr:hasOrderableChildNodes (BOOLEAN) protected mandatory</pre> */
    @Override
    public boolean hasOrderableChildNodes() {
        return getBoolean(Property.JCR_HAS_ORDERABLE_CHILD_NODES);
    }

    /** CND: <pre>- jcr:isQueryable (BOOLEAN) protected mandatory</pre> */
    @Override
    public boolean isQueryable() {
        return getBoolean("jcr:isQueryable"); // TODO: constant
    }

    /** CND: <pre>- jcr:primaryItemName (NAME) protected</pre> */
    @Override @CheckForNull
    public String getPrimaryItemName() {
        return getString(Property.JCR_PRIMARY_ITEM_NAME, null);
    }

    /** CND: <pre>+ jcr:propertyDefinition (nt:propertyDefinition) = nt:propertyDefinition protected sns</pre> */
    @Override
    public PropertyDefinition[] getDeclaredPropertyDefinitions() {
        List<PropertyDefinition> list = Lists.newArrayList();
        NodeIterator iterator = getNodes(Node.JCR_PROPERTY_DEFINITION);
        while (iterator.hasNext()) {
            list.add(new PropertyDefinitionImpl(this, iterator.nextNode()));
        }
        return list.toArray(new PropertyDefinition[list.size()]);
    }

    @Override
    public NodeDefinition[] getDeclaredChildNodeDefinitions() {
        List<NodeDefinition> list = Lists.newArrayList();
        NodeIterator iterator = getNodes(Node.JCR_CHILD_NODE_DEFINITION);
        while (iterator.hasNext()) {
            list.add(new NodeDefinitionImpl(
                    manager, this, iterator.nextNode()));
        }
        return list.toArray(new NodeDefinition[list.size()]);
    }

    @Override
    public NodeType[] getSupertypes() {
        Map<String, NodeType> types = Maps.newHashMap();
        Queue<String> queue = Queues.newArrayDeque();
        queue.addAll(asList(getDeclaredSupertypeNames()));
        while (!queue.isEmpty()) {
            String name = queue.remove();
            if (!types.containsKey(name)) {
                NodeType type = getType(manager, name);
                types.put(name, type);
                queue.addAll(asList(type.getDeclaredSupertypeNames()));
            }
        }
        return types.values().toArray(new NodeType[types.size()]);
    }

    @Override
    public NodeType[] getDeclaredSupertypes() {
        List<NodeType> types = Lists.newArrayList();
        for (String name : getDeclaredSupertypeNames()) {
            types.add(getType(manager, name));
        }
        return types.toArray(new NodeType[types.size()]);
    }

    @Override
    public NodeTypeIterator getSubtypes() {
        List<NodeType> types = Lists.newArrayList();
        try {
            String name = getName();
            NodeTypeIterator iterator = manager.getAllNodeTypes();
            while (iterator.hasNext()) {
                NodeType type = iterator.nextNodeType();
                if (!name.equals(type.getName()) && type.isNodeType(name)) {
                    types.add(type);
                }
            }
        } catch (RepositoryException e) {
            throw illegalState(e);
        }
        return new NodeTypeIteratorAdapter(types);
    }

    @Override
    public NodeTypeIterator getDeclaredSubtypes() {
        List<NodeType> types = Lists.newArrayList();
        try {
            String name = getName();
            NodeTypeIterator iterator = manager.getAllNodeTypes();
            while (iterator.hasNext()) {
                NodeType type = iterator.nextNodeType();
                if (asList(type.getDeclaredSupertypeNames()).contains(name)) {
                    types.add(type);
                }
            }
        } catch (RepositoryException e) {
            throw illegalState(e);
        }
        return new NodeTypeIteratorAdapter(types);
    }

    @Override
    public boolean isNodeType(String nodeTypeName) {
        if (nodeTypeName.equals(getName())) {
            return true;
        }

        for (NodeType type : getDeclaredSupertypes()) {
            if (type.isNodeType(nodeTypeName)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public PropertyDefinition[] getPropertyDefinitions() {
        List<PropertyDefinition> definitions = Lists.newArrayList();
        for (NodeType type : getSupertypes()) {
            definitions.addAll(asList(type.getDeclaredPropertyDefinitions()));
        }
        definitions.addAll(asList(getDeclaredPropertyDefinitions()));
        return definitions.toArray(new PropertyDefinition[definitions.size()]);
    }

    @Override
    public NodeDefinition[] getChildNodeDefinitions() {
        List<NodeDefinition> definitions = Lists.newArrayList();
        for (NodeType type : getSupertypes()) {
            definitions.addAll(asList(type.getDeclaredChildNodeDefinitions()));
        }
        definitions.addAll(asList(getDeclaredChildNodeDefinitions()));
        return definitions.toArray(new NodeDefinition[definitions.size()]);
    }

    @Override
    public boolean canSetProperty(String propertyName, Value value) {
        for (PropertyDefinition definition : getPropertyDefinitions()) {
            String name = definition.getName();
            if ((propertyName.equals(name) && !definition.isProtected())
                    || "*".equals(name)) {
                if (!definition.isMultiple()) {
                    // TODO: Check value type, constraints, etc.
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean canSetProperty(String propertyName, Value[] values) {
        for (PropertyDefinition definition : getPropertyDefinitions()) {
            String name = definition.getName();
            if ((propertyName.equals(name) && !definition.isProtected())
                    || "*".equals(name)) {
                if (definition.isMultiple()) {
                    // TODO: Check value type, constraints, etc.
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean canAddChildNode(String childNodeName) {
        for (NodeDefinition definition : getChildNodeDefinitions()) {
            String name = definition.getName();
            if ((childNodeName.equals(name) && !definition.isProtected())
                    || "*".equals(name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean canAddChildNode(String childNodeName, String nodeTypeName) {
        try {
            NodeType type = manager.getNodeType(nodeTypeName);
            for (NodeDefinition definition : getChildNodeDefinitions()) {
                String name = definition.getName();
                if ((childNodeName.equals(name) && !definition.isProtected())
                        || "*".equals(name)) {
                    for (String required : definition.getRequiredPrimaryTypeNames()) {
                        if (type.isNodeType(required)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        } catch (NoSuchNodeTypeException e) {
            return false;
        } catch (RepositoryException e) {
            throw illegalState(e);
        }
    }

    @Override
    public boolean canRemoveItem(String itemName) {
        return canRemoveNode(itemName) || canRemoveProperty(itemName);
    }

    @Override
    public boolean canRemoveNode(String nodeName) {
        for (PropertyDefinition definition : getPropertyDefinitions()) {
            String name = definition.getName();
            if (nodeName.equals(name)) {
                if (definition.isMandatory() || definition.isProtected()) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean canRemoveProperty(String propertyName) {
        for (PropertyDefinition definition : getPropertyDefinitions()) {
            String name = definition.getName();
            if (propertyName.equals(name)) {
                if (definition.isMandatory() || definition.isProtected()) {
                    return false;
                }
            }
        }
        return true;
    }

}

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.jcr.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.namepath.NameMapper;

class NodeTypeImpl implements NodeType {

    private final NodeTypeManager manager;
    private final NameMapper mapper;
    private final ValueFactoryImpl valueFactory;

    private final NodeTypeDelegate dlg;

    public NodeTypeImpl(NodeTypeManager manager, ValueFactoryImpl valueFactory, NameMapper mapper, NodeTypeDelegate delegate) {
        this.manager = manager;
        this.valueFactory = valueFactory;
        this.mapper = mapper;
        this.dlg = delegate;
    }

    @Override
    public String getName() {
        return mapper.getJcrName(dlg.getName());
    }

    @Override
    public String[] getDeclaredSupertypeNames() {
        List<String> names = new ArrayList<String>();
        boolean addNtBase = !isMixin();
        for (String name : dlg.getDeclaredSuperTypeNames()) {

            String jcrName = mapper.getJcrName(name);

            // TODO: figure out a more performant way
            // check of at least one declared super type being a non-mixin type
            if (addNtBase) {
                try {
                    NodeType nt = manager.getNodeType(jcrName);
                    if (!nt.isMixin()) {
                        addNtBase = false;
                    }
                } catch (RepositoryException ex) {
                    // ignored
                }
            }
            names.add(jcrName);
        }

        if (addNtBase) {
            names.add(mapper.getJcrName("nt:base"));
        }

        return names.toArray(new String[names.size()]);
    }

    @Override
    public boolean isAbstract() {
        return dlg.isAbstract();
    }

    @Override
    public boolean isMixin() {
        return dlg.isMixin();
    }

    @Override
    public boolean hasOrderableChildNodes() {
        return dlg.hasOrderableChildNodes();
    }

    @Override
    public boolean isQueryable() {
        return true;
    }

    @Override
    public String getPrimaryItemName() {
        String primaryItemName = dlg.getPrimaryItemName();
        if (primaryItemName != null) {
            return mapper.getJcrName(primaryItemName);
        } else {
            return null;
        }
    }

    @Override
    public PropertyDefinition[] getDeclaredPropertyDefinitions() {
        List<PropertyDefinitionDelegate> definitionDelegates = dlg.getPropertyDefinitionDelegates();
        PropertyDefinition[] result = new PropertyDefinition[definitionDelegates.size()];

        for (int i = 0; i < result.length; i++) {
            result[i] = new PropertyDefinitionImpl(this, mapper, valueFactory, definitionDelegates.get(i));
        }

        return result;
    }

    @Override
    public NodeDefinition[] getDeclaredChildNodeDefinitions() {
        List<NodeDefinitionDelegate> definitionDelegates = dlg.getChildNodeDefinitionDelegates();
        NodeDefinition[] result = new NodeDefinition[definitionDelegates.size()];

        for (int i = 0; i < result.length; i++) {
            result[i] = new NodeDefinitionImpl(manager, this, mapper, definitionDelegates.get(i));
        }

        return result;
    }

    @Override
    public NodeType[] getSupertypes() {
        try {
            Collection<NodeType> types = new ArrayList<NodeType>();
            Set<String> added = new HashSet<String>();
            Queue<String> queue = new LinkedList<String>(Arrays.asList(getDeclaredSupertypeNames()));
            while (!queue.isEmpty()) {
                String name = queue.remove();
                if (added.add(name)) {
                    NodeType type = manager.getNodeType(name);
                    types.add(type);
                    queue.addAll(Arrays.asList(type.getDeclaredSupertypeNames()));
                }
            }
            return types.toArray(new NodeType[types.size()]);
        } catch (RepositoryException e) {
            throw new IllegalStateException("Inconsistent node type: " + this, e);
        }
    }

    @Override
    public NodeType[] getDeclaredSupertypes() {
        try {
            String[] names = getDeclaredSupertypeNames();
            NodeType[] types = new NodeType[names.length];
            for (int i = 0; i < types.length; i++) {
                types[i] = manager.getNodeType(names[i]);
            }
            return types;
        } catch (RepositoryException e) {
            throw new IllegalStateException("Inconsistent node type: " + this, e);
        }
    }

    @Override
    public NodeTypeIterator getSubtypes() {
        try {
            Collection<NodeType> types = new ArrayList<NodeType>();
            NodeTypeIterator iterator = manager.getAllNodeTypes();
            while (iterator.hasNext()) {
                NodeType type = iterator.nextNodeType();
                if (type.isNodeType(getName()) && !isNodeType(type.getName())) {
                    types.add(type);
                }
            }
            return new NodeTypeIteratorAdapter(types);
        } catch (RepositoryException e) {
            throw new IllegalStateException("Inconsistent node type: " + this, e);
        }
    }

    @Override
    public NodeTypeIterator getDeclaredSubtypes() {
        try {
            Collection<NodeType> types = new ArrayList<NodeType>();
            NodeTypeIterator iterator = manager.getAllNodeTypes();
            while (iterator.hasNext()) {
                NodeType type = iterator.nextNodeType();
                String name = type.getName();
                if (type.isNodeType(getName()) && !isNodeType(name)) {
                    List<String> declaredSuperTypeNames = Arrays.asList(type.getDeclaredSupertypeNames());
                    if (declaredSuperTypeNames.contains(name)) {
                        types.add(type);
                    }
                }
            }
            return new NodeTypeIteratorAdapter(types);
        } catch (RepositoryException e) {
            throw new IllegalStateException("Inconsistent node type: " + this, e);
        }
    }

    @Override
    public boolean isNodeType(String nodeTypeName) {
        String oakName = mapper.getOakName(nodeTypeName);
        if (dlg.getName().equals(oakName)) {
            return true;
        }
        for (NodeType type : getSupertypes()) {
            if (((NodeTypeImpl) type).dlg.getName().equals(oakName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public PropertyDefinition[] getPropertyDefinitions() {
        Collection<PropertyDefinition> definitions = new ArrayList<PropertyDefinition>();
        for (NodeType type : getSupertypes()) {
            definitions.addAll(Arrays.asList(type.getDeclaredPropertyDefinitions()));
        }
        definitions.addAll(Arrays.asList(getDeclaredPropertyDefinitions()));
        return definitions.toArray(new PropertyDefinition[definitions.size()]);
    }

    @Override
    public NodeDefinition[] getChildNodeDefinitions() {
        Collection<NodeDefinition> definitions = new ArrayList<NodeDefinition>();
        for (NodeType type : getSupertypes()) {
            definitions.addAll(Arrays.asList(type.getDeclaredChildNodeDefinitions()));
        }
        definitions.addAll(Arrays.asList(getDeclaredChildNodeDefinitions()));
        return definitions.toArray(new NodeDefinition[definitions.size()]);
    }

    @Override
    public boolean canSetProperty(String propertyName, Value value) {
        return true; // TODO
    }

    @Override
    public boolean canSetProperty(String propertyName, Value[] values) {
        return true; // TODO
    }

    @Override
    public boolean canAddChildNode(String childNodeName) {
        return true; // TODO
    }

    @Override
    public boolean canAddChildNode(String childNodeName, String nodeTypeName) {
        return true; // TODO
    }

    @Override
    public boolean canRemoveItem(String itemName) {
        return true; // TODO
    }

    @Override
    public boolean canRemoveNode(String nodeName) {
        return true; // TODO
    }

    @Override
    public boolean canRemoveProperty(String propertyName) {
        return true; // TODO
    }

}

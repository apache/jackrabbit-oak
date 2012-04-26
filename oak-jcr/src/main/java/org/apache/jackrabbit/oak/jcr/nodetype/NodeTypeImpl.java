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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.OnParentVersionAction;

import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.namepath.NameMapper;

class NodeTypeImpl implements NodeType {

    private static final Pattern CND_PATTERN = Pattern.compile(
            "( > (\\S+(, \\S+)*))?(\n  mixin)?(\n  abstract)?"
            + "(\n  orderable)?(\n  primaryitem (\\S+))?(\n.*)*");

    private static final Pattern DEF_PATTERN = Pattern.compile(
            "  ([\\+\\-]) (\\S+) \\((.+?)\\)( = (\\S+))"
            + "(( (mandatory|autocreated|protected|multiple))*)"
            + "( ([A-Z])+)?.*");

    private final NodeTypeManager manager;

    private final NameMapper mapper;

    private final String name;

    private final String[] declaredSuperTypeNames;

    private boolean isAbstract;

    private final boolean isMixin;

    private final boolean hasOrderableChildNodes;

    private final String primaryItemName;

    private final List<PropertyDefinition> declaredPropertyDefinitions =
            new ArrayList<PropertyDefinition>();

    private final List<NodeDefinition> declaredChildNodeDefinitions =
            new ArrayList<NodeDefinition>();

    public NodeTypeImpl(
            NodeTypeManager manager, NameMapper mapper, String name,
            String cnd) {
        this.manager = manager;
        this.mapper = mapper;
        this.name = name;

        Matcher matcher = CND_PATTERN.matcher(cnd.replace("\r\n", "\n"));
        matcher.matches();

        this.declaredSuperTypeNames = matcher.group(2).split(", ");
        this.isAbstract = matcher.group(5) != null;
        this.isMixin = matcher.group(4) != null;
        this.hasOrderableChildNodes = matcher.group(7) != null;
        this.primaryItemName = matcher.group(8);

        for (String line : matcher.group(9).split("\n")) {
            matcher = DEF_PATTERN.matcher(line);
            matcher.matches();

            String defName = matcher.group(2);
            String defType = matcher.group(3);

            boolean mandatory = matcher.group(6).contains(" mandatory");
            boolean autoCreated = matcher.group(6).contains(" autocreated");
            boolean isProtected = matcher.group(6).contains(" protected");
            boolean multiple = matcher.group(6).contains(" multiple");

            int onParentVersionAction = OnParentVersionAction.COPY;
            if (matcher.group(9) != null) {
                onParentVersionAction =
                        OnParentVersionAction.valueFromName(matcher.group(9));
            }

            if ("+".equals(matcher.group(1))) {
                declaredChildNodeDefinitions.add(new NodeDefinitionImpl(
                        this, mapper, defName, autoCreated, mandatory,
                        onParentVersionAction, isProtected, manager,
                        defType.split(", "), matcher.group(5), false));
            } else {
                declaredPropertyDefinitions.add(new PropertyDefinitionImpl(
                        this, mapper, defName, autoCreated, mandatory,
                        onParentVersionAction, isProtected,
                        PropertyType.valueFromName(defType), multiple));
            }
        }
    }

    @Override
    public String getName() {
        return mapper.getJcrName(name);
    }

    @Override
    public String[] getDeclaredSupertypeNames() {
        String[] names = new String[declaredSuperTypeNames.length];
        for (int i = 0; i < names.length; i++) {
            names[i] = mapper.getJcrName(declaredSuperTypeNames[i]);
        }
        return names;
    }

    @Override
    public boolean isAbstract() {
        return isAbstract;
    }

    @Override
    public boolean isMixin() {
        return isMixin;
    }

    @Override
    public boolean hasOrderableChildNodes() {
        return hasOrderableChildNodes;
    }

    @Override
    public boolean isQueryable() {
        return true;
    }

    @Override
    public String getPrimaryItemName() {
        if (primaryItemName != null) {
            return mapper.getJcrName(primaryItemName);
        } else {
            return null;
        }
    }

    @Override
    public PropertyDefinition[] getDeclaredPropertyDefinitions() {
        return declaredPropertyDefinitions.toArray(
                new PropertyDefinition[declaredPropertyDefinitions.size()]);
    }

    @Override
    public NodeDefinition[] getDeclaredChildNodeDefinitions() {
        return declaredChildNodeDefinitions.toArray(
                new NodeDefinition[declaredChildNodeDefinitions.size()]);
    }

    @Override
    public NodeType[] getSupertypes() {
        return null;
    }

    @Override
    public NodeType[] getDeclaredSupertypes() {
        return null;
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
            throw new IllegalStateException(
                    "Inconsistent node type: " + this, e);
        }
    }

    @Override
    public NodeTypeIterator getDeclaredSubtypes() {
        try {
            Collection<NodeType> types =
                    new ArrayList<NodeType>(declaredSuperTypeNames.length);
            for (int i = 0; i < declaredSuperTypeNames.length; i++) {
                types.add(manager.getNodeType(
                        mapper.getJcrName(declaredSuperTypeNames[i])));
            }
            return new NodeTypeIteratorAdapter(types);
        } catch (RepositoryException e) {
            throw new IllegalStateException(
                    "Inconsistent node type: " + this, e);
        }
    }

    @Override
    public boolean isNodeType(String nodeTypeName) {
        if (nodeTypeName.equals(getName())) {
            return true;
        }
        for (NodeType type : getSupertypes()) {
            if (nodeTypeName.equals(type.getName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public PropertyDefinition[] getPropertyDefinitions() {
        Collection<PropertyDefinition> definitions =
                new ArrayList<PropertyDefinition>();
        for (NodeType type : getSupertypes()) {
            definitions.addAll(Arrays.asList(
                    type.getDeclaredPropertyDefinitions()));
        }
        definitions.addAll(Arrays.asList(getDeclaredPropertyDefinitions()));
        return definitions.toArray(new PropertyDefinition[definitions.size()]);
    }

    @Override
    public NodeDefinition[] getChildNodeDefinitions() {
        Collection<NodeDefinition> definitions =
                new ArrayList<NodeDefinition>();
        for (NodeType type : getSupertypes()) {
            definitions.addAll(Arrays.asList(
                    type.getDeclaredChildNodeDefinitions()));
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

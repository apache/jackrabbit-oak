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

import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.namepath.NameMapper;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.version.OnParentVersionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class NodeTypeImpl implements NodeType {

    private static final Pattern CND_PATTERN = Pattern.compile(
            "( > (([\\S, ]+)))?(\n  mixin)?(\n  abstract)?"
            + "(\n  orderable)?(\n  primaryitem (\\S+))?\n?(.*)",
            Pattern.DOTALL);

    private static final Pattern DEF_PATTERN = Pattern.compile(
            "  ([\\+\\-]) (\\S+) \\((\\S+?)\\)( = (\\S+))?"
            + "(( (mandatory|autocreated|protected|multiple|sns))*)"
            + "( ([A-Z]+))?(.*)");

    private final NodeTypeManager manager;

    private final NameMapper mapper;

    private final String name;

    private final String[] declaredSuperTypeNames;

    private final boolean isAbstract;

    private final boolean mixin;

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

        Matcher matcher = CND_PATTERN.matcher(cnd);
        matcher.matches();

        this.isAbstract = matcher.group(5) != null;
        this.mixin = matcher.group(4) != null;
        this.hasOrderableChildNodes = matcher.group(6) != null;
        this.primaryItemName = matcher.group(7);

        String supertypes = matcher.group(2);
        if (supertypes != null)  {
            this.declaredSuperTypeNames = supertypes.split(", ");
        } else if (mixin) {
            this.declaredSuperTypeNames = new String[0];
        } else {
            this.declaredSuperTypeNames = new String[] { "nt:base" };
        }

        String defs = matcher.group(9);
        if (defs != null && !defs.isEmpty()) {
            for (String line : defs.split("\n")) {
                matcher = DEF_PATTERN.matcher(line);
                if (!matcher.matches()) {
                    continue;
                }

                String defName = matcher.group(2);
                String defType = matcher.group(3);

                boolean mandatory = matcher.group(6).contains(" mandatory");
                boolean autoCreated = matcher.group(6).contains(" autocreated");
                boolean isProtected = matcher.group(6).contains(" protected");
                boolean multiple = matcher.group(6).contains(" multiple");
                boolean allowSNS = matcher.group(6).contains(" sns");

                int onParentVersionAction = OnParentVersionAction.COPY;
                if (matcher.group(10) != null) {
                    onParentVersionAction =
                            OnParentVersionAction.valueFromName(matcher.group(10));
                }

                if ("+".equals(matcher.group(1))) {
                    declaredChildNodeDefinitions.add(new NodeDefinitionImpl(
                            this, mapper, defName, autoCreated, mandatory,
                            onParentVersionAction, isProtected, manager,
                            defType.split(", "), matcher.group(5), allowSNS));
                } else if ("-".equals(matcher.group(1))) {
                    declaredPropertyDefinitions.add(new PropertyDefinitionImpl(
                            this, mapper, defName, autoCreated, mandatory,
                            onParentVersionAction, isProtected,
                            valueFromName(defType), multiple));
                }
            }
        }
    }

    private static int valueFromName(String name) {
        if (name.equalsIgnoreCase(PropertyType.TYPENAME_STRING)) {
            return PropertyType.STRING;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_BINARY)) {
            return PropertyType.BINARY;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_BOOLEAN)) {
            return PropertyType.BOOLEAN;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_LONG)) {
            return PropertyType.LONG;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_DOUBLE)) {
            return PropertyType.DOUBLE;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_DECIMAL)) {
            return PropertyType.DECIMAL;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_DATE)) {
            return PropertyType.DATE;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_NAME)) {
            return PropertyType.NAME;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_PATH)) {
            return PropertyType.PATH;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_REFERENCE)) {
            return PropertyType.REFERENCE;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_WEAKREFERENCE)) {
            return PropertyType.WEAKREFERENCE;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_URI)) {
            return PropertyType.URI;
        } else if (name.equalsIgnoreCase(PropertyType.TYPENAME_UNDEFINED)) {
            return PropertyType.UNDEFINED;
        } else {
            throw new IllegalArgumentException("unknown type: " + name);
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
        return mixin;
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
        try {
            Collection<NodeType> types = new ArrayList<NodeType>();
            Set<String> added = new HashSet<String>();
            Queue<String> queue = new LinkedList<String>(
                    Arrays.asList(getDeclaredSupertypeNames()));
            if (!isMixin()) {
                queue.add(mapper.getJcrName(mapper.getOakName(NodeType.NT_BASE)));
            }
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
            throw new IllegalStateException(
                    "Inconsistent node type: " + this, e);
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
            throw new IllegalStateException(
                    "Inconsistent node type: " + this, e);
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
            throw new IllegalStateException(
                    "Inconsistent node type: " + this, e);
        }
    }

    @Override
    public NodeTypeIterator getDeclaredSubtypes() {
        try {
            Collection<NodeType> types = new ArrayList<NodeType>();
            NodeTypeIterator iterator = manager.getAllNodeTypes();
            while (iterator.hasNext()) {
                NodeType type = iterator.nextNodeType();
                if (type.isNodeType(getName()) && !isNodeType(type.getName())) {
                    List<String> declaredSuperTypeNames = Arrays.asList(type.getDeclaredSupertypeNames());
                    if (declaredSuperTypeNames.contains(type)) {
                        types.add(type);
                    }
                }
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

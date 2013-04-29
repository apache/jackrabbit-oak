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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.PropertyDefinition;

import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.JcrNameParser;
import org.apache.jackrabbit.oak.namepath.JcrPathParser;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.nodetype.constraint.Constraints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.JcrConstants.JCR_CHILDNODEDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_HASORDERABLECHILDNODES;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYITEMNAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_PROPERTYDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_SUPERTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_QUERYABLE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.RESIDUAL_NAME;

/**
 * <pre>
 * [nt:nodeType]
 * - jcr:nodeTypeName (NAME) protected mandatory
 * - jcr:supertypes (NAME) protected multiple
 * - jcr:isAbstract (BOOLEAN) protected mandatory
 * - jcr:isQueryable (BOOLEAN) protected mandatory
 * - jcr:isMixin (BOOLEAN) protected mandatory
 * - jcr:hasOrderableChildNodes (BOOLEAN) protected mandatory
 * - jcr:primaryItemName (NAME) protected
 * + jcr:propertyDefinition (nt:propertyDefinition) = nt:propertyDefinition protected sns
 * + jcr:childNodeDefinition (nt:childNodeDefinition) = nt:childNodeDefinition protected sns
 * </pre>
 */
class NodeTypeImpl extends AbstractTypeDefinition implements NodeType {

    private static final Logger log = LoggerFactory.getLogger(NodeTypeImpl.class);

    private static final PropertyDefinition[] NO_PROPERTY_DEFINITIONS =
            new PropertyDefinition[0];

    private static final NodeDefinition[] NO_NODE_DEFINITIONS =
            new NodeDefinition[0];

    private static final NodeType[] NO_NODE_TYPES = new NodeType[0];

    private static final String[] NO_NAMES = new String[0];

    NodeTypeImpl(Tree type, NamePathMapper mapper) {
        super(type, mapper);
    }

    private String getOakName() {
        return getOakName(definition);
    }

    private String getOakName(Tree tree) {
        PropertyState property = tree.getProperty(JCR_NODETYPENAME);
        if (property != null) {
            return property.getValue(Type.NAME);
        } else {
            return tree.getName();
        }
    }

    //-----------------------------------------------------------< NodeType >---

    @Override
    public String getName() {
        return mapper.getJcrName(getOakName());
    }

    @Override
    public String[] getDeclaredSupertypeNames() {
        String[] names = getNames(JCR_SUPERTYPES);
        if (names != null) {
            for (int i = 0; i < names.length; i++) {
                names[i] = mapper.getJcrName(names[i]);
            }
        } else {
            names = NO_NAMES;
        }
        return names;
    }

    @Override
    public boolean isAbstract() {
        return getBoolean(JCR_IS_ABSTRACT);
    }

    @Override
    public boolean isMixin() {
        return getBoolean(JCR_ISMIXIN);
    }

    @Override
    public boolean hasOrderableChildNodes() {
        return getBoolean(JCR_HASORDERABLECHILDNODES);
    }

    @Override
    public boolean isQueryable() {
        return getBoolean(JCR_IS_QUERYABLE);
    }

    @Override
    public String getPrimaryItemName() {
        String oakName = getName(JCR_PRIMARYITEMNAME);
        if (oakName != null) {
            return mapper.getJcrName(oakName);
        } else {
            return null;
        }
    }

    @Override
    public PropertyDefinition[] getDeclaredPropertyDefinitions() {
        List<PropertyDefinition> definitions = Lists.newArrayList();
        for (Tree child : definition.getChildren()) {
            if (child.getName().startsWith(JCR_PROPERTYDEFINITION)) {
                definitions.add(new PropertyDefinitionImpl(child, this, mapper));
            }
        }
        return definitions.toArray(NO_PROPERTY_DEFINITIONS);
    }

    @Override
    public NodeDefinition[] getDeclaredChildNodeDefinitions() {
        List<NodeDefinition> definitions = Lists.newArrayList();
        for (Tree child : definition.getChildren()) {
            if (child.getName().startsWith(JCR_CHILDNODEDEFINITION)) {
                definitions.add(new NodeDefinitionImpl(child, this, mapper));
            }
        }
        return definitions.toArray(NO_NODE_DEFINITIONS);
    }

    @Override
    public NodeType[] getSupertypes() {
        Map<String, NodeType> supertypes = Maps.newLinkedHashMap();
        addSupertypes(definition, supertypes);
        return supertypes.values().toArray(NO_NODE_TYPES);
    }

    private void addSupertypes(Tree type, Map<String, NodeType> supertypes) {
        PropertyState property = type.getProperty(JCR_SUPERTYPES);
        if (property != null) {
            Tree root = definition.getParentOrNull();
            for (String oakName : property.getValue(Type.NAMES)) {
                if (!supertypes.containsKey(oakName)) {
                    Tree supertype = root.getChildOrNull(oakName);
                    checkState(supertype != null);
                    supertypes.put(
                            oakName, new NodeTypeImpl(supertype, mapper));
                    addSupertypes(supertype, supertypes);
                }
            }
        }
    }

    @Override
    public NodeType[] getDeclaredSupertypes() {
        NodeType[] supertypes = NO_NODE_TYPES;
        String[] oakNames = getNames(JCR_SUPERTYPES);
        if (oakNames != null && oakNames.length > 0) {
            supertypes = new NodeType[oakNames.length];
            Tree root = definition.getParentOrNull();
            for (int i = 0; i < oakNames.length; i++) {
                Tree type = root.getChildOrNull(oakNames[i]);
                checkState(type != null);
                supertypes[i] = new NodeTypeImpl(type, mapper);
            }
        }
        return supertypes;
    }

    @Override
    public NodeTypeIterator getSubtypes() {
        Map<String, Set<String>> inheritance = Maps.newHashMap();
        
        Tree root = definition.getParentOrNull();
        for (Tree child : root.getChildren()) {
            String oakName = getOakName(child);
            PropertyState supertypes = child.getProperty(JCR_SUPERTYPES);
            if (supertypes != null) {
                for (String supername : supertypes.getValue(Type.NAMES)) {
                    Set<String> subtypes = inheritance.get(supername);
                    if (subtypes == null) {
                        subtypes = Sets.newHashSet();
                        inheritance.put(supername, subtypes);
                    }
                    subtypes.add(oakName);
                }
            }
        }

        Map<String, NodeType> subtypes = Maps.newHashMap();
        addSubtypes(getOakName(), subtypes, root, inheritance);
        return new NodeTypeIteratorAdapter(subtypes.values());
    }

    private void addSubtypes(
            String typeName, Map<String, NodeType> subtypes,
            Tree root, Map<String, Set<String>> inheritance) {
        Set<String> subnames = inheritance.get(typeName);
        if (subnames != null) {
            for (String subname : subnames) {
                if (!subtypes.containsKey(subname)) {
                    Tree tree = root.getChildOrNull(subname);
                    subtypes.put(subname, new NodeTypeImpl(tree, mapper));
                }
            }
        }
    }

    @Override
    public NodeTypeIterator getDeclaredSubtypes() {
        List<NodeType> subtypes = Lists.newArrayList();

        String oakName = getOakName();
        Tree root = definition.getParentOrNull();
        for (Tree child : root.getChildren()) {
            PropertyState supertypes = child.getProperty(JCR_SUPERTYPES);
            if (supertypes != null) {
                for (String name : supertypes.getValue(Type.NAMES)) {
                    if (oakName.equals(name)) {
                        subtypes.add(new NodeTypeImpl(child, mapper));
                        break;
                    }
                }
            }
        }

        return new NodeTypeIteratorAdapter(subtypes);
    }

    @Override
    public boolean isNodeType(String nodeTypeName) {
        String oakName = mapper.getOakNameOrNull(nodeTypeName);
        return internalIsNodeType(oakName);
    }

    @Override
    public PropertyDefinition[] getPropertyDefinitions() {
        Collection<PropertyDefinition> definitions = internalGetPropertyDefinitions();
        return definitions.toArray(new PropertyDefinition[definitions.size()]);
    }

    @Override
    public NodeDefinition[] getChildNodeDefinitions() {
        Collection<NodeDefinition> definitions = internalGetChildDefinitions();
        return definitions.toArray(new NodeDefinition[definitions.size()]);
    }

    @Override
    public boolean canSetProperty(String propertyName, Value value) {
        if (value == null) {
            return canRemoveProperty(propertyName);
        }

        try {
            EffectiveNodeType effective =
                    new EffectiveNodeType(this, getManager());
            PropertyDefinition def = effective.getPropertyDefinition(
                    propertyName, false, value.getType(), false);
            return !def.isProtected() &&
                    meetsTypeConstraints(value, def.getRequiredType()) &&
                    meetsValueConstraints(value, def.getValueConstraints());
        } catch (RepositoryException e) {  // TODO don't use exceptions for flow control. Use internal method in ReadOnlyNodeTypeManager instead.
            log.debug(e.getMessage());
            return false;
        }
    }

    @Override
    public boolean canSetProperty(String propertyName, Value[] values) {
        if (values == null) {
            return canRemoveProperty(propertyName);
        }

        try {
            int type = (values.length == 0) ? PropertyType.STRING : values[0].getType();
            EffectiveNodeType effective =
                    new EffectiveNodeType(this, getManager());
            PropertyDefinition def = effective.getPropertyDefinition(
                    propertyName, true, type, false);
            return !def.isProtected() &&
                    meetsTypeConstraints(values, def.getRequiredType()) &&
                    meetsValueConstraints(values, def.getValueConstraints());
        } catch (RepositoryException e) {  // TODO don't use exceptions for flow control. Use internal method in ReadOnlyNodeTypeManager instead.
            log.debug(e.getMessage());
            return false;
        }
    }

        @Override
    public boolean canAddChildNode(String childNodeName) {
        // FIXME: properly calculate matching definition
        for (NodeDefinition definition : getChildNodeDefinitions()) {
            String name = definition.getName();
            if (matches(childNodeName, name) || RESIDUAL_NAME.equals(name)) {
                return !definition.isProtected() && definition.getDefaultPrimaryType() != null;
            }
        }
        return false;
    }

    @Override
    public boolean canAddChildNode(String childNodeName, String nodeTypeName) {
        NodeType type;
        try {
            type = getManager().getNodeType(nodeTypeName);
            if (type.isAbstract()) {
                return false;
            }
        } catch (NoSuchNodeTypeException e) {
            return false;
        } catch (RepositoryException e) {
            log.warn("Unable to access node type " + nodeTypeName, e);
            return false;
        }
        // FIXME: properly calculate matching definition
        for (NodeDefinition definition : getChildNodeDefinitions()) {
            String name = definition.getName();
            if (matches(childNodeName, name) || RESIDUAL_NAME.equals(name)) {
                if (definition.isProtected()) {
                    return false;
                }
                for (String required : definition.getRequiredPrimaryTypeNames()) {
                    if (type.isNodeType(required)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public boolean canRemoveItem(String itemName) {
        List<ItemDefinition> definitions = Lists.newArrayList();
        definitions.addAll(Arrays.asList(getChildNodeDefinitions()));
        definitions.addAll(Arrays.asList(getPropertyDefinitions()));
        return internalCanRemoveItem(itemName, definitions);
    }

    @Override
    public boolean canRemoveNode(String nodeName) {
        return internalCanRemoveItem(nodeName, Arrays.asList(getChildNodeDefinitions()));
    }

    @Override
    public boolean canRemoveProperty(String propertyName) {
        return internalCanRemoveItem(propertyName, Arrays.asList(getPropertyDefinitions()));
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public String toString() {
        return getName();
    }

    //-----------------------------------------------------------< internal >---

    private boolean internalCanRemoveItem(String itemName,
                                          Iterable<? extends ItemDefinition> definitions) {
        // FIXME: should properly calculate matching definition taking residual definitions into account.
        for (ItemDefinition definition : definitions) {
            String name = definition.getName();
            if (matches(itemName, name)) {
                if (definition.isMandatory() || definition.isProtected()) {
                    return false;
                }
            }
        }
        return definitions.iterator().hasNext();
    }

    private ReadOnlyNodeTypeManager getManager() {
        final Tree types = definition.getParentOrNull();
        return new ReadOnlyNodeTypeManager() {
            @Override @CheckForNull
            protected Tree getTypes() {
                return types;
            }
        };
    }

    boolean internalIsNodeType(String oakName) {
        if (getOakName().equals(oakName)) {
            return true;
        }
        for (NodeType type : getDeclaredSupertypes()) {
            if (((NodeTypeImpl) type).internalIsNodeType(oakName)) {
                return true;
            }
        }
        return false;
    }

    Collection<NodeDefinition> internalGetChildDefinitions() {
        // TODO distinguish between additive and overriding node definitions. See 3.7.6.8 Item Definitions in Subtypes
        Collection<NodeDefinition> definitions = new ArrayList<NodeDefinition>();
        definitions.addAll(Arrays.asList(getDeclaredChildNodeDefinitions()));
        for (NodeType type : getSupertypes()) {
            definitions.addAll(Arrays.asList(type.getDeclaredChildNodeDefinitions()));
        }
        return definitions;
    }

    Collection<PropertyDefinition> internalGetPropertyDefinitions() {
        // TODO distinguish between additive and overriding property definitions. See 3.7.6.8 Item Definitions in Subtypes
        Collection<PropertyDefinition> definitions = new ArrayList<PropertyDefinition>();
        definitions.addAll(Arrays.asList(getDeclaredPropertyDefinitions()));
        for (NodeType type : getSupertypes()) {
            definitions.addAll(Arrays.asList(type.getDeclaredPropertyDefinitions()));
        }
        return definitions;
    }

    Iterable<PropertyDefinition> getDeclaredNamedPropertyDefinitions(String oakName) {
        Tree named = definition.getChildOrNull("oak:namedPropertyDefinitions");
        if (named != null) {
            String escapedName;
            if (JCR_PRIMARYTYPE.equals(oakName)) {
                escapedName = "oak:primaryType";
            } else if (JCR_MIXINTYPES.equals(oakName)) {
                escapedName = "oak:mixinTypes";
            } else if (JCR_UUID.equals(oakName)) {
                escapedName = "oak:uuid";
            } else {
                escapedName = oakName;
            }
            Tree definitions = named.getChildOrNull(escapedName);
            if (definitions != null) {
                return Iterables.transform(
                        definitions.getChildren(),
                        new Function<Tree, PropertyDefinition>() {
                            @Override
                            public PropertyDefinition apply(Tree input) {
                                return new PropertyDefinitionImpl(
                                        input, NodeTypeImpl.this, mapper);
                            }
                        });
            }
        }
        return Collections.emptyList();
    }

    Iterable<PropertyDefinition> getDeclaredResidualPropertyDefinitions() {
        Tree definitions = definition.getChildOrNull("oak:residualPropertyDefinitions");
        if (definitions != null) {
            return Iterables.transform(
                    definitions.getChildren(),
                    new Function<Tree, PropertyDefinition>() {
                        @Override
                        public PropertyDefinition apply(Tree input) {
                            return new PropertyDefinitionImpl(
                                    input, NodeTypeImpl.this, mapper);
                        }
                    });
        }
        return Collections.emptyList();
    }

    Iterable<NodeDefinition> getDeclaredNamedNodeDefinitions(String oakName) {
        Tree named = definition.getChildOrNull("oak:namedChildNodeDefinitions");
        if (named != null) {
            Tree definitions = named.getChildOrNull(oakName);
            if (definitions != null) {
                return Iterables.transform(
                        definitions.getChildren(),
                        new Function<Tree, NodeDefinition>() {
                            @Override
                            public NodeDefinition apply(Tree input) {
                                return new NodeDefinitionImpl(
                                        input, NodeTypeImpl.this, mapper);
                            }
                        });
            }
        }
        return Collections.emptyList();
    }

    Iterable<NodeDefinition> getDeclaredResidualNodeDefinitions() {
        Tree definitions = definition.getChildOrNull("oak:residualChildNodeDefinitions");
        if (definitions != null) {
            return Iterables.transform(
                    definitions.getChildren(),
                    new Function<Tree, NodeDefinition>() {
                        @Override
                        public NodeDefinition apply(Tree input) {
                            return new NodeDefinitionImpl(
                                    input, NodeTypeImpl.this, mapper);
                        }
                    });
        }
        return Collections.emptyList();
    }

    //--------------------------------------------------------------------------
    private static boolean meetsTypeConstraints(Value value, int requiredType) {
        try {
            switch (requiredType) {
                case PropertyType.STRING:
                    value.getString();
                    return true;
                case PropertyType.BINARY:
                    value.getBinary();
                    return true;
                case PropertyType.LONG:
                    value.getLong();
                    return true;
                case PropertyType.DOUBLE:
                    value.getDouble();
                    return true;
                case PropertyType.DATE:
                    value.getDate();
                    return true;
                case PropertyType.BOOLEAN:
                    value.getBoolean();
                    return true;
                case PropertyType.NAME: {
                    int type = value.getType();
                    return type != PropertyType.DOUBLE &&
                            type != PropertyType.LONG &&
                            type != PropertyType.BOOLEAN &&
                            JcrNameParser.validate(value.getString());
                }
                case PropertyType.PATH: {
                    int type = value.getType();
                    return type != PropertyType.DOUBLE &&
                            type != PropertyType.LONG &&
                            type != PropertyType.BOOLEAN &&
                            JcrPathParser.validate(value.getString());
                }
                case PropertyType.REFERENCE:
                case PropertyType.WEAKREFERENCE:
                    return IdentifierManager.isValidUUID(value.getString());
                case PropertyType.URI:
                    new URI(value.getString());
                    return true;
                case PropertyType.DECIMAL:
                    value.getDecimal();
                    return true;
                case PropertyType.UNDEFINED:
                    return true;
                default:
                    log.warn("Invalid property type value: " + requiredType);
                    return false;
            }
        } catch (RepositoryException e) {
            return false;
        } catch (URISyntaxException e) {
            return false;
        }
    }

    private static boolean meetsTypeConstraints(Value[] values, int requiredType) {
        // Constraints must be met by all values
        for (Value value : values) {
            if (!meetsTypeConstraints(value, requiredType)) {
                return false;
            }
        }

        return true;
    }

    private static boolean meetsValueConstraints(Value value, String[] constraints) {
        if (constraints == null || constraints.length == 0) {
            return true;
        }

        // Any of the constraints must be met
        for (String constraint : constraints) {
            if (Constraints.valueConstraint(value.getType(), constraint).apply(value)) {
                return true;
            }
        }

        return false;
    }

    private static boolean meetsValueConstraints(Value[] values, String[] constraints) {
        if (constraints == null || constraints.length == 0) {
            return true;
        }

        // Constraints must be met by all values
        for (Value value : values) {
            if (!meetsValueConstraints(value, constraints)) {
                return false;
            }
        }

        return true;
    }

    private boolean matches(String childNodeName, String name) {
        String oakChildName = mapper.getOakNameOrNull(childNodeName);
        String oakName = mapper.getOakNameOrNull(name);
        // TODO need a better way to handle SNS
        return oakChildName != null && oakChildName.startsWith(oakName);
    }

}

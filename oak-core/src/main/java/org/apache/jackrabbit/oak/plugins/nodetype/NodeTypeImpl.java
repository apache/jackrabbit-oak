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
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newTreeMap;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.JcrConstants.JCR_HASORDERABLECHILDNODES;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYITEMNAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SUPERTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.NT_CHILDNODEDEFINITION;
import static org.apache.jackrabbit.JcrConstants.NT_PROPERTYDEFINITION;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_IS_QUERYABLE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_DECLARING_NODE_TYPE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_MIXIN_TYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_PRIMARY_TYPE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_RESIDUAL_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_RESIDUAL_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_UUID;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.RESIDUAL_NAME;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.ItemDefinition;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeDefinition;
import javax.jcr.nodetype.NodeTypeIterator;
import javax.jcr.nodetype.PropertyDefinition;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.commons.cnd.CompactNodeTypeDefWriter;
import org.apache.jackrabbit.commons.iterator.NodeTypeIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.JcrNameParser;
import org.apache.jackrabbit.oak.namepath.JcrPathParser;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.nodetype.constraint.Constraints;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /**
     * Returns the declared property definitions in their original order.
     *
     * @return declared property definitions
     */
    @Override @Nonnull
    public PropertyDefinition[] getDeclaredPropertyDefinitions() {
        Map<Integer, PropertyDefinition> definitions = newTreeMap();
        for (Tree child : Iterables.filter(definition.getChildren(), PrimaryTypePredicate.PROPERTY_DEF_PREDICATE)) {
            definitions.put(getIndex(child), new PropertyDefinitionImpl(child, this, mapper));
        }
        return definitions.values().toArray(NO_PROPERTY_DEFINITIONS);
    }

    /**
     * Returns the declared child node definitions in their original order.
     *
     * @return declared child node definitions
     */
    @Override @Nonnull
    public NodeDefinition[] getDeclaredChildNodeDefinitions() {
        Map<Integer, NodeDefinition> definitions = newTreeMap();
        for (Tree child : Iterables.filter(definition.getChildren(), PrimaryTypePredicate.CHILDNODE_DEF_PREDICATE)) {
            definitions.put(getIndex(child), new NodeDefinitionImpl(child, this, mapper));
        }
        return definitions.values().toArray(NO_NODE_DEFINITIONS);
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
            Tree root = definition.getParent();
            for (String oakName : property.getValue(Type.NAMES)) {
                if (!supertypes.containsKey(oakName)) {
                    Tree supertype = root.getChild(oakName);
                    checkState(supertype.exists());
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
            Tree root = definition.getParent();
            for (int i = 0; i < oakNames.length; i++) {
                Tree type = root.getChild(oakNames[i]);
                checkState(type.exists());
                supertypes[i] = new NodeTypeImpl(type, mapper);
            }
        }
        return supertypes;
    }

    @Override
    public NodeTypeIterator getSubtypes() {
        Map<String, Set<String>> inheritance = Maps.newHashMap();
        
        Tree root = definition.getParent();
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
                    Tree tree = root.getChild(subname);
                    subtypes.put(subname, new NodeTypeImpl(tree, mapper));
                }
            }
        }
    }

    @Override
    public NodeTypeIterator getDeclaredSubtypes() {
        List<NodeType> subtypes = Lists.newArrayList();

        String oakName = getOakName();
        Tree root = definition.getParent();
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
            EffectiveNodeTypeImpl effective =
                    new EffectiveNodeTypeImpl(this, getManager());
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
            EffectiveNodeTypeImpl effective =
                    new EffectiveNodeTypeImpl(this, getManager());
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

    /**
     * Returns the namespace neutral CND of the given node type definition.
     * @param def the node type definition
     * @return the CND
     */
    private static String getCnd(NodeTypeDefinition def) {
        StringWriter out = new StringWriter();
        CompactNodeTypeDefWriter cndWriter = new CompactNodeTypeDefWriter(out, new CompactNodeTypeDefWriter.NamespaceMapping(){
            @Override
            public String getNamespaceURI(String s) {
                return s;
            }
        }, false);
        try {
            cndWriter.write(def);
        } catch (IOException e) {
            // should never occur
            log.error("Error generating CND of " + def, e);
            throw new IllegalStateException(e);
        }
        return out.toString();
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public String toString() {
        return getName();
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof NodeType && getCnd(this).equals(getCnd((NodeType) o));
    }

    @Override
    public int hashCode() {
        return getCnd(this).hashCode();
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
        final Tree types = definition.getParent();
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

    List<PropertyDefinition> getDeclaredNamedPropertyDefinitions(String oakName) {
        String escapedName = oakName;
        if (JCR_PRIMARYTYPE.equals(oakName)) {
            escapedName = REP_PRIMARY_TYPE;
        } else if (JCR_MIXINTYPES.equals(oakName)) {
            escapedName = REP_MIXIN_TYPES;
        } else if (JCR_UUID.equals(oakName)) {
            escapedName = REP_UUID;
        }
        return getDeclaredPropertyDefs(definition
                .getChild(REP_NAMED_PROPERTY_DEFINITIONS)
                .getChild(escapedName));
    }

    List<PropertyDefinition> getDeclaredResidualPropertyDefinitions() {
        return getDeclaredPropertyDefs(definition
                .getChild(REP_RESIDUAL_PROPERTY_DEFINITIONS));
    }

    List<NodeDefinition> getDeclaredNamedNodeDefinitions(String oakName) {
        return getDeclaredNodeDefs(definition
                .getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)
                .getChild(oakName));
    }

    List<NodeDefinition> getDeclaredResidualNodeDefinitions() {
        return getDeclaredNodeDefs(definition
                .getChild(REP_RESIDUAL_CHILD_NODE_DEFINITIONS));
    }

    private List<PropertyDefinition> getDeclaredPropertyDefs(Tree definitions) {
        if (definitions.exists()) {
            List<PropertyDefinition> list = newArrayList();
            String typeName = getOakName();
            for (Tree def : definitions.getChildren()) {
                String declaringTypeName =
                        TreeUtil.getName(def, REP_DECLARING_NODE_TYPE);
                if (typeName.equals(declaringTypeName)) {
                    list.add(new PropertyDefinitionImpl(def, this, mapper));
                }
            }
            return list;
        } else {
            return emptyList();
        }
    }

    private List<NodeDefinition> getDeclaredNodeDefs(Tree defs) {
        if (defs.exists()) {
            List<NodeDefinition> list = newArrayList();
            String typeName = getOakName();
            for (Tree def : defs.getChildren()) {
                String declaringTypeName =
                        TreeUtil.getName(def, REP_DECLARING_NODE_TYPE);
                if (typeName.equals(declaringTypeName)) {
                    list.add(new NodeDefinitionImpl(def, this, mapper));
                }
            }
            return list;
        } else {
            return emptyList();
        }
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

    private static int getIndex(@Nonnull Tree tree) {
        String name = tree.getName();
        int i = name.lastIndexOf('[');
        return (i == -1) ? 1 : Integer.valueOf(name.substring(i+1, name.lastIndexOf(']')));
    }

    private boolean matches(String childNodeName, String name) {
        String oakChildName = mapper.getOakNameOrNull(childNodeName);
        String oakName = mapper.getOakNameOrNull(name);
        // TODO need a better way to handle SNS
        return oakChildName != null && oakChildName.startsWith(oakName);
    }

    private static final class PrimaryTypePredicate implements Predicate<Tree> {

        private static final PrimaryTypePredicate PROPERTY_DEF_PREDICATE = new PrimaryTypePredicate(NT_PROPERTYDEFINITION);
        private static final PrimaryTypePredicate CHILDNODE_DEF_PREDICATE = new PrimaryTypePredicate(NT_CHILDNODEDEFINITION);

        private final String primaryTypeName;

        private PrimaryTypePredicate(@Nonnull String primaryTypeName) {
            this.primaryTypeName = primaryTypeName;
        }

        @Override
        public boolean apply(Tree tree) {
            return primaryTypeName.equals(TreeUtil.getPrimaryTypeName(tree));
        }
    }
}

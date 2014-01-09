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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.JcrConstants.JCR_CHILDNODEDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MANDATORY;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_MULTIPLE;
import static org.apache.jackrabbit.JcrConstants.JCR_NAME;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_PROPERTYDEFINITION;
import static org.apache.jackrabbit.JcrConstants.JCR_PROTECTED;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDPRIMARYTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SUPERTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_CHILD_NODE_DEFINITION;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_DECLARING_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_HAS_PROTECTED_RESIDUAL_CHILD_NODES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_HAS_PROTECTED_RESIDUAL_PROPERTIES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_MANDATORY_CHILD_NODES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_MANDATORY_PROPERTIES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_MIXIN_SUBTYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_MIXIN_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_NAMED_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_NAMED_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_NAMED_SINGLE_VALUED_PROPERTIES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PRIMARY_SUBTYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PRIMARY_TYPE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PROPERTY_DEFINITION;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PROTECTED_CHILD_NODES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PROTECTED_PROPERTIES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_RESIDUAL_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_RESIDUAL_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_SUPERTYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_UUID;

/**
 * Editor that validates the consistency of the in-content node type registry
 * under {@code /jcr:system/jcr:nodeTypes} and maintains the access-optimized
 * versions of node type information as defined in {@code oak:NodeType}.
 *
 * <ul>
 *     <li>validate new definitions</li>
 *     <li>detect collisions,</li>
 *     <li>prevent circular inheritance,</li>
 *     <li>reject modifications to definitions that render existing content invalid,</li>
 *     <li>prevent un-registration of built-in node types.</li>
 * </ul>
 */
class RegistrationEditor extends DefaultEditor {

    private final NodeBuilder builder;

    private final Set<String> changedTypes = newHashSet();

    private final Set<String> removedTypes = newHashSet();

    private boolean modified = false;

    RegistrationEditor(NodeBuilder builder) {
        this.builder = checkNotNull(builder);
    }

    /**
     * Validates the inheritance hierarchy of the identified node type and
     * merges supertype information to the pre-compiled type information
     * fields. This makes full type information directly accessible without
     * having to traverse up the type hierarchy.
     *
     * @param types
     * @param type
     * @return
     * @throws CommitFailedException
     */
    private void mergeSupertypes(NodeBuilder types, NodeBuilder type)
            throws CommitFailedException {
        if (!type.hasProperty(OAK_SUPERTYPES)) {
            List<String> empty = Collections.emptyList();
            type.setProperty(OAK_SUPERTYPES, empty, NAMES);

            // - jcr:supertypes (NAME) protected multiple
            PropertyState supertypes = type.getProperty(JCR_SUPERTYPES);
            if (supertypes != null) {
                for (String supername : supertypes.getValue(NAMES)) {
                    if (types.hasChildNode(supername)) {
                        NodeBuilder supertype = types.child(supername);
                        mergeSupertypes(types, supertype);
                        mergeSupertype(type, supertype.getNodeState());
                    } else {
                        throw new CommitFailedException(
                                CONSTRAINT, 35,
                                "Missing supertype " + supername);
                    }
                }
            }

            if (!getBoolean(type, JCR_ISMIXIN)
                    && !contains(getNames(type, OAK_SUPERTYPES), NT_BASE)
                    && !NT_BASE.equals(type.getProperty(JCR_NODETYPENAME).getValue(NAME))) {
                if (types.hasChildNode(NT_BASE)) {
                    NodeBuilder supertype = types.child(NT_BASE);
                    mergeSupertypes(types, supertype);
                    mergeSupertype(type, supertype.getNodeState());
                } else {
                    throw new CommitFailedException(
                            CONSTRAINT, 35,
                            "Missing supertype " + NT_BASE);
                }
            }
        }
    }

    private boolean getBoolean(NodeBuilder builder, String name) {
        PropertyState property = builder.getProperty(name);
        return property != null && property.getValue(BOOLEAN);
    }

    private Iterable<String> getNames(NodeBuilder builder, String name) {
        PropertyState property = builder.getProperty(name);
        if (property != null) {
            return property.getValue(NAMES);
        } else {
            return Collections.<String>emptyList();
        }
    }

    private void mergeSupertype(NodeBuilder type, NodeState supertype) {
        String supername =
                supertype.getProperty(JCR_NODETYPENAME).getValue(NAME);
        addNameToList(type, OAK_SUPERTYPES, supername);
        mergeNameList(type, supertype, OAK_SUPERTYPES);
        mergeNameList(type, supertype, OAK_MANDATORY_PROPERTIES);
        mergeNameList(type, supertype, OAK_MANDATORY_CHILD_NODES);
        mergeNameList(type, supertype, OAK_PROTECTED_PROPERTIES);
        mergeNameList(type, supertype, OAK_PROTECTED_CHILD_NODES);
        if (supertype.getBoolean(OAK_HAS_PROTECTED_RESIDUAL_PROPERTIES)) {
            type.setProperty(OAK_HAS_PROTECTED_RESIDUAL_PROPERTIES, true);
        }
        if (supertype.getBoolean(OAK_HAS_PROTECTED_RESIDUAL_CHILD_NODES)) {
            type.setProperty(OAK_HAS_PROTECTED_RESIDUAL_CHILD_NODES, true);
        }
        mergeNameList(type, supertype, OAK_NAMED_SINGLE_VALUED_PROPERTIES);
        mergeSubtree(type, supertype, OAK_NAMED_PROPERTY_DEFINITIONS, 2);
        mergeSubtree(type, supertype, OAK_RESIDUAL_PROPERTY_DEFINITIONS, 1);
        mergeSubtree(type, supertype, OAK_NAMED_CHILD_NODE_DEFINITIONS, 2);
        mergeSubtree(type, supertype, OAK_RESIDUAL_CHILD_NODE_DEFINITIONS, 1);
    }

    private void mergeNameList(
            NodeBuilder builder, NodeState state, String listName) {
        LinkedHashSet<String> nameList =
                newLinkedHashSet(getNames(builder, listName));
        Iterables.addAll(
                nameList, state.getProperty(listName).getValue(NAMES));
        builder.setProperty(listName, nameList, NAMES);
    }

    private void mergeSubtree(NodeBuilder builder, NodeState state, String name, int depth) {
        NodeState subtree = state.getChildNode(name);
        if (subtree.exists()) {
            if (!builder.hasChildNode(name)) {
                builder.setChildNode(name, subtree);
            } else if (depth > 0) {
                NodeBuilder subbuilder = builder.child(name);
                for (String subname : subtree.getChildNodeNames()) {
                    mergeSubtree(subbuilder, subtree, subname, depth - 1);
                }
            }
        }
    }

    /**
     * Validates and pre-compiles the named node type.
     *
     * @param types builder for the /jcr:system/jcr:nodeTypes node
     * @param name name of the node type to validate and compile
     * @throws CommitFailedException if type validation fails
     */
    private void validateAndCompileType(NodeBuilder types, String name)
            throws CommitFailedException {
        NodeBuilder type = types.child(name);

        // - jcr:nodeTypeName (NAME) protected mandatory
        PropertyState nodeTypeName = type.getProperty(JCR_NODETYPENAME);
        if (nodeTypeName == null
                || !name.equals(nodeTypeName.getValue(NAME))) {
            throw new CommitFailedException(
                    CONSTRAINT, 34,
                    "Unexpected " + JCR_NODETYPENAME + " in type " + name);
        }

        // Prepare the type node pre-compilation of the oak:NodeType info
        Iterable<String> empty = emptyList();
        type.setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_NODE_TYPE, NAME);
        type.removeProperty(OAK_SUPERTYPES);
        type.setProperty(OAK_PRIMARY_SUBTYPES, empty, NAMES);
        type.setProperty(OAK_MANDATORY_PROPERTIES, empty, NAMES);
        type.setProperty(OAK_MANDATORY_CHILD_NODES, empty, NAMES);
        type.setProperty(OAK_PROTECTED_PROPERTIES, empty, NAMES);
        type.setProperty(OAK_PROTECTED_CHILD_NODES, empty, NAMES);
        type.setProperty(OAK_HAS_PROTECTED_RESIDUAL_PROPERTIES, false, BOOLEAN);
        type.setProperty(OAK_HAS_PROTECTED_RESIDUAL_CHILD_NODES, false, BOOLEAN);
        type.setProperty(OAK_NAMED_SINGLE_VALUED_PROPERTIES, empty, NAMES);
        type.getChildNode(OAK_NAMED_PROPERTY_DEFINITIONS).remove();
        type.getChildNode(OAK_RESIDUAL_PROPERTY_DEFINITIONS).remove();
        type.getChildNode(OAK_NAMED_CHILD_NODE_DEFINITIONS).remove();
        type.getChildNode(OAK_RESIDUAL_CHILD_NODE_DEFINITIONS).remove();

        // + jcr:propertyDefinition (nt:propertyDefinition)
        //   = nt:propertyDefinition protected sns
        // + jcr:childNodeDefinition (nt:childNodeDefinition)
        //   = nt:childNodeDefinition protected sns
        for (String childNodeName : type.getChildNodeNames()) {
            NodeState definition = type.child(childNodeName).getNodeState();
            if (childNodeName.startsWith(JCR_PROPERTYDEFINITION)) {
                validateAndCompilePropertyDefinition(type, name, definition);
            } else if (childNodeName.startsWith(JCR_CHILDNODEDEFINITION)) {
                validateAndCompileChildNodeDefinition(types, type, name, definition);
            }
        }
    }

    private void addNameToList(NodeBuilder type, String name, String value) {
        List<String> values;
        values = newArrayList(getNames(type, name));
        if (!values.contains(value)) {
            values.add(value);
        }
        type.setProperty(name, values, NAMES);
    }

    private void validateAndCompilePropertyDefinition(
            NodeBuilder type, String typeName, NodeState definition)
            throws CommitFailedException {
        // - jcr:name (NAME) protected 
        PropertyState name = definition.getProperty(JCR_NAME);
        NodeBuilder definitions;
        String propertyName = null;
        if (name != null) {
            propertyName = name.getValue(NAME);
            String escapedName = propertyName;
            if (JCR_PRIMARYTYPE.equals(escapedName)) {
                escapedName = OAK_PRIMARY_TYPE;
            } else if (JCR_MIXINTYPES.equals(escapedName)) {
                escapedName = OAK_MIXIN_TYPES;
            } else if (JCR_UUID.equals(escapedName)) {
                escapedName = OAK_UUID;
            }
            definitions = type.child(OAK_NAMED_PROPERTY_DEFINITIONS);
            definitions.setProperty(
                    JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_NAMED_PROPERTY_DEFINITIONS, NAME);
            definitions = definitions.child(escapedName);

            // - jcr:mandatory (BOOLEAN) protected mandatory
            if (definition.getBoolean(JCR_MANDATORY)) {
                addNameToList(type, OAK_MANDATORY_PROPERTIES, propertyName);
            }
            // - jcr:protected (BOOLEAN) protected mandatory
            if (definition.getBoolean(JCR_PROTECTED)) {
                addNameToList(type, OAK_PROTECTED_PROPERTIES, propertyName);
            }
        } else {
            definitions = type.child(OAK_RESIDUAL_PROPERTY_DEFINITIONS);

            // - jcr:protected (BOOLEAN) protected mandatory
            if (definition.getBoolean(JCR_PROTECTED)) {
                type.setProperty(OAK_HAS_PROTECTED_RESIDUAL_PROPERTIES, true);
            }
        }
        definitions.setProperty(
                JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_PROPERTY_DEFINITIONS, NAME);

        // - jcr:requiredType (STRING) protected mandatory
        // < 'STRING', 'URI', 'BINARY', 'LONG', 'DOUBLE',
        //   'DECIMAL', 'BOOLEAN', 'DATE', 'NAME', 'PATH',
        //   'REFERENCE', 'WEAKREFERENCE', 'UNDEFINED'
        String key = "UNDEFINED";
        PropertyState requiredType = definition.getProperty(JCR_REQUIREDTYPE);
        if (requiredType != null) {
            key = requiredType.getValue(STRING);
        }

        // - jcr:multiple (BOOLEAN) protected mandatory
        if (definition.getBoolean(JCR_MULTIPLE)) {
            if ("BINARY".equals(key)) {
                key = "BINARIES";
            } else {
                key = key + "S";
            }
        } else if (propertyName != null) {
            addNameToList(type, OAK_NAMED_SINGLE_VALUED_PROPERTIES, propertyName);
        }

        definitions.setChildNode(key, definition)
            .setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_PROPERTY_DEFINITION, NAME)
            .setProperty(OAK_DECLARING_NODE_TYPE, typeName, NAME);
    }

    private void validateAndCompileChildNodeDefinition(
            NodeBuilder types, NodeBuilder type, String typeName,
            NodeState definition) throws CommitFailedException {
        // - jcr:name (NAME) protected 
        PropertyState name = definition.getProperty(JCR_NAME);
        NodeBuilder definitions;
        if (name != null) {
            String childNodeName = name.getValue(NAME);
            definitions = type.child(OAK_NAMED_CHILD_NODE_DEFINITIONS);
            definitions.setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_NAMED_CHILD_NODE_DEFINITIONS, NAME);
            definitions = definitions.child(childNodeName);

            // - jcr:mandatory (BOOLEAN) protected mandatory
            if (definition.getBoolean(JCR_MANDATORY)) {
                addNameToList(type, OAK_MANDATORY_CHILD_NODES, childNodeName);
            }
            // - jcr:protected (BOOLEAN) protected mandatory
            if (definition.getBoolean(JCR_PROTECTED)) {
                addNameToList(type, OAK_PROTECTED_CHILD_NODES, childNodeName);
            }
        } else {
            definitions = type.child(OAK_RESIDUAL_CHILD_NODE_DEFINITIONS);

            // - jcr:protected (BOOLEAN) protected mandatory
            if (definition.getBoolean(JCR_PROTECTED)) {
                type.setProperty(OAK_HAS_PROTECTED_RESIDUAL_CHILD_NODES, true);
            }
        }
        definitions.setProperty(
                JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_CHILD_NODE_DEFINITIONS, NAME);

        // - jcr:requiredPrimaryTypes (NAME)
        //   = 'nt:base' protected mandatory multiple
        PropertyState requiredTypes =
                definition.getProperty(JCR_REQUIREDPRIMARYTYPES);
        if (requiredTypes != null) {
            for (String key : requiredTypes.getValue(NAMES)) {
                if (!types.hasChildNode(key)) {
                    throw new CommitFailedException(
                            "Constraint", 33,
                            "Unknown required primary type " + key);
                } else if (!definitions.hasChildNode(key)) {
                    definitions.setChildNode(key, definition)
                        .setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_CHILD_NODE_DEFINITION, NAME)
                        .setProperty(OAK_DECLARING_NODE_TYPE, typeName, NAME);
                }
            }
        }
    }

    //------------------------------------------------------------< Editor >--

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (modified) {
            NodeBuilder types = builder.child(JCR_SYSTEM).child(JCR_NODE_TYPES);
            for (String name : types.getChildNodeNames()) {
                validateAndCompileType(types, name);
            }
            for (String name : types.getChildNodeNames()) {
                mergeSupertypes(types, types.child(name));
            }
            for (String name : types.getChildNodeNames()) {
                NodeBuilder type = types.child(name);
                String listName = OAK_PRIMARY_SUBTYPES;
                if (type.getBoolean(JCR_ISMIXIN)) {
                    listName = OAK_MIXIN_SUBTYPES;
                }
                for (String supername : getNames(type, OAK_SUPERTYPES)) {
                    addNameToList(types.child(supername), listName, name);
                }
            }

            if (!changedTypes.isEmpty() || !removedTypes.isEmpty()) {
                // TODO: Find and re-validate any nodes in the repository that
                // refer to any of the changed (or removed) node types.
            }
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) {
        modified = true;
        return null;
    }

    @Override
    public Validator childNodeChanged(
            String name, NodeState before, NodeState after) {
        modified = true;
        changedTypes.add(name);
        return null;
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        modified = true;
        removedTypes.add(name);
        return null;
    }

}

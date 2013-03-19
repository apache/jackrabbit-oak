/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.nodetype;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MANDATORY;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SUPERTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VALUECONSTRAINTS;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.plugins.nodetype.constraint.Constraints.valueConstraint;

import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

/**
 * Validator implementation that check JCR node type constraints.
 *
 * TODO: check protected properties and the structure they enforce. some of
 *       those checks may have to go into separate validator classes. This class
 *       should only perform checks based on node type information. E.g. it
 *       cannot and should not check whether the value of the protected jcr:uuid
 *       is unique.
 */
class TypeEditor extends DefaultEditor {

    private final TypeEditor parent;

    private final String nodeName;

    private final NodeState types;

    private final List<NodeState> effective = Lists.newArrayList();

    TypeEditor(NodeState types) {
        this.parent = null;
        this.nodeName = null;
        this.types = checkNotNull(types);
    }

    private TypeEditor(TypeEditor parent, String name) {
        this.parent = checkNotNull(parent);
        this.nodeName = checkNotNull(name);
        this.types = parent.types;
    }

    /**
     * Computes the effective type of the modified type.
     */
    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        Iterable<String> names = computeEffectiveType(after);

        // find matching entry in the parent node's effective type
        if (parent != null) {
            parent.getDefinition(nodeName, names);
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        // TODO: add any auto-created items that are still missing

        // verify the presence of all mandatory items
        for (NodeState type : effective) {
            NodeState properties =
                    type.getChildNode("oak:namedPropertyDefinitions");
            if (properties != null) {
                for (ChildNodeEntry entry : properties.getChildNodeEntries()) {
                    String name = entry.getName();
                    if ("oak:primaryType".equals(name)) {
                        name = JCR_PRIMARYTYPE;
                    } else if ("oak:mixinTypes".equals(name)) {
                        name = JCR_MIXINTYPES;
                    } else if ("oak:uuid".equals(name)) {
                        name = JCR_UUID;
                    }
                    if (after.getProperty(name) == null
                            && isMandatory(name, entry.getNodeState())) {
                        throw constraintViolation(
                                "Missing mandatory property " + name);
                    }
                }
            }

            NodeState childNodes =
                    type.getChildNode("oak:namedChildNodeDefinitions");
            if (childNodes != null) {
                for (ChildNodeEntry entry : childNodes.getChildNodeEntries()) {
                    String name = entry.getName();
                    if (!after.hasChildNode(name)
                            && isMandatory(name, entry.getNodeState())) {
                        throw constraintViolation(
                                "Missing mandatory child node " + name);
                    }
                }
            }
        }
    }

    private boolean isMandatory(String name, NodeState definitions) {
        for (ChildNodeEntry entry : definitions.getChildNodeEntries()) {
            NodeState definition = entry.getNodeState();
            if (getBoolean(definition, JCR_MANDATORY)) {
                return true;
            }
        }
        return false;
    }

;    private CommitFailedException constraintViolation(String message) {
        return new CommitFailedException(
                new ConstraintViolationException(message));
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        NodeState definition = getDefinition(after);
        checkValueConstraints(definition, after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        NodeState definition = getDefinition(after);
        checkValueConstraints(definition, after);
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        return new TypeEditor(this, name);
    }

    @Override
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        return new TypeEditor(this, name);
    }

    private boolean getBoolean(NodeState node, String name) {
        PropertyState property = node.getProperty(name);
        return property != null
                && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

    //-----------------------------------------------------------< private >--

    private String getTypeKey(Type<?> type) {
        if (type == Type.BINARIES) {
            return "BINARIES";
        } else if (type == Type.BINARY) {
            return "BINARY";
        } else if (type == Type.BOOLEAN) {
            return "BOOLEAN";
        } else if (type == Type.BOOLEANS) {
            return "BOOLEANS";
        } else if (type == Type.DATE) {
            return "DATE";
        } else if (type == Type.DATES) {
            return "DATES";
        } else if (type == Type.DECIMAL) {
            return "DECIMAL";
        } else if (type == Type.DECIMALS) {
            return "DECIMALS";
        } else if (type == Type.DOUBLE) {
            return "DOUBLE";
        } else if (type == Type.DOUBLES) {
            return "DOUBLES";
        } else if (type == Type.LONG) {
            return "LONG";
        } else if (type == Type.LONGS) {
            return "LONGS";
        } else if (type == Type.NAME) {
            return "NAME";
        } else if (type == Type.NAMES) {
            return "NAMES";
        } else if (type == Type.PATH) {
            return "PATH";
        } else if (type == Type.PATHS) {
            return "PATHS";
        } else if (type == Type.REFERENCE) {
            return "REFERENCE";
        } else if (type == Type.REFERENCES) {
            return "REFERENCES";
        } else if (type == Type.STRING) {
            return "STRING";
        } else if (type == Type.STRINGS) {
            return "STRINGS";
        } else if (type == Type.URI) {
            return "URI";
        } else if (type == Type.URIS) {
            return "URIS";
        } else if (type == Type.WEAKREFERENCE) {
            return "WEAKREFERENCE";
        } else if (type == Type.WEAKREFERENCES) {
            return "WEAKREFERENCES";
        } else {
            return "unknown";
        }
    }

    /**
     * Finds a matching definition for a property with the given name and type.
     *
     * @param property modified property
     * @return matching property definition
     * @throws CommitFailedException if a matching definition was not found
     */
    @Nonnull
    private NodeState getDefinition(PropertyState property)
            throws CommitFailedException {
        String propertyName = property.getName();
        Type<?> propertyType = property.getType();

        String escapedName;
        if (JCR_PRIMARYTYPE.equals(propertyName)) {
            escapedName = "oak:primaryType";
        } else if (JCR_MIXINTYPES.equals(propertyName)) {
            escapedName = "oak:mixinTypes";
        } else if (JCR_UUID.equals(propertyName)) {
            escapedName = "oak:uuid";
        } else {
            escapedName = propertyName;
        }

        String definedType = getTypeKey(propertyType);
        String undefinedType;
        if (propertyType.isArray()) {
            undefinedType = "UNDEFINEDS";
        } else {
            undefinedType = "UNDEFINED";
        }

        // Find matching named property definition
        for (NodeState type : effective) {
            NodeState named = type.getChildNode("oak:namedPropertyDefinitions");
            if (named != null) {
                NodeState definitions = named.getChildNode(escapedName);
                if (definitions != null) {
                    NodeState definition = definitions.getChildNode(definedType);
                    if (definition == null) {
                        definition = definitions.getChildNode(undefinedType);
                    }
                    if (definition != null) {
                        return definition;
                    } else {
                        throw constraintViolation(
                                "No matching definition found for property "
                                        + propertyName);
                    }
                }
            }
        }

        // Find matching residual property definition
        for (NodeState type : effective) {
            NodeState residual =
                    type.getChildNode("oak:residualPropertyDefinitions");
            if (residual != null) {
                NodeState definition = residual.getChildNode(definedType);
                if (definition == null) {
                    definition = residual.getChildNode(undefinedType);
                }
                if (definition != null) {
                    return definition;
                }
            }
        }

        throw constraintViolation(
                "No matching definition found for property " + propertyName);
    }

    /**
     * Finds a matching definition for a child node with the given name and
     * types.
     *
     * @param nodeName child node name
     * @param nodeType effective types of the child node
     * @return matching child node definition
     * @throws CommitFailedException if a matching definition was not found
     */
    @Nonnull
    private NodeState getDefinition(String nodeName, Iterable<String> nodeType)
            throws CommitFailedException {
        boolean sns = false;
        int n = nodeName.length();
        if (n > 3 && nodeName.charAt(n - 1) == ']') {
            int i = n - 2;
            while (i > 1 && Character.isDigit(nodeName.charAt(i))) {
                i--;
            }
            if (nodeName.charAt(i) == '[') {
                nodeName = nodeName.substring(0, i);
                sns = true;
            }
        }

        // Find matching named property definition
        for (NodeState type : effective) {
            NodeState named = type.getChildNode("oak:namedChildNodeDefinitions");
            if (named != null) {
                NodeState definitions = named.getChildNode(nodeName);
                if (definitions != null) {
                    for (String typeName : nodeType) {
                        NodeState definition = definitions.getChildNode(typeName);
                        if (definition != null) {
                            return definition;
                        }
                    }

                    throw constraintViolation(
                            "Incorrect node type of child node " + nodeName);
                }
            }
        }

        // Find matching residual property definition
        for (NodeState type : effective) {
            NodeState residual =
                    type.getChildNode("oak:residualChildNodeDefinitions");
            if (residual != null) {
                for (String typeName : nodeType) {
                    NodeState definition = residual.getChildNode(typeName);
                    if (definition != null) {
                        return definition;
                    }
                }
            }
        }

        throw constraintViolation(
                "Incorrect node type of child node " + nodeName);
    }

    private void checkValueConstraints(
            NodeState definition, PropertyState property)
            throws CommitFailedException {
        if (property.count() == 0) {
            return;
        }

        PropertyState constraints =
                definition.getProperty(JCR_VALUECONSTRAINTS);
        if (constraints == null || constraints.count() == 0) {
            return;
        }

        PropertyState required = definition.getProperty(JCR_REQUIREDTYPE);
        if (required == null) {
            return;
        }

        int type;
        String value = required.getValue(STRING);
        if ("BINARY".equals(value)) {
            type = PropertyType.BINARY;
        } else if ("BOOLEAN".equals(value)) {
            type = PropertyType.BOOLEAN;
        } else if ("DATE".equals(value)) {
            type = PropertyType.DATE;
        } else if ("DECIMAL".equals(value)) {
            type = PropertyType.DECIMAL;
        } else if ("DOUBLE".equals(value)) {
            type = PropertyType.DOUBLE;
        } else if ("LONG".equals(value)) {
            type = PropertyType.LONG;
        } else if ("NAME".equals(value)) {
            type = PropertyType.NAME;
        } else if ("PATH".equals(value)) {
            type = PropertyType.PATH;
        } else if ("REFERENCE".equals(value)) {
            type = PropertyType.REFERENCE;
        } else if ("STRING".equals(value)) {
            type = PropertyType.STRING;
        } else if ("URI".equals(value)) {
            type = PropertyType.URI;
        } else if ("WEAKREFERENCE".equals(value)) {
            type = PropertyType.WEAKREFERENCE;
        } else {
            return;
        }

        for (String constraint : constraints.getValue(STRINGS)) {
            Predicate<Value> predicate = valueConstraint(type, constraint);
            for (Value v : ValueFactoryImpl.createValues(property, null)) {
                if (predicate.apply(v)) {
                    return;
                }
            }
        }
        throw constraintViolation("Value constraint violation");
    }

    /**
     * Collects the primary and mixin types and all related supertypes
     * of the given node and places them in the {@link #effective} list
     * of effective node type definitions.
     *
     * @param node node state
     * @return names of the types that make up the effective type
     * @throws CommitFailedException if the effective node type is invalid
     */
    private Iterable<String> computeEffectiveType(NodeState node)
            throws CommitFailedException {
        Set<String> names = Sets.newLinkedHashSet();

        // primary type
        PropertyState primary = node.getProperty(JCR_PRIMARYTYPE);
        if (primary != null && primary.getType() == NAME) {
            String name = primary.getValue(NAME);
            names.add(name);

            NodeState type = types.getChildNode(name);
            if (type == null) {
                throw constraintViolation(
                        "Primary node type " + name + " does not exist");
            } else if (getBoolean(type, JCR_ISMIXIN)) {
                throw constraintViolation(
                        "Can not use mixin type " + name + " as primary");
            } else if (getBoolean(type, JCR_IS_ABSTRACT)) {
                throw constraintViolation(
                        "Can not use abstract type " + name + " as primary");
            }

            effective.add(type);
        }

        // mixin types
        PropertyState mixins = node.getProperty(JCR_MIXINTYPES);
        if (mixins != null && mixins.getType() == NAMES) {
            for (String name : mixins.getValue(NAMES)) {
                if (names.add(name)) {
                    NodeState type = types.getChildNode(name);
                    if (type == null) {
                        throw constraintViolation(
                                "Mixin node type " + name + " does not exist");
                    } else if (!getBoolean(type, JCR_ISMIXIN)) {
                        throw constraintViolation(
                                "Can not use primary type " + name + " as mixin");
                    } else if (getBoolean(type, JCR_IS_ABSTRACT)) {
                        throw constraintViolation(
                                "Can not use abstract type " + name + " as mixin");
                    }

                    effective.add(type);
                }
            }
        }

        // supertypes
        Queue<NodeState> queue = Queues.newArrayDeque(effective);
        while (!queue.isEmpty()) {
            NodeState type = queue.remove();

            PropertyState supertypes = type.getProperty(JCR_SUPERTYPES);
            if (supertypes != null) {
                for (String name : supertypes.getValue(NAMES)) {
                    if (names.add(name)) {
                        NodeState supertype = types.getChildNode(name);
                        if (supertype != null) {
                            effective.add(supertype);
                            queue.add(supertype);
                        } else {
                            // TODO: ignore/warning/error?
                        }
                    }
                }
            }
        }

        // always include nt:base
        if (names.add(NT_BASE)) {
            NodeState base = types.getChildNode(NT_BASE);
            if (base != null) {
                effective.add(base);
            } else {
                // TODO: ignore/warning/error?
            }
        }

        return names;
    }

}

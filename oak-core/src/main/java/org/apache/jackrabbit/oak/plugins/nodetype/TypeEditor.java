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
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SUPERTYPES;
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

import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Joiner;
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

    private EffectiveType effective = null;

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

    private String getPath() {
        if (parent == null) {
            return "/";
        } else if (parent.parent == null) {
            return "/" + nodeName;
        } else {
            return parent.getPath() + "/" + nodeName;
        }
    }

    /**
     * Computes the effective type of the modified type.
     */
    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        Iterable<String> names = computeEffectiveType(after);

        // find matching entry in the parent node's effective type
        // TODO: this should be in childNodeAdded()
        if (parent != null
                && parent.effective.getDefinition(nodeName, names) == null) {
            throw constraintViolation(
                    "Incorrect node type of child node " + nodeName
                    + " with types " + Joiner.on(", ").join(names));
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        // TODO: add any auto-created items that are still missing

        // verify the presence of all mandatory items
        Set<String> missing = effective.findMissingMandatoryItems(after);
        if (!missing.isEmpty()) {
            throw constraintViolation(
                    "Missing mandatory items " + Joiner.on(", ").join(missing));
        }
    }

    private CommitFailedException constraintViolation(String message) {
        return new CommitFailedException(
                new ConstraintViolationException(getPath() + ": " + message));
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        NodeState definition = effective.getDefinition(after);
        if (definition == null) {
            throw constraintViolation(
                    "No matching property definition found for " + after);
        }
        checkValueConstraints(definition, after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        NodeState definition = effective.getDefinition(after);
        if (definition == null) {
            throw constraintViolation(
                    "No matching property definition found for " + after);
        }
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

    //-----------------------------------------------------------< private >--

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
        throw constraintViolation("Value constraint violation in " + property);
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
        List<NodeState> list = Lists.newArrayList();
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

            list.add(type);
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

                    list.add(type);
                }
            }
        }

        // supertypes
        Queue<NodeState> queue = Queues.newArrayDeque(list);
        while (!queue.isEmpty()) {
            NodeState type = queue.remove();

            PropertyState supertypes = type.getProperty(JCR_SUPERTYPES);
            if (supertypes != null) {
                for (String name : supertypes.getValue(NAMES)) {
                    if (names.add(name)) {
                        NodeState supertype = types.getChildNode(name);
                        if (supertype != null) {
                            list.add(supertype);
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
                list.add(base);
            } else {
                // TODO: ignore/warning/error?
            }
        }

        effective = new EffectiveType(list);
        return names;
    }

    private boolean getBoolean(NodeState node, String name) {
        PropertyState property = node.getProperty(name);
        return property != null
                && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

}

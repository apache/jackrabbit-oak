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
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_VALUECONSTRAINTS;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.plugins.nodetype.constraint.Constraints.valueConstraint;

import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

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

    private final List<String> typeNames = newArrayList();

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
        computeEffectiveType(after);

        // find matching entry in the parent node's effective type
        // TODO: this should be in childNodeAdded()
        if (parent != null && parent.effective.getDefinition(
                nodeName, effective.getTypeNames()) == null) {
            throw constraintViolation(
                    1, "Incorrect node type of child node " + nodeName);
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
                    2, "Missing mandatory items " + Joiner.on(", ").join(missing));
        }
    }

    private CommitFailedException constraintViolation(int code, String message) {
        return new CommitFailedException(
                CONSTRAINT, code, getPath() + ": " + message + " " + typeNames);
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        NodeState definition = effective.getDefinition(after);
        if (definition == null) {
            throw constraintViolation(
                    3, "No matching property definition found for " + after);
        }
        checkValueConstraints(definition, after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        NodeState definition = effective.getDefinition(after);
        if (definition == null) {
            throw constraintViolation(
                    4, "No matching property definition found for " + after);
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
        throw constraintViolation(5, "Value constraint violation in " + property);
    }

    /**
     * Collects the primary and mixin types and all related supertypes
     * of the given node and places them in the {@link #effective} list
     * of effective node type definitions.
     *
     * @param node node state
     * @throws CommitFailedException if the effective node type is invalid
     */
    private void computeEffectiveType(NodeState node)
            throws CommitFailedException {
        List<NodeState> list = Lists.newArrayList();

        // primary type
        PropertyState primary = node.getProperty(JCR_PRIMARYTYPE);
        if (primary != null && primary.getType() == NAME) {
            String name = primary.getValue(NAME);
            typeNames.add(name);

            NodeState type = types.getChildNode(name);
            if (!type.exists()) {
                throw constraintViolation(
                        6, "Primary node type " + name + " does not exist");
            } else if (getBoolean(type, JCR_ISMIXIN)) {
                throw constraintViolation(
                        7, "Can not use mixin type " + name + " as primary");
            } else if (getBoolean(type, JCR_IS_ABSTRACT)) {
                throw constraintViolation(
                        8, "Can not use abstract type " + name + " as primary");
            }

            list.add(type);
        }

        // mixin types
        PropertyState mixins = node.getProperty(JCR_MIXINTYPES);
        if (mixins != null && mixins.getType() == NAMES) {
            for (String name : mixins.getValue(NAMES)) {
                typeNames.add(name);

                NodeState type = types.getChildNode(name);
                if (!type.exists()) {
                    throw constraintViolation(
                            9, "Mixin node type " + name + " does not exist");
                } else if (!getBoolean(type, JCR_ISMIXIN)) {
                    throw constraintViolation(
                            10, "Can not use primary type " + name + " as mixin");
                } else if (getBoolean(type, JCR_IS_ABSTRACT)) {
                    throw constraintViolation(
                            11, "Can not use abstract type " + name + " as mixin");
                }

                list.add(type);
            }
        }

        effective = new EffectiveType(list);
    }

    private boolean getBoolean(NodeState node, String name) {
        PropertyState property = node.getProperty(name);
        return property != null
                && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

}

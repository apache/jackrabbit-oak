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

import java.util.List;

import javax.jcr.PropertyType;
import javax.jcr.Value;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VALUECONSTRAINTS;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager.isValidUUID;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.plugins.nodetype.constraint.Constraints.valueConstraint;

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

    private static final Logger log = LoggerFactory.getLogger(TypeEditor.class);

    private final TypeEditor parent;

    private final String nodeName;

    private final NodeState types;

    private EffectiveType effective;

    private final NodeBuilder builder;

    TypeEditor(
            NodeState types,
            String primary, Iterable<String> mixins, NodeBuilder builder)
            throws CommitFailedException {
        this.parent = null;
        this.nodeName = null;
        this.types = checkNotNull(types);
        this.effective = getEffectiveType(primary, mixins);
        this.builder = checkNotNull(builder);
    }

    private TypeEditor(
            TypeEditor parent, String name,
            String primary, Iterable<String> mixins, NodeBuilder builder)
            throws CommitFailedException {
        this.parent = checkNotNull(parent);
        this.nodeName = checkNotNull(name);
        this.types = parent.types;
        this.effective = getEffectiveType(primary, mixins);
        this.builder = checkNotNull(builder);
    }

    /**
     * Test constructor.
     */
    TypeEditor(EffectiveType effective) {
        this.parent = null;
        this.nodeName = null;
        this.types = EMPTY_NODE;
        this.effective = checkNotNull(effective);
        this.builder = EMPTY_NODE.builder();
    }

    private CommitFailedException constraintViolation(
            int code, String message) {
        if (effective != null) {
            return effective.constraintViolation(code, getPath(), message);
        } else {
            return new CommitFailedException(
                    CONSTRAINT, 0, getPath() + ": " + message);
        }
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

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        propertyChanged(null, after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        NodeState definition = effective.getDefinition(after);
        if (definition == null) {
            throw constraintViolation(
                    4, "No matching property definition found for " + after);
        } else if (JCR_UUID.equals(after.getName())
                && effective.isNodeType(MIX_REFERENCEABLE)) {
            // special handling for the jcr:uuid property of mix:referenceable
            // TODO: this should be done in a pluggable extension
            if (!isValidUUID(after.getValue(Type.STRING))) {
                throw constraintViolation(
                        12, "Invalid UUID value in the jcr:uuid property");
            }
        } else {
            checkValueConstraints(definition, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        String name = before.getName();
        if (effective.isMandatoryProperty(name)) {
            throw constraintViolation(
                    22, "Mandatory property " + name + " can not be removed");
        }
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        TypeEditor editor = childNodeChanged(name, MISSING_NODE, after);

        // TODO: add any auto-created items that are still missing

        // verify the presence of all mandatory items
        for (String property : editor.effective.getMandatoryProperties()) {
            if (!after.hasProperty(property)) {
                throw editor.constraintViolation(
                        21, "Mandatory property " + property
                        + " not found in a new node");
            }
        }
        for (String child : editor.effective.getMandatoryChildNodes()) {
            if (!after.hasChildNode(child)) {
                throw editor.constraintViolation(
                        25, "Mandatory child node " + child
                        + " not found in a new node");
            }
        }

        return editor;
    }

    @Override
    public TypeEditor childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        String primary = after.getName(JCR_PRIMARYTYPE);
        Iterable<String> mixins = after.getNames(JCR_MIXINTYPES);

        NodeBuilder childBuilder = builder.getChildNode(name);
        if (primary == null) {
            // no primary type defined, find and apply a default type
            primary = effective.getDefaultType(name);
            if (primary != null) {
                builder.setProperty(JCR_PRIMARYTYPE, primary, NAME);
            } else {
                throw constraintViolation(
                        4, "No default primary type available "
                        + " for child node " + name);
            }
        }

        TypeEditor editor =
                new TypeEditor(this, name, primary, mixins, childBuilder);
        if (!effective.isValidChildNode(name, editor.effective)) {
            throw constraintViolation(
                    1, "No matching definition found for child node " + name
                    + " with effective type " + editor.effective);
        }

        return editor;
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        if (effective.isMandatoryChildNode(name)) {
            throw constraintViolation(
                     26, "Mandatory child node " + name + " can not be removed");
        } else {
            return null; // no further checking needed for the removed subtree
        }
    }

    //-----------------------------------------------------------< private >--

    private EffectiveType getEffectiveType(
            String primary, Iterable<String> mixins)
            throws CommitFailedException {
        List<NodeState> list = Lists.newArrayList();

        NodeState type = types.getChildNode(primary);
        if (!type.exists()) {
            throw constraintViolation(
                    1, "The primary type " + primary + " does not exist");
        } else if (type.getBoolean(JCR_ISMIXIN)) {
            throw constraintViolation(
                    2, "Mixin type " + primary + " used as the primary type");
        } else {
            if (type.getBoolean(JCR_IS_ABSTRACT)) {
                log.warn("Abstract type " + primary
                        + " used as the primary type of node " + getPath());
            }
            list.add(type);
        }

        // mixin types
        for (String mixin : mixins) {
            type = types.getChildNode(mixin);
            if (!type.exists()) {
                throw constraintViolation(
                        5, "The mixin type " + mixin + " does not exist");
            } else if (!type.getBoolean(JCR_ISMIXIN)) {
                throw constraintViolation(
                        6, "Primary type " + mixin + " used as a mixin type");
            } else {
                if (type.getBoolean(JCR_IS_ABSTRACT)) {
                    log.warn("Abstract type " + mixin
                            + " used as a mixin type of node " + getPath());
                }
                list.add(type);
            }
        }

        return new EffectiveType(list);
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
        throw constraintViolation(5, "Value constraint violation in " + property);
    }

}

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

import java.util.Set;
import javax.jcr.PropertyType;
import javax.jcr.Value;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_REQUIREDTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VALUECONSTRAINTS;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.core.IdentifierManager.isValidUUID;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
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

    private final TypeEditor parent;

    private final String nodeName;

    private final NodeState types;

    private EffectiveType effective;

    private final NodeBuilder builder;

    TypeEditor(NodeState types, EffectiveType effective, NodeBuilder builder) {
        this.parent = null;
        this.nodeName = null;
        this.types = checkNotNull(types);
        this.effective = checkNotNull(effective);
        this.builder = checkNotNull(builder);
    }

    private TypeEditor(
            TypeEditor parent, String name,
            EffectiveType effective, NodeBuilder builder) {
        this.parent = checkNotNull(parent);
        this.nodeName = checkNotNull(name);
        this.types = parent.types;
        this.effective = checkNotNull(effective);
        this.builder = checkNotNull(builder);
    }

    private CommitFailedException constraintViolation(
            int code, String message) {
        return effective.constraintViolation(code, getPath(), message);
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
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        // TODO: add any auto-created items that are still missing

        // verify the presence of all mandatory items
        Set<String> missing = effective.findMissingMandatoryItems(builder.getNodeState());
        if (!missing.isEmpty()) {
            throw constraintViolation(
                    2, "Missing mandatory items " + Joiner.on(", ").join(missing));
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
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        return childNodeChanged(name, MISSING_NODE, after);
    }

    @Override
    public Editor childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        NodeBuilder childBuilder = builder.getChildNode(name);

        String primary = after.getName(JCR_PRIMARYTYPE);
        if (primary == null) {
            // no primary type defined, find and apply a default type
            primary = effective.getDefaultType(name);
            if (primary != null) {
                builder.setProperty(JCR_PRIMARYTYPE, primary, NAME);
            } else {
                throw constraintViolation(
                        5, "No default primary type available "
                        + " for child node " + name);
            }
        }

        EffectiveType childType = effective.computeEffectiveType(
                types, getPath(), // TODO: compute path only on demand
                name, primary, after.getNames(JCR_MIXINTYPES));

        if (effective.getDefinition(name, childType.getTypeNames()) == null) {
            throw constraintViolation(
                    1, "No matching definition found for child node " + name
                    + " with effective type " + childType.getTypeNames());
        }

        return new TypeEditor(this, name, childType, childBuilder);
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

}

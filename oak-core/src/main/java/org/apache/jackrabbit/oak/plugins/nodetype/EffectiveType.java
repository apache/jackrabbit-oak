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
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_DEFAULTPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_MANDATORY;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_NODETYPENAME;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SAMENAMESIBLINGS;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.api.Type.UNDEFINED;
import static org.apache.jackrabbit.oak.api.Type.UNDEFINEDS;
import static org.apache.jackrabbit.oak.commons.PathUtils.dropIndexFromName;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_MANDATORY_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_MANDATORY_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_RESIDUAL_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_RESIDUAL_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_SUPERTYPES;

class EffectiveType {

    private final List<NodeState> types;

    EffectiveType(@Nonnull List<NodeState> types) {
        this.types = checkNotNull(types);
    }

    /**
     * Checks whether this effective type contains the named type.
     *
     * @param name node type name
     * @return {@code true} if the named type is included,
     *         {@code false} otherwise
     */
    boolean isNodeType(@Nonnull String name) {
        for (NodeState type : types) {
            if (name.equals(type.getName(JCR_NODETYPENAME))
                    || contains(type.getNames(REP_SUPERTYPES), name)) {
                return true;
            }
        }
        return false;
    }

    boolean isMandatoryProperty(@Nonnull String name) {
        return nameSetContains(REP_MANDATORY_PROPERTIES, name);
    }

    @Nonnull
    Set<String> getMandatoryProperties() {
        return getNameSet(REP_MANDATORY_PROPERTIES);
    }

    boolean isMandatoryChildNode(@Nonnull String name) {
        return nameSetContains(REP_MANDATORY_CHILD_NODES, name);
    }

    @Nonnull
    Set<String> getMandatoryChildNodes() {
        return getNameSet(REP_MANDATORY_CHILD_NODES);
    }

    /**
     * Finds a matching definition for a property with the given name and type.
     *
     * @param property modified property
     * @return matching property definition, or {@code null}
     */
    @CheckForNull
    NodeState getDefinition(@Nonnull PropertyState property) {
        String propertyName = property.getName();
        Type<?> propertyType = property.getType();

        String escapedName;
        if (JCR_PRIMARYTYPE.equals(propertyName)) {
            escapedName = NodeTypeConstants.REP_PRIMARY_TYPE;
        } else if (JCR_MIXINTYPES.equals(propertyName)) {
            escapedName = NodeTypeConstants.REP_MIXIN_TYPES;
        } else if (JCR_UUID.equals(propertyName)) {
            escapedName = NodeTypeConstants.REP_UUID;
        } else {
            escapedName = propertyName;
        }

        String definedType = propertyType.toString();
        String undefinedType;
        if (propertyType.isArray()) {
            undefinedType = UNDEFINEDS.toString();
        } else {
            undefinedType = UNDEFINED.toString();
        }

        // Find matching named property definition
        for (NodeState type : types) {
            NodeState definitions = type
                    .getChildNode(REP_NAMED_PROPERTY_DEFINITIONS)
                    .getChildNode(escapedName);

            NodeState definition = definitions.getChildNode(definedType);
            if (definition.exists()) {
                return definition;
            }

            definition = definitions.getChildNode(undefinedType);
            if (definition.exists()) {
                return definition;
            }

            // OAK-822: a mandatory definition always overrides residual ones
            // TODO: unnecessary if the OAK-713 fallback wasn't needed below
            for (ChildNodeEntry entry : definitions.getChildNodeEntries()) {
                definition = entry.getNodeState();
                if (definition.getBoolean(JCR_MANDATORY)) {
                    return definition;
                }
            }

// TODO: Fall back to residual definitions until we have consensus on OAK-713
//          throw new ConstraintViolationException(
//                "No matching definition found for property " + propertyName);
        }

        // Find matching residual property definition
        for (NodeState type : types) {
            NodeState residual =
                    type.getChildNode(REP_RESIDUAL_PROPERTY_DEFINITIONS);
            NodeState definition = residual.getChildNode(definedType);
            if (!definition.exists()) {
                definition = residual.getChildNode(undefinedType);
            }
            if (definition.exists()) {
                return definition;
            }
        }

        return null;
    }

    /**
     * Finds a matching definition for a child node with the given name and
     * types.
     *
     * @param nameWithIndex child node name, possibly with an SNS index
     * @param effective effective types of the child node
     * @return {@code true} if there's a matching child node definition,
     *         {@code false} otherwise
     */
    boolean isValidChildNode(@Nonnull String nameWithIndex, @Nonnull EffectiveType effective) {
        String name = dropIndexFromName(nameWithIndex);
        boolean sns = !name.equals(nameWithIndex);
        Set<String> typeNames = effective.getTypeNames();

        // Find matching named child node definition
        for (NodeState type : types) {
            NodeState definitions = type
                    .getChildNode(REP_NAMED_CHILD_NODE_DEFINITIONS)
                    .getChildNode(name);

            for (String typeName : typeNames) {
                NodeState definition = definitions.getChildNode(typeName);
                if (definition.exists() && snsMatch(sns, definition)) {
                    return true;
                }
            }

            // OAK-822: a mandatory definition always overrides alternatives
            // TODO: unnecessary if the OAK-713 fallback wasn't needed below
            for (ChildNodeEntry entry : definitions.getChildNodeEntries()) {
                NodeState definition = entry.getNodeState();
                if (definition.getBoolean(JCR_MANDATORY)) {
                    return false;
                }
            }

// TODO: Fall back to residual definitions until we have consensus on OAK-713
//          throw new ConstraintViolationException(
//                  "Incorrect node type of child node " + nodeName);
        }

        // Find matching residual child node definition
        for (NodeState type : types) {
            NodeState residual =
                    type.getChildNode(REP_RESIDUAL_CHILD_NODE_DEFINITIONS);
            for (String typeName : typeNames) {
                NodeState definition = residual.getChildNode(typeName);
                if (definition.exists() && snsMatch(sns, definition)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Finds the default node type for a child node with the given name.
     *
     * @param nameWithIndex child node name, possibly with an SNS index
     * @return default type, or {@code null} if not found
     */
    @CheckForNull
    String getDefaultType(@Nonnull String nameWithIndex) {
        String name = dropIndexFromName(nameWithIndex);
        boolean sns = !name.equals(nameWithIndex);

        for (NodeState type : types) {
            NodeState named = type
                    .getChildNode(REP_NAMED_CHILD_NODE_DEFINITIONS)
                    .getChildNode(name);
            NodeState residual = type
                    .getChildNode(REP_RESIDUAL_CHILD_NODE_DEFINITIONS);

            for (ChildNodeEntry entry : concat(
                    named.getChildNodeEntries(),
                    residual.getChildNodeEntries())) {
                NodeState definition = entry.getNodeState();
                String defaultType = definition.getName(JCR_DEFAULTPRIMARYTYPE);
                if (defaultType != null && snsMatch(sns, definition)) {
                    return defaultType;
                }
            }
        }

        return null;
    }

    @Nonnull
    Set<String> getTypeNames() {
        Set<String> names = newHashSet();
        for (NodeState type : types) {
            names.add(type.getName(JCR_NODETYPENAME));
            addAll(names, type.getNames(REP_SUPERTYPES));
        }
        return names;
    }
    
    List<String> getDirectTypeNames() {
        List<String> names = newArrayListWithCapacity(types.size());
        for (NodeState type : types) {
            names.add(type.getName(JCR_NODETYPENAME));
        }
        return names;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return getDirectTypeNames().toString();
    }

    //-----------------------------------------------------------< private >--

    /**
     * Depending on the given SNS flag, checks whether the given child node
     * definition allows same-name-siblings.
     *
     * @param sns SNS flag, {@code true} if processing an SNS node
     * @param definition child node definition
     */
    private boolean snsMatch(boolean sns, @Nonnull NodeState definition) {
        return !sns || definition.getBoolean(JCR_SAMENAMESIBLINGS);
    }

    private boolean nameSetContains(@Nonnull String set, @Nonnull String name) {
        for (NodeState type : types) {
            if (contains(type.getNames(set), name)) {
                return true;
            }
        }
        return false;
    }

    @Nonnull
    private Set<String> getNameSet(@Nonnull String set) {
        Set<String> names = newHashSet();
        for (NodeState type : types) {
            addAll(names, type.getNames(set));
        }
        return names;
    }

}

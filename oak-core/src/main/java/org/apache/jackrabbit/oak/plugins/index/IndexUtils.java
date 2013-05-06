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
package org.apache.jackrabbit.oak.plugins.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_UNKNOWN;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.UNIQUE_PROPERTY_NAME;

/**
 * TODO document
 */
public class IndexUtils {

    public static NodeBuilder getOrCreateOakIndex(NodeBuilder root) {
        NodeBuilder index;
        if (!root.hasChildNode(INDEX_DEFINITIONS_NAME)) {
            index = root.child(INDEX_DEFINITIONS_NAME);
            // TODO: use property node type name
            index.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        } else {
            index = root.child(INDEX_DEFINITIONS_NAME);
        }
        return index;
    }

    /**
     * Create a new property2 index definition below the given {@code indexNode}.
     *
     * @param index         The oak:index node builder
     * @param indexDefName  The name of the new property index.
     * @param reindex       {@code true} if the the reindex flag should be turned on.
     * @param unique        {@code true} if the index is expected the assert property
     *                      uniqueness.
     * @param propertyNames The property names that should be indexed.
     * @param declaringNodeTypeNames The declaring node type names or {@code null}.
     * @return the NodeBuilder of the new index definition.
     */
    public static NodeBuilder createIndexDefinition(@Nonnull NodeBuilder index,
                                             @Nonnull String indexDefName,
                                             boolean reindex,
                                             boolean unique,
                                             @Nonnull Collection<String> propertyNames,
                                             @Nullable Collection<String> declaringNodeTypeNames) {
        NodeBuilder entry = index.child(indexDefName)
                .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, PropertyIndexEditorProvider.TYPE)
                .setProperty(REINDEX_PROPERTY_NAME, reindex);
        if (unique) {
            entry.setProperty(UNIQUE_PROPERTY_NAME, unique);
        }
        entry.setProperty(PropertyStates.createProperty(PROPERTY_NAMES, propertyNames, NAMES));
        if (declaringNodeTypeNames != null && !declaringNodeTypeNames.isEmpty()) {
            entry.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES, declaringNodeTypeNames, NAMES));
        }
        return entry;
    }

    /**
     * Create a new property2 index definition below the given {@code indexNode}.
     *
     * @param indexNode
     * @param indexDefName
     * @param unique
     * @param propertyNames
     * @param declaringNodeTypeNames
     */
    public static void createIndexDefinition(@Nonnull NodeUtil indexNode,
                                             @Nonnull String indexDefName,
                                             boolean unique,
                                             @Nonnull String[] propertyNames,
                                             @Nullable String[] declaringNodeTypeNames) {
        NodeUtil entry = indexNode.getOrAddChild(indexDefName, INDEX_DEFINITIONS_NODE_TYPE);
        entry.setString(TYPE_PROPERTY_NAME, PropertyIndexEditorProvider.TYPE);
        entry.setBoolean(REINDEX_PROPERTY_NAME, true);
        if (unique) {
            entry.setBoolean(UNIQUE_PROPERTY_NAME, true);
        }
        if (declaringNodeTypeNames != null && declaringNodeTypeNames.length > 0) {
            entry.setNames(DECLARING_NODE_TYPES, declaringNodeTypeNames);
        }
        entry.setNames(PROPERTY_NAMES, propertyNames);
    }

    /**
     * Builds a list of the existing index definitions.
     * <p/>
     * Checks only children of the provided state for an index definitions
     * container node, aka a node named {@link IndexConstants#INDEX_DEFINITIONS_NAME}
     *
     * @param state
     * @param indexConfigPath
     * @param typeFilter
     * @return A list of index definitions.
     */
    public static List<IndexDefinition> buildIndexDefinitions(NodeState state,
                                                              String indexConfigPath, String typeFilter) {
        NodeState definitions = state.getChildNode(INDEX_DEFINITIONS_NAME);
        if (!definitions.exists()) {
            return Collections.emptyList();
        }
        indexConfigPath = concat(indexConfigPath, INDEX_DEFINITIONS_NAME);

        List<IndexDefinition> defs = new ArrayList<IndexDefinition>();
        for (ChildNodeEntry c : definitions.getChildNodeEntries()) {
            IndexDefinition def = getDefinition(indexConfigPath, c, typeFilter);
            if (def == null) {
                continue;
            }
            defs.add(def);
        }
        return defs;
    }

    /**
     * Builds an {@link IndexDefinition} out of a {@link ChildNodeEntry}
     *
     * @param path
     * @param def {@code ChildNodeEntry} storing the index definition.
     * @param typeFilter
     * @return a new {@code IndexDefinition}
     */
    private static IndexDefinition getDefinition(String path,
                                                 ChildNodeEntry def, String typeFilter) {
        String name = def.getName();
        NodeState ns = def.getNodeState();
        PropertyState typeProp = ns.getProperty(TYPE_PROPERTY_NAME);
        String type = TYPE_UNKNOWN;
        if (typeProp != null && !typeProp.isArray()) {
            type = typeProp.getValue(STRING);
        }
        if (typeFilter != null && !typeFilter.equals(type)) {
            return null;
        }
        return new IndexDefinitionImpl(name, type, concat(path, name));
    }

    public static boolean isIndexNodeType(NodeState state) {
        PropertyState ps = state.getProperty(JCR_PRIMARYTYPE);
        return ps != null
                && ps.getValue(STRING).equals(INDEX_DEFINITIONS_NODE_TYPE);
    }

    public static boolean isIndexNodeType(NodeState state, String typeIn) {
        if (!isIndexNodeType(state)) {
            return false;
        }
        PropertyState type = state.getProperty(TYPE_PROPERTY_NAME);
        return type != null && !type.isArray()
                && type.getValue(Type.STRING).equals(typeIn);
    }

    public static String getString(NodeBuilder builder, String name) {
        PropertyState property = builder.getProperty(name);
        if (property != null && property.getType() == STRING) {
            return property.getValue(STRING);
        } else {
            return null;
        }
    }

    public static String getString(NodeState state, String name) {
        PropertyState property = state.getProperty(name);
        if (property != null && property.getType() == STRING) {
            return property.getValue(STRING);
        } else {
            return null;
        }
    }

    public static boolean getBoolean(NodeBuilder builder, String name) {
        PropertyState property = builder.getProperty(name);
        return property != null && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

    public static boolean getBoolean(NodeState state, String name) {
        PropertyState property = state.getProperty(name);
        return property != null && property.getType() == BOOLEAN
                && property.getValue(BOOLEAN);
    }

    public static NodeBuilder getChildOrNull(NodeBuilder node, String name) {
        if (node != null && node.hasChildNode(name)) {
            return node.child(name);
        } else {
            return null;
        }
    }

}

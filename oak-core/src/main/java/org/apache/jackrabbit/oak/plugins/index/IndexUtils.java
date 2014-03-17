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

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.UNIQUE_PROPERTY_NAME;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;

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
     * Create a new property index definition below the given {@code indexNode}.
     * 
     * @param index                  The oak:index node builder
     * @param indexDefName           The name of the new property index.
     * @param reindex                {@code true} if the the reindex flag should be turned on.
     * @param unique                 {@code true} if the index is expected the assert property
     *                               uniqueness.
     * @param propertyNames          The property names that should be indexed.
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
                                             @Nullable String[] declaringNodeTypeNames) throws RepositoryException {

        createIndexDefinition(indexNode, indexDefName, unique, propertyNames, declaringNodeTypeNames, PropertyIndexEditorProvider.TYPE);
    }

    /**
     * Create a new property index definition below the given {@code indexNode} of the provided {@code propertyIndexType}.
     * 
     * @param indexNode
     * @param indexDefName
     * @param unique
     * @param propertyNames
     * @param declaringNodeTypeNames
     * @param propertyIndexType
     * @throws RepositoryException
     */
    public static void createIndexDefinition(@Nonnull NodeUtil indexNode, 
                                             @Nonnull String indexDefName, 
                                             boolean unique, 
                                             @Nonnull String[] propertyNames, 
                                             @Nullable String[] declaringNodeTypeNames, 
                                             @Nonnull String propertyIndexType) throws RepositoryException {
        createIndexDefinition(indexNode, indexDefName, unique, propertyNames,
            declaringNodeTypeNames, propertyIndexType, null);
    }

    public static void createReferenceIndex(@Nonnull NodeBuilder index) {
        index.child(NodeReferenceConstants.NAME)
                .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, NodeReferenceConstants.TYPE);
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

    /**
     * Create a new property index definition below the given {@code indexNode} of the provided
     * {@code propertyIndexType}.
     * 
     * @param indexNode
     * @param indexDefName
     * @param unique
     * @param propertyNames
     * @param declaringNodeTypeNames
     * @param propertyIndexType
     * @param properties any additional property to be added to the index definition.
     * @throws RepositoryException
     */
    public static void createIndexDefinition(@Nonnull NodeUtil indexNode, 
                                             @Nonnull String indexDefName, 
                                             boolean unique, 
                                             @Nonnull String[] propertyNames, 
                                             @Nullable String[] declaringNodeTypeNames, 
                                             @Nonnull String propertyIndexType,
                                             Map<String, String> properties) throws RepositoryException {
        NodeUtil entry = indexNode.getOrAddChild(indexDefName, INDEX_DEFINITIONS_NODE_TYPE);
        entry.setString(TYPE_PROPERTY_NAME, propertyIndexType);
        entry.setBoolean(REINDEX_PROPERTY_NAME, true);
        if (unique) {
            entry.setBoolean(UNIQUE_PROPERTY_NAME, true);
        }
        if (declaringNodeTypeNames != null && declaringNodeTypeNames.length > 0) {
            entry.setNames(DECLARING_NODE_TYPES, declaringNodeTypeNames);
        }
        entry.setNames(PROPERTY_NAMES, propertyNames);

        if (properties != null) {
            for (String k : properties.keySet()) {
                entry.setString(k, properties.get(k));
            }
        }
    }

    /**
     * Create a new property index definition below the given {@code indexNode} of the provided
     * {@code propertyIndexType}.
     * 
     * @param indexNode                 the oak:index
     * @param indexDefName              the node for the index definition
     * @param unique                    true if uniqueness
     * @param propertyNames             the list of properties to be indexed
     * @param declaringNodeTypeNames
     * @param propertyIndexType         the type of the PropertyIndex
     * @param properties                any additional property to be added to the index definition.
     * @throws RepositoryException
     */
    public static void createIndexDefinition(@Nonnull NodeBuilder indexNode, 
                                             @Nonnull String indexDefName, 
                                             boolean unique, 
                                             @Nonnull Iterable<String> propertyNames, 
                                             @Nullable String[] declaringNodeTypeNames, 
                                             @Nonnull String propertyIndexType,
                                             Map<String, String> properties) throws RepositoryException {

        NodeBuilder entry = indexNode.child(indexDefName)
            .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
            .setProperty(TYPE_PROPERTY_NAME, propertyIndexType)
            .setProperty(REINDEX_PROPERTY_NAME, false);
        if (unique) {
            entry.setProperty(UNIQUE_PROPERTY_NAME, unique);
        }
        entry.setProperty(PropertyStates.createProperty(PROPERTY_NAMES, propertyNames, NAMES));
        if (declaringNodeTypeNames != null && declaringNodeTypeNames.length > 0) {
            entry.setProperty(PropertyStates.createProperty(DECLARING_NODE_TYPES,
                declaringNodeTypeNames, NAMES));
        }
        // additional properties
        if (properties != null) {
            for (String k : properties.keySet()) {
                entry.setProperty(k, properties.get(k));
            }
        }
    }
}

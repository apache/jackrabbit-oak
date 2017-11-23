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

import static com.google.common.base.Preconditions.checkArgument;
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
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;

/**
 * TODO document
 */
public class IndexUtils {

    private IndexUtils() {
    }

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
    public static Tree createIndexDefinition(@Nonnull Tree indexNode,
                                             @Nonnull String indexDefName,
                                             boolean unique,
                                             @Nonnull String[] propertyNames,
                                             @Nonnull String... declaringNodeTypeNames) throws RepositoryException {

        return createIndexDefinition(indexNode, indexDefName, unique, ImmutableList.copyOf(propertyNames), ImmutableList.copyOf(declaringNodeTypeNames), PropertyIndexEditorProvider.TYPE, null);
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
    public static Tree createIndexDefinition(@Nonnull Tree indexNode,
                                             @Nonnull String indexDefName,
                                             boolean unique,
                                             @Nonnull Collection<String> propertyNames,
                                             @CheckForNull Collection<String> declaringNodeTypeNames,
                                             @Nonnull String propertyIndexType,
                                             @CheckForNull Map<String, String> properties) throws RepositoryException {
        Tree entry = TreeUtil.getOrAddChild(indexNode, indexDefName, INDEX_DEFINITIONS_NODE_TYPE);
        entry.setProperty(TYPE_PROPERTY_NAME, propertyIndexType);
        entry.setProperty(REINDEX_PROPERTY_NAME, true);
        if (unique) {
            entry.setProperty(UNIQUE_PROPERTY_NAME, true);
        }
        if (declaringNodeTypeNames != null && declaringNodeTypeNames.size() > 0) {
            entry.setProperty(DECLARING_NODE_TYPES, declaringNodeTypeNames, Type.NAMES);
        }
        entry.setProperty(PROPERTY_NAMES, propertyNames, Type.NAMES);

        if (properties != null) {
            for (String k : properties.keySet()) {
                entry.setProperty(k, properties.get(k));
            }
        }
        return entry;
    }

    public static void createReferenceIndex(@Nonnull NodeBuilder index) {
        index.child(NodeReferenceConstants.NAME)
                .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, NodeReferenceConstants.TYPE)
                .setProperty("info", "Oak index for reference lookup.");
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
     * @param indexNode                 the oak:index
     * @param indexDefName              the node for the index definition
     * @param unique                    true if uniqueness
     * @param propertyNames             the list of properties to be indexed
     * @param declaringNodeTypeNames
     * @param propertyIndexType         the type of the PropertyIndex
     * @param properties                any additional property to be added to the index definition.
     * @throws RepositoryException
     */
    public static NodeBuilder createIndexDefinition(@Nonnull NodeBuilder indexNode, 
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
        return entry;
    }

    @CheckForNull
    public static String getAsyncLaneName(NodeState idxState, String indexPath) {
        return getAsyncLaneName(idxState, indexPath, idxState.getProperty(IndexConstants.ASYNC_PROPERTY_NAME));
    }

    @CheckForNull
    public static String getAsyncLaneName(NodeState idxState, String indexPath, PropertyState async) {
        if (async != null) {
            Set<String> asyncNames = Sets.newHashSet(async.getValue(Type.STRINGS));
            asyncNames.remove(IndexConstants.INDEXING_MODE_NRT);
            asyncNames.remove(IndexConstants.INDEXING_MODE_SYNC);
            checkArgument(!asyncNames.isEmpty(), "No valid async name found for " +
                    "index [%s], definition %s", indexPath, idxState);
            return Iterables.getOnlyElement(asyncNames);
        }
        return null;
    }
}

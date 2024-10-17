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

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
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
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
public final class IndexUtils {

    private IndexUtils() {
    }

    // Logger used for START/END/FAIL messages of indexing phases. Logged in a separate logger to make them easier to
    // identify and parse by log analysis tools.
    public static final Logger INDEXING_PHASE_LOGGER = LoggerFactory.getLogger("indexing-task");

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
    public static NodeBuilder createIndexDefinition(@NotNull NodeBuilder index,
                                                    @NotNull String indexDefName,
                                                    boolean reindex,
                                                    boolean unique,
                                                    @NotNull Collection<String> propertyNames,
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
    public static Tree createIndexDefinition(@NotNull Tree indexNode,
                                             @NotNull String indexDefName,
                                             boolean unique,
                                             @NotNull String[] propertyNames,
                                             @NotNull String... declaringNodeTypeNames) throws RepositoryException {

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
    public static Tree createIndexDefinition(@NotNull Tree indexNode,
                                             @NotNull String indexDefName,
                                             boolean unique,
                                             @NotNull Collection<String> propertyNames,
                                             @Nullable Collection<String> declaringNodeTypeNames,
                                             @NotNull String propertyIndexType,
                                             @Nullable Map<String, String> properties) throws RepositoryException {
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

    public static void createReferenceIndex(@NotNull NodeBuilder index) {
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
    public static NodeBuilder createIndexDefinition(@NotNull NodeBuilder indexNode, 
                                             @NotNull String indexDefName, 
                                             boolean unique, 
                                             @NotNull Iterable<String> propertyNames, 
                                             @Nullable String[] declaringNodeTypeNames, 
                                             @NotNull String propertyIndexType,
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

    @Nullable
    public static String getAsyncLaneName(NodeState idxState, String indexPath) {
        return getAsyncLaneName(idxState, indexPath, idxState.getProperty(IndexConstants.ASYNC_PROPERTY_NAME));
    }

    @Nullable
    public static String getAsyncLaneName(NodeState idxState, String indexPath, PropertyState async) {
        if (async != null) {
            Set<String> asyncNames = CollectionUtils.toSet(async.getValue(Type.STRINGS));
            asyncNames.remove(IndexConstants.INDEXING_MODE_NRT);
            asyncNames.remove(IndexConstants.INDEXING_MODE_SYNC);
            checkArgument(!asyncNames.isEmpty(), "No valid async name found for " +
                    "index [%s], definition %s", indexPath, idxState);
            return Iterables.getOnlyElement(asyncNames);
        }
        return null;
    }

    /**
     * Retrieves the calling class and method from the call stack; this is determined by unwinding
     * the stack until it finds a combination of full qualified classname + method (separated by ".") which
     * do not start with any of the values provided by the ignoredJavaPackages parameters. If the provided
     * parameters cover all stack frames, the whole query is considered to be internal, where the 
     * actual caller doesn't matter (or cannot be determined clearly). In this case a short message
     * indicating this is returned.
     *
     * If the ignoredJavaPackages parameter is null or empty, the caller is not looked up, but
     * instead it is assumed, that the feature is not configured; in that case a short messages
     * is returned indicating that the feature is not configured.
     *
     * @param ignoredJavaPackages the java packages or class names
     * @return the calling class or another non-null value
     */
    @NotNull
    public static String getCaller(@Nullable String[] ignoredJavaPackages) {
        if (ignoredJavaPackages == null || ignoredJavaPackages.length == 0) {
            return "(<function not configured>)";
        }

        // With java9 we would use https://docs.oracle.com/javase/9/docs/api/java/lang/StackWalker.html
        final StackTraceElement[] callStack = Thread.currentThread().getStackTrace();
        for (StackTraceElement stackFrame : callStack) {
            final String classAndMethod = stackFrame.getClassName() + "." + stackFrame.getMethodName();
            if (Stream.of(ignoredJavaPackages).noneMatch(classAndMethod::startsWith)) {
                return classAndMethod;
            }
        }
        // if every element is ignored, we assume it's an internal request
        return "(internal)";
    }

    /**
     * Returns a Map consisting of checkpoints and checkpointInfoMap filtered based on a given predicate
     * which can utilise checkpoint info map for filtering
     * @param store
     * @param predicate predicate used for filtering of checkpoints
     * @return Map<String, Map<String,String>> filteredCheckpoint Map
     */
    public static Map<String, Map<String,String>> getFilteredCheckpoints(NodeStore store, Predicate<Map.Entry<String, Map<String,String>>> predicate) {
        return StreamSupport.stream(store.checkpoints().spliterator(), false)
                .collect(Collectors.toMap(str -> str,
                        store::checkpointInfo))
                .entrySet()
                .stream()
                .filter(predicate)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Returns a map with checkpoint name as key and checkpoint metadata map as value, sorted on the basis of particular key in the metadata map.
     * For example - given the following checkpoints in the system along with their associated metadata maps -
     * checkpoint3 - {created-epoch=123458, creator=creator1}
     * checkpoint1 - {created-epoch=123456, creator=creator2}
     * checkpoint2 - {created-epoch=123457, creator=creator3}
     * This method should return -
     * {checkpoint1={created-epoch=123456, creator=creator2},
     * checkpoint2={created-epoch=123457, creator=creator3},
     * checkpoint3={created-epoch=123458, creator=creator1}}
     * @param checkpointMap - the map consisting of checkpoints as keys and checkpoint metadata map as values
     * @param keyForComparator - key in the metadata map of the checkpoint that can be used as comparator to sort on checkpoint creation time.
     * @return Map<String, Map<String,String>> sorted checkpoint map
     */
    public static Map<String, Map<String, String>> getSortedCheckpointMap(Map<String, Map<String, String>> checkpointMap,
                                                                                 String keyForComparator) {
        return checkpointMap.entrySet().stream()
                .filter(entry ->  entry.getValue().containsKey(keyForComparator))
                .sorted(Comparator.comparingLong(entry -> Long.parseLong(entry.getValue().get(keyForComparator))))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o, n) -> n, LinkedHashMap::new));
    }
}

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
package org.apache.jackrabbit.oak.plugins.index.optimizer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.diff.JsonNodeBuilder;
import org.apache.jackrabbit.oak.plugins.index.diff.RootIndexesListService;
import org.apache.jackrabbit.oak.plugins.index.diff.predicates.IncludedPathsPredicate;
import org.apache.jackrabbit.oak.plugins.index.diff.predicates.NoTagsPredicate;
import org.apache.jackrabbit.oak.plugins.index.diff.predicates.NodeTypesPredicate;
import org.apache.jackrabbit.oak.plugins.index.diff.predicates.TagSelectionPolicyPredicate;
import org.apache.jackrabbit.oak.plugins.index.diff.predicates.TagsPredicate;
import org.apache.jackrabbit.oak.query.stats.QueryRecorder;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiffIndexUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(DiffIndexUpdater.class);

    public static boolean applyIndexDefinition(NodeStore store, NodeState rootState, NodeBuilder builder, String jsonString, String statement) {
        String simplifiedStatement = QueryRecorder.simplifySafely(statement);
        LOG.info("indexDef {}", jsonString);
        if (!jsonString.trim().startsWith("{")) {
            return false;
        }
        NodeBuilder optimizer = builder.child("oak:index").child("diff.index.optimizer");
        optimizer.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        optimizer.setProperty("type", "disabled", Type.STRING);

        JsonObject json = JsonObject.fromJson(jsonString, true);
        PropertyState ps = rootState.getChildNode("oak:index").getChildNode("diff.index.optimizer").getChildNode("diff.json").getChildNode("jcr:content").getProperty("jcr:data");
        String old = "{}";
        if (ps != null) {
            old = ps.getValue(Type.STRING);
            LOG.info("Old diff.index {}", old);
        }
        JsonObject jsonContent = JsonObject.fromJson(old, true);
        JsonObject index = json.getChildren().get("index");
        if (!index.getProperties().containsKey("includedPaths")) {
            index.getProperties().put("warningNoIncludedPaths", "\"Warning: the query doesn't have a path restriction. This is not recommended. Consider adding a path restriction such as '/content'.\"");
        }
        if (!index.getProperties().containsKey("tags")) {
            index.getProperties().put("warningNoTag", "\"Warning: the query doesn't use a tag. Consider adding a tag using 'option(index tag xyz)' where 'xyz' is the name of the component of the application.\"");
        } else {
            index.getProperties().put("selectionPolicy", "\"tag\"");
        }
        index.getProperties().put("statement", JsopBuilder.encode(simplifiedStatement));
        // search in old indexes if we already optimized for this query
        for (JsonObject existing : jsonContent.getChildren().values()) {
            String oldStatement = existing.getProperties().get("statement");
            if (oldStatement != null && oldStatement.equals("\"" + simplifiedStatement + "\"")) {
                return false;
            }
        }
        Optional<String> bestIndexName = findMatchingIndexName(store, json.toString());
        String newIndexName = null;
        if (bestIndexName.isEmpty()) {
            // get the last number
            String prefix = "auto.indexOptimizer";
            int lastNumber = 0;
            for (String existing : jsonContent.getChildren().keySet()) {
                if (existing.startsWith(prefix)) {
                    String n = existing.substring(prefix.length());
                    try {
                        lastNumber = Math.max(lastNumber, Integer.parseInt(n));
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
            }
            newIndexName = "auto.indexOptimizer" + (lastNumber + 1);
        } else {
            newIndexName = bestIndexName.get();
            if (newIndexName.startsWith("/oak:index/")) {
                newIndexName = newIndexName.substring("/oak:index/".length());
            }
            int dash = newIndexName.indexOf('-');
            if (dash >= 0) {
                newIndexName = newIndexName.substring(0, dash);
            }
        }
        jsonContent.getChildren().put(newIndexName, index);
        String newJsonContent = jsonContent.toString();
        InputStream inputStream = new ByteArrayInputStream(newJsonContent.getBytes(StandardCharsets.UTF_8));
        try {
            Blob blob = store.createBlob(inputStream);
            NodeBuilder diffJson = optimizer.child("diff.json");
            diffJson.setProperty("jcr:primaryType", "nt:file", Type.NAME);
            NodeBuilder diffJsonContent = diffJson.child("jcr:content");
            diffJsonContent.setProperty("jcr:primaryType", "nt:resource", Type.NAME);
            diffJsonContent.setProperty("jcr:mimeType", "application/json");
            diffJsonContent.setProperty("jcr:lastModifiedBy", "Optimizer Service");
            diffJsonContent.setProperty("jcr:lastModified", ISO8601.format(Calendar.getInstance()), Type.DATE);
            diffJsonContent.setProperty("jcr:encoding", "utf-8");
            diffJsonContent.setProperty("jcr:data", blob);
        } catch (IOException e) {
            LOG.warn("Error writing blob", e);
        }
        return true;
    }

    /**
     * Get the included paths for the given index.
     *
     * @param index index JSON
     * @return set of included paths or empty set if no <code>includedPaths</code> property is defined in the index
     */
    private static Set<String> getIncludedPathsForIndex(JsonObject index) {
        Set<String> includedPaths;

        if (index.getProperties().containsKey(PathFilter.PROP_INCLUDED_PATHS)) {
            String[] includedPathsArray = JsonNodeBuilder.oakStringArrayValue(index, PathFilter.PROP_INCLUDED_PATHS);

            includedPaths = Set.of(ArrayUtils.nullToEmpty(includedPathsArray));
        } else {
            includedPaths = Set.of();
        }

        return includedPaths;
    }

    /**
     * Try to find an existing index that matches the node type, tag, and included paths of the provided index JSON.
     *
     * @param store node store
     * @param jsonString index JSON
     * @return name of matching index or <code>Optional.empty()</code> if not found
     */
    public static Optional<String> findMatchingIndexName(NodeStore store, String jsonString) {
        Map<String, JsonObject> indexes = RootIndexesListService.getRootIndexDefinitions(store, "lucene").getChildren();
        JsonObject json = JsonObject.fromJson(jsonString, true);
        JsonObject index = json.getChildren().get(FulltextIndexConstants.PROP_INDEX);

        Set<String> nodeTypes = getNodeTypesForIndex(index);

        Set<Map.Entry<String, JsonObject>> candidateIndexes = indexes.entrySet()
            .stream()
            .filter(entry -> new NodeTypesPredicate(nodeTypes).test(entry.getValue()))
            .collect(Collectors.toSet());

        if (candidateIndexes.size() == 1) {
            // If only one index matches the node type, return it
            return candidateIndexes.stream().map(Map.Entry::getKey).findFirst();
        }

        // Found multiple indexes matching node type, proceed with further filtering/matching
        candidateIndexes = findIndexesWithMatchingTags(index, candidateIndexes);

        if (candidateIndexes.size() == 1) {
            // If only one index matches the node type and tags, return it
            return candidateIndexes.stream().map(Map.Entry::getKey).findFirst();
        }

        // Found multiple indexes matching node type and tags, proceed with further filtering/matching
        candidateIndexes = findIndexesWithMatchingIncludedPaths(index, candidateIndexes);

        return candidateIndexes.stream().map(Map.Entry::getKey).findFirst();
    }

    /**
     * Find existing indexes that include the paths required for the provided index JSON.
     *
     * @param index index JSON
     * @param candidateIndexes set of existing candidate indexes
     * @return set of existing indexes that include the required paths
     */
    private static Set<Map.Entry<String, JsonObject>> findIndexesWithMatchingIncludedPaths(JsonObject index,
        Set<Map.Entry<String, JsonObject>> candidateIndexes) {
        Set<String> includedPaths = getIncludedPathsForIndex(index);

        if (includedPaths.isEmpty()) {
            return candidateIndexes;
        } else {
            Set<Map.Entry<String, JsonObject>> matchingIndexes = candidateIndexes.stream()
                .filter(entry -> new IncludedPathsPredicate(includedPaths).test(entry.getValue()))
                .collect(Collectors.toSet());

            // If no existing indexes match the included paths, return all candidates for further evaluation
            return matchingIndexes.isEmpty() ? candidateIndexes : matchingIndexes;
        }
    }

    /**
     * Find existing indexes that include the tags required for the provided index JSON. If the provided index does not
     * have any tags, then all candidates are returned. If no candidates have matching tags, then attempt to find
     * candidate indexes with no tags.
     *
     * @param index index JSON
     * @param candidateIndexes set of existing candidate indexes
     * @return set of existing indexes that include the required tags
     */
    private static Set<Map.Entry<String, JsonObject>> findIndexesWithMatchingTags(JsonObject index,
        Set<Map.Entry<String, JsonObject>> candidateIndexes) {
        Set<String> tags = getTagsForIndex(index);

        if (tags.isEmpty()) {
            // Filter indexes with a selection policy
            return candidateIndexes.stream()
                .filter(entry -> Predicate.not(TagSelectionPolicyPredicate.INSTANCE).test(entry.getValue()))
                .collect(Collectors.toSet());
        } else {
            // Need to find an index with either a matching tag, or an index with no tags
            Set<Map.Entry<String, JsonObject>> matchingIndexes = candidateIndexes.stream()
                .filter(entry -> new TagsPredicate(tags).test(entry.getValue()))
                .collect(Collectors.toSet());

            if (matchingIndexes.isEmpty()) {
                // No indexes with matching tags, instead try to find an index without tags
                return candidateIndexes.stream()
                    .filter(entry -> new NoTagsPredicate().test(entry.getValue()))
                    .collect(Collectors.toSet());
            } else {
                return matchingIndexes;
            }
        }
    }

    /**
     * Get the tags for the given index.
     *
     * @param index index JSON
     * @return set of tags or empty set if no <code>tags</code> property is defined in the index
     */
    private static Set<String> getTagsForIndex(JsonObject index) {
        Set<String> tags;

        if (index.getProperties().containsKey(IndexConstants.INDEX_TAGS)) {
            String[] tagsArray = JsonNodeBuilder.oakStringArrayValue(index, IndexConstants.INDEX_TAGS);

            tags = Set.of(ArrayUtils.nullToEmpty(tagsArray));
        } else {
            tags = Set.of();
        }

        return tags;
    }

    /**
     * Get the node types defined in the index rules for the given index.
     *
     * @param index index JSON
     * @return set of node types or empty set if no node types are defined in the index
     */
    private static Set<String> getNodeTypesForIndex(JsonObject index) {
        Set<String> nodeTypes;

        if (index.getChildren().containsKey(FulltextIndexConstants.INDEX_RULES)) {
            JsonObject indexRules = index.getChildren().get(FulltextIndexConstants.INDEX_RULES);

            nodeTypes = indexRules.getChildren().keySet()
                .stream()
                .filter(name -> !name.equals(JcrConstants.JCR_PRIMARYTYPE))
                .collect(Collectors.toSet());
        } else {
            nodeTypes = Set.of(JcrConstants.NT_BASE);
        }

        return nodeTypes;
    }


}

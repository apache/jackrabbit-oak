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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.diff.DiffIndex;
import org.apache.jackrabbit.oak.plugins.index.diff.JsonNodeBuilder;
import org.apache.jackrabbit.oak.plugins.index.diff.RootIndexesListService;
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
        LOG.debug("indexDef {}", jsonString);
        if (!jsonString.trim().startsWith("{")) {
            return false;
        }
        NodeBuilder optimizer = builder.child("oak:index").child("diff.index.optimizer");
        optimizer.setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        optimizer.setProperty("type", "disabled", Type.STRING);

        JsonObject json = JsonObject.fromJson(jsonString, true);
        PropertyState jcrData = rootState.getChildNode("oak:index").getChildNode("diff.index.optimizer").getChildNode("diff.json").getChildNode("jcr:content").getProperty("jcr:data");
        String old = "{}";
        if (jcrData != null) {
            old = DiffIndex.tryReadString(jcrData);
            LOG.debug("Old diff.index {}", old);
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
        String prefix;
        if (bestIndexName.isEmpty()) {
            prefix = "auto.indexOptimizer";
        } else {
            prefix = bestIndexName.get();
        }
        if (prefix.startsWith("/oak:index/")) {
            prefix = prefix.substring("/oak:index/".length());
        }
        int dash = prefix.indexOf('-');
        if (dash >= 0) {
            prefix = prefix.substring(0, dash);
        }
        // there might be multiple; if so, append a number
        // (alternatively, we could try to merge)
        int indexNumber = 0;
        for (String existing : jsonContent.getChildren().keySet()) {
            if (existing.startsWith(prefix)) {
                String n = existing.substring(prefix.length());
                if (n.isEmpty()) {
                    indexNumber = 1;
                } else {
                    try {
                        indexNumber = Math.max(indexNumber, Integer.parseInt(n) + 1);
                    } catch (NumberFormatException e) {
                        // ignore
                    }
                }
            }
        }
        String newIndexName = prefix + (indexNumber == 0 ? "" : "" + indexNumber);
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
        Map<String, JsonObject> indexes = RootIndexesListService.getRootIndexDefinitions(store, ".*").getChildren();
        JsonObject json = JsonObject.fromJson(jsonString, true);
        JsonObject index = json.getChildren().get(FulltextIndexConstants.PROP_INDEX);

        Set<String> nodeTypes = getNodeTypesForIndex(index);
        Set<String> includedPaths = getIncludedPathsForIndex(index);
        Set<String> tags = Set.of(ArrayUtils.nullToEmpty(JsonNodeBuilder.oakStringArrayValue(index, IndexConstants.INDEX_TAGS)));
        LOG.info("nodeTypes: {}", nodeTypes);
        LOG.info("includedPaths: {}", includedPaths);
        LOG.info("tags: {}", tags);

        if (nodeTypes.contains("nt:base") && tags.isEmpty()) {
            // do not recommend an index for nt:base, except if there is a tag
            return Optional.empty();
        }

        List<String> remaining = new ArrayList<>();
        for(Entry<String, JsonObject> candidate : indexes.entrySet()) {
            if (candidate.getKey().indexOf("-custom-") >= 0) {
                // ignore custom indexes
                continue;
            }
            JsonObject candidateIndex = candidate.getValue();

            // check node types
            if (!nodeTypes.isEmpty()) {
                // check only one node type (most queries only have one)
                String nodeType = nodeTypes.iterator().next();
                if (!getNodeTypesForIndex(candidateIndex).contains(nodeType)) {
                    // not a match
                    continue;
                }
            }

            // ignore indexes with excludedPaths
            if (candidateIndex.getProperties().containsKey(PathFilter.PROP_EXCLUDED_PATHS)) {
                continue;
            }

            // check includedPaths
            if (includedPaths.isEmpty()) {
                if (!getIncludedPathsForIndex(candidateIndex).isEmpty()) {
                    // not a match
                    continue;
                }
            } else {
                // check only one (the query can only have one path)
                String firstIncludedPaths = includedPaths.iterator().next();
                boolean found = false;
                // iterate over the includedPaths in the index
                // if any of them is a prefix of this path, it's fine
                for (String inc : getIncludedPathsForIndex(candidateIndex)) {
                    if (firstIncludedPaths.startsWith(inc)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    // not a match
                    continue;
                }
            }

            // check tag
            if (tags.isEmpty()) {
                // no tag: only consider without selection policy
                String selectionPolicy = JsonNodeBuilder.oakStringValue(index, IndexConstants.INDEX_SELECTION_POLICY);
                if (selectionPolicy != null) {
                    continue;
                }
            } else {
                // a tag: check if the first one (there's almost always only one in the query) matches
                String tag = tags.iterator().next();
                Set<String> tags2 = Set.of(ArrayUtils.nullToEmpty(JsonNodeBuilder.oakStringArrayValue(index, IndexConstants.INDEX_TAGS)));
                if (!tags2.contains(tag)) {
                    continue;
                }
            }
            remaining.add(candidate.getKey());
        }
        LOG.info("Candidate indexes: {}", remaining);


        return remaining.stream().findFirst();
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
            if ("\"property\"".equals(index.getProperties().get("type"))) {
                Set<String> decl = JsonNodeBuilder.getStringSet(index.getProperties().get("declaringNodeTypes"));
                LOG.info("Found property index with declaring node types: {}", decl);
                return decl == null ? Set.of() : decl;
            }
            nodeTypes = Set.of(JcrConstants.NT_BASE);
        }
        return nodeTypes;
    }


}

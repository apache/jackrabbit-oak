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
package org.apache.jackrabbit.oak.plugins.index.diff;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexName;
import org.apache.jackrabbit.oak.plugins.index.IndexSelectionPolicy;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.diff.predicates.IncludedPathsPredicate;
import org.apache.jackrabbit.oak.plugins.index.diff.predicates.NoTagsPredicate;
import org.apache.jackrabbit.oak.plugins.index.diff.predicates.NodeTypesPredicate;
import org.apache.jackrabbit.oak.plugins.index.diff.predicates.TagSelectionPolicyPredicate;
import org.apache.jackrabbit.oak.plugins.index.diff.predicates.TagsPredicate;
import org.apache.jackrabbit.oak.plugins.index.optimizer.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiffIndex {

    private static final Logger LOG = LoggerFactory.getLogger(DiffIndex.class);

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

    public static void applyChange(NodeStore store, String name, NodeBuilder definition) {
        if (!"disabled".equals(definition.getString("type"))) {
            // only process if the type is "disabled"
            return;
        }
        JsonObject repositoryDefinitions = RootIndexesListService.getRootIndexDefinitions(store, "lucene");

        LOG.debug("Index list {}", repositoryDefinitions.toString());

        NodeBuilder diffJson = definition.getChildNode("diff.json");
        if (!diffJson.exists()) {
            return;
        }
        NodeBuilder jcrContent = diffJson.getChildNode("jcr:content");
        if (!jcrContent.exists()) {
            return;
        }
        String diff = jcrContent.getProperty("jcr:data").getValue(Type.STRING);
        if (diff == null) {
            return;
        }
        JsonObject newImageLuceneDefinitions = new JsonObject();
        try {
            JsonObject diffIndex = new JsonObject();
            diffIndex.getProperties().put("jcr:primaryType",
                    "\"" + IndexConstants.INDEX_DEFINITIONS_NODE_TYPE + "\"");
            diffIndex.getProperties().put("includedPaths", "\"/same\"");
            diffIndex.getProperties().put("queryPaths", "\"/same\"");
            diffIndex.getProperties().put("type", "\"lucene\"");
            JsonObject diffObj = JsonObject.fromJson(diff, true);
            diffIndex.getChildren().put("diff", diffObj);

            newImageLuceneDefinitions.getChildren().put("/oak:index/" + DiffIndexMerger.DIFF_INDEX, diffIndex);
            definition.removeProperty("error");
        } catch (Exception e) {
            LOG.warn("Error parsing diff.index: {}", e.getMessage(), e);
            definition.setProperty("error", e.getMessage());
        }
        try {
            DiffIndexMerger.merge(newImageLuceneDefinitions, repositoryDefinitions, store);
            NodeBuilder rootBuilder = store.getRoot().builder();
            NodeBuilder builder = rootBuilder.child("oak:index");

            for (String m : newImageLuceneDefinitions.getChildren().keySet()) {
                if (m.equals("/oak:index/" + DiffIndexMerger.DIFF_INDEX)) {
                    continue;
                }
                JsonObject newDef = newImageLuceneDefinitions.getChildren().get(m);
                LOG.debug("newDef " + m + ": " + newDef.toString());
                String indexNodeName = PathUtils.getName(m);
                JsonNodeBuilder.addOrReplace(builder, store, indexNodeName, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, newDef.toString());
                disableOrRemoveOldVersions(builder, m);
                sortIndexes(builder);
            }

            List<IndexEditorProvider> indexEditors = List.of(
                    new ReferenceEditorProvider(), new PropertyIndexEditorProvider(), new NodeCounterEditorProvider());
            IndexEditorProvider provider = CompositeIndexEditorProvider.compose(indexEditors);
            EditorHook hook = new EditorHook(new IndexUpdateProvider(provider));
            try {
                store.merge(rootBuilder, hook, CommitInfo.EMPTY);
            } catch (CommitFailedException e) {
                LOG.warn("Can not store indexes", e);
            }

        } catch (Exception e) {
            LOG.warn("Error merging diff.index: {}", e.getMessage(), e);
            definition.setProperty("error", e.getMessage());
        }
    }

    private static void sortIndexes(NodeBuilder builder) {
        ArrayList<String> list = new ArrayList<>();
        for (String child : builder.getChildNodeNames()) {
            list.add(child);
        }
        list.sort(Comparator.naturalOrder());
        builder.setProperty(TreeConstants.OAK_CHILD_ORDER, list, Type.NAMES);
    }

    private static void disableOrRemoveOldVersions(NodeBuilder builder, String m) {
        if (m.startsWith("/oak:index/")) {
            m = m.substring("/oak:index/".length());
        }
        IndexName name = IndexName.parse(m);
        ArrayList<String> toRemove = new ArrayList<>();
        for (String child : builder.getChildNodeNames()) {
            if (child.indexOf("-custom-") < 0) {
                // not a customized or custom index
                continue;
            }
            IndexName n2 = IndexName.parse(child);
            if (name.getBaseName().equals(n2.getBaseName())) {
                if (m.equals(child)) {
                    if (!"disabled".equals(builder.getChildNode(m).getString("type"))) {
                        continue;
                    }
                }
                toRemove.add(child);
            }
        }
        if (toRemove.isEmpty()) {
            return;
        }
        for (String r : toRemove) {
            builder.child(r).remove();
        }
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

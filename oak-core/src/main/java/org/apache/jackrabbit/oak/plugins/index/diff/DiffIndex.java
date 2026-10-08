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

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexName;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processing of diff indexes, that is nodes under "/oak:index/diff.index". A
 * diff index contains differences to existing indexes, and possibly new
 * (custom) indexes in the form of JSON. These changes can then be merged
 * (applied) to the index definitions. This allows to simplify index management,
 * because it allows to modify (add, update) indexes in a simple way.
 */
@Component(service = {})
public class DiffIndex {

    private static final Logger LOG = LoggerFactory.getLogger(DiffIndex.class);

    private final static DiffIndexMerger MERGER = new DiffIndexMerger();

    public static final String LEGACY_DIFF_INDEX_TOGGLE = "FT_LEGACY_DIFF_INDEX_OAK-12441";

    private static final boolean LEGACY_DIFF_INDEX = Boolean.getBoolean("oak.diffIndex.legacy");

    private static volatile Feature legacyDiffIndexFeature;

    @Activate
    private void activate(BundleContext context) {
        legacyDiffIndexFeature = Feature.newFeature(LEGACY_DIFF_INDEX_TOGGLE, new OsgiWhiteboard(context));
    }

    @Deactivate
    private void deactivate() {
        Feature feature = legacyDiffIndexFeature;
        legacyDiffIndexFeature = null;
        if (feature != null) {
            feature.close();
        }
    }

    static boolean isLegacyMode() {
        Feature feature = legacyDiffIndexFeature;
        return LEGACY_DIFF_INDEX || (feature != null && feature.isEnabled());
    }

    private static void applyDiffIndexChangesLegacy(NodeStore store, NodeBuilder indexDefinitions) {
        JsonObject diffs = collectDiffsLegacy(indexDefinitions, MERGER);
        if (diffs != null) {
            processDiffs(store, indexDefinitions, diffs, MERGER);
        }
        storeOrRemoveWarnings(indexDefinitions, MERGER);
    }

    /**
     * Apply changes to the index definitions. That means merge the index diff with
     * the existing indexes, creating new index versions. It might also mean to
     * remove old (merged) indexes if the diff no longer contains them.
     *
     * @param store            the node store
     * @param indexDefinitions the /oak:index node
     */
    public static void applyDiffIndexChanges(NodeStore store, NodeBuilder indexDefinitions) {
        if (isLegacyMode()) {
            applyDiffIndexChangesLegacy(store, indexDefinitions);
            return;
        }
        JsonObject diffs = collectDiffs(indexDefinitions, MERGER);
        if (diffs != null) {
            processDiffs(store, indexDefinitions, diffs, MERGER);
            // only store or remove warnings if there was a change
            storeOrRemoveWarnings(indexDefinitions, MERGER);
        }
    }

    /**
     * Collect only changed diff.json files using the legacy behavior.
     *
     * @param indexDefinitions the node builder for /oak:index
     * @param merger the merger instance to use for collecting warnings
     * @return the diffs, or null if none
     */
    public static JsonObject collectDiffsLegacy(NodeBuilder indexDefinitions, DiffIndexMerger merger) {
        JsonObject diffs = null;
        for (String diffIndex : new String[] {
                DiffIndexMerger.DIFF_INDEX,
                DiffIndexMerger.DIFF_INDEX_OPTIMIZER }) {
            if (!indexDefinitions.hasChildNode(diffIndex)) {
                continue;
            }
            NodeBuilder diffIndexDefinition = indexDefinitions.child(diffIndex);
            NodeBuilder diffContent = diffIndexDefinition.getChildNode("diff.json").getChildNode("jcr:content");
            if (!diffContent.exists()) {
                continue;
            }
            PropertyState lastMod = diffContent.getProperty(NodeTypeConstants.JCR_LASTMODIFIED);
            if (lastMod == null) {
                continue;
            }
            String modified = lastMod.getValue(Type.DATE);
            PropertyState lastProcessed = diffContent.getProperty(DiffIndexMerger.LAST_PROCESSED);
            if (lastProcessed != null) {
                if (modified.equals(lastProcessed.getValue(Type.STRING))) {
                    // already processed
                    continue;
                }
            }
            // store now, so a change is only processed once
            diffContent.setProperty(DiffIndexMerger.LAST_PROCESSED, modified);
            PropertyState jcrData = diffContent.getProperty("jcr:data");
            String diff = tryReadString(jcrData);
            if (diff == null) {
                continue;
            }
            try {
                JsonObject diffObj = JsonObject.fromJson("{\"diff\": " + diff + "}", true);
                diffIndexDefinition.removeProperty("error");
                if (diffs == null) {
                    diffs = new JsonObject();
                }
                diffs.getChildren().put("/oak:index/" + diffIndex, diffObj);
            } catch (Exception e) {
                String message = "Error parsing " + diffIndex;
                merger.logAndCollectWarn("{}: {}", message, e.getMessage());
            }
            if (!diffIndexDefinition.hasProperty("info")) {
                diffIndexDefinition.setProperty("info", "This diff is automatically merged with other indexes. See https://oak-indexing.github.io/oakTools/simplified.html");
            }
        }
        return diffs;
    }

    /**
     * Collect the diffs from the diff.index and diff.index.optimizer.
     *
     * @param indexDefinitions the node builder for /oak:index
     * @param merger the merger instance to use for collecting warnings
     * @return the complete snapshot, or null if absent, unchanged, or invalid
     */
    public static JsonObject collectDiffs(NodeBuilder indexDefinitions, DiffIndexMerger merger) {
        JsonObject result = new JsonObject(true);
        List<String> readErrors = new ArrayList<>();
        boolean wasModified = false;
        for (String diffIndex : new String[] {
                DiffIndexMerger.DIFF_INDEX,
                DiffIndexMerger.DIFF_INDEX_OPTIMIZER }) {
            String path = "/oak:index/" + diffIndex;
            JsonObject files = new JsonObject(true);
            result.getChildren().put(path, files);
            if (!indexDefinitions.hasChildNode(diffIndex)) {
                // Override the old repository snapshot when a diff index was removed.
                files.getChildren().put("diff", new JsonObject(true));
                continue;
            }
            NodeBuilder diffIndexDefinition = indexDefinitions.child(diffIndex);
            List<String> sortedChildren = StreamSupport.stream(
                    diffIndexDefinition.getChildNodeNames().spliterator(), false)
                .sorted().collect(Collectors.toList());
            if (!diffIndexDefinition.hasProperty("info")) {
                diffIndexDefinition.setProperty("info", "This diff is automatically merged with other indexes. See https://oak-indexing.github.io/oakTools/simplified.html");
            }
            for (String child : sortedChildren) {
                String filePath = path + "/" + child;
                NodeBuilder diffContent = diffIndexDefinition.getChildNode(child).getChildNode("jcr:content");
                PropertyState lastMod = diffContent.getProperty(NodeTypeConstants.JCR_LASTMODIFIED);
                if (lastMod != null) {
                    String modified = lastMod.getValue(Type.DATE);
                    if (!modified.equals(diffContent.getString(DiffIndexMerger.LAST_PROCESSED))) {
                        wasModified = true;
                        // Record attempts, including failures, so unchanged files are not processed again.
                        diffContent.setProperty(DiffIndexMerger.LAST_PROCESSED, modified);
                    }
                }
                if (!diffContent.exists()) {
                    if (child.endsWith(".json")) {
                        readErrors.add("jcr:content is missing in " + filePath);
                    }
                    continue;
                }
                PropertyState jcrData = diffContent.getProperty("jcr:data");
                if (jcrData == null) {
                    readErrors.add("jcr:data is missing in " + filePath + "/jcr:content");
                    continue;
                }
                JsonObject content = new JsonObject(true);
                try (InputStream in = jcrData.getValue(Type.BINARY).getNewStream()) {
                    content.getProperties().put("jcr:data", JsopBuilder.encode(
                            ":blobId:" + Base64.getEncoder().encodeToString(in.readAllBytes())));
                } catch (IOException | IllegalArgumentException | IllegalStateException e) {
                    readErrors.add("Cannot read " + filePath + "/jcr:content/jcr:data: " + e.getMessage());
                    continue;
                }
                JsonObject file = new JsonObject(true);
                file.getChildren().put("jcr:content", content);
                files.getChildren().put(child, file);
            }
            if (files.getChildren().keySet().stream().noneMatch(name -> name.endsWith(".json"))) {
                files.getChildren().put("diff", new JsonObject(true));
            }
        }
        if (!wasModified) {
            return null;
        }
        for (String error : readErrors) {
            merger.logAndCollectWarn("{}", error);
        }
        if (!readErrors.isEmpty()) {
            storeOrRemoveWarnings(indexDefinitions, merger);
            return null;
        }
        for (String path : result.getChildren().keySet()) {
            if (merger.tryExtractDiffIndex(result, path, new HashMap<>()) != null) {
                storeOrRemoveWarnings(indexDefinitions, merger);
                return null;
            }
        }
        return result;
    }

    /**
     * Process the diffs.
     *
     * @param store the node store
     * @param indexDefinitions the node builder for /oak:index
     * @param diffs the json object with the combined diffs
     * @param merger the merger instance to use for collecting warnings
     */
    private static void processDiffs(NodeStore store, NodeBuilder indexDefinitions, JsonObject diffs, DiffIndexMerger merger) {
        LOG.info("Processing diffs");
        JsonObject repositoryDefinitions = RootIndexesListService.getRootIndexDefinitions(indexDefinitions);
        LOG.debug("Index list {}", repositoryDefinitions);
        try {
            merger.merge(diffs, repositoryDefinitions, store);
            for (String indexPath : diffs.getChildren().keySet()) {
                if (indexPath.startsWith("/oak:index/" + DiffIndexMerger.DIFF_INDEX)) {
                    continue;
                }
                JsonObject newDef = diffs.getChildren().get(indexPath);
                String indexName = PathUtils.getName(indexPath);
                JsonNodeUpdater.addOrReplace(indexDefinitions, store, indexName,
                        IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, newDef.toString());
                updateNodetypeIndexForPath(indexDefinitions, indexName, true);
                disableOrRemoveOldVersions(indexDefinitions, indexPath, indexName);
            }
            removeDisabledMergedIndexes(indexDefinitions);
            sortIndexes(indexDefinitions);
            if (!isLegacyMode() && indexDefinitions.hasChildNode(DiffIndexMerger.DIFF_INDEX)) {
                indexDefinitions.getChildNode(DiffIndexMerger.DIFF_INDEX).removeProperty("error");
            }
        } catch (Exception e) {
            LOG.warn("Error merging diffs: {}", e.getMessage(), e);
            NodeBuilder diffIndexDefinition = indexDefinitions.child(DiffIndexMerger.DIFF_INDEX);
            diffIndexDefinition.setProperty("error", e.getMessage());
        }
    }

    /**
     * Try to read a text from the (binary) jcr:data property. Edge cases such as
     * "property does not exist" and IO exceptions (blob not found) do not throw an
     * exception (IO exceptions are logged).
     *
     * @param jcrData the "jcr:data" property
     * @return the string, or null if reading fails
     */
    public static String tryReadString(PropertyState jcrData) {
        if (jcrData == null) {
            return null;
        }
        try (InputStream in = jcrData.getValue(Type.BINARY).getNewStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.warn("Can not read jcr:data", e);
            return null;
        }
    }

    /**
     * Store warnings in diff.index, if that node exists.
     * Warnings are stored in separate properties named "warn.01", "warn.02", etc.
     * Any existing "warn." properties are removed first.
     *
     * @param indexDefinitions the node builder for /oak:index
     * @param merger the merger instance to retrieve warnings from
     */
    public static void storeOrRemoveWarnings(NodeBuilder indexDefinitions, DiffIndexMerger merger) {
        if (!indexDefinitions.hasChildNode(DiffIndexMerger.DIFF_INDEX)) {
            return;
        }
        NodeBuilder diffIndexDefinition = indexDefinitions.getChildNode(DiffIndexMerger.DIFF_INDEX);
        // remove existing warn.* properties
        for (PropertyState ps : diffIndexDefinition.getNodeState().getProperties()) {
            if (ps.getName().startsWith("warn.")) {
                diffIndexDefinition.removeProperty(ps.getName());
            }
        }
        List<String> warnings = merger.getAndClearWarnings();
        for (int i = 0; i < warnings.size(); i++) {
            String name = String.format("warn.%02d", i + 1);
            diffIndexDefinition.setProperty(name, warnings.get(i));
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

    private static void removeDisabledMergedIndexes(NodeBuilder definitions) {
        ArrayList<String> toRemove = new ArrayList<>();
        for (String child : definitions.getChildNodeNames()) {
            if (!definitions.getChildNode(child).hasProperty(DiffIndexMerger.MERGE_CHECKSUM)) {
                continue;
            }
            if (IndexConstants.TYPE_DISABLED.equals(definitions.
                    getChildNode(child).
                    getString(IndexConstants.TYPE_PROPERTY_NAME))) {
                toRemove.add(child);
            }
        }
        for (String r : toRemove) {
            LOG.info("Removing disabled index {}", r);
            definitions.child(r).remove();
            updateNodetypeIndexForPath(definitions, r, false);
        }
    }

    /**
     * Try to remove or disable old version of merged indexes, if there are any.
     *
     * @param definitions the builder for /oak:index
     * @param indexPath the path
     * @param keep which index name (which version) to retain
     */
    public static void disableOrRemoveOldVersions(NodeBuilder definitions, String indexPath, String keep) {
        String indexName = indexPath;
        if (indexPath.startsWith("/oak:index/")) {
            indexName = indexPath.substring("/oak:index/".length());
        }
        String baseName = IndexName.parse(indexName).getBaseName();
        ArrayList<String> toRemove = new ArrayList<>();
        for (String child : definitions.getChildNodeNames()) {
            if (child.equals(keep) || child.indexOf("-custom-") < 0) {
                // the one to keep, or not a customized or custom index
                continue;
            }
            String childBaseName = IndexName.parse(child).getBaseName();
            if (baseName.equals(childBaseName)) {
                if (indexName.equals(child)) {
                    // we can not remove it unless it is disabled
                    if (!IndexConstants.TYPE_DISABLED.equals(definitions.
                            getChildNode(indexName).
                            getString(IndexConstants.TYPE_PROPERTY_NAME))) {
                        continue;
                    }
                }
                toRemove.add(child);
            }
        }
        for (String r : toRemove) {
            LOG.info("Removing old index {}", r);
            definitions.child(r).remove();
            updateNodetypeIndexForPath(definitions, r, false);
        }
    }

    private static void updateNodetypeIndexForPath(NodeBuilder indexDefinitions,
            String indexName, boolean add) {
        LOG.info("nodetype index update add={} name={}", add, indexName);
        if (!indexDefinitions.hasChildNode("nodetype")) {
            return;
        }
        NodeBuilder nodetypeIndex = indexDefinitions.getChildNode("nodetype");
        NodeBuilder indexContent = nodetypeIndex.child(":index");
        String key = URLEncoder.encode("oak:QueryIndexDefinition", StandardCharsets.UTF_8);
        String path = "/oak:index/" + indexName;
        if (add) {
            // insert entry
            NodeBuilder builder = indexContent.child(key);
            for (String name : PathUtils.elements(path)) {
                builder = builder.child(name);
            }
            LOG.info("nodetype index match");
            builder.setProperty("match", true);
        } else {
            // remove entry (for deleted indexes)
            NodeBuilder builder = indexContent.getChildNode(key);
            for (String name : PathUtils.elements(path)) {
                builder = builder.getChildNode(name);
            }
            if (builder.exists()) {
                LOG.info("nodetype index remove");
                builder.removeProperty("match");
            }
        }
    }

}

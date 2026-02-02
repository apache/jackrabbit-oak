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
import java.util.Comparator;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexName;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processing of diff indexes, that is nodes under "/oak:index/diff.index". A
 * diff index contains differences to existing indexes, and possibly new
 * (custom) indexes in the form of JSON. These changes can then be merged
 * (applied) to the index definitions. This allows to simplify index management,
 * because it allows to modify (add, update) indexes in a simple way.
 */
public class DiffIndex {

    private static final Logger LOG = LoggerFactory.getLogger(DiffIndex.class);

    /**
     * Apply changes to the index definitions. That means merge the index diff with
     * the existing indexes, creating new index versions. It might also mean to
     * remove old (merged) indexes if the diff no longer contains them.
     *
     * @param store            the node store
     * @param indexDefinitions the /oak:index node
     */
    public static void applyDiffIndexChanges(NodeStore store, NodeBuilder indexDefinitions) {
        JsonObject newImageLuceneDefinitions = null;
        for (String diffIndex : new String[] { DiffIndexMerger.DIFF_INDEX, DiffIndexMerger.DIFF_INDEX_OPTIMIZER }) {
            if (!indexDefinitions.hasChildNode(diffIndex)) {
                continue;
            }
            NodeBuilder diffIndexDefinition = indexDefinitions.child(diffIndex);
            NodeBuilder diffContent = diffIndexDefinition.getChildNode("diff.json").getChildNode("jcr:content");
            if (!diffContent.exists()) {
                continue;
            }
            PropertyState lastMod = diffContent.getProperty("jcr:lastModified");
            if (lastMod == null) {
                continue;
            }
            String modified = lastMod.getValue(Type.DATE);
            PropertyState lastProcessed = diffContent.getProperty(":lastProcessed");
            if (lastProcessed != null) {
                if (modified.equals(lastProcessed.getValue(Type.STRING))) {
                    // already processed
                    continue;
                }
            }
            // store now, so a change is only processed once
            diffContent.setProperty(":lastProcessed", modified);
            PropertyState jcrData = diffContent.getProperty("jcr:data");
            String diff = tryReadString(jcrData);
            if (diff == null) {
                continue;
            }
            try {
                JsonObject diffObj = JsonObject.fromJson("{\"diff\": " + diff + "}", true);
                diffIndexDefinition.removeProperty("error");
                if (newImageLuceneDefinitions == null) {
                    newImageLuceneDefinitions = new JsonObject();
                }
                newImageLuceneDefinitions.getChildren().put("/oak:index/" + diffIndex, diffObj);
            } catch (Exception e) {
                String message = "Error parsing " + diffIndex;
                LOG.warn(message + ": {}", e.getMessage(), e);
                diffIndexDefinition.setProperty("error", message + ": " + e.getMessage());
            }
        }
        if (newImageLuceneDefinitions == null) {
            // not a valid diff index, or already processed
            return;
        }
        LOG.info("Processing a new diff.index with node store {}", store);
        JsonObject repositoryDefinitions = RootIndexesListService.getRootIndexDefinitions(indexDefinitions);
        LOG.debug("Index list {}", repositoryDefinitions.toString());
        try {
            DiffIndexMerger.instance().merge(newImageLuceneDefinitions, repositoryDefinitions, store);
            for (String indexPath : newImageLuceneDefinitions.getChildren().keySet()) {
                if (indexPath.startsWith("/oak:index/" + DiffIndexMerger.DIFF_INDEX)) {
                    continue;
                }
                JsonObject newDef = newImageLuceneDefinitions.getChildren().get(indexPath);
                String indexName = PathUtils.getName(indexPath);
                JsonNodeBuilder.addOrReplace(indexDefinitions, store, indexName, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, newDef.toString());
                updateNodetypeIndexForPath(indexDefinitions, indexName, true);
                disableOrRemoveOldVersions(indexDefinitions, indexPath, indexName);
            }
            removeDisabledMergedIndexes(indexDefinitions);
            sortIndexes(indexDefinitions);
        } catch (Exception e) {
            LOG.warn("Error merging diff.index: {}", e.getMessage(), e);
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
        InputStream in = jcrData.getValue(Type.BINARY).getNewStream();
        try {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.warn("Can not read jcr:data", e);
            return null;
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
            if (!definitions.getChildNode(child).hasProperty("mergeChecksum")) {
                continue;
            }
            if ("disabled".equals(definitions.getChildNode(child).getString("type"))) {
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
    private static void disableOrRemoveOldVersions(NodeBuilder definitions, String indexPath, String keep) {
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
                    if (!"disabled".equals(definitions.getChildNode(indexName).getString("type"))) {
                        continue;
                    }
                }
                toRemove.add(child);
            }
        }
        for (String r : toRemove) {
            LOG.info("Removing old index " + r);
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

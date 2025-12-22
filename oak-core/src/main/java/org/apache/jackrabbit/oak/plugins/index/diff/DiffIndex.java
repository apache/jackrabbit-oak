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

public class DiffIndex {

    private static final Logger LOG = LoggerFactory.getLogger(DiffIndex.class);

    public static void createNewIndexesIfNeeded(NodeStore store, NodeBuilder indexDefinitions) {
        JsonObject newImageLuceneDefinitions = null;
        for (String diffIndex : new String[] { DiffIndexMerger.DIFF_INDEX, DiffIndexMerger.DIFF_INDEX_OPTIMIZER }) {
            if (!indexDefinitions.hasChildNode(diffIndex)) {
                continue;
            }
            NodeBuilder diffIndexDefinition = indexDefinitions.child(diffIndex);
            NodeBuilder diffJson = diffIndexDefinition.getChildNode("diff.json");
            if (!diffJson.exists()) {
                continue;
            }
            NodeBuilder jcrContent = diffJson.getChildNode("jcr:content");
            if (!jcrContent.exists()) {
                continue;
            }
            PropertyState lastMod = jcrContent.getProperty("jcr:lastModified");
            if (lastMod == null) {
                continue;
            }
            String modified = lastMod.getValue(Type.DATE);
            PropertyState lastProcessed = jcrContent.getProperty(":lastProcessed");
            if (lastProcessed != null) {
                if (modified.equals(lastProcessed.getValue(Type.STRING))) {
                    // already processed
                    continue;
                }
            }
            // store now, so a change is only processed once
            jcrContent.setProperty(":lastProcessed", modified);
            PropertyState jcrData = jcrContent.getProperty("jcr:data");
            String diff = readString(jcrData);
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
                String message = "Error parsing diff.index";
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
            DiffIndexMerger.merge(newImageLuceneDefinitions, repositoryDefinitions, store);
            for (String m : newImageLuceneDefinitions.getChildren().keySet()) {
                if (m.startsWith("/oak:index/" + DiffIndexMerger.DIFF_INDEX)) {
                    continue;
                }
                JsonObject newDef = newImageLuceneDefinitions.getChildren().get(m);
                String indexNodeName = PathUtils.getName(m);
                JsonNodeBuilder.addOrReplace(indexDefinitions, store, indexNodeName, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, newDef.toString());
                updateNodetypeIndexForPath(indexDefinitions, indexNodeName, true);
                disableOrRemoveOldVersions(indexDefinitions, m, indexNodeName);
            }
            removeDisabledMergedIndexes(indexDefinitions);
            sortIndexes(indexDefinitions);
        } catch (Exception e) {
            LOG.warn("Error merging diff.index: {}", e.getMessage(), e);
            NodeBuilder diffIndexDefinition = indexDefinitions.child(DiffIndexMerger.DIFF_INDEX);
            diffIndexDefinition.setProperty("error", e.getMessage());
        }
    }

    public static String readString(PropertyState jcrData) {
        InputStream in = jcrData.getValue(Type.BINARY).getNewStream();
        try {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
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
            LOG.info("Removing disabled index " + r);
            definitions.child(r).remove();
            updateNodetypeIndexForPath(definitions, r, false);
        }
    }

    private static void disableOrRemoveOldVersions(NodeBuilder definitions, String m, String except) {
        if (m.startsWith("/oak:index/")) {
            m = m.substring("/oak:index/".length());
        }
        IndexName name = IndexName.parse(m);
        ArrayList<String> toRemove = new ArrayList<>();
        for (String child : definitions.getChildNodeNames()) {
            if (child.indexOf("-custom-") < 0) {
                // not a customized or custom index
                continue;
            }
            if (child.equals(except)) {
                continue;
            }
            IndexName n2 = IndexName.parse(child);
            if (name.getBaseName().equals(n2.getBaseName())) {
                if (m.equals(child)) {
                    if (!"disabled".equals(definitions.getChildNode(m).getString("type"))) {
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

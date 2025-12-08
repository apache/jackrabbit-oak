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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexName;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiffIndex {

    private static final Logger LOG = LoggerFactory.getLogger(DiffIndex.class);

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

}

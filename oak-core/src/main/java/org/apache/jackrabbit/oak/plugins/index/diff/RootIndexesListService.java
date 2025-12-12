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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.inventory.IndexDefinitionPrinter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.Nullable;

class RootIndexesListService implements IndexPathService {

    private final NodeStore nodeStore;

    private RootIndexesListService(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    /**
     * Get the index definitions at /oak:index from a node store.
     *
     * @param nodeStore the source node store (may not be null)
     * @param typePattern the index types (may be null, meaning all)
     * @return a JSON object with all index definitions
     */
    public static JsonObject getRootIndexDefinitions(NodeStore nodeStore, @Nullable String typePattern) {
        if (nodeStore == null) {
            return new JsonObject();
        }
        RootIndexesListService imageIndexPathService = new RootIndexesListService(nodeStore);
        IndexDefinitionPrinter indexDefinitionPrinter = new IndexDefinitionPrinter(nodeStore, imageIndexPathService);
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        indexDefinitionPrinter.print(printWriter, Format.JSON, false);
        printWriter.flush();
        writer.flush();
        String json = writer.toString();
        JsonObject result = JsonObject.fromJson(json, true);
        if (typePattern != null) {
            for (String c : new ArrayList<>(result.getChildren().keySet())) {
                String type = result.getChildren().get(c).getProperties().get("type");
                if (type == null) {
                    continue;
                }
                type = JsopTokenizer.decodeQuoted(type);
                if (type != null && !type.matches(typePattern)) {
                    result.getChildren().remove(c);
                }
            }
        }
        return result;
    }

    @Override
    public Iterable<String> getIndexPaths() {
        ArrayList<String> list = new ArrayList<>();
        NodeState oakIndex = nodeStore.getRoot().getChildNode("oak:index");
        if (!oakIndex.exists()) {
            return list;
        }
        for (ChildNodeEntry cn : oakIndex.getChildNodeEntries()) {
            if (!IndexConstants.INDEX_DEFINITIONS_NODE_TYPE
                    .equals(cn.getNodeState().getName("jcr:primaryType"))) {
                continue;
            }
            list.add("/oak:index/" + cn.getName());
        }
        return list;
    }

}
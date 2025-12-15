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
import java.util.Optional;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.diff.DiffIndex;
import org.apache.jackrabbit.oak.query.stats.QueryRecorder;
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
//        Optional<String> bestIndexName = DiffIndex.findMatchingIndexName(store, json.toString());
        String newIndexName = null;
//        if (bestIndexName.isEmpty()) {
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
//        } else {
//            newIndexName = bestIndexName.get();
//            if (newIndexName.startsWith("/oak:index/")) {
//                newIndexName = newIndexName.substring("/oak:index/".length());
//            }
//            int dash = newIndexName.indexOf('-');
//            if (dash >= 0) {
//                newIndexName = newIndexName.substring(0, dash);
//            }
//        }
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

}

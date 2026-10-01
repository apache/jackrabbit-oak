/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.inventory;

import java.io.PrintWriter;

import org.apache.felix.inventory.Format;
import org.apache.felix.inventory.InventoryPrinter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.diff.DiffIndex;
import org.apache.jackrabbit.oak.plugins.index.diff.DiffIndexMerger;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(
        service = InventoryPrinter.class,
        property = {
                "felix.inventory.printer.name=oak-index-defn",
                "felix.inventory.printer.title=Oak Index Definitions",
                "felix.inventory.printer.format=JSON"
        })
public class IndexDefinitionPrinter implements InventoryPrinter {

    @Reference
    private IndexPathService indexPathService;

    @Reference
    private NodeStore nodeStore;
    
    private String filter = "{\"properties\":[\"*\", \"-:childOrder\"],\"nodes\":[\"*\", \"-:*\"]}";;

    public IndexDefinitionPrinter() {
    }

    public IndexDefinitionPrinter(NodeStore nodeStore, IndexPathService indexPathService) {
        this.indexPathService = indexPathService;
        this.nodeStore = nodeStore;
    }

    @Override
    public void print(PrintWriter printWriter, Format format, boolean isZip) {
        if (format == Format.JSON) {
            NodeState root = nodeStore.getRoot();
            JsopBuilder json = new JsopBuilder();
            json.object();
            for (String indexPath : indexPathService.getIndexPaths()) {
                json.key(indexPath);
                NodeState idxState = NodeStateUtils.getNode(root, indexPath);
                createSerializer(json).serialize(idxState);
            }
            // The "diff" indexes (diff.index / diff.index.optimizer) are not oak:QueryIndexDefinition nodes, so they
            // are not returned by the IndexPathService and would otherwise be missing from the output. Add them
            // explicitly, rendering their diff.json payload as inline JSON so the pending diff is readable.
            for (String name : new String[] {DiffIndexMerger.DIFF_INDEX, DiffIndexMerger.DIFF_INDEX_OPTIMIZER}) {
                String diffPath = "/oak:index/" + name;
                NodeState idxState = NodeStateUtils.getNode(root, diffPath);
                if (idxState.exists()) {
                    json.key(diffPath);
                    serializeDiffIndex(json, idxState);
                }
            }
            json.endObject();
            printWriter.print(JsopBuilder.prettyPrint(json.toString()));
        }
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    private JsonSerializer createSerializer(JsopBuilder json) {
        return new JsonSerializer(json, filter, new Base64BlobSerializer());
    }

    /**
     * Serialize a diff index node, inlining its {@code diff.json} payload as JSON. All other file child nodes are
     * rendered as base64 blobs, for backward compatibility.
     */
    private void serializeDiffIndex(JsopBuilder json, NodeState idxState) {
        json.object();
        JsonSerializer serializer = createSerializer(json);
        // definition properties (mirror the default filter, which drops :childOrder)
        for (PropertyState p : idxState.getProperties()) {
            if (":childOrder".equals(p.getName())) {
                continue;
            }
            json.key(p.getName());
            serializer.serialize(p);
        }
        // non-hidden child nodes other than diff.json, rendered normally
        for (ChildNodeEntry child : idxState.getChildNodeEntries()) {
            String childName = child.getName();
            if (childName.startsWith(":") || "diff.json".equals(childName)) {
                continue;
            }
            json.key(childName);
            createSerializer(json).serialize(child.getNodeState());
        }
        // diff.json: inline the JSON payload instead of a base64 blob
        NodeState content = idxState.getChildNode("diff.json").getChildNode("jcr:content");
        String diff = content.exists() ? DiffIndex.tryReadString(content.getProperty("jcr:data")) : null;
        if (diff != null) {
            json.key("diff.json");
            try {
                // Parse and re-serialize so a malformed diff.json cannot corrupt the whole output.
                JsonObject.fromJson(diff, true).toJson(json);
            } catch (Exception e) {
                // Not valid JSON - keep the endpoint well-formed by emitting the raw payload as a string.
                json.value(diff);
            }
        }
        json.endObject();
    }
}

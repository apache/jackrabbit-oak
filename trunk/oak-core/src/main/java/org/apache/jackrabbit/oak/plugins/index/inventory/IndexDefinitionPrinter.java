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
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
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
}

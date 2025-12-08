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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiffIndexUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(DiffIndexUpdater.class);

    public static boolean applyIndexDefinition(NodeStore store, NodeState rootState, NodeBuilder builder, String jsonString) {
        LOG.info("indexDef {}", jsonString);
        builder.child("oak:index").child("diff.index").setProperty("index", jsonString, Type.STRING);
        JsonObject json = JsonObject.fromJson(jsonString, true);
        PropertyState ps = rootState.getChildNode("oak:index").getChildNode("diff.index").getChildNode("diff.json").getChildNode("jcr:content").getProperty("jcr:data");
        String old = ps.getValue(Type.STRING);
        LOG.info("Old diff.index {}", old);
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
        jsonContent.getChildren().put("auto.indexOptimizer", index);
        String newJsonContent = jsonContent.toString();
        InputStream inputStream = new ByteArrayInputStream(newJsonContent.getBytes(StandardCharsets.UTF_8));
        try {
            Blob blob = store.createBlob(inputStream);
            builder.child("oak:index").child("diff.index").child("diff.json").child("jcr:content").setProperty("jcr:data", blob);
        } catch (IOException e) {
            LOG.warn("Error writing blob", e);
        }
        return true;
    }

}

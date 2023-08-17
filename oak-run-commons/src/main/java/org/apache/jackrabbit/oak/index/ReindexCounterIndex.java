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
package org.apache.jackrabbit.oak.index;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class ReindexCounterIndex {

    public static void reindexNodeCounter(NodeStore ns, String checkpoint, File outDir) throws CommitFailedException, IOException {
        NodeState before = EmptyNodeState.MISSING_NODE;
        NodeState root = ns.getRoot();
        NodeBuilder builder = root.getChildNode("oak:index").getChildNode("counter").builder();
        Editor editor = new NodeCounterEditorProvider().getIndexEditor("counter",
                builder, root, new IndexUpdateCallback() {
                    @Override
                    public void indexUpdate() throws CommitFailedException {
                        // nothing to do
                    }
        });
        CommitFailedException exception = EditorDiff.process(VisibleEditor.wrap(editor), before, root);
        if (exception != null) {
            throw exception;
        }
        JsopBuilder json = new JsopBuilder();
        json.object().key("/oak:index/counter");
        new JsonSerializer(json, "{}", new Base64BlobSerializer()).serialize(builder.getNodeState());
        json.endObject();

        String result = json.toString();
        File out = new File(outDir, "index-definitions.json");
        FileUtils.writeStringToFile(out, result, StandardCharsets.UTF_8);

        result = "checkpoint=" + checkpoint;
        out = new File(outDir, "indexer-info.properties");
        FileUtils.writeStringToFile(out, result, StandardCharsets.UTF_8);

        result = "indexPath=/oak:index/counter";
        out = new File(new File(outDir, "counter"), "index-details.txt");
        FileUtils.writeStringToFile(out, result, StandardCharsets.UTF_8);

        System.out.printf("Result stored in %s%n", outDir);

    }

}

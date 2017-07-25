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

package org.apache.jackrabbit.oak.plugins.index.importer;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonDeserializer;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.index.importer.NodeStoreUtils.childBuilder;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public class IndexDefinitionUpdater {
    /**
     * Name of file which would be check for presence of index-definitions
     */
    public static final String INDEX_DEFINITIONS_JSON = "index-definitions.json";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final File file;

    public IndexDefinitionUpdater(File file) {
        checkArgument(file.exists() && file.canRead(), "File [%s] cannot be read", file);
        this.file = file;
    }

    public void apply(NodeState root, NodeBuilder builder) throws IOException, CommitFailedException {
        for (Map.Entry<String, NodeState> cne : getIndexDefnStates().entrySet()) {
            String indexPath = cne.getKey();

            if (!indexPath.startsWith("/")) {
                String msg = String.format("Invalid format of index definitions. The key name [%s] should " +
                        "be index path ", indexPath);
                throw new IllegalArgumentException(msg);
            }

            String indexNodeName = PathUtils.getName(indexPath);

            NodeState newDefinition = cne.getValue();
            String parentPath = PathUtils.getParentPath(indexPath);
            NodeState parent = NodeStateUtils.getNode(root, parentPath);

            checkState(parent.exists(), "Parent node at path [%s] not found while " +
                    "adding new index definition for [%s]. Intermediate paths node must exist for new index " +
                    "nodes to be created", parentPath,indexPath);

            NodeState existing = parent.getChildNode(indexNodeName);


            if (!existing.exists()) {
                log.info("Adding new index definition at path [{}]", indexPath);
            } else {
                log.info("Updating index definition at path [{}]. Changes are ", indexPath);
                String diff = JsopDiff.diffToJsop(cloneVisibleState(existing), cloneVisibleState(newDefinition));
                log.info(diff);
            }


            NodeBuilder indexBuilder = childBuilder(builder, parentPath);
            indexBuilder.setChildNode(indexNodeName, newDefinition);
        }
    }

    private Map<String, NodeState> getIndexDefnStates() throws IOException {
        String json = FileUtils.readFileToString(file, Charsets.UTF_8);
        Base64BlobSerializer blobHandler = new Base64BlobSerializer();
        Map<String, NodeState> indexDefns = Maps.newHashMap();
        JsopReader reader = new JsopTokenizer(json);
        reader.read('{');
        if (!reader.matches('}')) {
            do {
                String indexPath = reader.readString();
                reader.read(':');
                if (reader.matches('{')) {
                    JsonDeserializer deserializer = new JsonDeserializer(blobHandler);
                    NodeState idxState = deserializer.deserialize(reader);
                    indexDefns.put(indexPath, idxState);
                }
            } while (reader.matches(','));
            reader.read('}');
        }
        return indexDefns;
    }

    private static NodeState cloneVisibleState(NodeState state){
        NodeBuilder builder = EMPTY_NODE.builder();
        new ApplyVisibleDiff(builder).apply(state);
        return builder.getNodeState();
    }

    private static class ApplyVisibleDiff extends ApplyDiff {
        public ApplyVisibleDiff(NodeBuilder builder) {
            super(builder);
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (NodeStateUtils.isHidden(name)){
                return true;
            }
            return after.compareAgainstBaseState(
                    EMPTY_NODE, new ApplyVisibleDiff(builder.child(name)));
        }
    }
}

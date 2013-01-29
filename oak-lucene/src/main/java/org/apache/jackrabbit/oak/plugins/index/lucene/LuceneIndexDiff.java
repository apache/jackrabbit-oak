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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;

/**
 * {@link IndexHook} implementation that is responsible for keeping the
 * {@link LuceneIndex} up to date
 * 
 * @see LuceneIndex
 * 
 */
public class LuceneIndexDiff implements IndexHook, LuceneIndexConstants {

    private final LuceneIndexDiff parent;

    private final NodeBuilder node;

    private final String name;

    private String path;

    private final Map<String, LuceneIndexUpdate> updates;

    private final Parser parser = new AutoDetectParser(
            TikaConfig.getDefaultConfig());

    private LuceneIndexDiff(LuceneIndexDiff parent, NodeBuilder node,
            String name, String path, Map<String, LuceneIndexUpdate> updates) {
        this.parent = parent;
        this.node = node;
        this.name = name;
        this.path = path;
        this.updates = updates;

        if (node != null && node.hasChildNode(INDEX_DEFINITIONS_NAME)) {
            NodeBuilder index = node.child(INDEX_DEFINITIONS_NAME);
            for (String indexName : index.getChildNodeNames()) {
                NodeBuilder child = index.child(indexName);
                if (isIndexNode(child) && !this.updates.containsKey(getPath())) {
                    this.updates.put(getPath(), new LuceneIndexUpdate(
                            getPath(), child, parser));
                }
            }
        }
        if (node != null && name != null && !NodeStateUtils.isHidden(name)) {
            for (LuceneIndexUpdate update : updates.values()) {
                update.insert(getPath(), node);
            }
        }
    }

    private LuceneIndexDiff(LuceneIndexDiff parent, String name) {
        this(parent, getChildNode(parent.node, name), name, null,
                parent.updates);
    }

    public LuceneIndexDiff(NodeBuilder root) {
        this(null, root, null, "/", new HashMap<String, LuceneIndexUpdate>());
    }

    private static NodeBuilder getChildNode(NodeBuilder node, String name) {
        if (node != null && node.hasChildNode(name)) {
            return node.child(name);
        } else {
            return null;
        }
    }

    public String getPath() {
        if (path == null) { // => parent != null
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    private static boolean isIndexNode(NodeBuilder node) {
        PropertyState ps = node.getProperty(JCR_PRIMARYTYPE);
        boolean isNodeType = ps != null && !ps.isArray()
                && ps.getValue(Type.STRING).equals(INDEX_DEFINITIONS_NODE_TYPE);
        if (!isNodeType) {
            return false;
        }
        PropertyState type = node.getProperty(TYPE_PROPERTY_NAME);
        boolean isIndexType = type != null && !type.isArray()
                && type.getValue(Type.STRING).equals(TYPE_LUCENE);
        return isIndexType;
    }

    // -----------------------------------------------------< NodeStateDiff >--

    @Override
    public void propertyAdded(PropertyState after) {
        for (LuceneIndexUpdate update : updates.values()) {
            update.insert(getPath(), node);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        for (LuceneIndexUpdate update : updates.values()) {
            update.insert(getPath(), node);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        for (LuceneIndexUpdate update : updates.values()) {
            update.insert(getPath(), node);
        }
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        if (NodeStateUtils.isHidden(name)) {
            return;
        }
        for (LuceneIndexUpdate update : updates.values()) {
            update.insert(concat(getPath(), name), new ReadOnlyBuilder(after));
        }
        after.compareAgainstBaseState(MemoryNodeState.EMPTY_NODE, child(name));
    }

    @Override
    public void childNodeChanged(String name, NodeState before, NodeState after) {
        if (NodeStateUtils.isHidden(name)) {
            return;
        }
        after.compareAgainstBaseState(before, child(name));
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        if (NodeStateUtils.isHidden(name)) {
            return;
        }
        for (LuceneIndexUpdate update : updates.values()) {
            update.remove(concat(getPath(), name));
        }
    }

    // -----------------------------------------------------< IndexHook >--

    @Override
    public void apply() throws CommitFailedException {
        for (LuceneIndexUpdate update : updates.values()) {
            update.apply();
        }
    }

    @Override
    public void reindex(NodeBuilder state) throws CommitFailedException {
        boolean reindex = false;
        for (LuceneIndexUpdate update : updates.values()) {
            if (update.getAndResetReindexFlag()) {
                reindex = true;
            }
        }
        if (reindex) {
            state.getNodeState().compareAgainstBaseState(
                    MemoryNodeState.EMPTY_NODE,
                    new LuceneIndexDiff(null, state, null, "/", updates));
        }
    }

    @Override
    public IndexHook child(String name) {
        return new LuceneIndexDiff(this, name);
    }

    @Override
    public void close() throws IOException {
        for (LuceneIndexUpdate update : updates.values()) {
            update.close();
        }
        updates.clear();
    }
}

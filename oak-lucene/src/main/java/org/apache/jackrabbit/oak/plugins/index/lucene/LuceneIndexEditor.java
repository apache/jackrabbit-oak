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

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getString;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getChildOrNull;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.isHidden;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;

/**
 * {@link IndexEditor} implementation that is responsible for keeping the
 * {@link LuceneIndex} up to date
 * 
 * @see LuceneIndex
 * 
 */
public class LuceneIndexEditor implements IndexEditor, Closeable {

    private final LuceneIndexEditor parent;

    private final NodeBuilder node;

    private final String name;

    private String path;

    private final Map<String, LuceneIndexUpdate> updates;

    /**
     * the root editor in charge of applying the updates
     */
    private final boolean isRoot;

    private final Parser parser = new AutoDetectParser(
            TikaConfig.getDefaultConfig());

    private LuceneIndexEditor(LuceneIndexEditor parent, NodeBuilder node,
            String name, String path, Map<String, LuceneIndexUpdate> updates,
            boolean isRoot) {
        this.parent = parent;
        this.node = node;
        this.name = name;
        this.path = path;
        this.updates = updates;
        this.isRoot = isRoot;
    }

    private LuceneIndexEditor(LuceneIndexEditor parent, String name) {
        this(parent, getChildOrNull(parent.node, name), name, null,
                parent.updates, false);
    }

    public LuceneIndexEditor(NodeBuilder root) {
        this(null, root, null, "/", new HashMap<String, LuceneIndexUpdate>(),
                true);
    }

    public String getPath() {
        if (path == null) { // => parent != null
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    private static boolean isIndexNode(NodeBuilder node) {
        return TYPE_LUCENE.equals(getString(node, TYPE_PROPERTY_NAME));
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
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
        if (node != null && name != null && !isHidden(name)) {
            for (LuceneIndexUpdate update : updates.values()) {
                update.insert(getPath(), node);
            }
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (!isRoot) {
            return;
        }
        for (LuceneIndexUpdate update : updates.values()) {
            update.apply();
        }
        updates.clear();
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        for (LuceneIndexUpdate update : updates.values()) {
            update.insert(getPath(), node);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        for (LuceneIndexUpdate update : updates.values()) {
            update.insert(getPath(), node);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        for (LuceneIndexUpdate update : updates.values()) {
            update.insert(getPath(), node);
        }
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        if (isHidden(name)) {
            return null;
        }
        for (LuceneIndexUpdate update : updates.values()) {
            update.insert(concat(getPath(), name), new ReadOnlyBuilder(after));
        }
        return childNodeChanged(name, EMPTY_NODE, after);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before,
            NodeState after) throws CommitFailedException {
        if (isHidden(name)) {
            return null;
        }
        return new LuceneIndexEditor(this, name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        if (isHidden(name)) {
            return null;
        }
        for (LuceneIndexUpdate update : updates.values()) {
            update.remove(concat(getPath(), name));
        }
        return null;
    }

    // -----------------------------------------------------< Closeable >--

    @Override
    public void close() throws IOException {
        for (LuceneIndexUpdate update : updates.values()) {
            update.close();
        }
        updates.clear();
    }
}

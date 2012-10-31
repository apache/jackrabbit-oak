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
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_UNKNOWN;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

/**
 * Keeps existing IndexHooks updated.
 * 
 * <p>
 * The existing index list is obtained via the IndexHookProvider.
 * </p>
 * 
 * @see IndexHook
 * @see IndexHookProvider
 * 
 */
public class IndexHookManager implements CommitHook {

    private final IndexHookProvider provider;

    public IndexHookManager(IndexHookProvider provider) {
        this.provider = provider;
    }

    @Override
    public NodeState processCommit(NodeState before, NodeState after)
            throws CommitFailedException {
        NodeBuilder builder = after.builder();

        // <path>, <builder>
        Map<String, NodeBuilder> allDefs = new HashMap<String, NodeBuilder>();
        after.compareAgainstBaseState(before,
                new IndexDefDiff(builder, allDefs));

        // <type, <<path>, <builder>>
        Map<String, Map<String, NodeBuilder>> updates = new HashMap<String, Map<String, NodeBuilder>>();
        for (String def : allDefs.keySet()) {
            NodeBuilder cb = allDefs.get(def);
            String type = TYPE_UNKNOWN;
            PropertyState typePS = cb.getProperty(TYPE_PROPERTY_NAME);
            if (typePS != null && !typePS.isArray()) {
                type = typePS.getValue(Type.STRING);
            }
            Map<String, NodeBuilder> defs = updates.get(type);
            if (defs == null) {
                defs = new HashMap<String, NodeBuilder>();
                updates.put(type, defs);
            }
            defs.put(def, cb);
        }

        // commit
        List<IndexHook> hooks = new ArrayList<IndexHook>();
        List<IndexHook> reindexHooks = new ArrayList<IndexHook>();

        for (String type : updates.keySet()) {
            Map<String, NodeBuilder> update = updates.get(type);
            for (String path : update.keySet()) {
                NodeBuilder updateBuiler = update.get(path);
                boolean reindex = getAndResetReindex(updateBuiler);
                if (reindex) {
                    reindexHooks.addAll(provider.getIndexHooks(type,
                            updateBuiler));
                } else {
                    hooks.addAll(provider.getIndexHooks(type, updateBuiler));
                }
            }
        }
        processIndexHooks(reindexHooks, MemoryNodeState.EMPTY_NODE, after);
        processIndexHooks(hooks, before, after);
        return builder.getNodeState();
    }

    private void processIndexHooks(List<IndexHook> hooks, NodeState before,
            NodeState after) throws CommitFailedException {
        try {

            List<NodeStateDiff> diffs = new ArrayList<NodeStateDiff>();
            for (IndexHook hook : hooks) {
                diffs.add(hook.preProcess());
            }
            after.compareAgainstBaseState(before, new CompositeNodeStateDiff(
                    diffs));
            for (IndexHook hook : hooks) {
                hook.postProcess();
            }

        } finally {
            for (IndexHook hook : hooks) {
                try {
                    hook.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new CommitFailedException(
                            "Failed to close the index hook", e);
                }
            }
        }
    }

    protected static boolean getAndResetReindex(NodeBuilder builder) {
        boolean isReindex = false;
        PropertyState ps = builder.getProperty(REINDEX_PROPERTY_NAME);
        if (ps != null) {
            isReindex = ps.getValue(Type.BOOLEAN);
            builder.setProperty(REINDEX_PROPERTY_NAME, false);
        }
        return isReindex;
    }

    protected static class IndexDefDiff implements NodeStateDiff {

        private final IndexDefDiff parent;

        private final NodeBuilder node;

        private final String name;

        private String path;

        private final Map<String, NodeBuilder> updates;

        public IndexDefDiff(IndexDefDiff parent, NodeBuilder node, String name,
                String path, Map<String, NodeBuilder> updates) {
            this.parent = parent;
            this.node = node;
            this.name = name;
            this.path = path;
            this.updates = updates;

            if (node != null
                    && isIndexNodeType(node.getProperty(JCR_PRIMARYTYPE))) {
                node.setProperty(REINDEX_PROPERTY_NAME, true);
                this.updates.put(getPath(), node);
            }

            if (node != null && node.hasChildNode(INDEX_DEFINITIONS_NAME)) {
                NodeBuilder index = node.child(INDEX_DEFINITIONS_NAME);
                for (String indexName : index.getChildNodeNames()) {
                    String indexPath = concat(getPath(),
                            INDEX_DEFINITIONS_NAME, indexName);
                    NodeBuilder indexChild = index.child(indexName);
                    if (isIndexNodeType(indexChild.getProperty(JCR_PRIMARYTYPE))) {
                        this.updates.put(indexPath, indexChild);
                    }
                }
            }
        }

        public IndexDefDiff(NodeBuilder root, Map<String, NodeBuilder> updates) {
            this(null, root, null, "/", updates);
        }

        public IndexDefDiff(IndexDefDiff parent, String name) {
            this(parent, getChildNode(parent.node, name), name, null,
                    parent.updates);
        }

        private static NodeBuilder getChildNode(NodeBuilder node, String name) {
            if (node != null && node.hasChildNode(name)) {
                return node.child(name);
            } else {
                return null;
            }
        }

        private String getPath() {
            if (path == null) { // => parent != null
                path = concat(parent.getPath(), name);
            }
            return path;
        }

        @Override
        public void propertyAdded(PropertyState after) {
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
        }

        @Override
        public void propertyDeleted(PropertyState before) {
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            childNodeChanged(name, MemoryNodeState.EMPTY_NODE, after);
        }

        @Override
        public void childNodeChanged(String name, NodeState before,
                NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                return;
            }
            after.compareAgainstBaseState(before, new IndexDefDiff(this, name));
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
        }

        private boolean isIndexNodeType(PropertyState ps) {
            return ps != null
                    && !ps.isArray()
                    && ps.getValue(Type.STRING).equals(
                            INDEX_DEFINITIONS_NODE_TYPE);
        }

    }
}

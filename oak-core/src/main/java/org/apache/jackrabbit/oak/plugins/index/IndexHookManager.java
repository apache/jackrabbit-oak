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
        Map<String, NodeBuilder> changedDefs = new HashMap<String, NodeBuilder>();
        // <path>, <builder>
        Map<String, NodeBuilder> existingDefs = new HashMap<String, NodeBuilder>();

        IndexDefDiff diff = new IndexDefDiff(builder, changedDefs, existingDefs);
        after.compareAgainstBaseState(before, diff);

        // <path>, <builder>
        Map<String, NodeBuilder> allDefs = new HashMap<String, NodeBuilder>(
                changedDefs);
        allDefs.putAll(existingDefs);

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
        for (String type : updates.keySet()) {
            Map<String, NodeBuilder> indexDefs = updates.get(type);
            for (String p : indexDefs.keySet()) {
                List<? extends IndexHook> hooks = provider.getIndexHooks(type,
                        indexDefs.get(p));
                for (IndexHook hook : hooks) {
                    boolean reindex = changedDefs.keySet().contains(p);
                    if (reindex) {
                        hook.processCommit(MemoryNodeState.EMPTY_NODE, after);
                    } else {
                        hook.processCommit(before, after);
                    }
                }
            }
        }
        return builder.getNodeState();
    }

    protected static class IndexDefDiff implements NodeStateDiff {

        private final IndexDefDiff parent;

        private final NodeBuilder node;

        private final String name;

        private String path;

        private final Map<String, NodeBuilder> updates;
        private final Map<String, NodeBuilder> existing;

        public IndexDefDiff(IndexDefDiff parent, NodeBuilder node, String name,
                String path, Map<String, NodeBuilder> updates,
                Map<String, NodeBuilder> existing) {
            this.parent = parent;
            this.node = node;
            this.name = name;
            this.path = path;
            this.updates = updates;
            this.existing = existing;

            if (node != null
                    && isIndexNodeType(node.getProperty(JCR_PRIMARYTYPE))) {
                getAndResetReindex(node);
                this.updates.put(getPath(), node);
                this.existing.remove(getPath());
            }

            if (node != null && node.hasChildNode(INDEX_DEFINITIONS_NAME)) {
                NodeBuilder index = node.child(INDEX_DEFINITIONS_NAME);
                for (String indexName : index.getChildNodeNames()) {
                    String indexPath = concat(getPath(),
                            INDEX_DEFINITIONS_NAME, indexName);
                    NodeBuilder indexChild = index.child(indexName);
                    if (isIndexNodeType(indexChild.getProperty(JCR_PRIMARYTYPE))) {
                        boolean reindex = getAndResetReindex(indexChild);
                        if (reindex) {
                            this.updates.put(indexPath, indexChild);
                        } else {
                            this.existing.put(indexPath, indexChild);
                        }
                    }
                }
            }
        }

        public IndexDefDiff(NodeBuilder root, Map<String, NodeBuilder> updates,
                Map<String, NodeBuilder> existing) {
            this(null, root, null, "/", updates, existing);
        }

        public IndexDefDiff(IndexDefDiff parent, String name) {
            this(parent, getChildNode(parent.node, name), name, null,
                    parent.updates, parent.existing);
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

        private boolean getAndResetReindex(NodeBuilder builder) {
            boolean isReindex = false;
            PropertyState ps = builder.getProperty(REINDEX_PROPERTY_NAME);
            if (ps != null) {
                isReindex = ps.getValue(Type.BOOLEAN);
                builder.setProperty(REINDEX_PROPERTY_NAME, false);
            }
            return isReindex;
        }
    }
}

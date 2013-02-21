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
package org.apache.jackrabbit.oak.plugins.index.solr.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.plugins.index.solr.OakSolrConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.solr.client.solrj.SolrServer;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

/**
 * {@link IndexHook} implementation that is responsible for keeping the
 * {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex} up to date
 * <p/>
 * This handles index updates by keeping a {@link Map} of <code>String</code>
 * and {@link SolrIndexUpdate} for each path.
 *
 * @see org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndex
 * @see SolrIndexHook
 */
public class SolrIndexDiff implements IndexHook {

    private final SolrIndexDiff parent;

    private final NodeBuilder node;

    private final String name;

    private String path;

    private final Map<String, SolrIndexUpdate> updates;

    private SolrServer solrServer;

    private OakSolrConfiguration configuration;

    private SolrIndexDiff(SolrIndexDiff parent, NodeBuilder node, SolrServer solrServer,
                          String name, String path, Map<String, SolrIndexUpdate> updates, OakSolrConfiguration configuration) {
        this.parent = parent;
        this.node = node;
        this.name = name;
        this.path = path;
        this.updates = updates;
        this.solrServer = solrServer;
        this.configuration = configuration;

        // TODO : test properly on PDFs

        if (node != null && node.hasChildNode("oak:index")) {
            NodeBuilder index = node.child("oak:index");
            for (String indexName : index.getChildNodeNames()) {
                NodeBuilder child = index.child(indexName);
                if (isIndexNode(child) && !this.updates.containsKey(getPath())) {
                    this.updates.put(getPath(), new SolrIndexUpdate(
                            getPath(), child, configuration));
                }
            }
        }
        if (node != null && name != null && !NodeStateUtils.isHidden(name)) {
            for (SolrIndexUpdate update : updates.values()) {
                update.insert(getPath(), node);
            }
        }
    }

    private SolrIndexDiff(SolrIndexDiff parent, SolrServer solrServer, String name) {
        this(parent, getChildNode(parent.node, name), solrServer, name, null,
                parent.updates, parent.configuration);
    }

    public SolrIndexDiff(NodeBuilder root, SolrServer solrServer, OakSolrConfiguration configuration) {
        this(null, root, solrServer, null, "/", new HashMap<String, SolrIndexUpdate>(), configuration);
    }

    private static NodeBuilder getChildNode(NodeBuilder node, String name) {
        if (node != null && node.hasChildNode(name)) {
            return node.child(name);
        } else {
            return null;
        }
    }

    public String getPath() {
        if (path == null) {
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    private static boolean isIndexNode(NodeBuilder node) {
        PropertyState ps = node.getProperty(JCR_PRIMARYTYPE);
        boolean isNodeType = ps != null && !ps.isArray()
                && ps.getValue(Type.STRING).equals("oak:queryIndexDefinition");
        if (!isNodeType) {
            return false;
        }
        PropertyState type = node.getProperty("type");
        boolean isIndexType = type != null && !type.isArray()
                && type.getValue(Type.STRING).equals(SolrQueryIndex.TYPE);
        return isIndexType;
    }

    // -----------------------------------------------------< NodeStateDiff >--

    @Override
    public void propertyAdded(PropertyState after) {
        for (SolrIndexUpdate update : updates.values()) {
            update.insert(getPath(), node);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        for (SolrIndexUpdate update : updates.values()) {
            update.insert(getPath(), node);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        for (SolrIndexUpdate update : updates.values()) {
            update.insert(getPath(), node);
        }
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        if (NodeStateUtils.isHidden(name)) {
            return;
        }
        for (SolrIndexUpdate update : updates.values()) {
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
        for (SolrIndexUpdate update : updates.values()) {
            update.remove(concat(getPath(), name));
        }
    }

    // -----------------------------------------------------< IndexHook >--

    @Override
    public void apply() throws CommitFailedException {
        for (SolrIndexUpdate update : updates.values()) {
            update.apply(solrServer);
        }
    }

    @Override
    public void reindex(NodeBuilder state) throws CommitFailedException {
        boolean reindex = false;
        for (SolrIndexUpdate update : updates.values()) {
            if (update.getAndResetReindexFlag()) {
                reindex = true;
            }
        }
        if (reindex) {
            state.getNodeState().compareAgainstBaseState(
                    MemoryNodeState.EMPTY_NODE,
                    new SolrIndexDiff(null, state, solrServer, null, "/", updates, configuration));
        }
    }

    @Override
    public IndexHook child(String name) {
        return new SolrIndexDiff(this, solrServer, name);
    }

    @Override
    public void close() throws IOException {
        for (SolrIndexUpdate update : updates.values()) {
            update.close();
        }
        updates.clear();
    }

}

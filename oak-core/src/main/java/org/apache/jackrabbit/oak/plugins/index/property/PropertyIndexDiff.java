/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndex.TYPE;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

/**
 * {@link IndexHook} implementation that is responsible for keeping the
 * {@link PropertyIndex} up to date
 * 
 * @see PropertyIndex
 * @see PropertyIndexLookup
 * 
 */
class PropertyIndexDiff implements IndexHook {

    private final PropertyIndexDiff parent;

    private final NodeBuilder node;

    private final String name;

    private String path;

    private final Map<String, List<PropertyIndexUpdate>> updates;

    private PropertyIndexDiff(
            PropertyIndexDiff parent,
            NodeBuilder node, String name, String path,
            Map<String, List<PropertyIndexUpdate>> updates) {
        this.parent = parent;
        this.node = node;
        this.name = name;
        this.path = path;
        this.updates = updates;

        if (node != null && node.hasChildNode(INDEX_DEFINITIONS_NAME)) {
            NodeBuilder index = node.child(INDEX_DEFINITIONS_NAME);
            for (String indexName : index.getChildNodeNames()) {
                NodeBuilder child = index.child(indexName);
                if (isIndexNode(child)) {
                    update(child, indexName);
                }
            }
        }
    }

    private PropertyIndexDiff(PropertyIndexDiff parent, String name) {
        this(parent, getChildNode(parent.node, name),
                name, null, parent.updates);
    }

    public PropertyIndexDiff(NodeBuilder root) {
        this(null, root, null, "/",
                new HashMap<String, List<PropertyIndexUpdate>>());
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

    private Iterable<PropertyIndexUpdate> getIndexes(String name) {
        List<PropertyIndexUpdate> indexes = updates.get(name);
        if (indexes != null) {
            return indexes;
        } else {
            return ImmutableList.of();
        }
    }

    private void update(NodeBuilder builder, String indexName) {
        PropertyState ps = builder.getProperty("propertyNames");
        Iterable<String> propertyNames = ps != null ? ps.getValue(Type.STRINGS)
                : ImmutableList.of(indexName);
        for (String pname : propertyNames) {
            List<PropertyIndexUpdate> list = this.updates.get(pname);
            if (list == null) {
                list = Lists.newArrayList();
                this.updates.put(pname, list);
            }
            boolean exists = false;
            for (PropertyIndexUpdate piu : list) {
                if (piu.getPath().equals(getPath())) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                list.add(new PropertyIndexUpdate(getPath(), builder));
            }
        }
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
                && type.getValue(Type.STRING).equals(TYPE);
        return isIndexType;
    }

    //-----------------------------------------------------< NodeStateDiff >--

    @Override
    public void propertyAdded(PropertyState after) {
        for (PropertyIndexUpdate update : getIndexes(after.getName())) {
            update.insert(getPath(), after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        for (PropertyIndexUpdate update : getIndexes(after.getName())) {
            update.remove(getPath(), before);
            update.insert(getPath(), after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        for (PropertyIndexUpdate update : getIndexes(before.getName())) {
            update.remove(getPath(), before);
        }
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        childNodeChanged(name, MemoryNodeState.EMPTY_NODE, after);
    }

    @Override
    public void childNodeChanged(
            String name, NodeState before, NodeState after) {
        if (!NodeStateUtils.isHidden(name)) {
            after.compareAgainstBaseState(before, child(name));
        }
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        childNodeChanged(name, before, MemoryNodeState.EMPTY_NODE);
    }

    // -----------------------------------------------------< IndexHook >--

    @Override
    public void apply() throws CommitFailedException {
        for (List<PropertyIndexUpdate> updateList : updates.values()) {
            for (PropertyIndexUpdate update : updateList) {
                update.apply();
            }
        }
    }

    @Override
    public void reindex(NodeBuilder state) throws CommitFailedException {
        boolean reindex = false;
        for (List<PropertyIndexUpdate> updateList : updates.values()) {
            for (PropertyIndexUpdate update : updateList) {
                if (update.getAndResetReindexFlag()) {
                    reindex = true;
                }
            }
        }
        if (reindex) {
            state.getNodeState().compareAgainstBaseState(
                    MemoryNodeState.EMPTY_NODE,
                    new PropertyIndexDiff(null, state, null, "/", updates));
        }
    }

    @Override
    public IndexHook child(String name) {
        return new PropertyIndexDiff(this, name);
    }

    @Override
    public void close() throws IOException {
        updates.clear();
    }
}
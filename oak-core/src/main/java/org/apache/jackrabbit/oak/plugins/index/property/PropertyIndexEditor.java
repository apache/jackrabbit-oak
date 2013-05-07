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

import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider.TYPE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_MIXIN_SUBTYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PRIMARY_SUBTYPES;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * {@link IndexEditor} implementation that is responsible for keeping the
 * {@link PropertyIndex} up to date.
 * <br>
 * There is a tree of PropertyIndexDiff objects, each object represents the
 * changes at a given node.
 * 
 * @see PropertyIndex
 * @see PropertyIndexLookup
 */
class PropertyIndexEditor implements IndexEditor, Closeable {

    private final IndexStoreStrategy store = new ContentMirrorStoreStrategy();

    /**
     * The parent (null if this is the root node).
     */
    private final PropertyIndexEditor parent;

    /**
     * The node (can be null in the case of a deleted node).
     */
    private final NodeBuilder node;

    /**
     * The node name (the path element). Null for the root node.
     */
    private final String nodeName;

    /**
     * The path of the changed node (built lazily).
     */
    private String path;

    private final List<PropertyIndexUpdate> updates = Lists.newArrayList();

    /**
     * The map of known indexes. Key: the property name. Value: the list of
     * indexes (it is possible to have multiple indexes for the same property
     * name).
     */
    private final Map<String, List<PropertyIndexUpdate>> indexMap;

    /**
     * The {@code /jcr:system/jcr:nodeTypes} subtree.
     */
    private final NodeState types;

    public PropertyIndexEditor(NodeBuilder builder) {
        this(null, builder, null, "/");
    }

    private PropertyIndexEditor(PropertyIndexEditor parent, String nodeName) {
        this(parent, IndexUtils.getChildOrNull(parent.node, nodeName), nodeName, null);
    }

    private PropertyIndexEditor(PropertyIndexEditor parent, NodeBuilder node,
            String nodeName, String path) {
        this.parent = parent;
        this.node = node;
        this.nodeName = nodeName;
        this.path = path;

        if (parent == null) {
            this.indexMap = new HashMap<String, List<PropertyIndexUpdate>>();
            if (node.hasChildNode(JCR_SYSTEM)) {
                NodeBuilder typeNB = node.getChildNode(JCR_SYSTEM)
                        .getChildNode(JCR_NODE_TYPES);
                this.types = typeNB.getNodeState();
            } else {
                this.types = EmptyNodeState.MISSING_NODE;
            }
        } else {
            this.indexMap = parent.indexMap;
            this.types = parent.types;
        }
    }

    public String getPath() {
        // build the path lazily
        if (path == null) {
            path = concat(parent.getPath(), nodeName);
        }
        return path;
    }

    /**
     * Get all the indexes for the given property name.
     * 
     * @param propertyName
     *            the property name
     * @return the indexes
     */
    private Iterable<PropertyIndexUpdate> getIndexes(String propertyName) {
        List<PropertyIndexUpdate> indexes = indexMap.get(propertyName);
        if (indexes == null) {
            return ImmutableList.of();
        }
        List<PropertyIndexUpdate> filtered = new ArrayList<PropertyIndexUpdate>();
        for (PropertyIndexUpdate pi : indexes) {
            if (node == null || pi.matchesNodeType(node, getPath())) {
                filtered.add(pi);
            }
        }
        return filtered;
    }

    /**
     * Add the index definitions to the in-memory set of known index
     * definitions.
     * 
     * @param state
     *            the node state that contains the index definition
     * @param indexName
     *            the name of the index
     */
    private void addIndexes(NodeState state, String indexName) {
        Set<String> primaryTypes = newHashSet();
        Set<String> mixinTypes = newHashSet();
        for (String typeName : state.getNames(DECLARING_NODE_TYPES)) {
            NodeState type = types.getChildNode(typeName);
            if (type.getBoolean(JCR_ISMIXIN)) {
                mixinTypes.add(typeName);
            } else {
                primaryTypes.add(typeName);
            }
            addAll(primaryTypes, type.getNames(OAK_PRIMARY_SUBTYPES));
            addAll(mixinTypes, type.getNames(OAK_MIXIN_SUBTYPES));
        }

        PropertyState ps = state.getProperty(PROPERTY_NAMES);
        Iterable<String> propertyNames = ps != null ? ps.getValue(Type.NAMES)
                : ImmutableList.of(indexName);
        for (String pname : propertyNames) {
            List<PropertyIndexUpdate> list = this.indexMap.get(pname);
            if (list == null) {
                list = newArrayList();
                this.indexMap.put(pname, list);
            }
            boolean exists = false;
            String localPath = getPath();
            for (PropertyIndexUpdate piu : list) {
                if (piu.matches(localPath, primaryTypes, mixinTypes)) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                PropertyIndexUpdate update = new PropertyIndexUpdate(
                        getPath(), node.child(INDEX_DEFINITIONS_NAME).child(indexName),
                        store, primaryTypes, mixinTypes);
                list.add(update);
                updates.add(update);
            }
        }
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        if (after != null && after.hasChildNode(INDEX_DEFINITIONS_NAME)) {
            NodeState index = after.getChildNode(INDEX_DEFINITIONS_NAME);
            for (String indexName : index.getChildNodeNames()) {
                NodeState child = index.getChildNode(indexName);
                if (IndexUtils.isIndexNodeType(child, TYPE)) {
                    addIndexes(child, indexName);
                }
            }
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        for (PropertyIndexUpdate update : updates) {
            update.checkUniqueKeys();
        }
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        for (PropertyIndexUpdate update : getIndexes(after.getName())) {
            update.insert(getPath(), after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        for (PropertyIndexUpdate update : getIndexes(after.getName())) {
            update.remove(getPath(), before);
            update.insert(getPath(), after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        for (PropertyIndexUpdate update : getIndexes(before.getName())) {
            update.remove(getPath(), before);
        }
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        return childNodeChanged(name, EMPTY_NODE, after);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before,
            NodeState after) throws CommitFailedException {
        return new PropertyIndexEditor(this, name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        return childNodeChanged(name, before, EMPTY_NODE);
    }

    // -----------------------------------------------------< Closeable >--

    @Override
    public void close() throws IOException {
        indexMap.clear();
    }
}

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
package org.apache.jackrabbit.oak.plugins.index.p2;

import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.p2.Property2IndexHookProvider.TYPE;
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
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.plugins.index.p2.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.p2.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * {@link IndexHook} implementation that is responsible for keeping the
 * {@link Property2Index} up to date.
 * <br>
 * There is a tree of PropertyIndexDiff objects, each object represents the
 * changes at a given node.
 * 
 * @see Property2Index
 * @see Property2IndexLookup
 */
class Property2IndexHook implements IndexHook, Closeable {

    protected static String propertyNames = "propertyNames";

    protected static String declaringNodeTypes = "declaringNodeTypes";

    private final IndexStoreStrategy store = new ContentMirrorStoreStrategy();

    /**
     * The parent (null if this is the root node).
     */
    private final Property2IndexHook parent;

    /**
     * The node (never null).
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

    private final List<Property2IndexHookUpdate> updates = Lists.newArrayList();

    /**
     * The map of known indexes. Key: the property name. Value: the list of
     * indexes (it is possible to have multiple indexes for the same property
     * name).
     */
    private final Map<String, List<Property2IndexHookUpdate>> indexMap;

    /**
     * The {@code /jcr:system/jcr:nodeTypes} subtree.
     */
    private final NodeState types;

    public Property2IndexHook(NodeBuilder builder, NodeState root) {
        this(null, builder, null, "/",
                new HashMap<String, List<Property2IndexHookUpdate>>(),
                root.getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES));
    }

    private Property2IndexHook(Property2IndexHook parent, String nodeName) {
        this(parent, getChildNode(parent.node, nodeName), nodeName, null,
                parent.indexMap, parent.types);
    }

    private Property2IndexHook(Property2IndexHook parent, NodeBuilder node,
            String nodeName, String path,
            Map<String, List<Property2IndexHookUpdate>> indexMap,
            NodeState types) {
        this.parent = parent;
        this.node = node;
        this.nodeName = nodeName;
        this.path = path;
        this.indexMap = indexMap;
        this.types = types;
    }

    private static NodeBuilder getChildNode(NodeBuilder node, String name) {
        if (node != null && node.hasChildNode(name)) {
            return node.child(name);
        } else {
            return null;
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
    private Iterable<Property2IndexHookUpdate> getIndexes(String propertyName) {
        List<Property2IndexHookUpdate> indexes = indexMap.get(propertyName);
        if (indexes == null) {
            return ImmutableList.of();
        }
        List<Property2IndexHookUpdate> filtered = new ArrayList<Property2IndexHookUpdate>();
        for (Property2IndexHookUpdate pi : indexes) {
            if (node == null || pi.matchesNodeType(node)) {
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
        for (String typeName : state.getNames(declaringNodeTypes)) {
            NodeState type = types.getChildNode(typeName);
            if (type.getBoolean(JCR_ISMIXIN)) {
                mixinTypes.add(typeName);
            } else {
                primaryTypes.add(typeName);
            }
            addAll(primaryTypes, type.getNames(OAK_PRIMARY_SUBTYPES));
            addAll(mixinTypes, type.getNames(OAK_MIXIN_SUBTYPES));
        }

        PropertyState ps = state.getProperty(propertyNames);
        Iterable<String> propertyNames = ps != null ? ps.getValue(Type.NAMES)
                : ImmutableList.of(indexName);
        for (String pname : propertyNames) {
            List<Property2IndexHookUpdate> list = this.indexMap.get(pname);
            if (list == null) {
                list = newArrayList();
                this.indexMap.put(pname, list);
            }
            boolean exists = false;
            String localPath = getPath();
            for (Property2IndexHookUpdate piu : list) {
                if (piu.matches(localPath, primaryTypes, mixinTypes)) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                Property2IndexHookUpdate update = new Property2IndexHookUpdate(
                        getPath(), node.child(INDEX_DEFINITIONS_NAME).child(indexName),
                        store, primaryTypes, mixinTypes);
                list.add(update);
                updates.add(update);
            }
        }
    }

    private static boolean isIndexNode(NodeState node) {
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

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        if (after != null && after.hasChildNode(INDEX_DEFINITIONS_NAME)) {
            NodeState index = after.getChildNode(INDEX_DEFINITIONS_NAME);
            for (String indexName : index.getChildNodeNames()) {
                NodeState child = index.getChildNode(indexName);
                if (isIndexNode(child)) {
                    addIndexes(child, indexName);
                }
            }
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        for (Property2IndexHookUpdate update : updates) {
            update.checkUniqueKeys();
        }
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        for (Property2IndexHookUpdate update : getIndexes(after.getName())) {
            update.insert(getPath(), after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        for (Property2IndexHookUpdate update : getIndexes(after.getName())) {
            update.remove(getPath(), before);
            update.insert(getPath(), after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        for (Property2IndexHookUpdate update : getIndexes(before.getName())) {
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
        return new Property2IndexHook(this, name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        return childNodeChanged(name, before, EMPTY_NODE);
    }

    @Override
    public Editor reindex(NodeState state) {
        boolean reindex = false;
        for (List<Property2IndexHookUpdate> updateList : indexMap.values()) {
            for (Property2IndexHookUpdate update : updateList) {
                if (update.getAndResetReindexFlag()) {
                    reindex = true;
                }
            }
        }
        if (reindex) {
            return new Property2IndexHook(node, types);
        }
        return null;
    }

    // -----------------------------------------------------< Closeable >--

    @Override
    public void close() throws IOException {
        indexMap.clear();
    }
}

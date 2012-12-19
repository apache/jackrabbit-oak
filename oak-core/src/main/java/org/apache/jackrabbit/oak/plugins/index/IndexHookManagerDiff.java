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
package org.apache.jackrabbit.oak.plugins.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_UNKNOWN;

/**
 * Acts as a composite NodeStateDiff, it delegates all the diff's events to the
 * existing IndexHooks.
 * 
 * This allows for a simultaneous update of all the indexes via a single
 * traversal of the changes.
 */
class IndexHookManagerDiff implements NodeStateDiff {

    private static final Logger LOG = LoggerFactory
            .getLogger(IndexHookManagerDiff.class);

    private final IndexHookProvider provider;

    private final IndexHookManagerDiff parent;

    private final NodeBuilder node;

    private final String name;

    private String path;

    /**
     * <type, <path, indexhook>>
     */
    private final Map<String, Map<String, List<IndexHook>>> updates;

    public IndexHookManagerDiff(IndexHookProvider provider, NodeBuilder root,
            Map<String, Map<String, List<IndexHook>>> updates)
            throws CommitFailedException {
        this(provider, null, root, null, "/", updates);
    }

    private IndexHookManagerDiff(IndexHookProvider provider,
            IndexHookManagerDiff parent, String name)
            throws CommitFailedException {
        this(provider, parent, getChildNode(parent.node, name), name, null,
                parent.updates);
    }

    private IndexHookManagerDiff(IndexHookProvider provider,
            IndexHookManagerDiff parent, NodeBuilder node, String name,
            String path, Map<String, Map<String, List<IndexHook>>> updates)
            throws CommitFailedException {
        this.provider = provider;
        this.parent = parent;
        this.node = node;
        this.name = name;
        this.path = path;
        this.updates = updates;

        if (node != null && isIndexNodeType(node.getProperty(JCR_PRIMARYTYPE))) {
            // to prevent double-reindex we only call reindex if:
            // - the flag exists and is set to true
            // OR
            // - the flag does not exist
            boolean reindex = node.getProperty(REINDEX_PROPERTY_NAME) == null
                    || node.getProperty(REINDEX_PROPERTY_NAME).getValue(
                            Type.BOOLEAN);
            if (reindex) {
                node.setProperty(REINDEX_PROPERTY_NAME, true);
                String type = TYPE_UNKNOWN;
                PropertyState typePS = node.getProperty(TYPE_PROPERTY_NAME);
                if (typePS != null && !typePS.isArray()) {
                    type = typePS.getValue(Type.STRING);
                }
                // TODO this is kinda fragile
                NodeBuilder rebuildBuilder = parent.parent.node;
                String relativePath = PathUtils.getAncestorPath(getPath(), 2);
                // find parent index by type and trigger a full reindex
                List<IndexHook> indexes = getIndexesWithRelativePaths(
                        relativePath, getIndexes(type));
                for (IndexHook ih : indexes) {
                    ih.reindex(rebuildBuilder);
                }
            }
        }

        if (node != null && node.hasChildNode(INDEX_DEFINITIONS_NAME)) {
            Set<String> existingTypes = new HashSet<String>();
            Set<String> reindexTypes = new HashSet<String>();

            NodeBuilder index = node.child(INDEX_DEFINITIONS_NAME);
            for (String indexName : index.getChildNodeNames()) {
                NodeBuilder indexChild = index.child(indexName);
                if (isIndexNodeType(indexChild.getProperty(JCR_PRIMARYTYPE))) {
                    // this reindex should only happen when the flag is set
                    // before the index impl is online
                    boolean reindex = indexChild
                            .getProperty(REINDEX_PROPERTY_NAME) != null
                            && indexChild.getProperty(REINDEX_PROPERTY_NAME)
                                    .getValue(Type.BOOLEAN);
                    String type = TYPE_UNKNOWN;
                    PropertyState typePS = indexChild
                            .getProperty(TYPE_PROPERTY_NAME);
                    if (typePS != null && !typePS.isArray()) {
                        type = typePS.getValue(Type.STRING);
                    }
                    if (reindex) {
                        reindexTypes.add(type);
                    }
                    existingTypes.add(type);
                }
            }
            existingTypes.remove(TYPE_UNKNOWN);
            reindexTypes.remove(TYPE_UNKNOWN);
            for (String type : existingTypes) {
                Map<String, List<IndexHook>> byType = this.updates.get(type);
                if (byType == null) {
                    byType = new TreeMap<String, List<IndexHook>>();
                    this.updates.put(type, byType);
                }
                List<IndexHook> hooks = byType.get(getPath());
                if (hooks == null) {
                    hooks = Lists.newArrayList();
                    byType.put(getPath(), hooks);
                }
                if (reindexTypes.contains(type)) {
                    for (IndexHook ih : provider.getIndexHooks(type, node)) {
                        ih.reindex(node);
                        // TODO proper cleanup of resources in the case of an
                        // exception?
                        hooks.add(ih);
                    }
                } else {
                    hooks.addAll(provider.getIndexHooks(type, node));
                }
            }
        }
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

    /**
     * Returns IndexHooks of all types that have the best match (are situated
     * the closest on the hierarchy) for the given path.
     */
    private Map<String, List<IndexHook>> getIndexes() {
        Map<String, List<IndexHook>> hooks = new HashMap<String, List<IndexHook>>();
        for (String type : this.updates.keySet()) {
            Map<String, List<IndexHook>> newIndexes = getIndexes(type);
            for (String key : newIndexes.keySet()) {
                if (hooks.containsKey(key)) {
                    hooks.get(key).addAll(newIndexes.get(key));
                } else {
                    hooks.put(key, newIndexes.get(key));
                }
            }
        }
        return hooks;
    }

    /**
     * Returns IndexHooks of the given type that have the best match (are
     * situated the closest on the hierarchy) for the current path.
     */
    private Map<String, List<IndexHook>> getIndexes(String type) {
        Map<String, List<IndexHook>> hooks = new HashMap<String, List<IndexHook>>();
        Map<String, List<IndexHook>> indexes = this.updates.get(type);
        if (indexes != null && !indexes.isEmpty()) {
            Iterator<String> iterator = indexes.keySet().iterator();
            String bestMatch = iterator.next();
            while (iterator.hasNext()) {
                String key = iterator.next();
                if (PathUtils.isAncestor(key, getPath())) {
                    bestMatch = key;
                } else {
                    break;
                }
            }
            List<IndexHook> existing = hooks.get(bestMatch);
            if (existing == null) {
                existing = new ArrayList<IndexHook>();
                hooks.put(bestMatch, existing);
            }
            existing.addAll(indexes.get(bestMatch));
        }
        return hooks;
    }

    /**
     * Fixes the relative paths on the best matching indexes so updates apply
     * properly
     */
    private List<IndexHook> getIndexesWithRelativePaths(String path,
            Map<String, List<IndexHook>> bestMatches) {
        List<IndexHook> hooks = new ArrayList<IndexHook>();
        for (String relativePath : bestMatches.keySet()) {
            for (IndexHook update : bestMatches.get(relativePath)) {
                IndexHook u = update;
                String downPath = path.substring(relativePath.length());
                for (String p : PathUtils.elements(downPath)) {
                    u = u.child(p);
                }
                hooks.add(u);
            }
        }
        return hooks;
    }

    private static boolean isIndexNodeType(PropertyState ps) {
        return ps != null && !ps.isArray()
                && ps.getValue(Type.STRING).equals(INDEX_DEFINITIONS_NODE_TYPE);
    }

    // -----------------------------------------------------< NodeStateDiff >---

    @Override
    public void propertyAdded(PropertyState after) {
        for (IndexHook update : getIndexesWithRelativePaths(getPath(),
                getIndexes())) {
            update.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        for (IndexHook update : getIndexesWithRelativePaths(getPath(),
                getIndexes())) {
            update.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        for (IndexHook update : getIndexesWithRelativePaths(getPath(),
                getIndexes())) {
            update.propertyDeleted(before);
        }
    }

    @Override
    public void childNodeAdded(String name, NodeState after) {
        childNodeChanged(name, MemoryNodeState.EMPTY_NODE, after);
    }

    @Override
    public void childNodeChanged(String name, NodeState before, NodeState after) {
        if (NodeStateUtils.isHidden(name)) {
            return;
        }
        getIndexesWithRelativePaths(concat(getPath(), name), getIndexes());

        try {
            after.compareAgainstBaseState(before, new IndexHookManagerDiff(
                    provider, this, name));
        } catch (CommitFailedException e) {
            // TODO ignore exception - is this a hack?
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) {
        childNodeChanged(name, before, MemoryNodeState.EMPTY_NODE);
    }
}
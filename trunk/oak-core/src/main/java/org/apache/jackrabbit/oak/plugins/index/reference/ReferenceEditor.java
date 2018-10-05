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
package org.apache.jackrabbit.oak.plugins.index.reference;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptySet;
import static javax.jcr.PropertyType.REFERENCE;
import static javax.jcr.PropertyType.WEAKREFERENCE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.api.CommitFailedException.INTEGRITY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;
import static org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants.REF_NAME;
import static org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants.WEAK_REF_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.VERSION_STORE_PATH;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Supplier;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.property.Multiplexers;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Index editor for keeping a references to a node up to date.
 * 
 */
class ReferenceEditor extends DefaultEditor implements IndexEditor {

    /** Parent editor, or {@code null} if this is the root editor. */
    private final ReferenceEditor parent;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    private final NodeState root;

    private final NodeBuilder definition;

    /**
     * <UUID, Set<paths-pointing-to-the-uuid>>
     */
    private final Map<String, Set<String>> newRefs;

    /**
     * <UUID, Set<paths-pointing-to-the-uuid>>
     */
    private final Map<String, Set<String>> rmRefs;

    /**
     * <UUID, Set<paths-pointing-to-the-uuid>>
     */
    private final Map<String, Set<String>> newWeakRefs;

    /**
     * <UUID, Set<paths-pointing-to-the-uuid>>
     */
    private final Map<String, Set<String>> rmWeakRefs;

    /**
     * set of removed Ids of nodes that have a :reference property. These UUIDs
     * need to be verified in the #after call
     */
    private final Set<String> rmIds;

    /**
     * set of ids that were added during this commit. we need it to reconcile
     * moves
     */
    private final Set<String> newIds;

    private final MountInfoProvider mountInfoProvider;

    /**
     * flag marking a reindex, case in which we don't need to keep track of the
     * newIds set
     */
    private boolean isReindex;

    public ReferenceEditor(NodeBuilder definition, NodeState root,MountInfoProvider mountInfoProvider) {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.definition = definition;
        this.root = root;
        this.newRefs = newHashMap();
        this.rmRefs = newHashMap();
        this.newWeakRefs = newHashMap();
        this.rmWeakRefs = newHashMap();
        this.rmIds = newHashSet();
        this.newIds = newHashSet();
        this.mountInfoProvider = mountInfoProvider;
    }

    private ReferenceEditor(ReferenceEditor parent, String name) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.definition = parent.definition;
        this.root = parent.root;
        this.newRefs = parent.newRefs;
        this.rmRefs = parent.rmRefs;
        this.newWeakRefs = parent.newWeakRefs;
        this.rmWeakRefs = parent.rmWeakRefs;
        this.rmIds = parent.rmIds;
        this.newIds = parent.newIds;
        this.isReindex = parent.isReindex;
        this.mountInfoProvider = parent.mountInfoProvider;
    }

    /**
     * Returns the path of this node, building it lazily when first requested.
     */
    private String getPath() {
        if (path == null) {
            path = concat(parent.getPath(), name);
        }
        return path;
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        if (MISSING_NODE == before && parent == null) {
            isReindex = true;
        }
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (parent == null) {
            Set<IndexStoreStrategy> refStores = getStrategies(false, REF_NAME);
            Set<IndexStoreStrategy> weakRefStores = getStrategies(false, WEAK_REF_NAME);
            // update references
            for (Entry<String, Set<String>> ref : rmRefs.entrySet()) {
                String uuid = ref.getKey();
                Set<String> rm = ref.getValue();
                Set<String> add = emptySet();
                if (newRefs.containsKey(uuid)) {
                    add = newRefs.remove(uuid);
                }
                update(refStores, definition, REF_NAME, uuid, add, rm);
            }
            for (Entry<String, Set<String>> ref : newRefs.entrySet()) {
                String uuid = ref.getKey();
                if (rmIds.contains(uuid)) {
                    continue;
                }
                Set<String> add = ref.getValue();
                Set<String> rm = emptySet();
                update(refStores, definition, REF_NAME, uuid, add, rm);
            }

            checkReferentialIntegrity(refStores, root, definition.getNodeState(),
                    Sets.difference(rmIds, newIds));

            // update weak references
            for (Entry<String, Set<String>> ref : rmWeakRefs.entrySet()) {
                String uuid = ref.getKey();
                Set<String> rm = ref.getValue();
                Set<String> add = emptySet();
                if (newWeakRefs.containsKey(uuid)) {
                    add = newWeakRefs.remove(uuid);
                }
                update(weakRefStores, definition, WEAK_REF_NAME, uuid, add, rm);
            }
            for (Entry<String, Set<String>> ref : newWeakRefs.entrySet()) {
                String uuid = ref.getKey();
                Set<String> add = ref.getValue();
                Set<String> rm = emptySet();
                update(weakRefStores, definition, WEAK_REF_NAME, uuid, add, rm);
            }
        }
    }

    Set<IndexStoreStrategy> getStrategies(boolean unique, String index) {
        return Multiplexers.getStrategies(unique, mountInfoProvider,
                definition, index);
    }

    @Override
    public void propertyAdded(PropertyState after) {
        propertyChanged(null, after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {

        if (before != null) {
            if (before.getType().tag() == REFERENCE) {
                if (!isVersionStorePath(getPath())) {
                    put(rmRefs, before.getValue(STRINGS),
                            concat(getPath(), before.getName()));
                }
            }
            if (before.getType().tag() == WEAKREFERENCE) {
                put(rmWeakRefs, before.getValue(STRINGS),
                        concat(getPath(), before.getName()));
            }
            if (JCR_UUID.equals(before.getName())) {
                // node remove + add -> changed uuid
                rmIds.add(before.getValue(STRING));
            }
        }
        if (after != null) {
            if (after.getType().tag() == REFERENCE) {
                if (!isVersionStorePath(getPath())) {
                    put(newRefs, after.getValue(STRINGS),
                            concat(getPath(), after.getName()));
                }
            }
            if (after.getType().tag() == WEAKREFERENCE) {
                put(newWeakRefs, after.getValue(STRINGS),
                        concat(getPath(), after.getName()));
            }
            if (JCR_UUID.equals(after.getName())) {
                // node remove + add -> changed uuid
                newIds.add(after.getValue(STRING));
            }
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        propertyChanged(before, null);
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        String uuid = after.getString(JCR_UUID);
        if (!isReindex && uuid != null) {
            newIds.add(uuid);
        }
        return new ReferenceEditor(this, name);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before,
            NodeState after) {
        return new ReferenceEditor(this, name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        String uuid = before.getString(JCR_UUID);
        if (uuid != null) {
            rmIds.add(uuid);
        }
        return new ReferenceEditor(this, name);
    }

    // ---------- Utils -----------------------------------------

    private static boolean isVersionStorePath(String oakPath) {
        return oakPath != null
                && oakPath.startsWith(VERSION_STORE_PATH);
    }

    private static void put(Map<String, Set<String>> map,
            Iterable<String> keys, String value) {
        String asRelative = isAbsolute(value) ? value.substring(1) : value;
        for (String key : keys) {
            Set<String> values = map.get(key);
            if (values == null) {
                values = newHashSet();
            }
            values.add(asRelative);
            map.put(key, values);
        }
    }

    private void update(Set<IndexStoreStrategy> refStores,
            NodeBuilder definition, String name, String key, Set<String> add,
            Set<String> rm) throws CommitFailedException {
        for (IndexStoreStrategy store : refStores) {
            Set<String> empty = of();
            for (String p : rm) {
                Supplier<NodeBuilder> index = memoize(() -> definition.child(store.getIndexNodeName()));
                store.update(index, p, name, definition, of(key), empty);
            }
            for (String p : add) {
                // TODO do we still need to encode the values?
                Supplier<NodeBuilder> index = memoize(() -> definition.child(store.getIndexNodeName()));
                store.update(index, p, name, definition, empty, of(key));
            }
        }
    }

    private static boolean hasReferences(IndexStoreStrategy refStore,
                                  NodeState root,
                                  NodeState definition,
                                  String name,
                                  String key) {
        return definition.hasChildNode(name)
                && refStore.count(root, definition, of(key), 1) > 0;
    }

    private static void checkReferentialIntegrity(Set<IndexStoreStrategy> refStores,
                                           NodeState root,
                                           NodeState definition,
                                           Set<String> idsOfRemovedNodes)
            throws CommitFailedException {
        for (IndexStoreStrategy store : refStores) {
            for (String id : idsOfRemovedNodes) {
                if (hasReferences(store, root, definition, REF_NAME, id)) {
                    throw new CommitFailedException(INTEGRITY, 1,
                            "Unable to delete referenced node");
                }
            }
        }
    }
}

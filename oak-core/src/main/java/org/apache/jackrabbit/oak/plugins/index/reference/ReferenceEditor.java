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

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static javax.jcr.PropertyType.REFERENCE;
import static javax.jcr.PropertyType.WEAKREFERENCE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.api.CommitFailedException.INTEGRITY;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;
import static org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants.REF_NAME;
import static org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants.WEAK_REF_NAME;
import static org.apache.jackrabbit.oak.plugins.version.VersionConstants.SYSTEM_PATHS;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Index editor for keeping a references to a node up to date.
 * 
 */
class ReferenceEditor extends DefaultEditor implements IndexEditor {

    private static final ContentMirrorStoreStrategy STORE = new ContentMirrorStoreStrategy();

    /** Parent editor, or {@code null} if this is the root editor. */
    private final ReferenceEditor parent;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    private final NodeState root;

    private final NodeBuilder definition;

    /**
     * the uuid of the current node, null if the node doesn't have this
     * property.
     */
    private final String uuid;

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
     * set of ids that changed. This can happen when a node with the same name
     * is deleted and added again
     * 
     */
    private final Set<String> discardedIds;

    private final Set<String> versionStoreIds;

    /**
     * set of ids that were added during this commit. we need it to reconcile
     * moves
     * 
     */
    private final Set<String> newIds;

    public ReferenceEditor(NodeBuilder definition, NodeState root) {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.definition = definition;
        this.root = root;
        this.uuid = null;
        this.newRefs = newHashMap();
        this.rmRefs = newHashMap();
        this.newWeakRefs = newHashMap();
        this.rmWeakRefs = newHashMap();
        this.rmIds = newHashSet();
        this.discardedIds = newHashSet();
        this.versionStoreIds = newHashSet();
        this.newIds = newHashSet();
    }

    private ReferenceEditor(ReferenceEditor parent, String name, String uuid) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.definition = parent.definition;
        this.root = parent.root;
        this.uuid = uuid;
        this.newRefs = parent.newRefs;
        this.rmRefs = parent.rmRefs;
        this.newWeakRefs = parent.newWeakRefs;
        this.rmWeakRefs = parent.rmWeakRefs;
        this.rmIds = parent.rmIds;
        this.discardedIds = parent.discardedIds;
        this.versionStoreIds = parent.versionStoreIds;
        this.newIds = parent.newIds;
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
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        if (parent == null) {
            Set<String> offending = newHashSet(rmIds);
            offending.removeAll(rmRefs.keySet());
            offending.removeAll(newIds);
            if (!offending.isEmpty()) {
                throw new CommitFailedException(INTEGRITY, 1,
                        "Unable to delete referenced node");
            }
            rmIds.addAll(discardedIds);

            // remove ids that are actually deleted (that exist in the rmRefs.keySet())
            versionStoreIds.removeAll(rmRefs.keySet());
            rmIds.addAll(versionStoreIds);

            // update references
            for (Entry<String, Set<String>> ref : rmRefs.entrySet()) {
                String uuid = ref.getKey();
                if (rmIds.contains(uuid)) {
                    continue;
                }
                Set<String> rm = ref.getValue();
                Set<String> add = newHashSet();
                if (newRefs.containsKey(uuid)) {
                    add = newRefs.remove(uuid);
                }
                update(definition, REF_NAME, uuid, add, rm);
            }
            for (Entry<String, Set<String>> ref : newRefs.entrySet()) {
                String uuid = ref.getKey();
                if (rmIds.contains(uuid)) {
                    continue;
                }
                Set<String> add = ref.getValue();
                Set<String> rm = newHashSet();
                update(definition, REF_NAME, uuid, add, rm);
            }

            // update weak references
            for (Entry<String, Set<String>> ref : rmWeakRefs.entrySet()) {
                String uuid = ref.getKey();
                if (rmIds.contains(uuid)) {
                    continue;
                }
                Set<String> rm = ref.getValue();
                Set<String> add = newHashSet();
                if (newWeakRefs.containsKey(uuid)) {
                    add = newWeakRefs.remove(uuid);
                }
                update(definition, WEAK_REF_NAME, uuid, add, rm);
            }
            for (Entry<String, Set<String>> ref : newWeakRefs.entrySet()) {
                String uuid = ref.getKey();
                if (rmIds.contains(uuid)) {
                    continue;
                }
                Set<String> add = ref.getValue();
                Set<String> rm = newHashSet();
                update(definition, WEAK_REF_NAME, uuid, add, rm);
            }
        }
    }

    @Override
    public void propertyAdded(PropertyState after) {
        propertyChanged(null, after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {

        if (before != null) {
            if (before.getType().tag() == REFERENCE) {
                if (isVersionStorePath(getPath())) {
                    addAll(versionStoreIds, before.getValue(STRINGS));
                } else {
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
                String beforeUuid = before.getValue(STRING);
                if (beforeUuid != null && !beforeUuid.equals(uuid)) {
                    discardedIds.add(beforeUuid);
                }
            }
        }
        if (after != null) {
            if (after.getType().tag() == REFERENCE) {
                if (isVersionStorePath(getPath())) {
                    addAll(versionStoreIds, after.getValue(STRINGS));
                } else {
                    put(newRefs, after.getValue(STRINGS),
                            concat(getPath(), after.getName()));
                }
            }
            if (after.getType().tag() == WEAKREFERENCE) {
                put(newWeakRefs, after.getValue(STRINGS),
                        concat(getPath(), after.getName()));
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
        if (uuid != null) {
            newIds.add(uuid);
        }
        return new ReferenceEditor(this, name, uuid);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before,
            NodeState after) {
        return new ReferenceEditor(this, name, after.getString(JCR_UUID));
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        String uuid = before.getString(JCR_UUID);
        if (uuid != null && check(root, definition.getNodeState(), REF_NAME, uuid)) {
            rmIds.add(uuid);
        }
        return new ReferenceEditor(this, name, uuid);
    }

    // ---------- Utils -----------------------------------------

    private static boolean isVersionStorePath(String oakPath) {
        if (oakPath == null) {
            return false;
        }
        if (oakPath.indexOf(JCR_SYSTEM) == 1) {
            for (String p : SYSTEM_PATHS) {
                if (oakPath.startsWith(p)) {
                    return true;
                }
            }
        }
        return false;
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

    private static void update(NodeBuilder child, String name, String key,
            Set<String> add, Set<String> rm) {
        NodeBuilder index = child.child(name);
        Set<String> empty = of();
        for (String p : rm) {
            STORE.update(index, p, name, child, of(key), empty);
        }
        for (String p : add) {
            // TODO do we still need to encode the values?
            STORE.update(index, p, name, child, empty, of(key));
        }
    }

    private static boolean check(NodeState root, NodeState definition, String name, String key) {
        return definition.hasChildNode(name)
                && STORE.count(root, definition, name, of(key), 1) > 0;
    }

}

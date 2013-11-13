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

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.api.CommitFailedException.INTEGRITY;
import static org.apache.jackrabbit.oak.api.Type.REFERENCE;
import static org.apache.jackrabbit.oak.api.Type.REFERENCES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.api.Type.WEAKREFERENCE;
import static org.apache.jackrabbit.oak.api.Type.WEAKREFERENCES;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants.REF_NAME;
import static org.apache.jackrabbit.oak.plugins.index.reference.NodeReferenceConstants.WEAK_REF_NAME;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Index editor for keeping a references to a node up to date.
 * 
 */
class ReferenceEditor extends DefaultEditor {

    // TODO
    // - look into using a storage strategy (trees)
    // - what happens when you move a node? who updates the backlinks?

    /** Parent editor, or {@code null} if this is the root editor. */
    private final ReferenceEditor parent;

    /** Name of this node, or {@code null} for the root node. */
    private final String name;

    /** Path of this editor, built lazily in {@link #getPath()}. */
    private String path;

    /** The Id Manager, built lazily in {@link #getIdManager()}. */
    private IdentifierManager idManager;

    private final NodeBuilder builder;

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

    public ReferenceEditor(NodeBuilder builder) {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.builder = builder;
        this.uuid = null;
        this.newRefs = newHashMap();
        this.rmRefs = newHashMap();
        this.newWeakRefs = newHashMap();
        this.rmWeakRefs = newHashMap();
        this.rmIds = newHashSet();
        this.discardedIds = newHashSet();
    }

    private ReferenceEditor(ReferenceEditor parent, String name, String uuid) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.builder = parent.builder;
        this.uuid = uuid;
        this.newRefs = parent.newRefs;
        this.rmRefs = parent.rmRefs;
        this.newWeakRefs = parent.newWeakRefs;
        this.rmWeakRefs = parent.rmWeakRefs;
        this.rmIds = parent.rmIds;
        this.discardedIds = parent.discardedIds;
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

    /**
     * Returns the id manager, building it lazily when first requested.
     */
    private IdentifierManager getIdManager() {
        if (idManager == null) {
            if (parent != null) {
                return parent.getIdManager();
            }
            this.idManager = new IdentifierManager(new ImmutableRoot(
                    this.builder.getNodeState()));
        }
        return idManager;
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
            if (!offending.isEmpty()) {
                throw new CommitFailedException(INTEGRITY, 1,
                        "Unable to delete referenced node");
            }
            rmIds.addAll(discardedIds);

            // local uuid-> nodebuilder cache
            Map<String, NodeBuilder> builders = newHashMap();
            for (Entry<String, Set<String>> ref : rmRefs.entrySet()) {
                String uuid = ref.getKey();
                if (rmIds.contains(uuid)) {
                    continue;
                }
                NodeBuilder child = resolveUUID(uuid, getIdManager(), builder,
                        builders);
                if (child == null) {
                    throw new CommitFailedException(INTEGRITY, 2,
                            "Unable to resolve UUID " + uuid);
                }
                Set<String> rm = ref.getValue();
                Set<String> add = newHashSet();
                if (newRefs.containsKey(uuid)) {
                    add = newRefs.remove(uuid);
                }
                set(child, REF_NAME, add, rm);
            }
            for (Entry<String, Set<String>> ref : newRefs.entrySet()) {
                String uuid = ref.getKey();
                if (rmIds.contains(uuid)) {
                    continue;
                }
                NodeBuilder child = resolveUUID(uuid, getIdManager(), builder,
                        builders);
                if (child == null) {
                    throw new CommitFailedException(INTEGRITY, 3,
                            "Unable to resolve UUID " + uuid);
                }
                Set<String> add = ref.getValue();
                Set<String> rm = newHashSet();
                set(child, REF_NAME, add, rm);
            }
            for (Entry<String, Set<String>> ref : rmWeakRefs.entrySet()) {
                String uuid = ref.getKey();
                if (rmIds.contains(uuid)) {
                    continue;
                }
                NodeBuilder child = resolveUUID(uuid, getIdManager(), builder,
                        builders);
                if (child == null) {
                    // TODO log warning?
                    continue;
                }
                Set<String> rm = ref.getValue();
                Set<String> add = newHashSet();
                if (newWeakRefs.containsKey(uuid)) {
                    add = newWeakRefs.remove(uuid);
                }
                set(child, WEAK_REF_NAME, add, rm);
            }
            for (Entry<String, Set<String>> ref : newWeakRefs.entrySet()) {
                String uuid = ref.getKey();
                if (rmIds.contains(uuid)) {
                    continue;
                }
                NodeBuilder child = resolveUUID(uuid, getIdManager(), builder,
                        builders);
                if (child == null) {
                    // TODO log warning?
                    continue;
                }
                Set<String> add = ref.getValue();
                Set<String> rm = newHashSet();
                set(child, WEAK_REF_NAME, add, rm);
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
            if (before.getType() == REFERENCE || before.getType() == REFERENCES) {
                put(rmRefs, before.getValue(STRINGS), concat(getPath(), before.getName()));
            }
            if (before.getType() == WEAKREFERENCE
                    || before.getType() == WEAKREFERENCES) {
                put(rmWeakRefs, before.getValue(STRINGS), concat(getPath(), before.getName()));
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
            if (after.getType() == REFERENCE || after.getType() == REFERENCES) {
                put(newRefs, after.getValue(STRINGS), concat(getPath(), after.getName()));
            }
            if (after.getType() == WEAKREFERENCE
                    || after.getType() == WEAKREFERENCES) {
                put(newWeakRefs, after.getValue(STRINGS), concat(getPath(), after.getName()));
            }
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) {
        propertyChanged(before, null);
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) {
        String path = concat(getPath(), name);
        if (isVersionStorePath(path)) {
            return null;
        }
        return new ReferenceEditor(this, name, after.getString(JCR_UUID));
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before,
            NodeState after) {
        String path = concat(getPath(), name);
        if (isVersionStorePath(path)) {
            return null;
        }
        return new ReferenceEditor(this, name, after.getString(JCR_UUID));
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        String path = concat(getPath(), name);
        if (isVersionStorePath(path)) {
            return null;
        }
        String uuid = before.getString(JCR_UUID);
        if (before.hasProperty(REF_NAME)) {
            if (uuid != null) {
                rmIds.add(uuid);
            }
        }
        return new ReferenceEditor(this, name, uuid);
    }

    // ---------- Utils -----------------------------------------

    private static NodeBuilder resolveUUID(String uuid,
            IdentifierManager idManager, NodeBuilder root,
            Map<String, NodeBuilder> builders) {
        if (builders.containsKey(uuid)) {
            return builders.get(uuid);
        }
        String path = idManager.resolveUUID(uuid);

        if (path == null) {
            return null;
        }
        NodeBuilder child = getChild(root, path);
        if (child != null) {
            builders.put(uuid, child);
        }
        return child;
    }

    private static NodeBuilder getChild(NodeBuilder root, String path) {
        NodeBuilder child = root;
        for (String p : elements(path)) {
            child = child.child(p);
        }
        return child;
    }

    private static boolean isVersionStorePath(@Nonnull String oakPath) {
        if (oakPath.indexOf(JcrConstants.JCR_SYSTEM) == 1) {
            for (String p : VersionConstants.SYSTEM_PATHS) {
                if (oakPath.startsWith(p)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void put(Map<String, Set<String>> map,
            Iterable<String> keys, String value) {
        for (String key : keys) {
            Set<String> values = map.get(key);
            if (values == null) {
                values = newHashSet();
            }
            values.add(value);
            map.put(key, values);
        }
    }

    private static void set(NodeBuilder child, String name, Set<String> add,
            Set<String> rm) {
        // TODO should we optimize for the remove/add case? intersect the
        // sets, work on the diffs?

        Set<String> vals;
        PropertyState ref = child.getProperty(name);
        if (ref != null) {
            vals = newHashSet(ref.getValue(STRINGS));
        } else {
            vals = newHashSet();
        }
        vals.addAll(add);
        vals.removeAll(rm);
        if (!vals.isEmpty()) {
            child.setProperty(name, vals, STRINGS);
        } else {
            child.removeProperty(name);
        }
    }

}

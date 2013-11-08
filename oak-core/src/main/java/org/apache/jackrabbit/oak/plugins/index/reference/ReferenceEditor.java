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
import static org.apache.jackrabbit.oak.api.Type.REFERENCE;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.api.Type.WEAKREFERENCE;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.api.CommitFailedException.INTEGRITY;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;

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
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkState;

/**
 * Index editor for keeping a references to a node up to date.
 * 
 */
class ReferenceEditor implements Editor {

    // TODO
    // - look into using a storage strategy (trees) -> OAK-1134
    // - what happens when you move a node? who updates the backlinks?
    // - clearer error messages

    private final static String REF_NAME = ":references";
    private final static String WEAK_REF_NAME = ":weakreferences";

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
     * set of legally removed Ids
     */
    private final Map<String, String> rmIds;

    public ReferenceEditor(NodeBuilder builder) {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.builder = builder;
        this.newRefs = newHashMap();
        this.rmRefs = newHashMap();
        this.newWeakRefs = newHashMap();
        this.rmWeakRefs = newHashMap();
        this.rmIds = newHashMap();
    }

    private ReferenceEditor(ReferenceEditor parent, String name) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.builder = parent.builder;
        this.newRefs = parent.newRefs;
        this.rmRefs = parent.rmRefs;
        this.newWeakRefs = parent.newWeakRefs;
        this.rmWeakRefs = parent.rmWeakRefs;
        this.rmIds = parent.rmIds;
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
            Set<String> offending = newHashSet(rmIds.keySet());
            offending.removeAll(rmRefs.keySet());
            if (!offending.isEmpty()) {
                throw new CommitFailedException(INTEGRITY, 1,
                        "Unable to delete referenced node");
            }

            // local uuid-> nodebuilder cache
            Map<String, NodeBuilder> builders = newHashMap();
            for (Entry<String, Set<String>> ref : rmRefs.entrySet()) {
                String uuid = ref.getKey();
                if (rmIds.containsKey(uuid)) {
                    continue;
                }
                NodeBuilder child = resolveUUID(uuid, builders);
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
                if (rmIds.containsKey(uuid)) {
                    continue;
                }
                NodeBuilder child = resolveUUID(uuid, builders);
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
                if (rmIds.containsKey(uuid)) {
                    continue;
                }
                NodeBuilder child = resolveUUID(uuid, builders);
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
                if (rmIds.containsKey(uuid)) {
                    continue;
                }
                NodeBuilder child = resolveUUID(uuid, builders);
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

    private NodeBuilder resolveUUID(String uuid,
            Map<String, NodeBuilder> builders) {
        checkState(parent == null);

        if (builders.containsKey(uuid)) {
            return builders.get(uuid);
        }
        String path = getIdManager().resolveUUID(uuid);

        if (path == null) {
            return null;
        }
        NodeBuilder child = getChild(builder, path);
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

    @Override
    public void propertyAdded(PropertyState after) {
        propertyChanged(null, after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) {
        if (before != null) {
            if (before.getType() == REFERENCE) {
                put(rmRefs, before.getValue(STRING), getPath());
            }
            if (before.getType() == WEAKREFERENCE) {
                put(rmWeakRefs, before.getValue(STRING), getPath());
            }
        }
        if (after != null) {
            if (after.getType() == REFERENCE) {
                put(newRefs, after.getValue(STRING), getPath());
            }
            if (after.getType() == WEAKREFERENCE) {
                put(newWeakRefs, after.getValue(STRING), getPath());
            }
        }
    }

    private static void put(Map<String, Set<String>> map, String key,
            String value) {
        Set<String> values = map.get(key);
        if (values == null) {
            values = newHashSet();
        }
        values.add(value);
        map.put(key, values);
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
        return new ReferenceEditor(this, name);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before,
            NodeState after) {
        String path = concat(getPath(), name);
        if (isVersionStorePath(path)) {
            return null;
        }
        return new ReferenceEditor(this, name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        String path = concat(getPath(), name);
        if (isVersionStorePath(path)) {
            return null;
        }

        for (PropertyState ps : before.getProperties()) {
            if (ps.getType() == REFERENCE) {
                put(rmRefs, ps.getValue(STRING), path);
            }
            if (ps.getType() == WEAKREFERENCE) {
                put(rmWeakRefs, ps.getValue(STRING), path);
            }
        }
        if (before.hasProperty(REF_NAME)) {
            String uuid = before.getString(JCR_UUID);
            if (uuid != null) {
                rmIds.put(uuid, path);
            }
        }
        return new ReferenceEditor(this, name);
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
}

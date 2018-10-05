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
package org.apache.jackrabbit.oak.upgrade.version;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PREDECESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.oak.plugins.memory.MultiGenericPropertyState.nameProperty;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.REP_VERSIONSTORAGE;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Joiner;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionHistoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(VersionHistoryUtil.class);

    public static String getRelativeVersionHistoryPath(String versionableUuid) {
        return Joiner.on('/').join(concat(
                singleton(""),
                getRelativeVersionHistoryPathSegments(versionableUuid),
                singleton(versionableUuid)));
    }

    /**
     * Constructs the version history path based on the versionable's UUID.
     *
     * @param versionStorage below which to look for the version.
     * @param versionableUuid The String representation of the versionable's UUID.
     * @return The NodeState corresponding to the version history, or {@code null}
     *         if it does not exist.
     */
    static NodeState getVersionHistoryNodeState(NodeState versionStorage, String versionableUuid) {
        NodeState historyParent = versionStorage;
        for (String segment : getRelativeVersionHistoryPathSegments(versionableUuid)) {
            historyParent = historyParent.getChildNode(segment);
        }
        return historyParent.getChildNode(versionableUuid);
    }

    public static NodeBuilder getVersionHistoryBuilder(NodeBuilder versionStorage, String versionableUuid) {
        NodeBuilder history = versionStorage;
        for (String segment : getRelativeVersionHistoryPathSegments(versionableUuid)) {
            history = history.getChildNode(segment);
        }
        return history.getChildNode(versionableUuid);
    }

    private static List<String> getRelativeVersionHistoryPathSegments(String versionableUuid) {
        final List<String> segments = new ArrayList<String>();
        for (int i = 0; i < 3; i++) {
            segments.add(versionableUuid.substring(i * 2, i * 2 + 2));
        }
        return segments;
    }

    public static NodeState getVersionStorage(NodeState root) {
        return root.getChildNode(JCR_SYSTEM).getChildNode(JCR_VERSIONSTORAGE);
    }

    public static NodeBuilder getVersionStorage(NodeBuilder root) {
        return root.getChildNode(JCR_SYSTEM).getChildNode(JCR_VERSIONSTORAGE);
    }

    public static NodeBuilder createVersionStorage(NodeBuilder root) {
        NodeBuilder vs = root.child(JCR_SYSTEM).child(JCR_VERSIONSTORAGE);
        if (!vs.hasProperty(JCR_PRIMARYTYPE)) {
            vs.setProperty(JCR_PRIMARYTYPE, REP_VERSIONSTORAGE, Type.NAME);
        }
        return vs;
    }

    public static List<String> getVersionableNodes(NodeState root, TypePredicate isVersionable, Calendar olderThan) {
        List<String> paths = new ArrayList<>();
        NodeState versionStorage = getVersionStorage(root);
        getVersionableNodes(root, versionStorage, isVersionable, olderThan, PathUtils.ROOT_PATH, paths);
        return paths;
    }


    private static void getVersionableNodes(NodeState node, NodeState versionStorage, TypePredicate isVersionable, Calendar olderThan, String path, List<String> paths) {
        if (isVersionable.apply(node)) {
            if (olderThan == null) {
                paths.add(path);
            } else {
                NodeState versionHistory = getVersionHistoryNodeState(versionStorage, node.getString(JCR_UUID));
                Calendar lastModified = getVersionHistoryLastModified(versionHistory);
                if (lastModified.before(olderThan)) {
                    paths.add(path);
                }
            }
        }
        for (ChildNodeEntry c : node.getChildNodeEntries()) {
            getVersionableNodes(c.getNodeState(), versionStorage, isVersionable, olderThan, PathUtils.concat(path, c.getName()), paths);
        }
    }

    public static Calendar getVersionHistoryLastModified(NodeState versionHistory) {
        Calendar youngest = Calendar.getInstance();
        youngest.setTimeInMillis(0);
        for (final ChildNodeEntry entry : versionHistory.getChildNodeEntries()) {
            final NodeState version = entry.getNodeState();
            if (version.hasProperty(JCR_CREATED)) {
                final Calendar created = ISO8601.parse(version.getProperty(JCR_CREATED).getValue(Type.DATE));
                if (created.after(youngest)) {
                    youngest = created;
                }
            }
        }
        return youngest;
    }

    public static void removeVersionProperties(NodeBuilder versionableBuilder, TypePredicate isReferenceable) {
        assert versionableBuilder.exists();

        removeMixin(versionableBuilder, MIX_VERSIONABLE);

        // we don't know if the UUID is otherwise referenced,
        // so make sure the node remains referencable
        if (!isReferenceable.apply(versionableBuilder.getNodeState())) {
            addMixin(versionableBuilder, MIX_REFERENCEABLE);
        }

        versionableBuilder.removeProperty(JCR_VERSIONHISTORY);
        versionableBuilder.removeProperty(JCR_PREDECESSORS);
        versionableBuilder.removeProperty(JCR_BASEVERSION);
        versionableBuilder.removeProperty(JCR_ISCHECKEDOUT);
    }

    static void addMixin(NodeBuilder builder, String name) {
        if (builder.hasProperty(JCR_MIXINTYPES)) {
            final Set<String> mixins = newHashSet(builder.getProperty(JCR_MIXINTYPES).getValue(Type.NAMES));
            if (mixins.add(name)) {
                builder.setProperty(nameProperty(JCR_MIXINTYPES, mixins));
            }
        } else {
            builder.setProperty(nameProperty(JCR_MIXINTYPES, of(name)));
        }
    }

    private static void removeMixin(NodeBuilder builder, String name) {
        if (builder.hasProperty(JCR_MIXINTYPES)) {
            final Set<String> mixins = newHashSet(builder.getProperty(JCR_MIXINTYPES).getValue(Type.NAMES));
            if (mixins.remove(name)) {
                if (mixins.isEmpty()) {
                    builder.removeProperty(JCR_MIXINTYPES);
                } else {
                    builder.setProperty(nameProperty(JCR_MIXINTYPES, mixins));
                }
            }
        }
    }

    public static NodeBuilder removeVersions(NodeState root,
                               List<String> toRemove) {
        NodeBuilder rootBuilder = root.builder();
        TypePredicate isReferenceable = new TypePredicate(root, MIX_REFERENCEABLE);
        NodeBuilder versionStorage = getVersionStorage(rootBuilder);
        for (String p : toRemove) {
            LOG.info("Removing version history for {}", p);
            NodeBuilder b = getBuilder(rootBuilder, p);
            String uuid = b.getString(JCR_UUID);
            VersionHistoryUtil.removeVersionProperties(b, isReferenceable);
            getVersionHistoryBuilder(versionStorage, uuid).remove();
        }
        return rootBuilder;
    }

    private static NodeBuilder getBuilder(NodeBuilder root, String path) {
        NodeBuilder builder = root;
        for (String e : PathUtils.elements(path)) {
            builder = builder.child(e);
        }
        return builder;
    }

}

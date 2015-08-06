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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PREDECESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.oak.plugins.memory.MultiGenericPropertyState.nameProperty;
import static org.apache.jackrabbit.oak.plugins.version.VersionConstants.MIX_REP_VERSIONABLE_PATHS;

/**
 * The VersionableEditor provides two possible ways to handle
 * versionable nodes:
 * <ul>
 *     <li>it can copy the version histories of versionable nodes, or</li>
 *     <li>
 *         it can skip copying version histories and remove the
 *         {@code mix:versionable} mixin together with any related
 *         properties (see {@link #removeVersionProperties(NodeBuilder)}).
 *     </li>
 * </ul>
 */
public class VersionableEditor extends DefaultEditor {

    private static final Set<String> SKIPPED_PATHS = of("/oak:index", "/jcr:system/jcr:versionStorage");

    private final Provider provider;

    private final NodeBuilder rootBuilder;

    private final TypePredicate isReferenceable;

    private final TypePredicate isVersionable;

    private final VersionCopier versionCopier;

    private String path;

    private VersionableEditor(Provider provider, NodeBuilder builder) {
        this.provider = provider;
        this.rootBuilder = builder;
        this.isVersionable = new TypePredicate(builder.getNodeState(), MIX_VERSIONABLE);
        this.isReferenceable = new TypePredicate(builder.getNodeState(), MIX_REFERENCEABLE);
        this.versionCopier = new VersionCopier(provider.sourceRoot, builder);
        this.path = "/";
    }

    public static class Provider implements EditorProvider {

        private final NodeState sourceRoot;

        private final String workspaceName;

        private final VersionCopyConfiguration config;

        public Provider(NodeState sourceRoot, String workspaceName, VersionCopyConfiguration config) {
            this.sourceRoot = sourceRoot;
            this.workspaceName = workspaceName;
            this.config = config;
        }

        @Override
        public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder, CommitInfo info) throws CommitFailedException {
            return new VersionableEditor(this, builder);
        }
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        final String path = PathUtils.concat(this.path, name);
        // skip deleted nodes and well known paths that may not contain versionable nodes
        if (after == null || SKIPPED_PATHS.contains(path)) {
            return null;
        }

        // assign path field only after checking that we don't skip this subtree
        this.path = path;

        final VersionCopyConfiguration c = provider.config;
        if (isVersionable.apply(after)) {
            final String versionableUuid = getProperty(after, JCR_UUID, Type.STRING);
            boolean versionHistoryExists = isVersionHistoryExists(versionableUuid);
            if (c.isCopyVersions() && c.skipOrphanedVersionsCopy()) {
                versionHistoryExists = copyVersionHistory(after);
            } else if (c.isCopyVersions() && !c.skipOrphanedVersionsCopy()) {
                // all version histories have been copied, but maybe the date
                // range for orphaned entries is narrower
                if (c.getOrphanedMinDate().after(c.getVersionsMinDate())) {
                    versionHistoryExists = copyVersionHistory(after);
                }
            } else {
                versionHistoryExists = false;
            }

            if (versionHistoryExists) {
                setVersionablePath(versionableUuid);
            } else {
                removeVersionProperties(getNodeBuilder(rootBuilder, this.path));
            }
        }

        return this;
    }

    private boolean copyVersionHistory(NodeState versionable) {
        assert versionable.exists();

        final String versionableUuid = versionable.getProperty(JCR_UUID).getValue(Type.STRING);
        return versionCopier.copyVersionHistory(versionableUuid, provider.config.getVersionsMinDate());
    }

    private void setVersionablePath(String versionableUuid) {
        final NodeBuilder versionHistory = VersionHistoryUtil.getVersionHistoryBuilder(rootBuilder, versionableUuid);
        versionHistory.setProperty(provider.workspaceName, path, Type.PATH);
        addMixin(versionHistory, MIX_REP_VERSIONABLE_PATHS);
    }

    private boolean isVersionHistoryExists(String versionableUuid) {
        return VersionHistoryUtil.getVersionHistoryNodeState(rootBuilder.getNodeState(), versionableUuid).exists();
    }

    private void removeVersionProperties(final NodeBuilder versionableBuilder) {
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

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return childNodeAdded(name, after);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        return childNodeAdded(name, null);
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        this.path = PathUtils.getParentPath(this.path);
    }

    private static <T> T getProperty(NodeState state, String name, Type<T> type) {
        if (state.hasProperty(name)) {
            return state.getProperty(name).getValue(type);
        }
        return null;
    }

    private static NodeBuilder getNodeBuilder(NodeBuilder root, String path) {
        NodeBuilder builder = root;
        for (String name : PathUtils.elements(path)) {
            builder = builder.getChildNode(name);
        }
        return builder;
    }

    private static void addMixin(NodeBuilder builder, String name) {
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
}

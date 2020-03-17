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
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENMIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_PREDECESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_ROOTVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_SUCCESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.NT_VERSION;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.REFERENCE;
import static org.apache.jackrabbit.oak.api.Type.REFERENCES;
import static org.apache.jackrabbit.oak.plugins.memory.MultiGenericPropertyState.nameProperty;
import static org.apache.jackrabbit.oak.upgrade.version.VersionHistoryUtil.getVersionHistoryNodeState;
import static org.apache.jackrabbit.oak.upgrade.version.VersionHistoryUtil.getVersionStorage;

/**
 * The VersionablePropertiesEditor adds missing versionable properties.
 */
public final class VersionablePropertiesEditor extends DefaultEditor {

    private static final String MIX_SIMPLE_VERSIONABLE = "mix:simpleVersionable";

    private static final Logger log = LoggerFactory.getLogger(VersionablePropertiesEditor.class);

    private final NodeBuilder rootBuilder;

    private final NodeBuilder versionStorage;

    private final NodeBuilder builder;

    private final TypePredicate isVersionable;

    private final TypePredicate isNtVersion;

    private final TypePredicate isFrozenNode;

    private VersionablePropertiesEditor(NodeBuilder rootBuilder) {
        this.builder = rootBuilder;
        this.rootBuilder = rootBuilder;
        this.versionStorage = getVersionStorage(rootBuilder);
        this.isVersionable = new TypePredicate(rootBuilder.getNodeState(), MIX_VERSIONABLE);
        this.isNtVersion = new TypePredicate(rootBuilder.getNodeState(), NT_VERSION);
        this.isFrozenNode = new TypePredicate(rootBuilder.getNodeState(), NT_FROZENNODE);
    }

    private VersionablePropertiesEditor(VersionablePropertiesEditor parent, NodeBuilder builder) {
        this.builder = builder;
        this.rootBuilder = parent.rootBuilder;
        this.versionStorage = parent.versionStorage;
        this.isVersionable = parent.isVersionable;
        this.isNtVersion = parent.isNtVersion;
        this.isFrozenNode = parent.isFrozenNode;
    }

    public static class Provider implements EditorProvider {
        @Override
        public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder, CommitInfo info)
                throws CommitFailedException {
            return new VersionablePropertiesEditor(builder);
        }

        @Override
        public String toString() {
            return "VersionablePropertiesEditor";
        }
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        NodeBuilder nodeBuilder = builder.getChildNode(name);
        if (isVersionable.apply(after)) {
            fixProperties(nodeBuilder);
        } else if (isFrozenNode.apply(after)) {
            updateFrozenMixins(nodeBuilder);
        }
        return new VersionablePropertiesEditor(this, nodeBuilder);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return childNodeAdded(name, after);
    }

    private static boolean updateFrozenMixins(NodeBuilder builder) {
        if (builder.hasProperty(JCR_FROZENMIXINTYPES)) {
            final Set<String> mixins = newHashSet(builder.getProperty(JCR_FROZENMIXINTYPES).getValue(NAMES));
            if (mixins.remove(MIX_SIMPLE_VERSIONABLE)) {
                mixins.add(MIX_VERSIONABLE);
                builder.setProperty(nameProperty(JCR_FROZENMIXINTYPES, mixins));
                return true;
            }
        }
        return false;
    }

    private void fixProperties(NodeBuilder node) {
        NodeState versionHistory = getVersionHistoryNodeState(versionStorage.getNodeState(), node.getString(JCR_UUID));
        if (!versionHistory.exists()) {
            log.warn("No version history for {}", node);
            return;
        }

        Set<String> updated = new HashSet<String>();
        if (!node.hasProperty(JCR_VERSIONHISTORY)) {
            node.setProperty(JCR_VERSIONHISTORY, versionHistory.getString(JCR_UUID), REFERENCE);
            updated.add(JCR_VERSIONHISTORY);
        }

        String baseVersion = null;
        if (!node.hasProperty(JCR_BASEVERSION)) {
            baseVersion = getLastVersion(versionHistory);
            node.setProperty(JCR_BASEVERSION, baseVersion, REFERENCE);
            updated.add(JCR_BASEVERSION);
        }

        if (!node.hasProperty(JCR_PREDECESSORS)) {
            baseVersion = baseVersion == null ? getLastVersion(versionHistory) : baseVersion;

            List<String> predecessors = new ArrayList<String>();
            if (node.getBoolean(JCR_ISCHECKEDOUT)) {
                predecessors.add(baseVersion);
            }
            node.setProperty(JCR_PREDECESSORS, predecessors, REFERENCES);
            updated.add(JCR_PREDECESSORS);
        }

        if (!updated.isEmpty()) {
            log.info("Updated versionable properties {} for {}", updated, node);
        }
    }

    private String getLastVersion(NodeState versionHistory) {
        NodeState lastVersion = versionHistory.getChildNode(JCR_ROOTVERSION);
        for (ChildNodeEntry child : versionHistory.getChildNodeEntries()) {
            NodeState v = child.getNodeState();
            if (!isNtVersion.apply(v)) {
                continue;
            }
            if (v.getProperty(JCR_SUCCESSORS).count() == 0) { // no successors
                lastVersion = v;
            }
        }
        return lastVersion.getString(JCR_UUID);
    }
}

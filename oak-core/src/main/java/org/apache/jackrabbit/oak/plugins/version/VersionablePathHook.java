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
package org.apache.jackrabbit.oak.plugins.version;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * Commit hook which is responsible for storing the path of the versionable
 * node with every version history. This includes creating the path property
 * for every workspace the version history is represented and updating the
 * path upon moving around a versionable node.
 */
public class VersionablePathHook implements CommitHook {

    private final String workspaceName;

    public VersionablePathHook(String workspaceName) {
        this.workspaceName = workspaceName;
    }

    @Nonnull
    @Override
    public NodeState processCommit(
            NodeState before, NodeState after, CommitInfo info)
            throws CommitFailedException {
        NodeBuilder rootBuilder = after.builder();
        NodeBuilder vsRoot = rootBuilder.child(NodeTypeConstants.JCR_SYSTEM).child(NodeTypeConstants.JCR_VERSIONSTORAGE);
        ReadWriteVersionManager vMgr = new ReadWriteVersionManager(vsRoot, rootBuilder);
        List<CommitFailedException> exceptions = new ArrayList<CommitFailedException>();
        after.compareAgainstBaseState(before,
                new Diff(vMgr, new Node(rootBuilder), exceptions));
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
        return rootBuilder.getNodeState();
    }

    @Override
    public String toString() {
        return "VersionablePathHook : workspaceName = " + workspaceName;
    }

    private final class Diff extends DefaultNodeStateDiff implements VersionConstants {

        private final ReadWriteVersionManager versionManager;
        private final Node nodeAfter;
        private final List<CommitFailedException> exceptions;

        private Diff(@Nonnull ReadWriteVersionManager versionManager,
                     @Nonnull Node node,
                     @Nonnull List<CommitFailedException> exceptions) {
            this.versionManager = versionManager;
            this.nodeAfter = node;
            this.exceptions = exceptions;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            return setVersionablePath(after);
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            return setVersionablePath(after);
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            return childNodeChanged(name, EMPTY_NODE, after);
        }

        @Override
        public boolean childNodeChanged(
                String name, NodeState before, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                // do not traverse into hidden trees
                return true;
            }
            Node node = new Node(nodeAfter, name);
            return after.compareAgainstBaseState(
                    before, new Diff(versionManager, node, exceptions));
        }

        private boolean setVersionablePath(PropertyState after) {
            if (JcrConstants.JCR_VERSIONHISTORY.equals(after.getName()) && nodeAfter.isVersionable(versionManager)) {
                NodeBuilder vhBuilder;
                try {
                    vhBuilder = versionManager.getOrCreateVersionHistory(
                            nodeAfter.builder, Collections.<String, Object>emptyMap());
                } catch (CommitFailedException e) {
                    exceptions.add(e);
                    // stop further comparison
                    return false;
                }

                if (!vhBuilder.hasProperty(JcrConstants.JCR_MIXINTYPES)) {
                    vhBuilder.setProperty(
                            JcrConstants.JCR_MIXINTYPES,
                            ImmutableSet.of(MIX_REP_VERSIONABLE_PATHS),
                            Type.NAMES);
                }

                String versionablePath = nodeAfter.path;
                vhBuilder.setProperty(workspaceName, versionablePath, Type.PATH);
            }
            return true;
        }
    }

    private static final class Node {

        private final String path;
        private final NodeBuilder builder;

        private Node(NodeBuilder rootBuilder) {
            this.path = "/";
            this.builder = rootBuilder;
        }

        private Node(Node parent, String name) {
            this.builder = parent.builder.child(name);
            this.path = PathUtils.concat(parent.path, name);
        }

        private boolean isVersionable(ReadWriteVersionManager versionManager) {
            Tree tree = new ImmutableTree(ImmutableTree.ParentProvider.UNSUPPORTED, PathUtils.getName(path), builder.getNodeState());
            return versionManager.isVersionable(tree);
        }
    }
}
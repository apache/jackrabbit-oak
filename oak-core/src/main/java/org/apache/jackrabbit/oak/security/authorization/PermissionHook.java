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
package org.apache.jackrabbit.oak.security.authorization;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.core.TreeImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code CommitHook} implementation that processes any modification made to
 * access control content and updates persisted permission caches associated
 * with access control related data stored in the repository.
 */
public class PermissionHook implements CommitHook, AccessControlConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionHook.class);

    @Nonnull
    @Override
    public NodeState processCommit(final NodeState before, NodeState after) throws CommitFailedException {
        NodeBuilder rootBuilder = after.builder();

        // TODO: retrieve workspace name
        String workspaceName = "default";
        NodeBuilder permissionRoot = getPermissionRoot(rootBuilder, workspaceName);
        ReadOnlyNodeTypeManager ntMgr = ReadOnlyNodeTypeManager.getInstance(before);

        after.compareAgainstBaseState(before, new Diff(new Node(rootBuilder), permissionRoot, ntMgr));
        return rootBuilder.getNodeState();
    }

    private NodeBuilder getPermissionRoot(NodeBuilder rootBuilder, String workspaceName) {
        NodeBuilder store = rootBuilder.child(NodeTypeConstants.JCR_SYSTEM).child(REP_PERMISSION_STORE);
        NodeBuilder permissionRoot;
        if (!store.hasChildNode(workspaceName)) {
            permissionRoot = store.child(workspaceName)
                    .setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE);
        } else {
            permissionRoot = store.child(workspaceName);
        }
        return permissionRoot;
    }

    private static class Diff implements NodeStateDiff {

        private final ReadOnlyNodeTypeManager ntMgr;
        private final NodeBuilder permissionRoot;
        private final Node parentAfter;

        private Diff(@Nonnull Node node, NodeBuilder permissionRoot, ReadOnlyNodeTypeManager ntMgr) {
            this.ntMgr = ntMgr;
            this.permissionRoot = permissionRoot;
            this.parentAfter = node;
        }

        @Override
        public void propertyAdded(PropertyState after) {
            // nothing to do
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (isACL(parentAfter) && TreeImpl.OAK_CHILD_ORDER.equals(before.getName())) {
                updateEntries();
            }
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            // nothing to do
        }

        @Override
        public void childNodeAdded(String name, NodeState after) {
            if (isACE(name, after)) {
                addEntry(name, after);
            } else {
                NodeState before = MemoryNodeState.EMPTY_NODE;
                Node node = new Node(parentAfter, name);
                after.compareAgainstBaseState(before, new Diff(node, permissionRoot, ntMgr));
            }
        }

        @Override
        public void childNodeChanged(String name, NodeState before, NodeState after) {
            if (isACE(name, before) || isACE(name, after)) {
                updateEntry(name, before, after);
            } else {
                Node node = new Node(parentAfter, name);
                after.compareAgainstBaseState(before, new Diff(node, permissionRoot, ntMgr));
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            if (isACE(name, before)) {
                removeEntry(name, before);
            } else {
                Node after = new Node(parentAfter.path, name);
                after.builder.getNodeState().compareAgainstBaseState(before, new Diff(after, permissionRoot, ntMgr));
            }
        }

        //--------------------------------------------------------< private >---
        private boolean isACL(Node parent) {
            try {
                return ntMgr.isNodeType(getTree(parent.getName(), parent.getNodeState()), NT_REP_POLICY);
            } catch (RepositoryException e) {
                return false;
            }
        }

        private boolean isACE(String name, NodeState nodeState) {
            try {
                return ntMgr.isNodeType(getTree(name, nodeState), NT_REP_ACE);
            } catch (RepositoryException e) {
                return false;
            }
        }

        private static String getAccessControlledPath(Node aclNode) {
            return Text.getRelativeParent(aclNode.path, 1);
        }

        private static Tree getTree(String name, NodeState nodeState) {
            // FIXME: this readonlytree is not properly connect to it's parent
            return new ReadOnlyTree(null, name, nodeState);
        }

        private void addEntry(String name, NodeState after) {
            String accessControlledPath = getAccessControlledPath(parentAfter);
            // TODO
            //log.info("add entry:" + name);
        }

        private void removeEntry(String name, NodeState after) {
            String accessControlledPath = getAccessControlledPath(parentAfter);
            // TODO
            //log.info("remove entry" + name);
        }

        private void updateEntry(String name, NodeState after, NodeState before) {
            String accessControlledPath = getAccessControlledPath(parentAfter);
            // TODO
            //log.info("update"+ name);
        }

        private void updateEntries() {
            String accessControlledPath = getAccessControlledPath(parentAfter);
            NodeState aclState = parentAfter.getNodeState();

            // TODO
        }
    }

    private static final class Node {

        private final String path;
        private final NodeBuilder builder;

        private Node(NodeBuilder rootBuilder) {
            this.path = "/";
            this.builder = rootBuilder;
        }

        private Node(String parentPath, String name) {
            this.path = PathUtils.concat(parentPath, name);
            this.builder = MemoryNodeState.EMPTY_NODE.builder();
        }

        private Node(Node parent, String name) {
            this.builder = parent.builder.child(name);
            this.path = PathUtils.concat(parent.path, name);
        }

        private String getName() {
            return Text.getName(path);
        }

        private NodeState getNodeState() {
            return builder.getNodeState();
        }
    }
}
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

import java.util.Collections;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyRoot;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.core.TreeImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.PropertyBuilder;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;

/**
 * {@code CommitHook} implementation that processes any modification made to
 * access control content and updates persisted permission caches associated
 * with access control related data stored in the repository.
 */
public class PermissionHook implements CommitHook, AccessControlConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionHook.class);

    private final String workspaceName;

    PermissionHook(String workspaceName) {
        this.workspaceName = workspaceName;
    }

    @Nonnull
    @Override
    public NodeState processCommit(final NodeState before, NodeState after) throws CommitFailedException {
        NodeBuilder rootAfter = after.builder();

        NodeBuilder permissionRoot = getPermissionRoot(rootAfter, workspaceName);
        ReadOnlyNodeTypeManager ntMgr = ReadOnlyNodeTypeManager.getInstance(before);
        PrivilegeBitsProvider bitsProvider = new PrivilegeBitsProvider(new ReadOnlyRoot(before));

        after.compareAgainstBaseState(before, new Diff(new BeforeNode(before), new Node(rootAfter), permissionRoot, bitsProvider, ntMgr));
        return rootAfter.getNodeState();
    }

    @Nonnull
    private NodeBuilder getPermissionRoot(NodeBuilder rootBuilder, String workspaceName) {
        NodeBuilder permissionStore = rootBuilder.child(NodeTypeConstants.JCR_SYSTEM).child(REP_PERMISSION_STORE);
        if (permissionStore.getProperty(JCR_PRIMARYTYPE) == null) {
            permissionStore.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
        }
        NodeBuilder permissionRoot;
        if (!permissionStore.hasChildNode(workspaceName)) {
            permissionRoot = permissionStore.child(workspaceName)
                    .setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE);
        } else {
            permissionRoot = permissionStore.child(workspaceName);
        }
        return permissionRoot;
    }

    private static Tree getTree(String name, NodeState nodeState) {
        // FIXME: this readonlytree is not properly connect to it's parent
        return new ReadOnlyTree(null, name, nodeState);
    }

    private static int getAceIndex(BaseNode aclNode, String aceName) {
        PropertyState ordering = checkNotNull(aclNode.getNodeState().getProperty(TreeImpl.OAK_CHILD_ORDER));
        return Lists.newArrayList(ordering.getValue(Type.STRINGS)).indexOf(aceName);
    }

    private static String generateName(NodeBuilder principalRoot, Entry entry) {
        StringBuilder name = new StringBuilder();
        name.append((entry.isAllow) ? 'a' : 'd').append('-').append(principalRoot.getChildNodeCount());
        return name.toString();
    }

    private static class Diff implements NodeStateDiff {

        private final BeforeNode parentBefore;
        private final Node parentAfter;
        private final NodeBuilder permissionRoot;
        private final PrivilegeBitsProvider bitsProvider;
        private final ReadOnlyNodeTypeManager ntMgr;

        private Diff(@Nonnull BeforeNode parentBefore, @Nonnull Node parentAfter,
                     @Nonnull NodeBuilder permissionRoot,
                     @Nonnull PrivilegeBitsProvider bitsProvider,
                     @Nonnull ReadOnlyNodeTypeManager ntMgr) {
            this.parentBefore = parentBefore;
            this.parentAfter = parentAfter;
            this.permissionRoot = permissionRoot;
            this.bitsProvider = bitsProvider;
            this.ntMgr = ntMgr;
        }

        @Override
        public void propertyAdded(PropertyState after) {
            // nothing to do
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            if (isACL(parentAfter) && TreeImpl.OAK_CHILD_ORDER.equals(before.getName())) {
                // TODO: update if order has changed without child-node modifications
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
                BeforeNode before = new BeforeNode(parentBefore.getPath(), name, MemoryNodeState.EMPTY_NODE);
                Node node = new Node(parentAfter, name);
                after.compareAgainstBaseState(before.getNodeState(), new Diff(before, node, permissionRoot, bitsProvider, ntMgr));
            }
        }

        @Override
        public void childNodeChanged(String name, final NodeState before, NodeState after) {
            if (isACE(name, before) || isACE(name, after)) {
                updateEntry(name, before, after);
            } else if (REP_RESTRICTIONS.equals(name)) {
                updateEntry(parentAfter.getName(), parentBefore.getNodeState(), parentAfter.getNodeState());
            } else {
                BeforeNode nodeBefore = new BeforeNode(parentBefore.getPath(), name, before);
                Node nodeAfter = new Node(parentAfter, name);
                after.compareAgainstBaseState(before, new Diff(nodeBefore, nodeAfter, permissionRoot, bitsProvider, ntMgr));
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            if (isACE(name, before)) {
                removeEntry(name, before);
            } else {
                BeforeNode nodeBefore = new BeforeNode(parentBefore.getPath(), name, before);
                Node after = new Node(parentAfter.getPath(), name, MemoryNodeState.EMPTY_NODE);
                after.getNodeState().compareAgainstBaseState(before, new Diff(nodeBefore, after, permissionRoot, bitsProvider, ntMgr));
            }
        }

        //--------------------------------------------------------< private >---
        private boolean isACL(Node parent) {
            return ntMgr.isNodeType(getTree(parent.getName(), parent.getNodeState()), NT_REP_POLICY);
        }

        private boolean isACE(String name, NodeState nodeState) {
            return ntMgr.isNodeType(getTree(name, nodeState), NT_REP_ACE);
        }

        private static String getAccessControlledPath(BaseNode aclNode) {
            if (REP_REPO_POLICY.equals(aclNode.getName())) {
                return "";
            } else {
                return Text.getRelativeParent(aclNode.getPath(), 1);
            }
        }

        private void addEntry(String name, NodeState ace) {
            Entry entry = createEntry(name, ace, parentAfter);
            entry.writeTo(permissionRoot.child(entry.principalName));
        }

        private void removeEntry(String name, NodeState ace) {
            Entry entry = createEntry(name, ace, parentBefore);
            String permissionName = getPermissionNodeName(entry);
            if (permissionName != null) {
                permissionRoot.child(entry.principalName).removeNode(permissionName);
            }
        }

        private void updateEntry(String name, NodeState before, NodeState after) {
            removeEntry(name, before);
            addEntry(name, after);
        }

        @CheckForNull
        private String getPermissionNodeName(Entry aceEntry) {
            if (permissionRoot.hasChildNode(aceEntry.principalName)) {
                NodeBuilder principalRoot = permissionRoot.child(aceEntry.principalName);
                for (String childName : principalRoot.getChildNodeNames()) {
                    NodeState state = principalRoot.child(childName).getNodeState();
                    if (aceEntry.isSame(childName, state)) {
                        return childName;
                    }
                }
                log.warn("No entry node for " + aceEntry);
            } else {
                // inconsistency: removing an ACE that doesn't have a corresponding
                // entry in the permission store.
                log.warn("Missing permission node for principal " + aceEntry.principalName);
            }
            return null;
        }

        @Nonnull
        private Entry createEntry(String name, NodeState ace, BaseNode acl) {
            Tree aceTree = getTree(name, ace);
            String principalName = checkNotNull(TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME));
            PrivilegeBits privilegeBits = bitsProvider.getBits(TreeUtil.getString(aceTree, REP_PRIVILEGES));
            boolean isAllow = NT_REP_GRANT_ACE.equals(TreeUtil.getPrimaryTypeName(aceTree));
            // TODO: respect restrictions

            String accessControlledPath = getAccessControlledPath(acl);
            int index = getAceIndex(acl, name);

            return new Entry(accessControlledPath, index, principalName, privilegeBits, isAllow);
        }
    }

    private static abstract class BaseNode {

        private final String path;

        private BaseNode(String path) {
            this.path = path;
        }

        private BaseNode(String parentPath, String name) {
            this.path = PathUtils.concat(parentPath, new String[]{name});
        }

        String getName() {
            return Text.getName(path);
        }

        String getPath() {
            return path;
        }

        abstract NodeState getNodeState();
    }

    private static class BeforeNode extends BaseNode {

        private final NodeState nodeState;

        BeforeNode(NodeState root) {
            super("/");
            this.nodeState = root;
        }


        BeforeNode(String parentPath, String name, NodeState nodeState) {
            super(parentPath, name);
            this.nodeState = nodeState;
        }

        @Override
        NodeState getNodeState() {
            return nodeState;
        }
    }

    private static class Node extends BaseNode {

        private final NodeBuilder builder;

        private Node(NodeBuilder rootBuilder) {
            super("/");
            this.builder = rootBuilder;
        }

        private Node(String parentPath, String name, NodeState state) {
            super(parentPath, name);
            this.builder = state.builder();
        }

        private Node(Node parent, String name) {
            super(parent.getPath(), name);
            this.builder = parent.builder.child(name);
        }

        NodeState getNodeState() {
            return builder.getNodeState();
        }
    }

    private static final class Entry {

        private final String accessControlledPath;
        private final int index;

        private final String principalName;
        private final PrivilegeBits privilegeBits;
        private final boolean isAllow;

        private Entry(@Nonnull String accessControlledPath,
                      int index,
                      @Nonnull String principalName,
                      @Nonnull PrivilegeBits privilegeBits,
                      boolean isAllow) {
            this.accessControlledPath = accessControlledPath;
            this.index = index;

            this.principalName = principalName;
            this.privilegeBits = privilegeBits;
            this.isAllow = isAllow;
        }

        private void writeTo(NodeBuilder principalRoot) {
            String entryName = generateName(principalRoot, this);
            principalRoot.child(entryName)
                    .setProperty("rep:accessControlledPath", accessControlledPath)
                    .setProperty("rep:index", index)
                    .setProperty(privilegeBits.asPropertyState("rep:privileges"));
            // TODO: append restrictions

            PropertyState ordering = principalRoot.getProperty(TreeImpl.OAK_CHILD_ORDER);
            if (ordering == null) {
                principalRoot.setProperty(TreeImpl.OAK_CHILD_ORDER, Collections.singleton(entryName), Type.NAMES);
            } else {
                PropertyBuilder pb = MemoryPropertyBuilder.copy(Type.NAME, ordering);
                // TODO: determine ordering index
                int index = 0;
                pb.setValue(entryName, index);
                principalRoot.setProperty(pb.getPropertyState());
            }
        }

        private boolean isSame(String name, NodeState node) {
            Tree entry = getTree(name, node);

            if (isAllow == (name.charAt(0) == 'a')) {
                return false;
            }
            if (!privilegeBits.equals(PrivilegeBits.getInstance(node.getProperty(REP_PRIVILEGES)))) {
                return false;
            }
            if (!principalName.equals(TreeUtil.getString(entry, REP_PRINCIPAL_NAME))) {
                return false;
            }
            if (index != entry.getProperty("rep:index").getValue(Type.LONG)) {
                return false;
            }
            if (!accessControlledPath.equals(TreeUtil.getString(entry, "rep:accessControlledPath"))) {
                return false;
            }
            // TODO: respect restrictions

            return true;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("entry: ").append(accessControlledPath);
            sb.append(';').append(principalName);
            sb.append(';').append(isAllow ? "allow" : "deny");
            sb.append(';').append(privilegeBits);
            return sb.toString();
        }
    }
}
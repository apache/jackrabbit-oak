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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.core.TreeImpl;
import org.apache.jackrabbit.oak.core.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.commit.PostValidationHook;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * {@code CommitHook} implementation that processes any modification made to
 * access control content and updates persisted permission caches associated
 * with access control related data stored in the repository.
 */
public class PermissionHook implements PostValidationHook, AccessControlConstants, PermissionConstants {

    private final RestrictionProvider restrictionProvider;
    private final String workspaceName;

    private NodeBuilder permissionRoot;
    private ReadOnlyNodeTypeManager ntMgr;
    private PrivilegeBitsProvider bitsProvider;

    public PermissionHook(String workspaceName, RestrictionProvider restrictionProvider) {
        this.workspaceName = workspaceName;
        this.restrictionProvider = restrictionProvider;
    }

    @Nonnull
    @Override
    public NodeState processCommit(final NodeState before, NodeState after) throws CommitFailedException {
        NodeBuilder rootAfter = after.builder();

        permissionRoot = getPermissionRoot(rootAfter);
        ntMgr = ReadOnlyNodeTypeManager.getInstance(before);
        bitsProvider = new PrivilegeBitsProvider(new ImmutableRoot(before));

        after.compareAgainstBaseState(before, new Diff(new BeforeNode(before), new AfterNode(rootAfter)));
        return rootAfter.getNodeState();
    }

    @Nonnull
    private NodeBuilder getPermissionRoot(NodeBuilder rootBuilder) {
        NodeBuilder permissionStore = rootBuilder.child(JCR_SYSTEM).child(REP_PERMISSION_STORE);
        if (!permissionStore.hasProperty(JCR_PRIMARYTYPE)) {
            permissionStore.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
        }
        NodeBuilder permissionRoot;
        if (!permissionStore.hasChildNode(workspaceName)) {
            permissionRoot = permissionStore.child(workspaceName)
                    .setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
        } else {
            permissionRoot = permissionStore.child(workspaceName);
        }
        return permissionRoot;
    }

    @CheckForNull
    private NodeBuilder getPrincipalRoot(String principalName) {
        if (permissionRoot.hasChildNode(principalName)) {
            return permissionRoot.child(principalName);
        } else {
            return null;
        }
    }

    private static Tree getTree(String name, NodeState nodeState) {
        return new ImmutableTree(ImmutableTree.ParentProvider.UNSUPPORTED, name, nodeState, TreeTypeProvider.EMPTY);
    }

    private static String getAccessControlledPath(Node aclNode) {
        if (REP_REPO_POLICY.equals(aclNode.getName())) {
            return "";
        } else {
            return Text.getRelativeParent(aclNode.getPath(), 1);
        }
    }

    private static int getAceIndex(Node aclNode, String aceName) {
        PropertyState ordering = checkNotNull(aclNode.getNodeState().getProperty(TreeImpl.OAK_CHILD_ORDER));
        return Lists.newArrayList(ordering.getValue(Type.STRINGS)).indexOf(aceName);
    }

    private Set<Restriction> getRestrictions(String accessControlledPath, Tree aceTree) {
        return restrictionProvider.readRestrictions(Strings.emptyToNull(accessControlledPath), aceTree);
    }

    private class Diff implements NodeStateDiff {

        private final Node parentBefore;
        private final AfterNode parentAfter;

        private Diff(@Nonnull Node parentBefore, @Nonnull AfterNode parentAfter) {
            this.parentBefore = parentBefore;
            this.parentAfter = parentAfter;
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
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
            } else if (isACE(name, after)) {
                addEntry(name, after);
            } else {
                Node before = new BeforeNode(parentBefore.getPath(), name, EMPTY_NODE);
                AfterNode node = new AfterNode(parentAfter, name);
                after.compareAgainstBaseState(before.getNodeState(), new Diff(before, node));
            }
        }

        @Override
        public void childNodeChanged(String name, final NodeState before, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
            } else if (isACE(name, before) || isACE(name, after)) {
                updateEntry(name, before, after);
            } else {
                BeforeNode nodeBefore = new BeforeNode(parentBefore.getPath(), name, before);
                AfterNode nodeAfter = new AfterNode(parentAfter, name);
                after.compareAgainstBaseState(before, new Diff(nodeBefore, nodeAfter));
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
            } else if (isACE(name, before)) {
                removeEntry(name, before);
            } else {
                BeforeNode nodeBefore = new BeforeNode(parentBefore.getPath(), name, before);
                AfterNode after = new AfterNode(parentAfter.getPath(), name, EMPTY_NODE);
                after.getNodeState().compareAgainstBaseState(before, new Diff(nodeBefore, after));
            }
        }

        //--------------------------------------------------------< private >---
        private boolean isACL(Node parent) {
            return ntMgr.isNodeType(getTree(parent.getName(), parent.getNodeState()), NT_REP_POLICY);
        }

        private boolean isACE(String name, NodeState nodeState) {
            return ntMgr.isNodeType(getTree(name, nodeState), NT_REP_ACE);
        }

        private void addEntry(String name, NodeState ace) {
            PermissionEntry entry = createPermissionEntry(name, ace, parentAfter);
            entry.writeTo(permissionRoot);
        }

        private void removeEntry(String name, NodeState ace) {
            PermissionEntry entry = createPermissionEntry(name, ace, parentBefore);
            NodeBuilder principalRoot = getPrincipalRoot(entry.principalName);
            if (principalRoot != null) {
                principalRoot.removeNode(entry.nodeName);
            }
        }

        private void updateEntry(String name, NodeState before, NodeState after) {
            removeEntry(name, before);
            addEntry(name, after);
        }

        @Nonnull
        private PermissionEntry createPermissionEntry(String name, NodeState ace, Node acl) {
            Tree aceTree = getTree(name, ace);
            String accessControlledPath = getAccessControlledPath(acl);
            String principalName = checkNotNull(TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME));
            PrivilegeBits privilegeBits = bitsProvider.getBits(TreeUtil.getStrings(aceTree, REP_PRIVILEGES));
            boolean isAllow = NT_REP_GRANT_ACE.equals(TreeUtil.getPrimaryTypeName(aceTree));

            return new PermissionEntry(accessControlledPath, getAceIndex(acl, name), principalName,
                    privilegeBits, isAllow, getRestrictions(accessControlledPath, aceTree));
        }
    }

    private static abstract class Node {

        private final String path;

        private Node(String path) {
            this.path = path;
        }

        private Node(String parentPath, String name) {
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

    private static final class BeforeNode extends Node {

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

    private static final class AfterNode extends Node {

        private final NodeBuilder builder;

        private AfterNode(NodeBuilder rootBuilder) {
            super("/");
            this.builder = rootBuilder;
        }

        private AfterNode(String parentPath, String name, NodeState state) {
            super(parentPath, name);
            this.builder = state.builder();
        }

        private AfterNode(AfterNode parent, String name) {
            super(parent.getPath(), name);
            this.builder = parent.builder.child(name);
        }

        NodeState getNodeState() {
            return builder.getNodeState();
        }
    }

    private final class PermissionEntry {

        private final String accessControlledPath;
        private final int index;
        private final String principalName;
        private final PrivilegeBits privilegeBits;
        private final boolean isAllow;
        private final Set<Restriction> restrictions;

        private final String nodeName;

        private PermissionEntry(@Nonnull String accessControlledPath,
                      int index,
                      @Nonnull String principalName,
                      @Nonnull PrivilegeBits privilegeBits,
                      boolean isAllow, Set<Restriction> restrictions) {
            this.accessControlledPath = accessControlledPath;
            this.index = index;

            this.principalName = Text.escapeIllegalJcrChars(principalName);
            this.privilegeBits = privilegeBits;
            this.isAllow = isAllow;
            this.restrictions = restrictions;

            // create node name from ace definition
            StringBuilder name = new StringBuilder();
            name.append((isAllow) ? PREFIX_ALLOW : PREFIX_DENY).append('-');
            name.append(Objects.hashCode(accessControlledPath, principalName, index, privilegeBits, isAllow, restrictions));
            nodeName = name.toString();
        }

        private void writeTo(NodeBuilder permissionRoot) {
            NodeBuilder principalRoot = permissionRoot.child(principalName);
            if (!principalRoot.hasProperty(JCR_PRIMARYTYPE)) {
                principalRoot.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
            }
            NodeBuilder entry = principalRoot.child(nodeName)
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSIONS, Type.NAME)
                    .setProperty(REP_ACCESS_CONTROLLED_PATH, accessControlledPath)
                    .setProperty(REP_INDEX, index)
                    .setProperty(privilegeBits.asPropertyState(REP_PRIVILEGE_BITS));
            for (Restriction restriction : restrictions) {
                entry.setProperty(restriction.getProperty());
            }
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("permission entry: ").append(accessControlledPath);
            sb.append(';').append(index);
            sb.append(';').append(principalName);
            sb.append(';').append(isAllow ? "allow" : "deny");
            sb.append(';').append(privilegeBits);
            sb.append(';').append(restrictions);
            return sb.toString();
        }
    }
}

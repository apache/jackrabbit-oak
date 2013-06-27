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
import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
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
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.commit.PostValidationHook;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(PermissionHook.class);

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
        // permission root has been created during workspace initialization
        return rootBuilder.getChildNode(JCR_SYSTEM).getChildNode(REP_PERMISSION_STORE).getChildNode(workspaceName);
    }

    private static Tree getTree(String name, NodeState nodeState) {
        return new ImmutableTree(ImmutableTree.ParentProvider.UNSUPPORTED, name, nodeState, TreeTypeProvider.EMPTY);
    }

    private class Diff extends DefaultNodeStateDiff {

        private final Node parentBefore;
        private final AfterNode parentAfter;

        private Diff(@Nonnull Node parentBefore, @Nonnull AfterNode parentAfter) {
            this.parentBefore = parentBefore;
            this.parentAfter = parentAfter;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
            } else if (isACL(name, after)) {
                addEntries(name, after);
            } else {
                Node before = new BeforeNode(parentBefore.getPath(), name, EMPTY_NODE);
                AfterNode node = new AfterNode(parentAfter, name);
                after.compareAgainstBaseState(before.getNodeState(), new Diff(before, node));
            }
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, final NodeState before, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
            } else if (isACL(name, before) || isACL(name, after)) {
                removeEntries(name, before);
                addEntries(name, after);
            } else {
                BeforeNode nodeBefore = new BeforeNode(parentBefore.getPath(), name, before);
                AfterNode nodeAfter = new AfterNode(parentAfter, name);
                after.compareAgainstBaseState(before, new Diff(nodeBefore, nodeAfter));
            }
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
            } else if (isACL(name, before)) {
                removeEntries(name, before);
            } else {
                BeforeNode nodeBefore = new BeforeNode(parentBefore.getPath(), name, before);
                AfterNode after = new AfterNode(parentAfter.getPath(), name, EMPTY_NODE);
                after.getNodeState().compareAgainstBaseState(before, new Diff(nodeBefore, after));
            }
            return true;
        }

        //--------------------------------------------------------< private >---

        private boolean isACL(String name, NodeState nodeState) {
            return ntMgr.isNodeType(getTree(name, nodeState), NT_REP_ACL);
        }

        private boolean isACE(String name, NodeState nodeState) {
            return ntMgr.isNodeType(getTree(name, nodeState), NT_REP_ACE);
        }

        private void addEntries(String aclName, NodeState acl) {
            for (ChildNodeEntry cne : acl.getChildNodeEntries()) {
                NodeState child = cne.getNodeState();
                if (isACE(cne.getName(), child)) {
                    PermissionEntry entry = createPermissionEntry(cne.getName(),
                            child, new AfterNode(parentAfter, aclName));
                    entry.add();
                }
            }
        }

        private void removeEntries(String aclName, NodeState acl) {
            for (ChildNodeEntry cne : acl.getChildNodeEntries()) {
                NodeState child = cne.getNodeState();
                if (isACE(cne.getName(), child)) {
                    PermissionEntry entry = createPermissionEntry(cne.getName(),
                            child, new BeforeNode(parentBefore, aclName, acl));
                    entry.remove();
                }
            }
        }

        @Nonnull
        private PermissionEntry createPermissionEntry(String name, NodeState ace, Node acl) {
            String accessControlledPath = (REP_REPO_POLICY.equals(acl.getName()) ? "" : Text.getRelativeParent(acl.getPath(), 1));
            PropertyState ordering = checkNotNull(acl.getNodeState().getProperty(TreeImpl.OAK_CHILD_ORDER));
            int index = Lists.newArrayList(ordering.getValue(Type.STRINGS)).indexOf(name);
            return new PermissionEntry(getTree(name, ace), accessControlledPath, index);
        }
    }

    private abstract static class Node {

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

        BeforeNode(Node parent, String name, NodeState nodeState) {
            super(parent.getPath(), name);
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
        private final long index;
        private final String principalName;
        private final PrivilegeBits privilegeBits;
        private final boolean isAllow;
        private final Set<Restriction> restrictions;
        private final String nodeName;

        private PermissionEntry(@Nonnull Tree aceTree, @Nonnull String accessControlledPath, long index) {
            this.accessControlledPath = accessControlledPath;
            this.index = index;

            principalName = Text.escapeIllegalJcrChars(checkNotNull(TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME)));
            privilegeBits = bitsProvider.getBits(TreeUtil.getStrings(aceTree, REP_PRIVILEGES));
            isAllow = NT_REP_GRANT_ACE.equals(TreeUtil.getPrimaryTypeName(aceTree));
            restrictions = restrictionProvider.readRestrictions(Strings.emptyToNull(accessControlledPath), aceTree);

            // create node name from ace definition
            int depth = PathUtils.getDepth(accessControlledPath);
            StringBuilder name = new StringBuilder();
            name.append(depth).append('_').append(hashCode());
            nodeName = name.toString();
        }

        private void add() {
            NodeBuilder principalRoot = permissionRoot.child(principalName);
            if (!principalRoot.hasProperty(JCR_PRIMARYTYPE)) {
                principalRoot.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
            }
            NodeBuilder parent = getParent(principalRoot);
            NodeBuilder entry = parent.child(nodeName)
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSIONS, Type.NAME)
                    .setProperty(REP_ACCESS_CONTROLLED_PATH, accessControlledPath)
                    .setProperty(REP_IS_ALLOW, isAllow)
                    .setProperty(REP_INDEX, index)
                    .setProperty(privilegeBits.asPropertyState(REP_PRIVILEGE_BITS));
            for (Restriction restriction : restrictions) {
                entry.setProperty(restriction.getProperty());
            }
            for (String childName : parent.getChildNodeNames()) {
                if (nodeName.equals(childName)) {
                    continue;
                }
                NodeBuilder child = parent.getChildNode(childName);
                String childPath = child.getProperty(REP_ACCESS_CONTROLLED_PATH).getValue(Type.STRING);
                long childIndex = child.getProperty(REP_INDEX).getValue(Type.LONG);
                boolean reconnect = false;
                if (Text.isDescendant(accessControlledPath, childPath)) {
                    reconnect = true;
                } else if (childPath.equals(accessControlledPath)) {
                    reconnect = index < childIndex;
                }
                if (reconnect) {
                    entry.setChildNode(childName, child.getNodeState());
                    child.remove();
                }
            }
        }

        private void remove() {
            String msg = "Unable to remove permission entry";
            if (permissionRoot.hasChildNode(principalName)) {
                NodeBuilder principalRoot = permissionRoot.getChildNode(principalName);
                NodeBuilder parent = getParent(principalRoot);
                if (parent.hasChildNode(nodeName)) {
                    NodeBuilder target = parent.getChildNode(nodeName);
                    for (String childName : target.getChildNodeNames()) {
                        NodeBuilder child = target.getChildNode(childName);
                        parent.setChildNode(childName, child.getNodeState());
                        child.remove();
                    }
                    target.remove();
                } else {
                    log.error("{} {}. No corresponding node found in permission store.", msg, this);
                }
            } else {
                log.error("{} {}: Principal root missing.", msg, this);
            }
        }

        private NodeBuilder getParent(NodeBuilder parent) {
            if (parent.hasChildNode(nodeName)) {
                return parent;
            }
            for (String childName : parent.getChildNodeNames()) {
                NodeBuilder child = parent.getChildNode(childName);
                String childPath = child.getProperty(REP_ACCESS_CONTROLLED_PATH).getValue(Type.STRING);
                long childIndex = child.getProperty(REP_INDEX).getValue(Type.LONG);
                if (Text.isDescendant(childPath, accessControlledPath)) {
                    return getParent(child);
                } else if (childPath.equals(accessControlledPath) && index > childIndex) {
                    return getParent(child);
                }
            }
            return parent;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(accessControlledPath, principalName, index, privilegeBits, isAllow, restrictions);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof PermissionEntry) {
                PermissionEntry other = (PermissionEntry) o;
                return index == other.index && isAllow == other.isAllow
                        && privilegeBits.equals(other.privilegeBits)
                        && principalName.equals(other.principalName)
                        && accessControlledPath.equals(other.accessControlledPath)
                        && restrictions.equals(other.restrictions);
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("permission entry: ").append(accessControlledPath);
            sb.append(';').append(index);
            sb.append(';').append(principalName);
            sb.append(';').append(isAllow ? "allow" : "deny");
            sb.append(';').append(bitsProvider.getPrivilegeNames(privilegeBits));
            sb.append(';').append(restrictions);
            return sb.toString();
        }
    }
}

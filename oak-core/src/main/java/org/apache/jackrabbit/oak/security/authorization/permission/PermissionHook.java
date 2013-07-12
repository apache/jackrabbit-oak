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

import java.util.ArrayList;
import java.util.List;
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
import org.apache.jackrabbit.oak.core.AbstractTree;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.core.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.commit.PostValidationHook;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
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
 * access control content and updates persisted permission store associated
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

    private static List<String> getAceNames(NodeState aclState) {
        PropertyState ordering = checkNotNull(aclState.getProperty(AbstractTree.OAK_CHILD_ORDER));
        return Lists.newArrayList(ordering.getValue(Type.STRINGS));
    }

    private static String getAccessControlledPath(Node acl) {
        return (REP_REPO_POLICY.equals(acl.getName()) ? "" : Text.getRelativeParent(acl.getPath(), 1));
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
                int index = 0;
                for (String aceName : getAceNames(after)) {
                    Tree child = getTree(aceName, after.getChildNode(aceName));
                    if (isACE(child)) {
                        PermissionEntry entry = createPermissionEntry(child, index, new AfterNode(parentAfter, name));
                        entry.add();
                    }
                    index++;
                }
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
                List<AcEntry> entriesBefore = createEntries(new BeforeNode(parentBefore, name, before));
                List<AcEntry> entriesAfter = createEntries(new AfterNode(parentAfter, name));

                for (int indexBefore = 0; indexBefore < entriesBefore.size(); indexBefore++) {
                    AcEntry ace = entriesBefore.get(indexBefore);
                    if (entriesAfter.isEmpty() || !entriesAfter.contains(ace)) {
                        new PermissionEntry(ace, indexBefore).remove();
                    }
                }

                List<PermissionEntry> toRemove = new ArrayList<PermissionEntry>();
                List<PermissionEntry> toAdd = new ArrayList<PermissionEntry>();
                for (int indexAfter = 0; indexAfter < entriesAfter.size(); indexAfter++) {
                    AcEntry ace = entriesAfter.get(indexAfter);
                    int indexBefore = entriesBefore.indexOf(ace);
                    if (indexBefore == -1) {
                        toAdd.add(new PermissionEntry(ace, indexAfter));
                    } else if (indexBefore != indexAfter) {
                        toRemove.add(new PermissionEntry(entriesBefore.get(indexBefore), indexBefore));
                        toAdd.add(new PermissionEntry(ace, indexAfter));
                    } // else: nothing to do
                }
                for (PermissionEntry pe : toRemove) {
                    pe.remove();
                }
                for (PermissionEntry pe : toAdd) {
                    pe.add();
                }
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
                int index = 0;
                for (String aceName : getAceNames(before)) {
                    Tree child = getTree(aceName, before.getChildNode(aceName));
                    if (isACE(child)) {
                        PermissionEntry entry = createPermissionEntry(child, index,
                                new BeforeNode(parentBefore, name, before));
                        entry.remove();
                    }
                    index++;
                }
            } else {
                BeforeNode nodeBefore = new BeforeNode(parentBefore.getPath(), name, before);
                AfterNode after = new AfterNode(parentAfter.getPath(), name, EMPTY_NODE);
                after.getNodeState().compareAgainstBaseState(before, new Diff(nodeBefore, after));
            }
            return true;
        }

        //--------------------------------------------------------< private >---

        private boolean isACL(@Nonnull String name, @Nonnull NodeState nodeState) {
            return ntMgr.isNodeType(getTree(name, nodeState), NT_REP_ACL);
        }

        private boolean isACE(@Nonnull Tree tree) {
            return ntMgr.isNodeType(tree, NT_REP_ACE);
        }

        @Nonnull
        private PermissionEntry createPermissionEntry(@Nonnull Tree ace,
                                                      int index,
                                                      @Nonnull Node acl) {
            String accessControlledPath = getAccessControlledPath(acl);
            return new PermissionEntry(ace, accessControlledPath, index);
        }

        @Nonnull
        private List<AcEntry> createEntries(Node acl) {
            List<AcEntry> acEntries = new ArrayList<AcEntry>();
            NodeState aclState = acl.getNodeState();
            for (String aceName : getAceNames(aclState)) {
                Tree child = getTree(aceName, aclState.getChildNode(aceName));
                if (isACE(child)) {
                    acEntries.add(new AcEntry(child, getAccessControlledPath(acl)));
                }
            }
            return acEntries;
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

    private class AcEntry {

        private final String accessControlledPath;
        private final String principalName;
        private final PrivilegeBits privilegeBits;
        private final boolean isAllow;
        private final Set<Restriction> restrictions;

        private int hashCode = -1;

        private AcEntry(@Nonnull Tree aceTree, @Nonnull String accessControlledPath) {
            this.accessControlledPath = accessControlledPath;
            principalName = Text.escapeIllegalJcrChars(checkNotNull(TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME)));
            privilegeBits = bitsProvider.getBits(TreeUtil.getStrings(aceTree, REP_PRIVILEGES));
            isAllow = NT_REP_GRANT_ACE.equals(TreeUtil.getPrimaryTypeName(aceTree));
            restrictions = restrictionProvider.readRestrictions(Strings.emptyToNull(accessControlledPath), aceTree);
        }

        @Override
        public int hashCode() {
            if (hashCode == -1) {
                hashCode = Objects.hashCode(accessControlledPath, principalName, privilegeBits, isAllow, restrictions);
            }
            return hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof AcEntry) {
                AcEntry other = (AcEntry) o;
                return isAllow == other.isAllow
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
            sb.append(accessControlledPath);
            sb.append(';').append(principalName);
            sb.append(';').append(isAllow ? "allow" : "deny");
            sb.append(';').append(bitsProvider.getPrivilegeNames(privilegeBits));
            sb.append(';').append(restrictions);
            return sb.toString();
        }
    }

    private final class PermissionEntry {

        private final AcEntry ace;
        private final long index;
        private final String nodeName;

        private int hashCode = -1;

        private PermissionEntry(@Nonnull Tree aceTree, @Nonnull String accessControlledPath, long index) {
            this(new AcEntry(aceTree, accessControlledPath), index);
        }

        private PermissionEntry(@Nonnull AcEntry ace, long index) {
            this.ace = ace;
            this.index = index;

            // create node name from ace definition
            nodeName = PermissionUtil.getEntryName(ace.accessControlledPath);
        }

        private void add() {
            NodeBuilder principalRoot = permissionRoot.child(ace.principalName);
            if (!principalRoot.hasProperty(JCR_PRIMARYTYPE)) {
                principalRoot.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
            }
            NodeBuilder parent = getParent(principalRoot);
            // create the new entry using a unique name to avoid overwriting
            // entries with the same access controlled path
            String tmpName = String.valueOf(hashCode());
            NodeBuilder toAdd = parent.child(tmpName)
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSIONS, Type.NAME)
                    .setProperty(REP_ACCESS_CONTROLLED_PATH, ace.accessControlledPath)
                    .setProperty(REP_IS_ALLOW, ace.isAllow)
                    .setProperty(REP_INDEX, index)
                    .setProperty(ace.privilegeBits.asPropertyState(REP_PRIVILEGE_BITS));
            for (Restriction restriction : ace.restrictions) {
                toAdd.setProperty(restriction.getProperty());
            }

            // reconnect existing entries for that path
            if (parent.hasChildNode(nodeName)) {
                NodeBuilder child = parent.getChildNode(nodeName);
                toAdd.setChildNode(nodeName, child.getNodeState());
                child.remove();
            }

            // finally connect 'toAdd' to the parent using the correct name
            parent.setChildNode(nodeName, toAdd.getNodeState());
            toAdd.remove();
        }

        private void remove() {
            String msg = "Unable to remove permission entry";
            if (permissionRoot.hasChildNode(ace.principalName)) {
                NodeBuilder principalRoot = permissionRoot.getChildNode(ace.principalName);
                NodeBuilder parent = getParent(principalRoot);
                if (parent.hasChildNode(nodeName)) {
                    NodeBuilder target = parent.getChildNode(nodeName);
                    // reconnect children
                    for (String childName : target.getChildNodeNames()) {
                        NodeBuilder child = target.getChildNode(childName);
                        parent.setChildNode(childName, child.getNodeState());
                        child.remove();
                    }
                    // remove the target node
                    target.remove();
                } else {
                    log.error("{} {}. No corresponding node found in permission store.", msg, this);
                }
            } else {
                log.error("{} {}: Principal root missing.", msg, this);
            }
        }

        private NodeBuilder getParent(NodeBuilder principalRoot) {
            NodeBuilder parent = principalRoot;
            while (parent.hasChildNode(nodeName)) {
                NodeBuilder child = parent.getChildNode(nodeName);
                long childIndex = child.getProperty(REP_INDEX).getValue(Type.LONG);
                if (index < childIndex) {
                    parent = child;
                } else {
                    break;
                }
            }
            return parent;
        }

        @Override
        public int hashCode() {
            if (hashCode == -1) {
                hashCode = Objects.hashCode(ace, index);
            }
            return hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof PermissionEntry) {
                PermissionEntry other = (PermissionEntry) o;
                return index == other.index && ace.equals(other.ace);
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("permission entry: ").append(ace.toString()).append('-').append(index);
            return sb.toString();
        }
    }
}

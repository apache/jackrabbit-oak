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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.base.Strings;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.commit.PostValidationHook;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.core.AbstractTree.OAK_CHILD_ORDER;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * {@code CommitHook} implementation that processes any modification made to
 * access control content and updates persisted permission store associated
 * with access control related data stored in the repository.
 * <p>
 * The access control entries are grouped by principal and store below the store root based on the hash value of the
 * access controllable path. hash collisions are handled by adding subnodes accordingly.
 * <pre>
 *   /jcr:system/rep:permissionStore/crx.default
 *      /everyone
 *          /552423  [rep:PermissionStore]
 *              /0     [rep:Permissions]
 *              /1     [rep:Permissions]
 *              /c0     [rep:PermissionStore]
 *                  /0      [rep:Permissions]
 *                  /1      [rep:Permissions]
 *                  /2      [rep:Permissions]
 *              /c1     [rep:PermissionStore]
 *                  /0      [rep:Permissions]
 *                  /1      [rep:Permissions]
 *                  /2      [rep:Permissions]
 * </pre>
 */
public class PermissionHook implements PostValidationHook, AccessControlConstants, PermissionConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionHook.class);

    private final RestrictionProvider restrictionProvider;
    private final String workspaceName;
    private final PermissionEntryCache cache;

    private NodeBuilder permissionRoot;
    private PrivilegeBitsProvider bitsProvider;

    private TypePredicate isACL;
    private TypePredicate isACE;
    private TypePredicate isGrantACE;

    private Map<String, Acl> modified = new HashMap<String, Acl>();
    private Map<String, Acl> deleted = new HashMap<String, Acl>();

    public PermissionHook(String workspaceName, RestrictionProvider restrictionProvider, PermissionEntryCache cache) {
        this.workspaceName = workspaceName;
        this.restrictionProvider = restrictionProvider;
        this.cache = cache;
    }

    @Nonnull
    @Override
    public NodeState processCommit(final NodeState before, NodeState after) throws CommitFailedException {
        NodeBuilder rootAfter = after.builder();

        permissionRoot = getPermissionRoot(rootAfter);
        bitsProvider = new PrivilegeBitsProvider(new ImmutableRoot(before));

        isACL = new TypePredicate(after, NT_REP_ACL);
        isACE = new TypePredicate(after, NT_REP_ACE);
        isGrantACE = new TypePredicate(after, NT_REP_GRANT_ACE);

        Diff diff = new Diff("");
        after.compareAgainstBaseState(before, diff);
        apply();
        return rootAfter.getNodeState();
    }

    private void apply() {
        Set<String> principalNames = new HashSet<String>();
        for (Map.Entry<String, Acl> entry : deleted.entrySet()) {
            entry.getValue().remove(principalNames);
        }
        for (Map.Entry<String, Acl> entry : modified.entrySet()) {
            entry.getValue().update(principalNames);
        }
        cache.flush(principalNames);
    }

    @Nonnull
    private NodeBuilder getPermissionRoot(NodeBuilder rootBuilder) {
        // permission root has been created during workspace initialization
        return rootBuilder.getChildNode(JCR_SYSTEM).getChildNode(REP_PERMISSION_STORE).getChildNode(workspaceName);
    }

    private class Diff extends DefaultNodeStateDiff {

        private final String parentPath;

        private Diff(String parentPath) {
            this.parentPath = parentPath;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
                return true;
            }
            String path = parentPath + '/' + name;
            if (isACL.apply(after)) {
                Acl acl = new Acl(parentPath, name, new AfterNode(path, after));
                modified.put(acl.accessControlledPath, acl);
            } else {
                after.compareAgainstBaseState(EMPTY_NODE, new Diff(path));
            }
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
                return true;
            }
            String path = parentPath + '/' + name;
            if (isACL.apply(before)) {
                if (isACL.apply(after)) {
                    Acl acl = new Acl(parentPath, name, new AfterNode(path, after));
                    modified.put(acl.accessControlledPath, acl);

                    // also consider to remove the ACL from removed entries of other principals
                    Acl beforeAcl = new Acl(parentPath, name, new BeforeNode(path, before));
                    beforeAcl.entries.keySet().removeAll(acl.entries.keySet());
                    if (!beforeAcl.entries.isEmpty()) {
                        deleted.put(parentPath, beforeAcl);
                    }

                } else {
                    Acl acl = new Acl(parentPath, name, new BeforeNode(path, before));
                    deleted.put(acl.accessControlledPath, acl);
                }
            } else if (isACL.apply(after)) {
                Acl acl = new Acl(parentPath, name, new AfterNode(path, after));
                modified.put(acl.accessControlledPath, acl);
            } else {
                after.compareAgainstBaseState(before, new Diff(path));
            }
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            if (NodeStateUtils.isHidden(name)) {
                // ignore hidden nodes
                return true;
            }
            String path = parentPath + '/' + name;
            if (isACL.apply(before)) {
                Acl acl = new Acl(parentPath, name, new BeforeNode(path, before));
                deleted.put(acl.accessControlledPath, acl);
            } else {
                EMPTY_NODE.compareAgainstBaseState(before, new Diff(path));
            }
            return true;
        }
    }

    private abstract static class Node {

        private final String path;

        private Node(String path) {
            this.path = path;
        }

        String getName() {
            return Text.getName(path);
        }

        abstract NodeState getNodeState();
    }

    private static final class BeforeNode extends Node {

        private final NodeState nodeState;

        BeforeNode(String parentPath, NodeState nodeState) {
            super(parentPath);
            this.nodeState = nodeState;
        }

        @Override
        NodeState getNodeState() {
            return nodeState;
        }
    }

    private static final class AfterNode extends Node {

        private final NodeBuilder builder;

        private AfterNode(String path, NodeState state) {
            super(path);
            this.builder = state.builder();
        }

        @Override
        NodeState getNodeState() {
            return builder.getNodeState();
        }
    }

    private final class Acl {

        private final String accessControlledPath;

        private final String nodeName;

        private final Map<String, List<AcEntry>> entries = new HashMap<String, List<AcEntry>>();

        private Acl(String aclPath, String name, @Nonnull Node node) {
            if (name.equals(REP_REPO_POLICY)) {
                this.accessControlledPath = "";
            } else {
                this.accessControlledPath = aclPath.length() == 0 ? "/" : aclPath;
            }
            nodeName = PermissionUtil.getEntryName(accessControlledPath);

            NodeState acl = node.getNodeState();
            Set<String> orderedChildNames =
                    newLinkedHashSet(acl.getNames(OAK_CHILD_ORDER));
            long n = orderedChildNames.size();
            if (acl.getChildNodeCount(n + 1) > n) {
                addAll(orderedChildNames, acl.getChildNodeNames());
            }

            int index = 0;
            for (String childName : orderedChildNames) {
                NodeState ace = acl.getChildNode(childName);
                if (isACE.apply(ace)) {
                    AcEntry entry = new AcEntry(new ImmutableTree(ace), accessControlledPath, index);
                    List<AcEntry> list = entries.get(entry.principalName);
                    if (list == null) {
                        list = new ArrayList<AcEntry>();
                        entries.put(entry.principalName, list);
                    }
                    list.add(entry);
                    index++;
                }
            }
        }

        private void remove(Set<String> principalNames) {
            String msg = "Unable to remove permission entry";
            for (String principalName: entries.keySet()) {
                if (permissionRoot.hasChildNode(principalName)) {
                    principalNames.add(principalName);
                    NodeBuilder principalRoot = permissionRoot.getChildNode(principalName);

                    // find the ACL node that for this path and principal
                    NodeBuilder parent = principalRoot.getChildNode(nodeName);
                    if (!parent.exists()) {
                        continue;
                    }

                    long numEntries = PermissionUtil.getNumPermissions(principalRoot);

                    // check if the node is the correct one
                    if (PermissionUtil.checkACLPath(parent, accessControlledPath)) {
                        // remove and reconnect child nodes
                        NodeBuilder newParent = null;
                        for (String childName : parent.getChildNodeNames()) {
                            if (childName.charAt(0) != 'c') {
                                numEntries--;
                                continue;
                            }
                            NodeBuilder child = parent.getChildNode(childName);
                            if (newParent == null) {
                                newParent = child;
                            } else {
                                newParent.setChildNode(childName, child.getNodeState());
                                child.remove();
                            }
                        }
                        parent.remove();
                        if (newParent != null) {
                            principalRoot.setChildNode(nodeName, newParent.getNodeState());
                        }
                    } else {
                        // check if any of the child nodes match
                        for (String childName : parent.getChildNodeNames()) {
                            if (childName.charAt(0) != 'c') {
                                continue;
                            }
                            NodeBuilder child = parent.getChildNode(childName);
                            if (PermissionUtil.checkACLPath(child, accessControlledPath)) {
                                // remove child
                                for (String n: child.getChildNodeNames()) {
                                    numEntries--;
                                }
                                child.remove();
                            }
                        }
                    }
                    principalRoot.setProperty(REP_NUM_PERMISSIONS, numEntries);
                    principalRoot.setProperty(REP_TIMESTAMP, System.currentTimeMillis());
                } else {
                    log.error("{} {}: Principal root missing.", msg, this);
                }
            }
        }

        private void update(Set<String> principalNames) {
            for (String principalName: entries.keySet()) {
                principalNames.add(principalName);
                NodeBuilder principalRoot = permissionRoot.child(principalName);
                if (!principalRoot.hasProperty(JCR_PRIMARYTYPE)) {
                    principalRoot.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
                }
                NodeBuilder parent = principalRoot.child(nodeName);
                if (!parent.hasProperty(JCR_PRIMARYTYPE)) {
                    parent.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
                }

                // check if current parent already has the correct path
                if (parent.hasProperty(REP_ACCESS_CONTROLLED_PATH)) {
                    if (!PermissionUtil.checkACLPath(parent, accessControlledPath)) {
                        // hash collision, find a new child
                        NodeBuilder child = null;
                        int idx = 0;
                        for (String childName : parent.getChildNodeNames()) {
                            if (childName.charAt(0) != 'c') {
                                continue;
                            }
                            child = parent.getChildNode(childName);
                            if (PermissionUtil.checkACLPath(child, accessControlledPath)) {
                                break;
                            }
                            child = null;
                            idx++;
                        }
                        while (child == null) {
                            String name = 'c' + String.valueOf(idx++);
                            child = parent.getChildNode(name);
                            if (child.exists()) {
                                child = null;
                            } else {
                                child = parent.child(name);
                                child.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE, Type.NAME);
                            }
                        }
                        parent = child;
                        parent.setProperty(REP_ACCESS_CONTROLLED_PATH, accessControlledPath);
                    }
                } else {
                    // new parent
                    parent.setProperty(REP_ACCESS_CONTROLLED_PATH, accessControlledPath);
                }
                long numEntries = PermissionUtil.getNumPermissions(principalRoot);
                numEntries+= updateEntries(parent, entries.get(principalName));
                principalRoot.setProperty(REP_NUM_PERMISSIONS, numEntries);
                principalRoot.setProperty(REP_TIMESTAMP, System.currentTimeMillis());
            }
        }

        private long updateEntries(NodeBuilder parent, List<AcEntry> list) {
            // remove old entries
            long numEntries = 0;
            for (String childName : parent.getChildNodeNames()) {
                if (childName.charAt(0) != 'c') {
                    parent.getChildNode(childName).remove();
                    numEntries--;
                }
            }
            for (AcEntry ace: list) {
                PermissionEntry.write(parent, ace.isAllow, ace.index, ace.privilegeBits, ace.restrictions);
                numEntries++;
            }
            return numEntries;
        }
    }

    private final class AcEntry {

        private final String accessControlledPath;
        private final String principalName;
        private final PrivilegeBits privilegeBits;
        private final boolean isAllow;
        private final Set<Restriction> restrictions;
        private final int index;
        private int hashCode = -1;

        private AcEntry(@Nonnull ImmutableTree aceTree, @Nonnull String accessControlledPath, int index) {
            this.accessControlledPath = accessControlledPath;
            this.index = index;

            principalName = Text.escapeIllegalJcrChars(checkNotNull(TreeUtil.getString(aceTree, REP_PRINCIPAL_NAME)));
            privilegeBits = bitsProvider.getBits(TreeUtil.getStrings(aceTree, REP_PRIVILEGES));
            isAllow = isGrantACE.apply(aceTree.getNodeState());
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
}

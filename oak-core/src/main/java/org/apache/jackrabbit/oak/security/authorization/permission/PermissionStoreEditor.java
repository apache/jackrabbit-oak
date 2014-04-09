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
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.tree.ImmutableTree;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;

final class PermissionStoreEditor implements AccessControlConstants, PermissionConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionStoreEditor.class);

    final String accessControlledPath;
    final String nodeName;
    final Map<String, List<AcEntry>> entries = Maps.<String, List<AcEntry>>newHashMap();

    private final NodeBuilder permissionRoot;
    private final TypePredicate isACE;
    private final RestrictionProvider restrictionProvider;

    PermissionStoreEditor(@Nonnull String aclPath, @Nonnull String name,
                          @Nonnull NodeState node, @Nonnull NodeBuilder permissionRoot,
                          @Nonnull TypePredicate isACE, @Nonnull TypePredicate isGrantACE,
                          @Nonnull PrivilegeBitsProvider bitsProvider,
                          @Nonnull RestrictionProvider restrictionProvider) {
        this.permissionRoot = permissionRoot;
        this.isACE = isACE;
        this.restrictionProvider = restrictionProvider;

        if (name.equals(REP_REPO_POLICY)) {
            accessControlledPath = "";
        } else {
            accessControlledPath = aclPath.length() == 0 ? "/" : aclPath;
        }
        nodeName = PermissionUtil.getEntryName(accessControlledPath);

        Set<String> orderedChildNames = newLinkedHashSet(node.getNames(OAK_CHILD_ORDER));
        long n = orderedChildNames.size();
        if (node.getChildNodeCount(n + 1) > n) {
            addAll(orderedChildNames, node.getChildNodeNames());
        }

        int index = 0;
        for (String childName : orderedChildNames) {
            NodeState ace = node.getChildNode(childName);
            if (isACE.apply(ace)) {
                boolean isAllow = isGrantACE.apply(ace);
                PrivilegeBits privilegeBits = bitsProvider.getBits(ace.getNames(REP_PRIVILEGES));
                Set<Restriction> restrictions = restrictionProvider.readRestrictions(Strings.emptyToNull(accessControlledPath), new ImmutableTree(ace));


                AcEntry entry = new AcEntry(ace, accessControlledPath, index, isAllow, privilegeBits, restrictions);
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

    void removePermissionEntry(@Nonnull String principalName, @Nonnull PermissionEntry permissionEntry) {
        if (permissionRoot.hasChildNode(principalName)) {
            NodeBuilder principalRoot = permissionRoot.getChildNode(principalName);

            // find the ACL node that for this path and principal
            NodeBuilder parent = principalRoot.getChildNode(nodeName);
            if (!parent.exists()) {
                log.error("Unable to remove permission entry {}: Parent for node " + nodeName + " missing.", this);
                return;
            }

            // check if the node is the correct one
            if (!PermissionUtil.checkACLPath(parent, accessControlledPath)) {
                parent = null;
                // find the right collision node
                for (String childName : parent.getChildNodeNames()) {
                    if (childName.charAt(0) != 'c') {
                        continue;
                    }
                    NodeBuilder child = parent.getChildNode(childName);
                    if (PermissionUtil.checkACLPath(child, accessControlledPath)) {
                        parent = child;
                        break;
                    }
                }
                if (parent == null) {
                    log.error("Unable to remove permission entry {}: Parent for node " + nodeName + " missing.", this);
                    return;
                }
            }

            for (String childName : parent.getChildNodeNames()) {
                if (childName.charAt(0) == 'c') {
                    continue;
                }

                NodeBuilder entryNode = parent.getChildNode(childName);
                if (permissionEntry.equals(new PermissionEntry(accessControlledPath, new ImmutableTree(entryNode.getNodeState()), restrictionProvider))) {
                    entryNode.remove();
                }
            }
        } else {
            log.error("Unable to remove permission entry {}: Principal root missing.", this);
        }
    }

    void removePermissionEntries() {
        for (String principalName : entries.keySet()) {
            if (permissionRoot.hasChildNode(principalName)) {
                NodeBuilder principalRoot = permissionRoot.getChildNode(principalName);

                // find the ACL node that for this path and principal
                NodeBuilder parent = principalRoot.getChildNode(nodeName);
                if (!parent.exists()) {
                    continue;
                }

                // check if the node is the correct one
                if (PermissionUtil.checkACLPath(parent, accessControlledPath)) {
                    // remove and reconnect child nodes
                    NodeBuilder newParent = null;
                    for (String childName : parent.getChildNodeNames()) {
                        if (childName.charAt(0) != 'c') {
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
                            child.remove();
                        }
                    }
                }
            } else {
                log.error("Unable to remove permission entry {}: Principal root missing.", this);
            }
        }
    }

    void updatePermissionEntries() {
        for (String principalName: entries.keySet()) {
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
            updateEntries(parent, entries.get(principalName));
        }
    }

    private void updateEntries(NodeBuilder parent, List<AcEntry> list) {
        // remove old entries
        for (String childName : parent.getChildNodeNames()) {
            if (childName.charAt(0) != 'c') {
                parent.getChildNode(childName).remove();
            }
        }
        for (AcEntry ace: list) {
            ace.writeToPermissionStore(parent);
        }
    }

    final class AcEntry {

        final String accessControlledPath;
        final String principalName;
        final PrivilegeBits privilegeBits;
        final boolean isAllow;
        final Set<Restriction> restrictions;
        final int index;
        int hashCode = -1;

        private TypePredicate isACE;

        AcEntry(@Nonnull NodeState node, @Nonnull String accessControlledPath, int index,
                boolean isAllow, PrivilegeBits privilegeBits, Set<Restriction> restrictions) {
            this.accessControlledPath = accessControlledPath;
            this.index = index;

            this.principalName = Text.escapeIllegalJcrChars(node.getString(REP_PRINCIPAL_NAME));
            this.privilegeBits = privilegeBits;
            this.isAllow = isAllow;
            this.restrictions = restrictions;
        }

        PermissionEntry asPermissionEntry() {
            return new PermissionEntry(accessControlledPath, isAllow, index, privilegeBits, restrictionProvider.getPattern(accessControlledPath, restrictions));
        }

        void writeToPermissionStore(NodeBuilder parent) {
            NodeBuilder n = parent.child(String.valueOf(index))
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSIONS, Type.NAME)
                    .setProperty(REP_IS_ALLOW, isAllow)
                    .setProperty(privilegeBits.asPropertyState(REP_PRIVILEGE_BITS));
            for (Restriction restriction : restrictions) {
                n.setProperty(restriction.getProperty());
            }
        }

        //-------------------------------------------------------------< Object >---
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
            sb.append(';').append(privilegeBits);
            sb.append(';').append(restrictions);
            return sb.toString();
        }
    }
}

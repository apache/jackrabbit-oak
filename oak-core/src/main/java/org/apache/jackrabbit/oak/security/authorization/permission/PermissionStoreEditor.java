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
import com.google.common.primitives.Longs;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
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

    private final String accessControlledPath;
    private final String nodeName;
    private final Map<String, List<AcEntry>> entries = Maps.newHashMap();
    private final NodeBuilder permissionRoot;

    PermissionStoreEditor(@Nonnull String aclPath, @Nonnull String name,
                          @Nonnull NodeState node, @Nonnull NodeBuilder permissionRoot,
                          @Nonnull TypePredicate isACE, @Nonnull TypePredicate isGrantACE,
                          @Nonnull PrivilegeBitsProvider bitsProvider,
                          @Nonnull RestrictionProvider restrictionProvider,
                          @Nonnull TreeProvider treeProvider) {
        this.permissionRoot = permissionRoot;
        if (name.equals(REP_REPO_POLICY)) {
            accessControlledPath = "";
        } else {
            accessControlledPath = aclPath.isEmpty() ? "/" : aclPath;
        }
        nodeName = PermissionUtil.getEntryName(accessControlledPath);

        Set<String> orderedChildNames = newLinkedHashSet(node.getNames(OAK_CHILD_ORDER));
        long n = orderedChildNames.size();
        if (node.getChildNodeCount(n + 1) > n) {
            addAll(orderedChildNames, node.getChildNodeNames());
        }

        PrivilegeBits jcrAll = bitsProvider.getBits(PrivilegeConstants.JCR_ALL);
        int index = 0;
        for (String childName : orderedChildNames) {
            NodeState ace = node.getChildNode(childName);
            if (isACE.apply(ace)) {
                boolean isAllow = isGrantACE.apply(ace);
                PrivilegeBits privilegeBits = bitsProvider.getBits(ace.getNames(REP_PRIVILEGES));
                Set<Restriction> restrictions = restrictionProvider.readRestrictions(Strings.emptyToNull(accessControlledPath), treeProvider.createReadOnlyTree(ace));

                AcEntry entry = (privilegeBits.equals(jcrAll)) ?
                        new JcrAllAcEntry(ace, accessControlledPath, index, isAllow, privilegeBits, restrictions) :
                        new AcEntry(ace, accessControlledPath, index, isAllow, privilegeBits, restrictions);
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

    String getPath() {
        return accessControlledPath;
    }

    boolean isEmpty() {
        return entries.isEmpty();
    }

    void removePermissionEntries(PermissionStoreEditor otherEditor) {
        entries.keySet().removeAll(otherEditor.entries.keySet());
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
        for (Map.Entry<String, List<AcEntry>> entry: entries.entrySet()) {
            String principalName = entry.getKey();
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
            updateEntries(parent, entry.getValue());
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

    private final class JcrAllAcEntry extends AcEntry {

        private JcrAllAcEntry(@Nonnull NodeState node,
                              @Nonnull String accessControlledPath,
                              int index, boolean isAllow,
                              @Nonnull PrivilegeBits privilegeBits,
                              @Nonnull Set<Restriction> restrictions) {
            super(node, accessControlledPath, index, isAllow, privilegeBits, restrictions);
        }

        @Override
        protected PropertyState getPrivilegeBitsProperty() {
            return PropertyStates.createProperty(REP_PRIVILEGE_BITS, Longs.asList(PermissionStore.DYNAMIC_ALL_BITS), Type.LONGS);
        }
    }

    private class AcEntry {

        private final String accessControlledPath;
        private final String principalName;
        private final PrivilegeBits privilegeBits;
        private final boolean isAllow;
        private final Set<Restriction> restrictions;
        private final int index;
        private int hashCode = -1;

        private AcEntry(@Nonnull NodeState node, @Nonnull String accessControlledPath, int index,
                        boolean isAllow, @Nonnull PrivilegeBits privilegeBits,
                        @Nonnull Set<Restriction> restrictions) {
            this.accessControlledPath = accessControlledPath;
            this.index = index;

            this.principalName = Text.escapeIllegalJcrChars(node.getString(REP_PRINCIPAL_NAME));
            this.privilegeBits = privilegeBits;
            this.isAllow = isAllow;
            this.restrictions = restrictions;
        }

        private void writeToPermissionStore(NodeBuilder parent) {
            NodeBuilder n = parent.child(String.valueOf(index))
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSIONS, Type.NAME)
                    .setProperty(REP_IS_ALLOW, isAllow)
                    .setProperty(getPrivilegeBitsProperty());
            for (Restriction restriction : restrictions) {
                n.setProperty(restriction.getProperty());
            }
        }

        protected PropertyState getPrivilegeBitsProperty() {
            return privilegeBits.asPropertyState(REP_PRIVILEGE_BITS);
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

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
import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.security.authorization.accesscontrol.ValidationEntry;
import org.apache.jackrabbit.oak.security.authorization.monitor.AuthorizationMonitor;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.JcrAllUtil;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.guava.common.collect.Iterables.addAll;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;

final class PermissionStoreEditor implements AccessControlConstants, PermissionConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionStoreEditor.class);

    private final String accessControlledPath;
    private final String nodeName;
    private final Map<String, List<AcEntry>> entries = Maps.newHashMap();
    private final NodeBuilder permissionRoot;
    private final PrivilegeBitsProvider bitsProvider;
    private final AuthorizationMonitor monitor;

    PermissionStoreEditor(@NotNull String aclPath, @NotNull String name,
                          @NotNull NodeState node, @NotNull NodeBuilder permissionRoot,
                          @NotNull TypePredicate isACE, @NotNull TypePredicate isGrantACE,
                          @NotNull PrivilegeBitsProvider bitsProvider,
                          @NotNull RestrictionProvider restrictionProvider,
                          @NotNull ProviderCtx providerCtx) {
        this.permissionRoot = permissionRoot;
        this.bitsProvider = bitsProvider;
        this.monitor = providerCtx.getMonitor();
        if (name.equals(REP_REPO_POLICY)) {
            accessControlledPath = "";
        } else {
            accessControlledPath = aclPath.isEmpty() ? "/" : aclPath;
        }
        nodeName = PermissionUtil.getEntryName(accessControlledPath);

        Set<String> orderedChildNames = CollectionUtils.toLinkedSet(node.getNames(OAK_CHILD_ORDER));
        long n = orderedChildNames.size();
        if (node.getChildNodeCount(n + 1) > n) {
            addAll(orderedChildNames, node.getChildNodeNames());
        }

        int index = 0;
        for (String childName : orderedChildNames) {
            NodeState ace = node.getChildNode(childName);
            if (isACE.test(ace)) {
                boolean isAllow = isGrantACE.test(ace);
                PrivilegeBits privilegeBits = bitsProvider.getBits(ace.getNames(REP_PRIVILEGES));
                Set<Restriction> restrictions = restrictionProvider.readRestrictions(Strings.emptyToNull(accessControlledPath), providerCtx.getTreeProvider().createReadOnlyTree(ace));

                String principalName = Text.escapeIllegalJcrChars(ace.getString(REP_PRINCIPAL_NAME));
                AcEntry entry = new AcEntry(principalName, index, isAllow, privilegeBits, restrictions);
                List<AcEntry> list = entries.computeIfAbsent(principalName, k -> new ArrayList<>());
                list.add(entry);
                index++;
            }
        }
    }

    @NotNull
    String getPath() {
        return accessControlledPath;
    }

    boolean isEmpty() {
        return entries.isEmpty();
    }

    void removePermissionEntries(@NotNull PermissionStoreEditor otherEditor) {
        entries.keySet().removeAll(otherEditor.entries.keySet());
    }

    void removePermissionEntries() {
        for (String principalName : entries.keySet()) {
            if (permissionRoot.hasChildNode(principalName)) {
                NodeBuilder principalRoot = permissionRoot.getChildNode(principalName);
                boolean removed = false;

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
                        }
                    }

                    if (newParent != null) {
                        // replace the 'parent', which got removed
                        principalRoot.setChildNode(nodeName, newParent.getNodeState());
                        removed = true;
                    } else {
                        removed = parent.remove();
                    }
                } else {
                    // check if any of the child nodes match
                    for (String childName : parent.getChildNodeNames()) {
                        if (childName.charAt(0) != 'c') {
                            continue;
                        }
                        NodeBuilder child = parent.getChildNode(childName);
                        if (PermissionUtil.checkACLPath(child, accessControlledPath)) {
                            removed = child.remove();
                        }
                    }
                }
                if (removed) {
                    updateNumEntries(principalName, principalRoot, -1, monitor);
                }
            } else {
                monitor.permissionError();
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

            if (parent.isNew()) {
                updateNumEntries(principalName, principalRoot, +1, monitor);
            }
        }
    }

    private void updateEntries(@NotNull NodeBuilder parent, @NotNull List<AcEntry> list) {
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

    private static void updateNumEntries(@NotNull String principalName, @NotNull NodeBuilder principalRoot, int cnt, @NotNull AuthorizationMonitor monitor) {
        PropertyState ps = principalRoot.getProperty(REP_NUM_PERMISSIONS);
        if (ps == null && !principalRoot.isNew()) {
            // existing principal root that doesn't have the rep:numEntries set
            return;
        }

        long numEntries = ((ps == null) ? 0 : ps.getValue(Type.LONG)) + cnt;
        if  (numEntries < 0) {
            // numEntries unexpectedly turned negative
            monitor.permissionError();
            log.error("NumEntries counter for principal '{}' turned negative -> removing 'rep:numPermissions' property.", principalName);
            principalRoot.removeProperty(REP_NUM_PERMISSIONS);
        } else {
            principalRoot.setProperty(REP_NUM_PERMISSIONS, numEntries, Type.LONG);
        }
    }

    private class AcEntry extends ValidationEntry {

        AcEntry(@NotNull String principalName, int index,
                boolean isAllow, @NotNull PrivilegeBits privilegeBits,
                @NotNull Set<Restriction> restrictions) {
            super(principalName, privilegeBits, isAllow, restrictions, index);
        }

        private void writeToPermissionStore(@NotNull NodeBuilder parent) {
            NodeBuilder n = parent.child(String.valueOf(index))
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSIONS, Type.NAME)
                    .setProperty(REP_IS_ALLOW, isAllow)
                    .setProperty(getPrivilegeBitsProperty());
            for (Restriction restriction : restrictions) {
                n.setProperty(restriction.getProperty());
            }
        }

        @NotNull
        private PropertyState getPrivilegeBitsProperty() {
            return JcrAllUtil.asPropertyState(REP_PRIVILEGE_BITS, privilegeBits, bitsProvider);
        }
    }
}

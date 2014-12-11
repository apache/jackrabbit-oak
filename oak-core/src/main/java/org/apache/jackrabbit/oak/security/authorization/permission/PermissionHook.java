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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.PostValidationHook;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * {@code CommitHook} implementation that processes any modification made to
 * access control content and updates persisted permission store associated
 * with access control related data stored in the repository.
 * <p>
 * The access control entries are grouped by principal and stored below the store root based on the hash value of the
 * access controllable path. hash collisions are handled by adding subnodes accordingly.
 * <pre>
 *   /jcr:system/rep:permissionStore/workspace-name
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

    private NodeBuilder permissionRoot;
    private PrivilegeBitsProvider bitsProvider;

    private TypePredicate isACL;
    private TypePredicate isACE;
    private TypePredicate isGrantACE;

    private Map<String, PermissionStoreEditor> modified = new HashMap<String, PermissionStoreEditor>();
    private Map<String, PermissionStoreEditor> deleted = new HashMap<String, PermissionStoreEditor>();

    public PermissionHook(String workspaceName, RestrictionProvider restrictionProvider) {
        this.workspaceName = workspaceName;
        this.restrictionProvider = restrictionProvider;
    }

    @Nonnull
    @Override
    public NodeState processCommit(
            NodeState before, NodeState after, CommitInfo info)
            throws CommitFailedException {
        NodeBuilder rootAfter = after.builder();

        permissionRoot = getPermissionRoot(rootAfter);
        bitsProvider = new PrivilegeBitsProvider(new ImmutableRoot(after));

        isACL = new TypePredicate(after, NT_REP_ACL);
        isACE = new TypePredicate(after, NT_REP_ACE);
        isGrantACE = new TypePredicate(after, NT_REP_GRANT_ACE);

        Diff diff = new Diff("");
        after.compareAgainstBaseState(before, diff);
        apply();
        return rootAfter.getNodeState();
    }

    private void apply() {
        for (Map.Entry<String, PermissionStoreEditor> entry : deleted.entrySet()) {
            entry.getValue().removePermissionEntries();
        }
        for (Map.Entry<String, PermissionStoreEditor> entry : modified.entrySet()) {
            entry.getValue().updatePermissionEntries();
        }
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
                PermissionStoreEditor psEditor = createPermissionStoreEditor(name, after);
                modified.put(psEditor.accessControlledPath, psEditor);
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
                    PermissionStoreEditor psEditor = createPermissionStoreEditor(name, after);
                    modified.put(psEditor.accessControlledPath, psEditor);

                    // also consider to remove the ACL from removed entries of other principals
                    PermissionStoreEditor beforeEditor = createPermissionStoreEditor(name, before);
                    beforeEditor.entries.keySet().removeAll(psEditor.entries.keySet());
                    if (!beforeEditor.entries.isEmpty()) {
                        deleted.put(parentPath, beforeEditor);
                    }

                } else {
                    PermissionStoreEditor psEditor = createPermissionStoreEditor(name, before);
                    deleted.put(psEditor.accessControlledPath, psEditor);
                }
            } else if (isACL.apply(after)) {
                PermissionStoreEditor psEditor = createPermissionStoreEditor(name, after);
                modified.put(psEditor.accessControlledPath, psEditor);
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
                PermissionStoreEditor psEditor = createPermissionStoreEditor(name, before);
                deleted.put(psEditor.accessControlledPath, psEditor);
            } else {
                EMPTY_NODE.compareAgainstBaseState(before, new Diff(path));
            }
            return true;
        }

        private PermissionStoreEditor createPermissionStoreEditor(@Nonnull String nodeName, @Nonnull NodeState nodeState) {
            return new PermissionStoreEditor(parentPath, nodeName, nodeState, permissionRoot, isACE, isGrantACE, bitsProvider, restrictionProvider);
        }
    }
}

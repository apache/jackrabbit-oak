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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.TreeImpl;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.util.TreeUtil;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Validator implementation that checks for sufficient permission for all
 * write operations executed by a given content session.
 */
class PermissionValidator extends DefaultValidator {

    /* TODO
     * - Renaming nodes or Move with same parent are reflected as remove+add -> needs special handling
     * - Proper handling of jcr:nodeTypeManagement privilege.
     */

    private final Tree parentBefore;
    private final Tree parentAfter;
    private final PermissionProvider permissionProvider;
    private final PermissionValidatorProvider provider;

    private final long permission;

    PermissionValidator(Tree parentBefore, Tree parentAfter,
                        PermissionProvider permissionProvider,
                        PermissionValidatorProvider provider) {
        this(parentBefore, parentAfter, permissionProvider, provider, Permissions.NO_PERMISSION);
    }

    PermissionValidator(Tree parentBefore, Tree parentAfter,
                        PermissionProvider permissionProvider,
                        PermissionValidatorProvider provider,
                        long permission) {
        this.permissionProvider = permissionProvider;
        this.provider = provider;
        this.parentBefore = parentBefore;
        this.parentAfter = parentAfter;
        if (Permissions.NO_PERMISSION == permission) {
            this.permission = Permissions.getPermission(getPath(parentBefore, parentAfter), Permissions.NO_PERMISSION);
        } else {
            this.permission = permission;
        }
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        checkPermissions(parentAfter, after, Permissions.ADD_PROPERTY);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        if (TreeImpl.OAK_CHILD_ORDER.equals(after.getName())) {
            checkPermissions(parentAfter, false, Permissions.MODIFY_CHILD_NODE_COLLECTION);
        } else {
            checkPermissions(parentAfter, after, Permissions.MODIFY_PROPERTY);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        checkPermissions(parentBefore, before, Permissions.REMOVE_PROPERTY);
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        Tree child = checkNotNull(parentAfter.getChild(name));
        if (isVersionstorageTree(child)) {
            child = getVersionHistoryTree(child);
            if (child == null) {
                throw new CommitFailedException("New version storage node without version history: cannot verify permissions.");
            }
        }
        return checkPermissions(child, false, Permissions.ADD_NODE);
    }


    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        Tree childBefore = parentBefore.getChild(name);
        Tree childAfter = parentAfter.getChild(name);

        // TODO

        return nextValidator(childBefore, childAfter);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        Tree child = checkNotNull(parentBefore.getChild(name));
        if (isVersionstorageTree(child)) {
            // TODO: check again
            throw new CommitFailedException("Attempt to remove versionstorage node: Fail to verify delete permission.");
        }
        return checkPermissions(child, true, Permissions.REMOVE_NODE);
    }

    //------------------------------------------------------------< private >---
    private Validator nextValidator(@Nullable Tree parentBefore, @Nullable Tree parentAfter) {
        return new PermissionValidator(parentBefore, parentAfter, permissionProvider, provider, permission);
    }

    private Validator checkPermissions(@Nonnull Tree tree, boolean isBefore,
                                       long defaultPermission) throws CommitFailedException {
        long toTest = getPermission(tree, defaultPermission);
        if (Permissions.isRepositoryPermission(toTest)) {
            if (!permissionProvider.isGranted(toTest)) {
                throw new CommitFailedException(new AccessDeniedException());
            }
            return null; // no need for further validation down the subtree
        } else {
            if (!permissionProvider.isGranted(tree, null, toTest)) {
                throw new CommitFailedException(new AccessDeniedException());
            }
            if (noTraverse(toTest)) {
                return null;
            } else {
                return (isBefore) ?
                    nextValidator(tree, null) :
                    nextValidator(null, tree);
            }
        }
    }

    private void checkPermissions(@Nonnull Tree parent, @Nonnull PropertyState property,
                                  long defaultPermission) throws CommitFailedException {
        if (!NodeStateUtils.isHidden((property.getName()))) {
            long toTest = getPermission(parent, property, defaultPermission);
            if (!permissionProvider.isGranted(parent, property, toTest)) {
                throw new CommitFailedException(new AccessDeniedException());
            }
        }
    }

    @CheckForNull
    private static String getPath(@Nullable Tree parentBefore, @Nullable Tree parentAfter) {
        String path = null;
        if (parentBefore != null) {
            path = parentBefore.getPath();
        } else if (parentAfter != null) {
            path = parentAfter.getPath();
        }
        return path;
    }

    public long getPermission(@Nonnull Tree tree, long defaultPermission) {
        if (permission != Permissions.NO_PERMISSION) {
            return permission;
        }
        long perm;
        if (provider.getAccessControlContext().definesTree(tree)) {
            perm = Permissions.MODIFY_ACCESS_CONTROL;
        } else if (provider.getUserContext().definesTree(tree)) {
            perm = Permissions.USER_MANAGEMENT;
        } else {
            // TODO: identify renaming/move of nodes that only required MODIFY_CHILD_NODE_COLLECTION permission
            perm = defaultPermission;
        }
        return perm;
    }

    public long getPermission(@Nonnull Tree parent, @Nonnull PropertyState propertyState, long defaultPermission) {
        if (permission != Permissions.NO_PERMISSION) {
            return permission;
        }
        String name = propertyState.getName();
        long perm;
        if (JcrConstants.JCR_PRIMARYTYPE.equals(name) || JcrConstants.JCR_MIXINTYPES.equals(name)) {
            // FIXME: distinguish between autocreated and user-supplied modification (?)
            perm = Permissions.NODE_TYPE_MANAGEMENT;
        } else if (isLockProperty(name)) {
            perm = Permissions.LOCK_MANAGEMENT;
        } else if (VersionConstants.VERSION_PROPERTY_NAMES.contains(name)) {
            perm = Permissions.VERSION_MANAGEMENT;
        } else if (provider.getAccessControlContext().definesProperty(parent, propertyState)) {
            perm = Permissions.MODIFY_ACCESS_CONTROL;
        } else if (provider.getUserContext().definesProperty(parent, propertyState)) {
            perm = Permissions.USER_MANAGEMENT;
        } else {
            perm = defaultPermission;
        }
        return perm;
    }

    private static boolean isLockProperty(String name) {
        return JcrConstants.JCR_LOCKISDEEP.equals(name) || JcrConstants.JCR_LOCKOWNER.equals(name);
    }

    // TODO
    public static boolean noTraverse(long permission) {
        return permission == Permissions.MODIFY_ACCESS_CONTROL ||
                permission == Permissions.VERSION_MANAGEMENT;
    }

    // TODO
    private boolean isVersionstorageTree(Tree tree) {
        return permission == Permissions.VERSION_MANAGEMENT &&
                VersionConstants.REP_VERSIONSTORAGE.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    // TODO
    private Tree getVersionHistoryTree(Tree versionstorageTree) throws CommitFailedException {
        Tree versionHistory = null;
        for (Tree child : versionstorageTree.getChildren()) {
            if (VersionConstants.NT_VERSIONHISTORY.equals(TreeUtil.getPrimaryTypeName(child))) {
                versionHistory = child;
            } else if (isVersionstorageTree(child)) {
                versionHistory = getVersionHistoryTree(child);
            } else {
                // TODO:
                throw new CommitFailedException("unexpected node");
            }
        }
        return versionHistory;
    }
}
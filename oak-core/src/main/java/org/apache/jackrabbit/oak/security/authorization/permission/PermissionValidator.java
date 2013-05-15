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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.TreeImpl;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.util.TreeUtil;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.CommitFailedException.ACCESS;

/**
 * Validator implementation that checks for sufficient permission for all
 * write operations executed by a given content session.
 */
class PermissionValidator extends DefaultValidator {

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
            String childName = new ChildOrderDiff(before, after).firstReordered();
            if (childName != null) {
                Tree child = parentAfter.getChild(childName);
                checkPermissions(child, false, Permissions.MODIFY_CHILD_NODE_COLLECTION);
            } // else: no re-order but only internal update
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
                throw new CommitFailedException(
                        ACCESS, 21, "New version storage node without version history: cannot verify permissions.");
            }
        }
        return checkPermissions(child, false, Permissions.ADD_NODE);
    }


    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        Tree childBefore = parentBefore.getChild(name);
        Tree childAfter = parentAfter.getChild(name);
        return nextValidator(childBefore, childAfter);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        Tree child = checkNotNull(parentBefore.getChild(name));
        if (isVersionstorageTree(child)) {
            throw new CommitFailedException(
                    ACCESS, 22, "Attempt to remove versionstorage node: Fail to verify delete permission.");
        }
        return checkPermissions(child, true, Permissions.REMOVE_NODE);
    }

    //------------------------------------------------------------< private >---
    private Validator nextValidator(@Nullable Tree parentBefore, @Nullable Tree parentAfter) {
        Validator validator = new PermissionValidator(parentBefore, parentAfter, permissionProvider, provider, permission);
        return new VisibleValidator(validator, true, false);
    }

    private Validator checkPermissions(@Nonnull Tree tree, boolean isBefore,
                                       long defaultPermission) throws CommitFailedException {
        long toTest = getPermission(tree, defaultPermission);
        if (Permissions.isRepositoryPermission(toTest)) {
            if (!permissionProvider.isGranted(toTest)) {
                throw new CommitFailedException(ACCESS, 0, "Access denied");
            }
            return null; // no need for further validation down the subtree
        } else {
            if (!permissionProvider.isGranted(tree, null, toTest)) {
                throw new CommitFailedException(ACCESS, 0, "Access denied");
            }
            if (noTraverse(toTest, defaultPermission)) {
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
        if (NodeStateUtils.isHidden(property.getName())) {
            // ignore any hidden properties (except for OAK_CHILD_ORDER which has
            // been covered in "propertyChanged"
            return;
        }
        long toTest = getPermission(parent, property, defaultPermission);
        if (Permissions.isRepositoryPermission(toTest)) {
            if (!permissionProvider.isGranted(toTest)) {
                throw new CommitFailedException(ACCESS, 0, "Access denied");
            }
        } else if (!permissionProvider.isGranted(parent, property, toTest)) {
            throw new CommitFailedException(ACCESS, 0, "Access denied");
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
        } else if (provider.getUserContext().definesTree(tree)
                && !provider.requiresJr2Permissions(Permissions.USER_MANAGEMENT)) {
            perm = Permissions.USER_MANAGEMENT;
        } else {
            // FIXME: OAK-710 (identify renaming/move of nodes that only required MODIFY_CHILD_NODE_COLLECTION permission)
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
        if (JcrConstants.JCR_PRIMARYTYPE.equals(name)) {
            if (defaultPermission == Permissions.MODIFY_PROPERTY) {
                perm = Permissions.NODE_TYPE_MANAGEMENT;
            } else {
                // can't determine if this was  a user supplied modification of
                // the primary type -> omit permission check.
                // Node#addNode(String, String) and related methods need to
                // perform the permission check (as it used to be in jackrabbit 2.x).
                perm = Permissions.NO_PERMISSION;
            }
        } else if (JcrConstants.JCR_MIXINTYPES.equals(name)) {
            perm = Permissions.NODE_TYPE_MANAGEMENT;
        } else if (JcrConstants.JCR_UUID.equals(name)) {
            if (provider.getNodeTypeManager().isNodeType(parent, JcrConstants.MIX_REFERENCEABLE)) {
                perm = Permissions.NO_PERMISSION;
            } else {
                /* the parent is not referenceable -> check regular permissions
                   as this instance of jcr:uuid is not the mandatory/protected
                   property defined by mix:referenceable */
                perm = defaultPermission;
            }
        } else if (isLockProperty(name)) {
            perm = Permissions.LOCK_MANAGEMENT;
        } else if (VersionConstants.VERSION_PROPERTY_NAMES.contains(name)) {
            perm = Permissions.VERSION_MANAGEMENT;
        } else if (provider.getAccessControlContext().definesProperty(parent, propertyState)) {
            perm = Permissions.MODIFY_ACCESS_CONTROL;
        } else if (provider.getUserContext().definesProperty(parent, propertyState)
                 && !provider.requiresJr2Permissions(Permissions.USER_MANAGEMENT)) {
            perm = Permissions.USER_MANAGEMENT;
        } else {
            perm = defaultPermission;
        }
        return perm;
    }

    private static boolean isLockProperty(String name) {
        return JcrConstants.JCR_LOCKISDEEP.equals(name) || JcrConstants.JCR_LOCKOWNER.equals(name);
    }

    public boolean noTraverse(long permission, long defaultPermission) {
        if (defaultPermission == Permissions.REMOVE_NODE && provider.requiresJr2Permissions(Permissions.REMOVE_NODE)) {
            return false;
        } else {
            return permission == Permissions.MODIFY_ACCESS_CONTROL ||
                    permission == Permissions.VERSION_MANAGEMENT ||
                    permission == Permissions.REMOVE_NODE ||
                    defaultPermission == Permissions.REMOVE_NODE;
        }
    }

    private boolean isVersionstorageTree(Tree tree) {
        return permission == Permissions.VERSION_MANAGEMENT &&
                VersionConstants.REP_VERSIONSTORAGE.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    private Tree getVersionHistoryTree(Tree versionstorageTree) throws CommitFailedException {
        Tree versionHistory = null;
        for (Tree child : versionstorageTree.getChildren()) {
            if (VersionConstants.NT_VERSIONHISTORY.equals(TreeUtil.getPrimaryTypeName(child))) {
                versionHistory = child;
            } else if (isVersionstorageTree(child)) {
                versionHistory = getVersionHistoryTree(child);
            } else {
                throw new CommitFailedException("Misc", 0, "unexpected node");
            }
        }
        return versionHistory;
    }
}

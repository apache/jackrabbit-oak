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
package org.apache.jackrabbit.oak.security.authorization;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * PermissionValidator... TODO
 */
class PermissionValidator implements Validator {

    /* TODO
     * - special permissions for protected items (versioning, access control, etc.)
     * - Renaming nodes or Move with same parent are reflected as remove+add -> needs special handling
     * - review usage of OAK_CHILD_ORDER property (in particular if the property was removed
     */

    private final Tree parentBefore;
    private final Tree parentAfter;
    private final PermissionProvider permissionProvider;
    private final PermissionValidatorProvider provider;

    private final long permission;

    PermissionValidator(Tree parentBefore, Tree parentAfter,
                        PermissionProvider permissionProvider,
                        PermissionValidatorProvider provider) {
        this(parentBefore, parentAfter, permissionProvider, provider,
                Permissions.getPermission(getPath(parentBefore, parentAfter), Permissions.NO_PERMISSION));
    }

    PermissionValidator(Tree parentBefore, Tree parentAfter,
                        PermissionProvider permissionProvider,
                        PermissionValidatorProvider provider,
                        long permission) {
        this.permissionProvider = permissionProvider;
        this.provider = provider;
        this.parentBefore = parentBefore;
        this.parentAfter = parentAfter;
        this.permission = permission;
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        checkPermissions(parentAfter, after, Permissions.ADD_PROPERTY);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        checkPermissions(parentAfter, after, Permissions.MODIFY_PROPERTY);
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        checkPermissions(parentBefore, before, Permissions.REMOVE_PROPERTY);
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        Tree child = parentAfter.getChild(name);
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
        Tree child = parentBefore.getChild(name);
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
            if (!permissionProvider.isGranted(tree, toTest)) {
                throw new CommitFailedException(new AccessDeniedException());
            }
            return (isBefore) ?
                    nextValidator(tree, null) :
                    nextValidator(null, tree);
        }
    }

    private void checkPermissions(@Nonnull Tree parent, @Nonnull PropertyState property,
                                  long defaultPermission) throws CommitFailedException {
        long toTest = getPermission(parent, property, defaultPermission);
        if (!permissionProvider.isGranted(parent, property, toTest)) {
            throw new CommitFailedException(new AccessDeniedException());
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
}
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

import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;

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

    PermissionValidator(Tree parentBefore, Tree parentAfter,
                        PermissionProvider permissionProvider,
                        PermissionValidatorProvider provider) {
        this.permissionProvider = permissionProvider;
        this.parentBefore = parentBefore;
        this.parentAfter = parentAfter;
        this.provider = provider;
    }

    private Validator nextValidator(@Nullable Tree parentBefore, @Nullable Tree parentAfter) {
        return new PermissionValidator(parentBefore, parentAfter, permissionProvider, provider);
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

    private Validator checkPermissions(Tree tree, boolean isBefore, long defaultPermission) throws CommitFailedException {
        checkNotNull(tree);

        long permission = permissionProvider.getPermission(tree, defaultPermission);
        if (Permissions.isRepositoryPermission(permission)) {
            if (!permissionProvider.isGranted(permission)) {
                throw new CommitFailedException(new AccessDeniedException());
            }
            return null; // no need for further validation down the subtree
        } else {
            if (!permissionProvider.isGranted(tree, permission)) {
                throw new CommitFailedException(new AccessDeniedException());
            }
            return (isBefore) ?
                    nextValidator(tree, null) :
                    nextValidator(null, tree);
        }
    }

    private void checkPermissions(Tree parent, PropertyState property, long defaultPermission) throws CommitFailedException {
        long permission = permissionProvider.getPermission(parent, property, defaultPermission);
        if (!permissionProvider.isGranted(parent, property, permission)) {
            throw new CommitFailedException(new AccessDeniedException());
        }
    }
}
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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.api.CommitFailedException.ACCESS;

public class MoveAwarePermissionValidator extends PermissionValidator {

    private final MoveContext moveCtx;

    MoveAwarePermissionValidator(@Nonnull NodeState rootBefore,
                                 @Nonnull NodeState rootAfter,
                                 @Nonnull PermissionProvider permissionProvider,
                                 @Nonnull PermissionValidatorProvider provider,
                                 @Nonnull MoveTracker moveTracker) {
        super(rootBefore, rootAfter, permissionProvider, provider);
        moveCtx = new MoveContext(moveTracker, provider.createReadOnlyRoot(rootBefore), provider.createReadOnlyRoot(rootAfter));
    }

    private MoveAwarePermissionValidator(@Nullable Tree parentBefore,
                                         @Nullable Tree parentAfter,
                                         @Nonnull TreePermission parentPermission,
                                         @Nonnull PermissionValidator parentValidator) {
        super(parentBefore, parentAfter, parentPermission, parentValidator);

        MoveAwarePermissionValidator pv = (MoveAwarePermissionValidator) parentValidator;
        moveCtx = pv.moveCtx;
    }

    @Nonnull
    @Override
    PermissionValidator createValidator(@Nullable Tree parentBefore,
                                        @Nullable Tree parentAfter,
                                        @Nonnull TreePermission parentPermission,
                                        @Nonnull PermissionValidator parentValidator) {
        if (moveCtx.containsMove(parentBefore, parentAfter)) {
            return new MoveAwarePermissionValidator(parentBefore, parentAfter, parentPermission, parentValidator);
        } else {
            return super.createValidator(parentBefore, parentAfter, parentPermission, parentValidator);
        }
    }

    private Validator visibleValidator(@Nonnull Tree source,
                                       @Nonnull Tree dest) {
        // TODO improve: avoid calculating the 'before' permissions in case the current parent permissions already point to the correct tree.
        ImmutableTree immutableTree = (ImmutableTree) moveCtx.rootBefore.getTree("/");
        TreePermission tp = getPermissionProvider().getTreePermission(immutableTree
                , TreePermission.EMPTY);
        for (String n : PathUtils.elements(source.getPath())) {
            immutableTree = immutableTree.getChild(n);
            tp = tp.getChildPermission(n, immutableTree.getNodeState());
        }
        Validator validator = createValidator(source, dest, tp, this);
        return new VisibleValidator(validator, true, false);
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        if (moveCtx.processAdd(getParentAfter(), name, this)) {
            return null;
        } else {
            return super.childNodeAdded(name, after);
        }
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        if (moveCtx.processDelete(getParentBefore(), name, this)) {
            return null;
        } else {
            return super.childNodeDeleted(name, before);
        }
    }

    //--------------------------------------------------------------------------
    private final class MoveContext {

        private final MoveTracker moveTracker;

        private final Root rootBefore;
        private final Root rootAfter;

        private MoveContext(@Nonnull MoveTracker moveTracker,
                            @Nonnull Root before,
                            @Nonnull Root after) {
            this.moveTracker = moveTracker;
            rootBefore = before;
            rootAfter = after;
        }

        private boolean containsMove(@Nullable Tree parentBefore, @Nullable Tree parentAfter) {
            return moveTracker.containsMove(PermissionUtil.getPath(parentBefore, parentAfter));
        }

        private boolean processAdd(@CheckForNull Tree parent, @Nonnull String name , @Nonnull MoveAwarePermissionValidator validator) throws CommitFailedException {
            if (parent == null) {
                return false;
            }
            ImmutableTree child = (ImmutableTree) parent.getChild(name);
            String sourcePath = moveTracker.getSourcePath(child.getPath());
            if (sourcePath != null) {
                ImmutableTree source = (ImmutableTree) rootBefore.getTree(sourcePath);
                if (source.exists()) {
                    // check permissions for adding the moved node at the target location.
                    validator.checkPermissions(child, false, Permissions.ADD_NODE | Permissions.NODE_TYPE_MANAGEMENT);
                    checkPermissions(source, Permissions.REMOVE_NODE);
                    return diff(source, child, validator);
                }
            }
            return false;
        }

        private boolean processDelete(@CheckForNull Tree parent, @Nonnull String name, @Nonnull MoveAwarePermissionValidator validator) throws CommitFailedException {
            if (parent == null) {
                return false;
            }
            ImmutableTree child = (ImmutableTree) parent.getChild(name);
            String destPath = moveTracker.getDestPath(child.getPath());
            if (destPath != null) {
                ImmutableTree dest = (ImmutableTree) rootAfter.getTree(destPath);
                if (dest.exists()) {
                    // check permissions for removing that node.
                    validator.checkPermissions(child, true, Permissions.REMOVE_NODE);
                    checkPermissions(dest, Permissions.ADD_NODE|Permissions.NODE_TYPE_MANAGEMENT);
                    return diff(child, dest, validator);
                }
            }

            return false;
        }

        private boolean diff(@Nonnull ImmutableTree source, @Nonnull ImmutableTree dest,
                             @Nonnull MoveAwarePermissionValidator validator) throws CommitFailedException {
            Validator nextValidator = validator.visibleValidator(source, dest);
            CommitFailedException e = EditorDiff.process(nextValidator , source.getNodeState(), dest.getNodeState());
            if (e != null) {
                throw e;
            }
            return true;
        }

        private void checkPermissions(@Nonnull Tree tree, long permissions) throws CommitFailedException {
            if (!getPermissionProvider().isGranted(tree, null, permissions)) {
                throw new CommitFailedException(ACCESS, 0, "Access denied");
            }
        }
    }
}
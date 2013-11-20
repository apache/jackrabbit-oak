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

import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * MoveAwarePermissionValidator... TODO
 */
public class MoveAwarePermissionValidator extends PermissionValidator {

    private final MoveContext moveCtx;

    MoveAwarePermissionValidator(@Nonnull ImmutableTree rootTreeBefore,
                                 @Nonnull ImmutableTree rootTreeAfter,
                                 @Nonnull PermissionProvider permissionProvider,
                                 @Nonnull PermissionValidatorProvider provider,
                                 @Nonnull MoveTracker moveTracker) {
        super(rootTreeBefore, rootTreeAfter, permissionProvider, provider);
        moveCtx = new MoveContext(moveTracker, rootTreeBefore, rootTreeAfter);
    }

    MoveAwarePermissionValidator(@Nullable Tree parentBefore,
                                 @Nullable Tree parentAfter,
                                 @Nullable TreePermission parentPermission,
                                 @Nonnull PermissionValidator parentValidator) {
        super(parentBefore, parentAfter, parentPermission, parentValidator);

        MoveAwarePermissionValidator pv = (MoveAwarePermissionValidator) parentValidator;
        moveCtx = pv.moveCtx;
    }

    @Override
    PermissionValidator createValidator(@Nullable Tree parentBefore, @Nullable Tree parentAfter, @Nullable TreePermission parentPermission, @Nonnull PermissionValidator parentValidator) {
        if (moveCtx.containsMove(parentBefore, parentAfter)) {
            return new MoveAwarePermissionValidator(parentBefore, parentAfter, parentPermission, parentValidator);
        } else {
            return super.createValidator(parentBefore, parentAfter, parentPermission, parentValidator);
        }
    }

    private Validator visibleValidator(ImmutableTree source, ImmutableTree dest) {
        TreePermission tp = getParentPermission().getChildPermission(source.getName(), source.unwrap());
        Validator validator = super.createValidator(source, dest, tp, this);
        return new VisibleValidator(validator, true, false);
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        if (moveCtx.processAdd((ImmutableTree) getParentAfter().getChild(name), this)) {
            return null;
        } else {
            return super.childNodeAdded(name, after);
        }
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        if (moveCtx.processDelete((ImmutableTree) getParentBefore().getChild(name), this)) {
            return null;
        } else {
            return super.childNodeDeleted(name, before);
        }
    }

    //--------------------------------------------------------------------------
    private static final class MoveContext {

        private final MoveTracker moveTracker;

        private final ImmutableRoot rootBefore;
        private final ImmutableRoot rootAfter;

        private final Set<ImmutableTree> processed = new HashSet<ImmutableTree>();

        private MoveContext(@Nonnull MoveTracker moveTracker,
                            @Nonnull ImmutableTree treeBefore,
                            @Nonnull ImmutableTree treeAfter) {
            this.moveTracker = moveTracker;
            rootBefore = new ImmutableRoot(treeBefore);
            rootAfter = new ImmutableRoot(treeAfter);
        }

        private boolean containsMove(Tree parentBefore, Tree parentAfter) {
            return moveTracker.containsMove(PermissionUtil.getPath(parentBefore, parentAfter));
        }

        private boolean processAdd(ImmutableTree child, MoveAwarePermissionValidator validator) throws CommitFailedException {
            // check permissions for adding the moved node at the target location.
            validator.checkPermissions(child, false, Permissions.ADD_NODE | Permissions.NODE_TYPE_MANAGEMENT);

            if (processed.contains(child)) {
                return true;
            } else {
                // FIXME: respect and properly handle move-operations in the subtree
                String sourcePath = moveTracker.getOriginalSourcePath(child.getPath());
                if (sourcePath != null) {
                    ImmutableTree source = rootBefore.getTree(sourcePath);
                    if (source.exists()) {
                        return diff(source, child, validator);
                    } // FIXME: else...
                }
                return false;
            }
        }

        private boolean processDelete(ImmutableTree child, MoveAwarePermissionValidator validator) throws CommitFailedException {
            // check permissions for removing that node.
            validator.checkPermissions(child, true, Permissions.REMOVE_NODE);

            if (processed.contains(child)) {
                return true;
            } else {
                // FIXME: respect and properly handle move-operations in the subtree
                String destPath = moveTracker.getDestPath(child.getPath());
                if (destPath != null) {
                    ImmutableTree dest = rootAfter.getTree(destPath);
                    if (dest.exists()) {
                        return diff(child, dest, validator);
                    } // FIXME: else...
                }

                return false;
            }
        }

        private boolean diff(ImmutableTree source, ImmutableTree dest,
                             MoveAwarePermissionValidator validator) throws CommitFailedException {
            Validator nextValidator = validator.visibleValidator(source, dest);
            CommitFailedException e = EditorDiff.process(nextValidator , source.unwrap(), dest.unwrap());
            if (e == null) {
                processed.add(source);
                processed.add(dest);
                return true;
            } else {
                throw e;
            }
        }
    }
}
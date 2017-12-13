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
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.lock.LockConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.oak.api.CommitFailedException.ACCESS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_CREATEDBY;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.MIX_CREATED;

/**
 * Validator implementation that checks for sufficient permission for all
 * write operations executed by a given content session.
 */
class PermissionValidator extends DefaultValidator {

    private final Tree parentBefore;
    private final Tree parentAfter;
    private final TreePermission parentPermission;
    private final PermissionProvider permissionProvider;
    private final PermissionValidatorProvider provider;

    private final TypePredicate isReferenceable;
    private final TypePredicate isCreated;

    private final long permission;

    PermissionValidator(@Nonnull NodeState rootBefore,
                        @Nonnull NodeState rootAfter,
                        @Nonnull PermissionProvider permissionProvider,
                        @Nonnull PermissionValidatorProvider provider) {
        this.parentBefore = provider.createReadOnlyTree(rootBefore);
        this.parentAfter = provider.createReadOnlyTree(rootAfter);
        this.parentPermission = permissionProvider.getTreePermission(parentBefore, TreePermission.EMPTY);

        this.permissionProvider = permissionProvider;
        this.provider = provider;

        this.isReferenceable = new TypePredicate(rootAfter, MIX_REFERENCEABLE);
        this.isCreated = new TypePredicate(rootAfter, MIX_CREATED);

        permission = Permissions.getPermission(PermissionUtil.getPath(parentBefore, parentAfter), Permissions.NO_PERMISSION);
    }

    PermissionValidator(@Nullable Tree parentBefore,
                        @Nullable Tree parentAfter,
                        @Nullable TreePermission parentPermission,
                        @Nonnull PermissionValidator parentValidator) {
        this.parentBefore = parentBefore;
        this.parentAfter = parentAfter;
        this.parentPermission = parentPermission;

        permissionProvider = parentValidator.permissionProvider;
        provider = parentValidator.provider;

        this.isReferenceable = parentValidator.isReferenceable;
        this.isCreated = parentValidator.isCreated;

        if (Permissions.NO_PERMISSION == parentValidator.permission) {
            this.permission = Permissions.getPermission(PermissionUtil.getPath(parentBefore, parentAfter), Permissions.NO_PERMISSION);
        } else {
            this.permission = parentValidator.permission;
        }
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        String name = after.getName();
        if (!TreeConstants.OAK_CHILD_ORDER.equals(name) && !isImmutableProperty(name, parentAfter)) {
            checkPermissions(parentAfter, after, Permissions.ADD_PROPERTY);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        String name = after.getName();
        if (TreeConstants.OAK_CHILD_ORDER.equals(name)) {
            String childName = ChildOrderDiff.firstReordered(before, after);
            if (childName != null) {
                checkPermissions(parentAfter, false, Permissions.MODIFY_CHILD_NODE_COLLECTION);
            } // else: no re-order but only internal update
        } else if (isImmutableProperty(name, parentAfter)) {
            // parent node has been removed and and re-added as
            checkPermissions(parentAfter, false, Permissions.ADD_NODE|Permissions.REMOVE_NODE);
        } else {
            checkPermissions(parentAfter, after, Permissions.MODIFY_PROPERTY);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        String name = before.getName();
        if (!TreeConstants.OAK_CHILD_ORDER.equals(name) && !isImmutableProperty(name, parentBefore)) {
            checkPermissions(parentBefore, before, Permissions.REMOVE_PROPERTY);
        }
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
        return nextValidator(childBefore, childAfter, parentPermission.getChildPermission(name, before));
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        Tree child = parentBefore.getChild(name);
        if (isVersionstorageTree(child)) {
            throw new CommitFailedException(
                    ACCESS, 22, "Attempt to remove versionstorage node: Fail to verify delete permission.");
        }
        return checkPermissions(child, true, Permissions.REMOVE_NODE);
    }

    //-------------------------------------------------< internal / private >---
    @Nonnull
    PermissionValidator createValidator(@Nullable Tree parentBefore,
                                        @Nullable Tree parentAfter,
                                        @Nonnull TreePermission parentPermission,
                                        @Nonnull PermissionValidator parentValidator) {
        return new PermissionValidator(parentBefore, parentAfter, parentPermission, parentValidator);
    }

    @CheckForNull
    Tree getParentAfter() {
        return parentAfter;
    }

    @CheckForNull
    Tree getParentBefore() {
        return parentBefore;
    }

    @Nonnull
    PermissionProvider getPermissionProvider() {
        return permissionProvider;
    }

    @CheckForNull
    Validator checkPermissions(@Nonnull Tree tree, boolean isBefore,
                               long defaultPermission) throws CommitFailedException {
        long toTest = getPermission(tree, defaultPermission);
        if (Permissions.isRepositoryPermission(toTest)) {
            if (!permissionProvider.getRepositoryPermission().isGranted(toTest)) {
                throw new CommitFailedException(ACCESS, 0, "Access denied");
            }
            return null; // no need for further validation down the subtree
        } else {
            NodeState ns = getNodeState(tree);
            if (ns == null) {
                throw new CommitFailedException(ACCESS, 0, "Access denied");
            }

            TreePermission tp = parentPermission.getChildPermission(tree.getName(), ns);
            if (!tp.isGranted(toTest)) {
                throw new CommitFailedException(ACCESS, 0, "Access denied");
            }
            if (noTraverse(toTest, defaultPermission)) {
                return null;
            } else {
                return (isBefore) ?
                        nextValidator(tree, null, tp) :
                        nextValidator(null, tree, tp);
            }
        }
    }

    private void checkPermissions(@Nonnull Tree parent,
                                  @Nonnull PropertyState property,
                                  long defaultPermission) throws CommitFailedException {
        if (NodeStateUtils.isHidden(property.getName())) {
            // ignore any hidden properties (except for OAK_CHILD_ORDER which has
            // been covered in "propertyChanged"
            return;
        }
        long toTest = getPermission(parent, property, defaultPermission);
        if (toTest != Permissions.NO_PERMISSION) {
            boolean isGranted;
            if (Permissions.isRepositoryPermission(toTest)) {
                isGranted = permissionProvider.getRepositoryPermission().isGranted(toTest);
            } else {
                isGranted = parentPermission.isGranted(toTest, property);
            }

            if (!isGranted) {
                throw new CommitFailedException(ACCESS, 0, "Access denied");
            }
        }
    }

    @Nonnull
    private Validator nextValidator(@Nullable Tree parentBefore,
                                    @Nullable Tree parentAfter,
                                    @Nonnull TreePermission treePermission) {
        Validator validator = createValidator(parentBefore, parentAfter, treePermission, this);
        return new VisibleValidator(validator, true, false);
    }

    private long getPermission(@Nonnull Tree tree, long defaultPermission) {
        if (permission != Permissions.NO_PERMISSION) {
            return permission;
        }
        long perm;
        if (testAccessControlPermission(tree)) {
            perm = Permissions.MODIFY_ACCESS_CONTROL;
        } else if (testUserPermission(tree)) {
            perm = Permissions.USER_MANAGEMENT;
        } else if (isIndexDefinition(tree)) {
            perm = Permissions.INDEX_DEFINITION_MANAGEMENT;
        } else {
            perm = defaultPermission;
        }
        return perm;
    }

    private long getPermission(@Nonnull Tree parent, @Nonnull PropertyState propertyState, long defaultPermission) {
        if (permission != Permissions.NO_PERMISSION) {
            return permission;
        }
        String name = propertyState.getName();
        long perm;
        if (JcrConstants.JCR_PRIMARYTYPE.equals(name)) {
            if (defaultPermission == Permissions.MODIFY_PROPERTY) {
                perm = getPermission(parent, Permissions.NODE_TYPE_MANAGEMENT);
            } else {
                // can't determine if this was  a user supplied modification of
                // the primary type -> omit permission check.
                // Node#addNode(String, String) and related methods need to
                // perform the permission check (as it used to be in jackrabbit 2.x).
                perm = Permissions.NO_PERMISSION;
            }
        } else if (JcrConstants.JCR_MIXINTYPES.equals(name)) {
            perm = Permissions.NODE_TYPE_MANAGEMENT;
        } else if (LockConstants.LOCK_PROPERTY_NAMES.contains(name)) {
            perm = Permissions.LOCK_MANAGEMENT;
        } else if (VersionConstants.VERSION_PROPERTY_NAMES.contains(name)) {
            perm = Permissions.VERSION_MANAGEMENT;
        } else if (provider.getAccessControlContext().definesProperty(parent, propertyState)) {
            perm = Permissions.MODIFY_ACCESS_CONTROL;
        } else if (provider.getUserContext().definesProperty(parent, propertyState)
                 && !provider.requiresJr2Permissions(Permissions.USER_MANAGEMENT)) {
            perm = Permissions.USER_MANAGEMENT;
        } else if (isIndexDefinition(parent)) {
            perm = Permissions.INDEX_DEFINITION_MANAGEMENT;
        } else {
            perm = defaultPermission;
        }
        return perm;
    }

    private boolean noTraverse(long permission, long defaultPermission) {
        if (defaultPermission == Permissions.REMOVE_NODE && provider.requiresJr2Permissions(Permissions.REMOVE_NODE)) {
            return false;
        } else {
            return permission == Permissions.MODIFY_ACCESS_CONTROL ||
                    permission == Permissions.VERSION_MANAGEMENT ||
                    permission == Permissions.REMOVE_NODE ||
                    defaultPermission == Permissions.REMOVE_NODE;
        }
    }

    private boolean isImmutableProperty(@Nonnull String name, @Nonnull Tree parent) {
        // NOTE: we cannot rely on autocreated/protected definition as this
        // doesn't reveal if a given property is expected to be never modified
        // after creation.
        NodeState parentNs = getNodeState(parent);
        if (JcrConstants.JCR_UUID.equals(name) && isReferenceable.apply(parentNs)) {
            return true;
        } else {
            return (JCR_CREATED.equals(name) || JCR_CREATEDBY.equals(name))
                    && isCreated.apply(parentNs);
        }
    }

    private boolean testUserPermission(@Nonnull Tree tree) {
        return provider.getUserContext().definesTree(tree) && !provider.requiresJr2Permissions(Permissions.USER_MANAGEMENT);
    }

    private boolean testAccessControlPermission(@Nonnull Tree tree) {
        return provider.getAccessControlContext().definesTree(tree);
    }

    private boolean isVersionstorageTree(Tree tree) {
        return permission == Permissions.VERSION_MANAGEMENT &&
                VersionConstants.REP_VERSIONSTORAGE.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    @CheckForNull
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

    private boolean isIndexDefinition(@Nonnull Tree tree) {
        return tree.getPath().contains(IndexConstants.INDEX_DEFINITIONS_NAME);
    }

    @CheckForNull
    private static NodeState getNodeState(@Nonnull Tree tree) {
        if (tree instanceof ImmutableTree) {
            return ((ImmutableTree) tree).getNodeState();
        } else {
            return null;
        }
    }
}

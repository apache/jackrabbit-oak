/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.version;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.util.TreeUtil;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@code ReadOnlyVersionManager} provides implementations for read-only
 * version operations modeled after the ones available in {@link javax.jcr.version.VersionManager}.
 */
public abstract class ReadOnlyVersionManager {

    /**
     * @return the read-only {@link Tree} for the jcr:versionStorage node. The
     *         returned {@code Tree} instance must be up-to-date with the
     *         {@code Root} returned by {@link #getWorkspaceRoot()}.
     */
    @Nonnull
    protected abstract Tree getVersionStorage();

    /**
    /**
     * @return the {@code Root} of the workspace.
     */
    @Nonnull
    protected abstract Root getWorkspaceRoot();

    /**
     * @return the node type manager of this repository.
     */
    @Nonnull
    protected abstract ReadOnlyNodeTypeManager getNodeTypeManager();

    /**
     * Returns {@code true} if the tree is checked out; otherwise
     * {@code false}. The root node is always considered checked out.
     *
     * @param tree the tree to check.
     * @return whether the tree is checked out or not.
     */
    public boolean isCheckedOut(@Nonnull Tree tree) {
        if (checkNotNull(tree).exists()) {
            PropertyState p = tree.getProperty(VersionConstants.JCR_ISCHECKEDOUT);
            if (p != null) {
                return p.getValue(Type.BOOLEAN);
            }
        } else {
            // FIXME: this actually means access to the tree is restricted
            // and may result in wrong isCheckedOut value. This should never
            // be the case in a commit hook because it operates on non-access-
            // controlled NodeStates. This means consistency is not at risk
            // but it may mean oak-jcr sees a node as checked out even though
            // it is in fact read-only because of a checked-in ancestor.
        }
        if (tree.isRoot()) {
            return true;
        } else {
            // otherwise return checkedOut status of parent
            return isCheckedOut(tree.getParent());
        }
    }

    /**
     * Returns the tree representing the version history of the given
     * versionable tree or {@code null} if none exists yet.
     *
     * @param versionable the versionable tree.
     * @return the version history or {@code null} if none exists yet.
     * @throws UnsupportedRepositoryOperationException
     *                             if the versionable tree is not actually
     *                             versionable.
     * @throws RepositoryException if an error occurs while checking the node
     *                             type of the tree.
     */
    @CheckForNull
    public Tree getVersionHistory(@Nonnull Tree versionable)
            throws UnsupportedRepositoryOperationException,
            RepositoryException {
        checkVersionable(versionable);
        String uuid = versionable.getProperty(VersionConstants.JCR_UUID).getValue(Type.STRING);
        return TreeUtil.getTree(getVersionStorage(), getVersionHistoryPath(uuid));
    }

    /**
     * Returns the path of the version history for the given {@code uuid}.
     * The returned path is relative to the version storage tree as returned
     * by {@link #getVersionStorage()}.
     *
     * @param uuid the uuid of the versionable node
     * @return the relative path of the version history for the given uuid.
     */
    @Nonnull
    public String getVersionHistoryPath(@Nonnull String uuid) {
        String relPath = "";
        for (int i = 0; i < 3; i++) {
            String name = uuid.substring(i * 2, i * 2 + 2);
            relPath = PathUtils.concat(relPath, name);
        }
        return PathUtils.concat(relPath, uuid);
    }

    /**
     * Returns the tree representing the base version of the given versionable
     * tree or {@code null} if none exists yet. This is the case when a
     * versionable node is created, but is not yet saved.
     *
     * @param versionable the versionable tree.
     * @return the tree representing the base version or {@code null}.
     * @throws UnsupportedRepositoryOperationException
     *                             if the versionable tree is not actually
     *                             versionable.
     * @throws RepositoryException if an error occurs while checking the node
     *                             type of the tree.
     */
    @CheckForNull
    public Tree getBaseVersion(@Nonnull Tree versionable)
            throws UnsupportedRepositoryOperationException,
            RepositoryException {
        checkVersionable(versionable);
        PropertyState p = versionable.getProperty(VersionConstants.JCR_BASEVERSION);
        if (p == null) {
            // version history does not yet exist
            return null;
        }
        return getIdentifierManager().getTree(p.getValue(Type.STRING));
    }

    //----------------------------< internal >----------------------------------
    /**
     * @return an identifier manager that is able to resolve identifiers of
     *         nodes in the version storage.
     */
    protected IdentifierManager getIdentifierManager() {
        // FIXME: may need to revise this, because getVersionStorageTree()
        // is not the same Root as getWorkspaceRoot()
        return new IdentifierManager(getWorkspaceRoot());
    }

    /**
     * Checks if the given {@code tree} is versionable and throws a {@link
     * UnsupportedRepositoryOperationException} if it is not.
     *
     * @param tree the tree to check.
     * @return the passed tree.
     * @throws UnsupportedRepositoryOperationException
     *                             if the tree is not versionable.
     * @throws RepositoryException if an error occurs while checking the node
     *                             type of the tree.
     */
    @Nonnull
    protected Tree checkVersionable(@Nonnull Tree tree)
            throws UnsupportedRepositoryOperationException,
            RepositoryException {
        if (!isVersionable(checkNotNull(tree))) {
            throw new UnsupportedRepositoryOperationException("Node at " +
                    tree.getPath() + " is not versionable");
        }
        return tree;
    }

    /**
     * Returns {@code true} if the given {@code tree} is of type
     * {@code mix:versionable}; {@code false} otherwise.
     *
     * @param tree the tree to check.
     * @return whether the {@code tree} is versionable.
     * @throws RepositoryException if an error occurs while checking the node
     *                             type of the tree.
     */
    protected boolean isVersionable(@Nonnull Tree tree) {
        return getNodeTypeManager().isNodeType(
                checkNotNull(tree), VersionConstants.MIX_VERSIONABLE);
    }
}

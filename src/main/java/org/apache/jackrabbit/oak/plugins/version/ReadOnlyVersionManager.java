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
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.version.VersionConstants;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <code>ReadOnlyVersionManager</code> provides implementations for read-only
 * version operations modeled after the ones available in {@link javax.jcr.version.VersionManager}.
 */
public abstract class ReadOnlyVersionManager {

    /**
     * @return the read-only {@link Tree} for the jcr:versionStorage node. The
     *         returned <code>Tree</code> instance must be up-to-date with the
     *         <code>Root</code> returned by {@link #getWorkspaceRoot()}.
     */
    @Nonnull
    protected abstract Tree getVersionStorageTree();

    /**
     * @return the <code>Root</code> of the workspace.
     */
    @Nonnull
    protected abstract Root getWorkspaceRoot();

    /**
     * @return the node type manager of this repository.
     */
    @Nonnull
    protected abstract ReadOnlyNodeTypeManager getNodeTypeManager();

    /**
     * Returns <code>true</code> if the tree is checked out; otherwise
     * <code>false</code>.
     *
     * @param tree the tree to check.
     * @return whether the tree is checked out or not.
     */
    public boolean isCheckedOut(@Nonnull Tree tree) {
        return isCheckedOut(checkNotNull(tree).getLocation());
    }

    /**
     * Returns <code>true</code> if the tree at the given absolute Oak path is
     * checked out; otherwise <code>false</code>.
     *
     * @param absOakPath an absolute path.
     * @return whether the tree at the given location is checked out or not.
     */
    public boolean isCheckedOut(@Nonnull String absOakPath) {
        return isCheckedOut(getWorkspaceRoot().getLocation(checkNotNull(absOakPath)));
    }

    /**
     * Returns the tree representing the version history of the given
     * versionable tree or <code>null</code> if none exists yet.
     *
     * @param versionable the versionable tree.
     * @return the version history or <code>null</code> if none exists yet.
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
        String uuid = versionable.getProperty(
                VersionConstants.JCR_UUID).getValue(Type.STRING);
        return getVersionStorageTree().getLocation().getChild(
                getVersionHistoryPath(uuid)).getTree();
    }

    /**
     * Returns the path of the version history for the given <code>uuid</code>.
     * The returned path is relative to the version storage tree as returned
     * by {@link #getVersionStorageTree()}.
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
     * tree or <code>null</code> if none exists yet. This is the case when a
     * versionable node is created, but is not yet saved.
     *
     * @param versionable the versionable tree.
     * @return the tree representing the base version or <code>null</code>.
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

    @Nonnull
    private static String getRelativePath(@Nonnull String absJcrPath) {
        checkArgument(checkNotNull(absJcrPath).startsWith("/"),
                "Path is not absolute: " + absJcrPath);
        String relPath = absJcrPath.substring(1);
        checkArgument(!relPath.startsWith("/"), "Invalid path: " + absJcrPath);
        return relPath;
    }

    /**
     * @return an identifier manager that is able to resolve identifiers of
     *         nodes in the version storage.
     */
    protected IdentifierManager getIdentifierManager() {
        // FIXME: may need to revise this, because getVersionStorageTree()
        // is not the same Root as getWorkspaceRoot()
        return new IdentifierManager(getWorkspaceRoot());
    }

    protected static boolean isCheckedOut(@Nonnull TreeLocation location) {
        Tree t = checkNotNull(location).getTree();
        if (t != null) {
            PropertyState p = t.getProperty(VersionConstants.JCR_ISCHECKEDOUT);
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
        // otherwise return checkedOut status of parent
        if (location.getParent() == TreeLocation.NULL) {
            // root tree
            return true;
        } else {
            return isCheckedOut(location.getParent());
        }
    }

    /**
     * Checks if the given <code>tree</code> is versionable and throws a {@link
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
     * Returns <code>true</code> if the given <code>tree</code> is of type
     * <code>mix:versionable</code>; <code>false</code> otherwise.
     *
     * @param tree the tree to check.
     * @return whether the <code>tree</code> is versionable.
     * @throws RepositoryException if an error occurs while checking the node
     *                             type of the tree.
     */
    protected boolean isVersionable(@Nonnull Tree tree) throws RepositoryException {
        checkNotNull(tree);
        // the first check for the jcr:isCheckedOut property will fail fast
        // if the node is not versionable. the second check is to make sure
        // the node is in fact versionable.
        return tree.hasProperty(VersionConstants.JCR_ISCHECKEDOUT)
                && getNodeTypeManager().isNodeType(tree, VersionConstants.MIX_VERSIONABLE);
    }
}

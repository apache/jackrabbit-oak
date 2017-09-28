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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@code ReadOnlyVersionManager} provides implementations for read-only
 * version operations modeled after the ones available in {@link javax.jcr.version.VersionManager}.
 */
public abstract class ReadOnlyVersionManager {

    private static final Logger log = LoggerFactory.getLogger(ReadOnlyVersionManager.class);

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

    //--------------------------------------------------------------------------

    /**
     * Return a new instance of {@code ReadOnlyVersionManager} that reads version
     * information from the tree at {@link VersionConstants#VERSION_STORE_PATH}.
     *
     * @param root The root to read version information from.
     * @param namePathMapper The {@code NamePathMapper} to use.
     * @return a new instance of {@code ReadOnlyVersionManager}.
     */
    @Nonnull
    public static ReadOnlyVersionManager getInstance(final Root root,
                                                     final NamePathMapper namePathMapper) {
        return new ReadOnlyVersionManager() {
            @Nonnull
            @Override
            protected Tree getVersionStorage() {
                return root.getTree(VersionConstants.VERSION_STORE_PATH);
            }

            @Nonnull
            @Override
            protected Root getWorkspaceRoot() {
                return root;
            }

            @Nonnull
            @Override
            protected ReadOnlyNodeTypeManager getNodeTypeManager() {
                return ReadOnlyNodeTypeManager.getInstance(root, namePathMapper);
            }
        };
    }

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
     * Returns the version tree with the given uuid.
     *
     * @param uuid the uuid of the version tree.
     * @return the version tree or {@code null} if there is none.
     */
    @CheckForNull
    public Tree getVersion(@Nonnull String uuid) {
        return getIdentifierManager().getTree(uuid);
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

    /**
     * Returns {@code true} if the specified tree has {@link VersionConstants#REP_VERSIONSTORAGE}
     * defines as primary node type i.e. is part of the intermediate version storage
     * structure that contains the version histories and the versions.
     *
     * @param tree The tree to be tested.
     * @return {@code true} if the target node has {@link VersionConstants#REP_VERSIONSTORAGE}
     * defines as primary node type; {@code false} otherwise.
     */
    public static boolean isVersionStoreTree(@Nonnull Tree tree) {
        return VersionConstants.REP_VERSIONSTORAGE.equals(TreeUtil.getPrimaryTypeName(tree));
    }

    /**
     * Tries to retrieve the tree corresponding to specified {@code versionTree}
     * outside of the version storage based on versionable path information
     * stored with the version history. The following cases are distinguished:
     *
     * <ul>
     *     <li>Version History: If the given tree is a version history the
     *     associated versionable tree in the specified workspace is being returned
     *     based on the information stored in the versionable path property. If
     *     no versionable path property is present {@code null} is returned.</li>
     *     <li>Version: Same as for version history.</li>
     *     <li>Version Labels: Same as for version history.</li>
     *     <li>Frozen Node: If the given tree forms part of a frozen node the
     *     path of the target node is computed from the versionable path and
     *     the relative path of the frozen node.</li>
     *     <li>Other Nodes: If the specified tree is not part of the tree structure
     *     defined by a version history, {@code null} will be returned.</li>
     * </ul>
     *
     * Please note that this method will not verify if the tree at the versionable
     * path or the computed subtree actually exists. This must be asserted by
     * the caller before operating on the tree.
     *
     * @param versionTree The tree from within the version storage for which
     *                    that versionable correspondent should be retrieved.
     * @param workspaceName The name of the workspace for which the target should be retrieved.
     * @return A existing or non-existing tree pointing to the location of the
     * correspondent tree outside of the version storage or {@code null} if the
     * versionable path property for the specified workspace is missing or if
     * the given tree is not located within the tree structure defined by a version history.
     *
     * @see VersionablePathHook
     * @see VersionConstants#MIX_REP_VERSIONABLE_PATHS
     */
    @CheckForNull
    public Tree getVersionable(@Nonnull Tree versionTree, @Nonnull String workspaceName) {
        Root root = getWorkspaceRoot();
        String relPath = "";
        Tree t = versionTree;
        while (t.exists() && !isVersionStoreTree(t) && !t.isRoot()) {
            String ntName = TreeUtil.getPrimaryTypeName(t);
            if (VersionConstants.NT_FROZENNODE.equals(ntName)) {
                relPath = PathUtils.relativize(t.getPath(), versionTree.getPath());
            } else if (JcrConstants.NT_VERSIONHISTORY.equals(ntName)) {
                PropertyState prop = t.getProperty(workspaceName);
                if (prop != null) {
                    return root.getTree(PathUtils.concat(prop.getValue(Type.PATH), relPath));
                } else {
                    // version history is missing the versionable path property for the given workspace name
                    log.warn("Missing versionable path property for {} at {}", workspaceName, t.getPath());
                    break;
                }
            }
            t = t.getParent();
        }

        // intermediate node in the version storage that matches none of the special
        // conditions checked above and cannot be resolve to a versionable tree.
        return null;
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
     */
    protected boolean isVersionable(@Nonnull Tree tree) {
        return getNodeTypeManager().isNodeType(
                checkNotNull(tree), VersionConstants.MIX_VERSIONABLE);
    }

    /**
     * Returns {@code true} if the given {@code versionableCandidate} is of type
     * {@code mix:versionable}; {@code false} otherwise.
     *
     * @param versionableCandidate node state to check.
     * @return whether the {@code versionableCandidate} is versionable.
     */
    boolean isVersionable(NodeState versionableCandidate) {
        // this is not 100% correct, because t.getPath() will
        // not return the correct path for node after, but is
        // sufficient to check if it is versionable
        return isVersionable(TreeFactory.createReadOnlyTree(versionableCandidate));
    }
}

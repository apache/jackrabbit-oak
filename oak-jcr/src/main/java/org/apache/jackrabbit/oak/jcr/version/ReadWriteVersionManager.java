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
package org.apache.jackrabbit.oak.jcr.version;

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.version.VersionConstants;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <code>ReadWriteVersionManager</code>...
 */
public class ReadWriteVersionManager extends ReadOnlyVersionManager {

    private final TreeLocation versionStorageLocation;
    private final Root workspaceRoot;

    public ReadWriteVersionManager(@Nonnull TreeLocation versionStorageLocation,
                                   @Nonnull Root workspaceRoot) {
        this.versionStorageLocation = checkNotNull(versionStorageLocation);
        this.workspaceRoot = checkNotNull(workspaceRoot);
    }

    /**
     * Called by the write methods to refresh the state of the possible
     * session associated with this instance. The default implementation
     * of this method does nothing, but a subclass can use this callback
     * to keep a session in sync with the persisted version changes.
     *
     * @throws RepositoryException if the session could not be refreshed
     */
    protected void refresh() throws RepositoryException {
        // do nothing
    }

    @Override
    @Nonnull
    protected Tree getVersionStorageTree() {
        return versionStorageLocation.getTree();
    }

    @Override
    @Nonnull
    protected Root getWorkspaceRoot() {
        return workspaceRoot;
    }

    @Override
    @Nonnull
    protected ReadOnlyNodeTypeManager getNodeTypeManager() {
        return ReadOnlyNodeTypeManager.getInstance(
                workspaceRoot, NamePathMapper.DEFAULT);
    }

    /**
     * Performs a checkin on a versionable tree and returns the tree that
     * represents the created version.
     *
     * @param versionable the versionable node to check in.
     * @return the created version.
     * @throws InvalidItemStateException if the current root has pending
     *                                   changes.
     * @throws UnsupportedRepositoryOperationException
     *                                   if the versionable tree isn't actually
     *                                   versionable.
     * @throws RepositoryException       if an error occurs while checking the
     *                                   node type of the tree.
     */
    @Nonnull
    public Tree checkin(@Nonnull Tree versionable)
            throws RepositoryException, InvalidItemStateException,
            UnsupportedRepositoryOperationException {
        if (workspaceRoot.hasPendingChanges()) {
            throw new InvalidItemStateException("Unable to perform checkin. " +
                    "Session has pending changes.");
        }
        if (!isVersionable(versionable)) {
            throw new UnsupportedRepositoryOperationException(
                    versionable.getPath() + " is not versionable");
        }
        TreeLocation location = versionable.getLocation();
        if (isCheckedOut(location)) {
            versionable.setProperty(VersionConstants.JCR_ISCHECKEDOUT,
                    Boolean.FALSE, Type.BOOLEAN);
            try {
                getWorkspaceRoot().commit();
                refresh();
            } catch (CommitFailedException e) {
                getWorkspaceRoot().refresh();
                throw new RepositoryException(e);
            }
        }
        return getBaseVersion(getWorkspaceRoot().getTree(location.getPath()));
    }

    /**
     * Performs a checkout on a versionable tree.
     *
     * @param versionable the versionable node to check out.
     * @throws InvalidItemStateException if the current root has pending
     *                                   changes.
     * @throws UnsupportedRepositoryOperationException
     *                                   if the versionable tree isn't actually
     *                                   versionable.
     * @throws RepositoryException       if an error occurs while checking the
     *                                   node type of the tree.
     */
    public void checkout(@Nonnull Tree versionable)
            throws UnsupportedRepositoryOperationException,
            InvalidItemStateException, RepositoryException {
        if (workspaceRoot.hasPendingChanges()) {
            throw new InvalidItemStateException("Unable to perform checkout. " +
                    "Session has pending changes.");
        }
        if (!isVersionable(versionable)) {
            throw new UnsupportedRepositoryOperationException(
                    versionable.getPath() + " is not versionable");
        }
        TreeLocation location = versionable.getLocation();
        if (!isCheckedOut(location)) {
            versionable.setProperty(VersionConstants.JCR_ISCHECKEDOUT,
                    Boolean.TRUE, Type.BOOLEAN);
            try {
                getWorkspaceRoot().commit();
                refresh();
            } catch (CommitFailedException e) {
                getWorkspaceRoot().refresh();
                throw new RepositoryException(e);
            }
        }
    }

    // TODO: more methods that modify versions
}

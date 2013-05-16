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
import javax.jcr.AccessDeniedException;
import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.version.VersionException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@code ReadWriteVersionManager}...
 */
public class ReadWriteVersionManager extends ReadOnlyVersionManager {

    private static final Logger log = LoggerFactory.getLogger(ReadWriteVersionManager.class);
    private final Tree versionStorage;
    private final Root workspaceRoot;

    public ReadWriteVersionManager(@Nonnull Tree versionStorage,
            @Nonnull Root workspaceRoot) {
        this.versionStorage = checkNotNull(versionStorage);
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
    protected Tree getVersionStorage() {
        return versionStorage;
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
        if (isCheckedOut(versionable)) {
            versionable.setProperty(VersionConstants.JCR_ISCHECKEDOUT,
                    Boolean.FALSE, Type.BOOLEAN);
            try {
                getWorkspaceRoot().commit();
                refresh();
            } catch (CommitFailedException e) {
                getWorkspaceRoot().refresh();
                throw newRepositoryException(e);
            }
        }
        return getBaseVersion(getWorkspaceRoot().getTree(versionable.getPath()));
    }

    /**
     * Performs a checkout on a versionable tree.
     *
     * @param versionable the versionable node to check out.
     * @throws UnsupportedRepositoryOperationException
     *                                   if the versionable tree isn't actually
     *                                   versionable.
     * @throws RepositoryException       if an error occurs while checking the
     *                                   node type of the tree.
     */
    public void checkout(@Nonnull Tree versionable)
            throws UnsupportedRepositoryOperationException,
            InvalidItemStateException, RepositoryException {
        if (!isVersionable(versionable)) {
            throw new UnsupportedRepositoryOperationException(
                    versionable.getPath() + " is not versionable");
        }
        if (!isCheckedOut(versionable)) {
            if (workspaceRoot.hasPendingChanges()) {
                // TODO: perform checkout on separate root and refresh session
                //       while keeping pending changes.
                log.warn("Session has pending changes. Checkout operation will " +
                        "save those changes as well.");
            }
            versionable.setProperty(VersionConstants.JCR_ISCHECKEDOUT,
                    Boolean.TRUE, Type.BOOLEAN);
            try {
                getWorkspaceRoot().commit();
                refresh();
            } catch (CommitFailedException e) {
                getWorkspaceRoot().refresh();
                throw newRepositoryException(e);
            }
        }
    }

    // TODO: more methods that modify versions

    /**
     * Wraps the given {@link CommitFailedException} instance using the
     * appropriate {@link RepositoryException} subclass based on the
     * {@link CommitFailedException#getType() type} of the given exception.
     *
     * @param exception typed commit failure exception
     * @return matching repository exception
     */
    private static RepositoryException newRepositoryException(@Nonnull CommitFailedException exception) {
        if (exception.isConstraintViolation()) {
            return new ConstraintViolationException(exception);
        } else if (exception.isAccessViolation()) {
            return new AccessDeniedException(exception);
        } else if (exception.isOfType("State")) {
            return new InvalidItemStateException(exception);
        } else if (exception.isOfType(CommitFailedException.VERSION)) {
            return new VersionException(exception);
        } else if (exception.isOfType("Lock")) {
            return new LockException(exception);
        } else {
            return new RepositoryException(exception);
        }
    }
}

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
package org.apache.jackrabbit.oak.jcr.delegate;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.InvalidItemStateException;
import javax.jcr.ItemExistsException;
import javax.jcr.PathNotFoundException;
import javax.jcr.ReferentialIntegrityException;
import javax.jcr.RepositoryException;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.version.VersionException;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
public class SessionDelegate {
    static final Logger log = LoggerFactory.getLogger(SessionDelegate.class);

    private final ContentSession contentSession;
    private final Root root;
    private final IdentifierManager idManager;

    private boolean isAlive = true;
    private int sessionOpCount;

    public SessionDelegate(@Nonnull ContentSession contentSession) {
        this.contentSession = checkNotNull(contentSession);
        this.root = contentSession.getLatestRoot();
        this.idManager = new IdentifierManager(root);
    }

    /**
     * Called by {@link #perform(SessionOperation)} when the session needs to be
     * refreshed before the actual {@link SessionOperation} is executed.
     * This default implementation is empty.
     */
    protected void refresh() {
    }

    /**
     * Performs the passed {@code SessionOperation} in a safe execution context. This
     * context ensures that the session is refreshed if necessary and that refreshing
     * occurs before the session operation is performed and the refreshing is done only
     * once.
     *
     * @param sessionOperation  the {@code SessionOperation} to perform
     * @param <T>  return type of {@code sessionOperation}
     * @return  the result of {@code sessionOperation.perform()}
     * @throws RepositoryException
     */
    public synchronized <T> T perform(SessionOperation<T> sessionOperation) throws RepositoryException {
        // Synchronize to avoid conflicting refreshes from concurrent JCR API calls
        if (sessionOpCount == 0) {
            // Refresh and checks only for non re-entrant session operations
            refresh();
            sessionOperation.checkPreconditions();
        }
        try {
            sessionOpCount++;
            return sessionOperation.perform();
        } finally {
            sessionOpCount--;
        }
    }

    @Nonnull
    public ContentSession getContentSession() {
        return contentSession;
    }

    /**
     * Determine whether this session is alive and has not been logged
     * out or become stale by other means.
     * @return {@code true} if this session is alive, {@code false} otherwise.
     */
    public boolean isAlive() {
        return isAlive;
    }

    /**
     * Check that this session is alive.
     * @throws RepositoryException if this session is not alive
     * @see #isAlive()
     */
    public void checkAlive() throws RepositoryException {
        if (!isAlive()) {
            throw new RepositoryException("This session has been closed.");
        }
    }

    @Nonnull
    public AuthInfo getAuthInfo() {
        return contentSession.getAuthInfo();
    }

    public void logout() {
        if (!isAlive) {
            // ignore
            return;
        }

        isAlive = false;
        // TODO

        try {
            contentSession.close();
        } catch (IOException e) {
            log.warn("Error while closing connection", e);
        }
    }

    @Nonnull
    public IdentifierManager getIdManager() {
        return idManager;
    }

    @CheckForNull
    public NodeDelegate getRootNode() {
        return getNode("/");
    }

    /**
     * {@code NodeDelegate} at the given path
     * @param path Oak path
     * @return  The {@code NodeDelegate} at {@code path} or {@code null} if
     * none exists or not accessible.
     */
    @CheckForNull
    public NodeDelegate getNode(String path) {
        return NodeDelegate.create(this, root.getTree(path));
    }

    @CheckForNull
    public NodeDelegate getNodeByIdentifier(String id) {
        Tree tree = idManager.getTree(id);
        return (tree == null) ? null : new NodeDelegate(this, tree);
    }

    /**
     * {@code PropertyDelegate} at the given path
     * @param path Oak path
     * @return  The {@code PropertyDelegate} at {@code path} or {@code null} if
     * none exists or not accessible.
     */
    @CheckForNull
    public PropertyDelegate getProperty(String path) {
        Tree parent = root.getTree(PathUtils.getParentPath(path));
        String name = PathUtils.getName(path);
        return PropertyDelegate.create(this, parent, name);
    }

    public boolean hasPendingChanges() {
        return root.hasPendingChanges();
    }

    public void save() throws RepositoryException {
        try {
            root.commit();
        } catch (CommitFailedException e) {
            throw newRepositoryException(e);
        }
    }

    public void refresh(boolean keepChanges) {
        if (keepChanges) {
            root.rebase();
        } else {
            root.refresh();
        }
    }

    //----------------------------------------------------------< Workspace >---

    @Nonnull
    public String getWorkspaceName() {
        return contentSession.getWorkspaceName();
    }

    /**
     * Copy a node
     * @param srcPath  oak path to the source node to copy
     * @param destPath  oak path to the destination
     * @throws RepositoryException
     */
    public void copy(String srcPath, String destPath) throws RepositoryException {
        // check destination
        Tree dest = root.getTree(destPath);
        if (dest.exists()) {
            throw new ItemExistsException(destPath);
        }

        // check parent of destination
        String destParentPath = PathUtils.getParentPath(destPath);
        Tree destParent = root.getTree(destParentPath);
        if (!destParent.exists()) {
            throw new PathNotFoundException(PathUtils.getParentPath(destPath));
        }

        // check source exists
        Tree src = root.getTree(srcPath);
        if (!src.exists()) {
            throw new PathNotFoundException(srcPath);
        }

        try {
            Root currentRoot = contentSession.getLatestRoot();
            currentRoot.copy(srcPath, destPath);
            currentRoot.commit();
        } catch (CommitFailedException e) {
            throw newRepositoryException(e);
        }
    }

    /**
     * Move a node
     * @param srcPath  oak path to the source node to copy
     * @param destPath  oak path to the destination
     * @param transientOp  whether or not to perform the move in transient space
     * @throws RepositoryException
     */
    public void move(String srcPath, String destPath, boolean transientOp)
            throws RepositoryException {

        Root moveRoot = transientOp ? root : contentSession.getLatestRoot();

        // check destination
        Tree dest = moveRoot.getTree(destPath);
        if (dest.exists()) {
            throw new ItemExistsException(destPath);
        }

        // check parent of destination
        String destParentPath = PathUtils.getParentPath(destPath);
        Tree destParent = moveRoot.getTree(destParentPath);
        if (!destParent.exists()) {
            throw new PathNotFoundException(PathUtils.getParentPath(destPath));
        }

        // check source exists
        Tree src = moveRoot.getTree(srcPath);
        if (!src.exists()) {
            throw new PathNotFoundException(srcPath);
        }

        try {
            moveRoot.move(srcPath, destPath);
            if (!transientOp) {
                moveRoot.commit();
            }
        } catch (CommitFailedException e) {
            throw newRepositoryException(e);
        }
    }

    @Nonnull
    public QueryEngine getQueryEngine() {
        return root.getQueryEngine();
    }

    //-----------------------------------------------------------< internal >---

    @Nonnull  // FIXME this should be package private. OAK-672
    public Root getRoot() {
        return root;
    }

    /**
     * Wraps the given {@link CommitFailedException} instance using the
     * appropriate {@link RepositoryException} subclass based on the
     * {@link CommitFailedException#getType() type} of the given exception.
     *
     * @param exception typed commit failure exception
     * @return matching repository exception
     */
    private static RepositoryException newRepositoryException(
            CommitFailedException exception) {
        checkNotNull(exception);
        if (exception.isConstraintViolation()) {
            return new ConstraintViolationException(exception);
        } else if (exception.isOfType("Type")) {
            return new NoSuchNodeTypeException(exception);
        } else if (exception.isAccessViolation()) {
            return new AccessDeniedException(exception);
        } else if (exception.isOfType("Integrity")) {
            return new ReferentialIntegrityException(exception);
        } else if (exception.isOfType("State")) {
            return new InvalidItemStateException(exception);
        } else if (exception.isOfType("Version")) {
            return new VersionException(exception);
        } else if (exception.isOfType("Lock")) {
            return new LockException(exception);
        } else {
            return new RepositoryException(exception);
        }
    }

}

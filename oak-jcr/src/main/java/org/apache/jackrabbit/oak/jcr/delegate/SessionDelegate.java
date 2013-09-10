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
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;

import java.io.IOException;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.ItemExistsException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.apache.jackrabbit.oak.jcr.RefreshStrategy;
import org.apache.jackrabbit.oak.jcr.operation.SessionOperation;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.FailingValidator;
import org.apache.jackrabbit.oak.spi.commit.SubtreeExcludingValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
public class SessionDelegate {
    static final Logger log = LoggerFactory.getLogger(SessionDelegate.class);

    private final ContentSession contentSession;
    private final RefreshStrategy refreshStrategy;

    private final Root root;
    private final IdentifierManager idManager;
    private final PermissionProvider permissionProvider;

    private boolean isAlive = true;
    private int sessionOpCount;
    private long updateCount = 0;

    /**
     * Create a new session delegate for a {@code ContentSession}. The refresh behaviour of the
     * session is governed by the value of the {@code refreshInterval} argument: if the session
     * has been idle longer than that value, an implicit refresh will take place.
     * In addition a refresh can always be scheduled from the next access by an explicit call
     * to {@link #refreshAtNextAccess()}. This is typically done from within the observation event
     * dispatcher in order.
     *
     * @param contentSession  the content session
     * @param refreshStrategy  the refresh strategy used for auto refreshing this session
     * @param securityProvider the security provider
     */
    public SessionDelegate(@Nonnull ContentSession contentSession, RefreshStrategy refreshStrategy,
            SecurityProvider securityProvider) {
        this.contentSession = checkNotNull(contentSession);
        this.refreshStrategy = checkNotNull(refreshStrategy);
        this.root = contentSession.getLatestRoot();
        this.idManager = new IdentifierManager(root);
        this.permissionProvider = checkNotNull(securityProvider)
                .getConfiguration(AuthorizationConfiguration.class)
                .getPermissionProvider(root, contentSession.getAuthInfo().getPrincipals());
    }

    public synchronized void refreshAtNextAccess() {
        refreshStrategy.accept(RefreshStrategy.Once.RESETTING_VISITOR);
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
     * @see #getRoot()
     */
    public synchronized <T> T perform(SessionOperation<T> sessionOperation)
            throws RepositoryException {
        // Synchronize to avoid conflicting refreshes from concurrent JCR API calls
        if (sessionOpCount == 0) {
            // Refresh and precondition checks only for non re-entrant session operations
            if (refreshStrategy.needsRefresh(sessionOperation)) {
                refresh(true);
                refreshStrategy.refreshed();
                updateCount++;
            }
            sessionOperation.checkPreconditions();
        }
        try {
            sessionOpCount++;
            return sessionOperation.perform();
        } finally {
            sessionOpCount--;
            if (sessionOperation.isUpdate()) {
                updateCount++;
            }
            if (sessionOperation.isSave()) {
                refreshStrategy.saved();
            } else if (sessionOperation.isRefresh()) {
                refreshStrategy.refreshed();
            }
        }
    }

    /**
     * Same as {@link #perform(SessionOperation)} unless this method expects
     * {@link SessionOperation#perform} <em>not</em> to throw a {@code RepositoryException}.
     * Such exceptions will be wrapped into a {@code RuntimeException} and rethrown as they
     * are considered an internal error.
     *
     * @param sessionOperation  the {@code SessionOperation} to perform
     * @param <T>  return type of {@code sessionOperation}
     * @return  the result of {@code sessionOperation.perform()}
     * @see #getRoot()
     */
    public <T> T safePerform(SessionOperation<T> sessionOperation) {
        try {
            return perform(sessionOperation);
        } catch (RepositoryException e) {
            throw new RuntimeException("Unexpected exception thrown by operation " +
                    sessionOperation, e);
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

    /**
     * @return session update counter
     */
    public long getUpdateCount() {
        return updateCount;
    }

    public void checkProtectedNode(String path) throws RepositoryException {
        NodeDelegate node = getNode(path);
        if (node == null) {
            throw new PathNotFoundException(
                    "Node " + path + " does not exist.");
        } else if (node.isProtected()) {
            throw new ConstraintViolationException(
                    "Node " + path + " is protected.");
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

    /**
     * Returns the node or property delegate at the given path.
     *
     * @param path Oak path
     * @return node or property delegate, or {@code null} if none exists
     */
    @CheckForNull
    public ItemDelegate getItem(String path) {
        String name = PathUtils.getName(path);
        if (name.isEmpty()) {
            return getRootNode();
        } else {
            Tree parent = root.getTree(PathUtils.getParentPath(path));
            if (parent.hasProperty(name)) {
                return PropertyDelegate.create(this, parent, name);
            }
            Tree child = parent.getChild(name);
            if (child.exists()) {
                return NodeDelegate.create(this, child);
            } else {
                return null;
            }
        }
    }

    @CheckForNull
    public NodeDelegate getNodeByIdentifier(String id) {
        Tree tree = idManager.getTree(id);
        return (tree == null || !tree.exists()) ? null : new NodeDelegate(this, tree);
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
        permissionProvider.refresh();
    }

    /**
     * Save the subtree rooted at the given {@code path}.
     * <p>
     * This implementation only performs the save if the subtree rooted at {@code path} contains
     * all transient changes and will throw an
     * {@link javax.jcr.UnsupportedRepositoryOperationException} otherwise.
     *
     * @param path
     * @throws RepositoryException
     */
    public void save(final String path) throws RepositoryException {
        if (denotesRoot(path)) {
            save();
        } else {
            try {
                root.commit(new EditorHook(new EditorProvider() {
                    @Override
                    public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder) {
                        return new ItemSaveValidator(path);
                    }
                }));
            } catch (CommitFailedException e) {
                throw newRepositoryException(e);
            }
        }
        permissionProvider.refresh();
    }

    public void refresh(boolean keepChanges) {
        if (keepChanges && hasPendingChanges()) {
            root.rebase();
        } else {
            root.refresh();
        }
        permissionProvider.refresh();
    }

    //----------------------------------------------------------< Workspace >---

    @Nonnull
    public String getWorkspaceName() {
        return contentSession.getWorkspaceName();
    }

    /**
     * Move a node
     * @param srcPath  oak path to the source node to copy
     * @param destPath  oak path to the destination
     * @param transientOp  whether or not to perform the move in transient space
     * @throws RepositoryException
     */
    public void move(String srcPath, String destPath, boolean transientOp, AccessManager accessManager)
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

        accessManager.checkPermissions(destPath, Permissions.getString(Permissions.NODE_TYPE_MANAGEMENT));

        try {
            if (!moveRoot.move(srcPath, destPath)) {
                throw new RepositoryException("Cannot move node at " + srcPath + " to " + destPath);
            }
            if (!transientOp) {
                moveRoot.commit();
                refresh(true);
            }
        } catch (CommitFailedException e) {
            throw newRepositoryException(e);
        }
    }

    @Nonnull
    public QueryEngine getQueryEngine() {
        return root.getQueryEngine();
    }

    @Nonnull
    public PermissionProvider getPermissionProvider() {
        return permissionProvider;
    }

    /**
     * The current {@code Root} instance this session delegate instance operates on.
     * To ensure the returned root reflects the correct repository revision access
     * should only be done from within a {@link SessionOperation} closure through
     * {@link #perform(SessionOperation)}.
     *
     * @return  current root
     */
    @Nonnull
    public Root getRoot() {
        return root;
    }

    @Override
    public String toString() {
        return contentSession.toString();
    }

    //------------------------------------------------------------< internal >---

    /**
     * Wraps the given {@link CommitFailedException} instance using the
     * appropriate {@link RepositoryException} subclass based on the
     * {@link CommitFailedException#getType() type} of the given exception.
     *
     * @param exception typed commit failure exception
     * @return matching repository exception
     */
    private static RepositoryException newRepositoryException(CommitFailedException exception) {
        return exception.asRepositoryException();
    }

    /**
     * This validator checks that all changes are contained within the subtree
     * rooted at a given path.
     */
    private static class ItemSaveValidator extends SubtreeExcludingValidator {

        /**
         * Name of the property whose {@link #propertyChanged(PropertyState, PropertyState)} to
         * ignore or {@code null} if no property should be ignored.
         */
        private final String ignorePropertyChange;

        /**
         * Create a new validator that only throws a {@link CommitFailedException} whenever
         * there are changes not contained in the subtree rooted at {@code path}.
         * @param path
         */
        public ItemSaveValidator(String path) {
            this(new FailingValidator(CommitFailedException.UNSUPPORTED, 0,
                    "Failed to save subtree at " + path + ". There are " +
                            "transient modifications outside that subtree."),
                    newArrayList(elements(path)));
        }

        private ItemSaveValidator(Validator validator, List<String> path) {
            super(validator, path);
            // Ignore property changes if this is the head of the path.
            // This allows for calling save on a changed property.
            ignorePropertyChange = path.size() == 1 ? path.get(0) : null;
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after)
                throws CommitFailedException {
            if (!before.getName().equals(ignorePropertyChange)) {
                super.propertyChanged(before, after);
            }
        }

        @Override
        protected SubtreeExcludingValidator createValidator(
                Validator validator, final List<String> path) {
            return new ItemSaveValidator(validator, path);
        }
    }
}

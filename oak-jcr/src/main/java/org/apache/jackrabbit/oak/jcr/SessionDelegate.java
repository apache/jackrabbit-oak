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
package org.apache.jackrabbit.oak.jcr;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Map;
import java.util.Timer;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Workspace;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.observation.ObservationManager;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ChangeExtractor;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ConflictHandler;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.DefaultConflictHandler;
import org.apache.jackrabbit.oak.jcr.observation.ObservationManagerImpl;
import org.apache.jackrabbit.oak.jcr.util.LazyValue;
import org.apache.jackrabbit.oak.jcr.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.namepath.AbstractNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.value.AnnotatingConflictHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionDelegate {
    static final Logger log = LoggerFactory.getLogger(SessionDelegate.class);

    private final NamePathMapper namePathMapper = new NamePathMapperImpl(new SessionNameMapper());
    private final Repository repository;
    private final LazyValue<Timer> observationTimer;
    private final ContentSession contentSession;
    private final ValueFactoryImpl valueFactory;
    private final Workspace workspace;
    private final Session session;
    private final Root root;
    private final ConflictHandler conflictHandler;

    private ObservationManagerImpl observationManager;
    private boolean isAlive = true;

    SessionDelegate(Repository repository, LazyValue<Timer> observationTimer, ContentSession contentSession)
            throws RepositoryException {
        assert repository != null;
        assert contentSession != null;

        this.repository = repository;
        this.observationTimer = observationTimer;
        this.contentSession = contentSession;
        this.valueFactory = new ValueFactoryImpl(contentSession.getCoreValueFactory(), namePathMapper);
        this.workspace = new WorkspaceImpl(this);
        this.session = new SessionImpl(this);
        this.root = contentSession.getCurrentRoot();
        this.conflictHandler = new AnnotatingConflictHandler(contentSession.getCoreValueFactory());
    }

    public boolean isAlive() {
        return isAlive;
    }

    @Nonnull
    public Session getSession() {
        return session;
    }

    @Nonnull
    public AuthInfo getAuthInfo() {
        return contentSession.getAuthInfo();
    }

    @Nonnull
    public Repository getRepository() {
        return repository;
    }

    public void logout() {
        if (!isAlive) {
            // ignore
            return;
        }

        isAlive = false;
        if (observationManager != null) {
            observationManager.dispose();
        }
        // TODO

        try {
            contentSession.close();
        } catch (IOException e) {
            log.warn("Error while closing connection", e);
        }
    }

    @Nonnull
    public NodeDelegate getRoot() {
        Tree root = getTree("");
        if (root == null) {
            throw new IllegalStateException("No root node");
        }

        return new NodeDelegate(this, root);
    }

    @CheckForNull
    public NodeDelegate getNode(String path) {
        Tree tree = getTree(path);
        return tree == null ? null : new NodeDelegate(this, tree);
    }

    public NodeDelegate getNode(Node jcrNode) {
        if (jcrNode instanceof NodeImpl) {
            return ((NodeImpl) jcrNode).getNodeDelegate();
        } else {
            throw new IllegalArgumentException("NodeImpl expected");
        }
    }

    @CheckForNull
    public NodeDelegate getNodeByIdentifier(String id) {
        if (id.startsWith("/")) {
            Tree tree = getTree(id);
            return tree == null ? null : new NodeDelegate(this, tree);
        }
        else {
            // referenceable
            return findByJcrUuid(id);
        }
    }

    @Nonnull
    public ValueFactoryImpl getValueFactory() {
        return valueFactory;
    }

    @Nonnull
    public NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    public boolean hasPendingChanges() {
        return root.hasPendingChanges();
    }

    public void save() throws RepositoryException {
        try {
            root.commit(conflictHandler);
        }
        catch (CommitFailedException e) {
            e.throwRepositoryException();
        }
    }

    public void refresh(boolean keepChanges) {
        if (keepChanges) {
            root.rebase(conflictHandler);
        }
        else {
            root.refresh();
        }
    }

    /**
     * Shortcut for {@code SessionDelegate.getNamePathMapper().getOakPath(jcrPath)}.
     *
     * @param jcrPath JCR path
     * @return Oak path, or {@code null}
     */
    @CheckForNull
    public String getOakPathOrNull(String jcrPath) {
        return getNamePathMapper().getOakPath(jcrPath);
    }

    /**
     * Shortcut for {@code SessionDelegate.getOakPathKeepIndex(jcrPath)}.
     *
     * @param jcrPath JCR path
     * @return Oak path, or {@code null}, with indexes left intact
     * @throws PathNotFoundException 
     */
    @Nonnull
    public String getOakPathKeepIndexOrThrowNotFound(String jcrPath) throws PathNotFoundException {
        String oakPath = getNamePathMapper().getOakPathKeepIndex(jcrPath);
        if (oakPath != null) {
            return oakPath;
        } else {
            throw new PathNotFoundException(jcrPath);
        }
    }

    /**
     * Returns the Oak path for the given JCR path, or throws a
     * {@link PathNotFoundException} if the path can not be mapped.
     *
     * @param jcrPath JCR path
     * @return Oak path
     * @throws PathNotFoundException if the path can not be mapped
     */
    @Nonnull
    public String getOakPathOrThrowNotFound(String jcrPath) throws PathNotFoundException {
        String oakPath = getOakPathOrNull(jcrPath);
        if (oakPath != null) {
            return oakPath;
        } else {
            throw new PathNotFoundException(jcrPath);
        }
    }

    /**
     * Returns the Oak path for the given JCR path, or throws a
     * {@link RepositoryException} if the path can not be mapped.
     *
     * @param jcrPath JCR path
     * @return Oak path
     * @throws RepositoryException if the path can not be mapped
     */
    @Nonnull
    public String getOakPathOrThrow(String jcrPath)
            throws RepositoryException {
        String oakPath = getOakPathOrNull(jcrPath);
        if (oakPath != null) {
            return oakPath;
        } else {
            throw new RepositoryException("Invalid name or path: " + jcrPath);
        }
    }

    @Nonnull
    public ChangeExtractor getChangeExtractor() {
        return root.getChangeExtractor();
    }

    //----------------------------------------------------------< Workspace >---

    @Nonnull
    public Workspace getWorkspace() {
        return workspace;
    }

    @Nonnull
    public String getWorkspaceName() {
        return contentSession.getWorkspaceName();
    }

    public void copy(String srcAbsPath, String destAbsPath) throws RepositoryException {
        String srcPath = PathUtils.relativize("/", srcAbsPath);
        String destPath = PathUtils.relativize("/", destAbsPath);

        // check destination
        Tree dest = getTree(destPath);
        if (dest != null) {
            throw new ItemExistsException(destAbsPath);
        }

        // check parent of destination
        String destParentPath = PathUtils.getParentPath(destPath);
        Tree destParent = getTree(destParentPath);
        if (destParent == null) {
            throw new PathNotFoundException(PathUtils.getParentPath(destAbsPath));
        }

        // check source exists
        Tree src = getTree(srcPath);
        if (src == null) {
            throw new PathNotFoundException(srcAbsPath);
        }

        try {
            Root currentRoot = contentSession.getCurrentRoot();
            currentRoot.copy(srcPath, destPath);
            currentRoot.commit(DefaultConflictHandler.OURS);
        }
        catch (CommitFailedException e) {
            e.throwRepositoryException();
        }
    }

    public void move(String srcAbsPath, String destAbsPath, boolean transientOp)
            throws RepositoryException {

        String srcPath = PathUtils.relativize("/", srcAbsPath);
        String destPath = PathUtils.relativize("/", destAbsPath);
        Root moveRoot = transientOp ? root : contentSession.getCurrentRoot();

        // check destination
        Tree dest = moveRoot.getTree(destPath);
        if (dest != null) {
            throw new ItemExistsException(destAbsPath);
        }

        // check parent of destination
        String destParentPath = PathUtils.getParentPath(destPath);
        Tree destParent = moveRoot.getTree(destParentPath);
        if (destParent == null) {
            throw new PathNotFoundException(PathUtils.getParentPath(destAbsPath));
        }

        // check source exists
        Tree src = moveRoot.getTree(srcPath);
        if (src == null) {
            throw new PathNotFoundException(srcAbsPath);
        }

        try {
            moveRoot.move(srcPath, destPath);
            if (!transientOp) {
                moveRoot.commit(DefaultConflictHandler.OURS);
            }
        }
        catch (CommitFailedException e) {
            e.throwRepositoryException();
        }
    }

    @Nonnull
    public LockManager getLockManager() throws RepositoryException {
        return workspace.getLockManager();
    }

    @Nonnull
    public QueryEngine getQueryEngine() {
        return contentSession.getQueryEngine();
    }

    @Nonnull
    public QueryManager getQueryManager() throws RepositoryException {
        return workspace.getQueryManager();
    }

    @Nonnull
    public NodeTypeManager getNodeTypeManager() throws RepositoryException {
        return workspace.getNodeTypeManager();
    }

    @Nonnull
    public VersionManager getVersionManager() throws RepositoryException {
        return workspace.getVersionManager();
    }

    @Nonnull
    public ObservationManager getObservationManager() {
        if (observationManager == null) {
            observationManager = new ObservationManagerImpl(this, observationTimer);
        }
        return observationManager;
    }

    @Nonnull
    public ContentSession getContentSession() {
        return contentSession;
    }

    //-----------------------------------------------------------< internal >---

    @CheckForNull
    Tree getTree(String path) {
        return root.getTree(path);
    }

    @CheckForNull
    NodeDelegate findByJcrUuid(String id) {
        try {
            Map<String, CoreValue> bindings = Collections.singletonMap("id", getValueFactory().getCoreValueFactory()
                    .createValue(id));

            Result result = getQueryEngine().executeQuery("SELECT * FROM [nt:base] WHERE [jcr:uuid] = $id", Query.JCR_SQL2,
                    getContentSession(), Long.MAX_VALUE, 0, bindings, namePathMapper);

            String path = null;

            for (ResultRow rr : result.getRows()) {
                if (path != null) {
                    log.error("multiple results for identifier lookup: " + path + " vs. " + rr.getPath());
                    return null;
                } else {
                    path = rr.getPath();
                }
            }

            return path == null ? null : getNode(path);
        } catch (ParseException ex) {
            log.error("query failed", ex);
            return null;
        }
    }

    //--------------------------------------------------< SessionNameMapper >---

    private class SessionNameMapper extends AbstractNameMapper {

        @Override
        @CheckForNull
        protected String getJcrPrefix(String oakPrefix) {
            try {
                String ns = getWorkspace().getNamespaceRegistry().getURI(oakPrefix);
                return session.getNamespacePrefix(ns);
            } catch (RepositoryException e) {
                log.debug("Could not get JCR prefix for OAK prefix " + oakPrefix);
                return null;
            }
        }

        @Override
        @CheckForNull
        protected String getOakPrefix(String jcrPrefix) {
            try {
                String ns = getSession().getNamespaceURI(jcrPrefix);
                return getWorkspace().getNamespaceRegistry().getPrefix(ns);
            } catch (RepositoryException e) {
                log.debug("Could not get OAK prefix for JCR prefix " + jcrPrefix);
                return null;
            }
        }

        @Override
        @CheckForNull
        protected String getOakPrefixFromURI(String uri) {
            try {
                return getWorkspace().getNamespaceRegistry().getPrefix(uri);
            } catch (RepositoryException e) {
                log.debug("Could not get OAK prefix for URI " + uri);
                return null;
            }
        }

        @Override
        public boolean hasSessionLocalMappings() {
            if (session instanceof SessionImpl) {
                return ((SessionImpl)session).hasSessionLocalMappings();
            }
            else {
                // we don't know
                return true;
            }
        }
    }

    //------------------------------------------------------------< AnnotatingConflictHandler >---

}

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

import javax.annotation.CheckForNull;
import javax.jcr.ItemExistsException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.PathNotFoundException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Workspace;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.query.QueryManager;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.namespace.NamespaceRegistryImpl;
import org.apache.jackrabbit.oak.jcr.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.namepath.AbstractNameMapper;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionDelegate {
    static final Logger log = LoggerFactory.getLogger(SessionDelegate.class);

    private final NameMapper nameMapper = new SessionNameMapper();
    private final NamePathMapper namePathMapper = new NamePathMapperImpl(nameMapper);
    private final Repository repository;
    private final ContentSession contentSession;
    private final ValueFactoryImpl valueFactory;
    private final NamespaceRegistry nsRegistry;
    private final Workspace workspace;
    private final Session session;

    private boolean isAlive = true;
    private Root root;

    SessionDelegate(Repository repository, ContentSession contentSession) throws RepositoryException {
        this.repository = repository;
        this.contentSession = contentSession;
        this.valueFactory = new ValueFactoryImpl(contentSession.getCoreValueFactory(), namePathMapper);
        this.nsRegistry = new NamespaceRegistryImpl(contentSession);
        this.workspace = new WorkspaceImpl(this, nsRegistry);
        this.session = new SessionImpl(this);
        this.root = contentSession.getCurrentRoot();
    }

    public boolean isAlive() {
        return isAlive;
    }

    public Session getSession() {
        return session;
    }

    public AuthInfo getAuthInfo() {
        return contentSession.getAuthInfo();
    }

    public Repository getRepository() {
        return repository;
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

    public NodeDelegate getRoot() {
        return new NodeDelegate(this, getTree(""));
    }

    public NodeDelegate getNode(String path) {
        Tree tree = getTree(path);
        return tree == null ? null : new NodeDelegate(this, tree);
    }

    public NodeDelegate getNodeByIdentifier(String id) {
        // TODO: for now the OAK path is the identifier
        Tree tree = getTree(id);
        return tree == null ? null : new NodeDelegate(this, tree);
    }

    public ValueFactoryImpl getValueFactory() {
        return valueFactory;
    }

    public NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    public boolean hasPendingChanges() {
        return root.hasPendingChanges();
    }

    public void save() throws RepositoryException {
        try {
            root.commit();
            root = contentSession.getCurrentRoot();
        }
        catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    public void refresh(boolean keepChanges) {
        if (keepChanges) {
            root.rebase();
        }
        else {
            root = contentSession.getCurrentRoot();
        }
    }

    /**
     * Shortcut for {@code SessionDelegate.getNamePathMapper().getOakPath(jcrPath)}.
     *
     * @param jcrPath JCR path
     * @return Oak path, or {@code null}
     */
    public String getOakPathOrNull(String jcrPath) {
        return getNamePathMapper().getOakPath(jcrPath);
    }

    /**
     * Returns the Oak path for the given JCR path, or throws a
     * {@link PathNotFoundException} if the path can not be mapped.
     *
     * @param jcrPath JCR path
     * @return Oak path
     * @throws PathNotFoundException if the path can not be mapped
     */
    public String getOakPathOrThrowNotFound(String jcrPath)
            throws PathNotFoundException {
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
    public String getOakPathOrThrow(String jcrPath)
            throws RepositoryException {
        String oakPath = getOakPathOrNull(jcrPath);
        if (oakPath != null) {
            return oakPath;
        } else {
            throw new RepositoryException("Invalid name or path: " + jcrPath);
        }
    }

    //----------------------------------------------------------< Workspace >---

    public Workspace getWorkspace() {
        return workspace;
    }

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
            currentRoot.commit();
        }
        catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    public void move(String srcAbsPath, String destAbsPath, boolean transientOp)
            throws RepositoryException {

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
            if (transientOp) {
                root.move(srcPath, destPath);
            }
            else {
                Root currentRoot = contentSession.getCurrentRoot();
                currentRoot.move(srcPath, destPath);
                currentRoot.commit();
            }
        }
        catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    public LockManager getLockManager() throws RepositoryException {
        return workspace.getLockManager();
    }

    public QueryEngine getQueryEngine() {
        return contentSession.getQueryEngine();
    }

    public QueryManager getQueryManager() throws RepositoryException {
        return workspace.getQueryManager();
    }

    public NodeTypeManager getNodeTypeManager() throws RepositoryException {
        return workspace.getNodeTypeManager();
    }

    public VersionManager getVersionManager() throws RepositoryException {
        return workspace.getVersionManager();
    }

    public ContentSession getContentSession() {
        return contentSession;
    }

    //------------------------------------------------------------< internal >---

    Tree getTree(String path) {
        return root.getTree(path);
    }

    //--------------------------------------------------< SessionNameMapper >---

    private class SessionNameMapper extends AbstractNameMapper {

        @Override
        @CheckForNull
        protected String getJcrPrefix(String oakPrefix) {
            try {
                String ns = nsRegistry.getURI(oakPrefix);
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
                return nsRegistry.getPrefix(ns);
            } catch (RepositoryException e) {
                log.debug("Could not get OAK prefix for JCR prefix " + jcrPrefix);
                return null;
            }
        }

        @Override
        @CheckForNull
        protected String getOakPrefixFromURI(String uri) {
            try {
                return nsRegistry.getPrefix(uri);
            } catch (RepositoryException e) {
                log.debug("Could not get OAK prefix for URI " + uri);
                return null;
            }
        }
    }
}

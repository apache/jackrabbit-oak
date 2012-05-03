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

import javax.jcr.NamespaceRegistry;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Workspace;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.version.VersionManager;
import java.io.IOException;

public class SessionDelegate {
    static final Logger log = LoggerFactory.getLogger(SessionDelegate.class);

    private final NameMapper nameMapper = new SessionNameMapper();
    private final NamePathMapper namePathMapper = new NamePathMapperImpl(nameMapper);
    private final GlobalContext context;
    private final ContentSession contentSession;
    private final ValueFactoryImpl valueFactory;
    private final NamespaceRegistry nsRegistry;
    private final Workspace workspace;
    private final Session session;

    private boolean isAlive = true;
    private Root root;

    public SessionDelegate(GlobalContext context, ContentSession contentSession)
            throws RepositoryException {

        this.context = context;
        this.contentSession = contentSession;
        this.valueFactory = new ValueFactoryImpl(contentSession.getCoreValueFactory(), namePathMapper);
        this.nsRegistry = new NamespaceRegistryImpl(contentSession);
        this.workspace = new WorkspaceImpl(this, this.nsRegistry);
        this.root = contentSession.getCurrentRoot();
        this.session = new SessionImpl(this);
    }

    public Repository getRepository() {
        return context.getInstance(Repository.class);
    }

    public Session getSession() {
        return session;
    }

    public Workspace getWorkspace() {
        return workspace;
    }

    public String getWorkspaceName() {
        return contentSession.getWorkspaceName();
    }

    public boolean isAlive() {
        return isAlive;
    }

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

    public ValueFactoryImpl getValueFactory() {
        return valueFactory;
    }

    public NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    public NodeTypeManager getNodeTypeManager() throws RepositoryException {
        return workspace.getNodeTypeManager();
    }

    public VersionManager getVersionManager() throws RepositoryException {
        return workspace.getVersionManager();
    }

    public LockManager getLockManager() throws RepositoryException {
        return workspace.getLockManager();
    }

    public QueryEngine getQueryEngine() {
        return contentSession.getQueryEngine();
    }

    public Tree getTree(String path) {
        return root.getTree(path);
    }

    public void move(String srcAbsPath, String destAbsPath, boolean transientOp)
            throws RepositoryException {

        String srcPath = PathUtils.relativize("/", srcAbsPath);  // TODO: is this needed?
        String destPath = PathUtils.relativize("/", destAbsPath);
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

    public void copy(String srcAbsPath, String destAbsPath) throws RepositoryException {
        String srcPath = PathUtils.relativize("/", srcAbsPath);
        String destPath = PathUtils.relativize("/", destAbsPath);
        try {
            Root currentRoot = contentSession.getCurrentRoot();
            currentRoot.copy(srcPath, destPath);
            currentRoot.commit();
        }
        catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    public boolean hasPendingChanges() {
        return root.hasPendingChanges();
    }

    public void save() throws RepositoryException {
        try {
            root.commit();
            root = contentSession.getCurrentRoot();
        } catch (CommitFailedException e) {
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

    //-------------------------------------------< SessionNamespaceResolver >---

    private class SessionNameMapper extends AbstractNameMapper {

        @Override
        protected String getJcrPrefix(String oakPrefix) {
            try {
                String ns = nsRegistry.getURI(oakPrefix);
                return session.getNamespacePrefix(ns);
            } catch (RepositoryException e) {
                // TODO
                return null;
            }
        }

        @Override
        protected String getOakPrefix(String jcrPrefix) {
            try {
                String ns = getSession().getNamespaceURI(jcrPrefix);
                return nsRegistry.getPrefix(ns);
            } catch (RepositoryException e) {
                // TODO
                return null;
            }
        }

        @Override
        protected String getOakPrefixFromURI(String uri) {
            try {
                return nsRegistry.getPrefix(uri);
            } catch (RepositoryException e) {
                // TODO
                return null;
            }
        }
    }

}

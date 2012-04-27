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

import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.jcr.nodetype.NodeTypeManagerImpl;
import org.apache.jackrabbit.oak.jcr.query.QueryManagerImpl;
import org.apache.jackrabbit.oak.jcr.security.privileges.PrivilegeManagerImpl;
import org.apache.jackrabbit.oak.namepath.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;

import javax.jcr.NamespaceRegistry;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.observation.ObservationManager;
import javax.jcr.query.QueryManager;
import javax.jcr.version.Version;
import javax.jcr.version.VersionManager;
import java.io.IOException;
import java.io.InputStream;

/**
 * {@code WorkspaceImpl}...
 */
public class WorkspaceImpl implements JackrabbitWorkspace {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(WorkspaceImpl.class);

    private final SessionContext sessionContext;

    private QueryManagerImpl queryManager;

    private final NamespaceRegistry nsRegistry;

    private final NodeTypeManager nodeTypeManager;

    public WorkspaceImpl(SessionContext sessionContext, NamespaceRegistry nsRegistry)
            throws RepositoryException {

        this.sessionContext = sessionContext;
        this.nsRegistry = nsRegistry;
        this.nodeTypeManager =
                new NodeTypeManagerImpl(sessionContext.getNameMapper());
    }

    //----------------------------------------------------------< Workspace >---
    @Override
    public Session getSession() {
        return sessionContext.getSession();
    }

    @Override
    public String getName() {
        return sessionContext.getWorkspaceName();
    }

    @Override
    public void copy(String srcAbsPath, String destAbsPath) throws RepositoryException {
        copy(getName(), srcAbsPath, destAbsPath);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void copy(String srcWorkspace, String srcAbsPath, String destAbsPath) throws RepositoryException {
        ensureSupportedOption(Repository.LEVEL_2_SUPPORTED);
        ensureIsAlive();

        if (!getName().equals(srcWorkspace)) {
            throw new UnsupportedRepositoryOperationException("Not implemented.");
        }

        try {
            ContentSession contentSession = sessionContext.getContentSession();
            Root root = contentSession.getCurrentRoot();

            String srcPath = Paths.relativize("/", srcAbsPath);
            String destPath = Paths.relativize("/", destAbsPath);
            root.copy(srcPath, destPath);

            root.commit();
        }
        catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void clone(String srcWorkspace, String srcAbsPath, String destAbsPath, boolean removeExisting) throws RepositoryException {
        ensureSupportedOption(Repository.LEVEL_2_SUPPORTED);
        ensureIsAlive();

        // TODO -> SPI

    }

    @SuppressWarnings("deprecation")
    @Override
    public void move(String srcAbsPath, String destAbsPath) throws RepositoryException {
        ensureSupportedOption(Repository.LEVEL_2_SUPPORTED);
        ensureIsAlive();

        try {
            ContentSession contentSession = sessionContext.getContentSession();
            Root root = contentSession.getCurrentRoot();

            String srcPath = Paths.relativize("/", srcAbsPath);
            String destPath = Paths.relativize("/", destAbsPath);
            root.move(srcPath, destPath);

            root.commit();
        }
        catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public void restore(Version[] versions, boolean removeExisting) throws RepositoryException {
        getVersionManager().restore(versions, removeExisting);
    }

    @Override
    public LockManager getLockManager() throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_LOCKING_SUPPORTED);
        throw new UnsupportedRepositoryOperationException("TODO: Workspace.getLockManager");
    }

    @Override
    public QueryManager getQueryManager() throws RepositoryException {
        ensureIsAlive();
        if (queryManager == null) {
            queryManager = new QueryManagerImpl(this, sessionContext);
        }
        return queryManager;
    }

    @Override
    public NamespaceRegistry getNamespaceRegistry() {
        return nsRegistry;
    }

    @Override
    public NodeTypeManager getNodeTypeManager() {
        return nodeTypeManager;
    }

    @Override
    public ObservationManager getObservationManager() throws RepositoryException {
        ensureSupportedOption(Repository.OPTION_OBSERVATION_SUPPORTED);
        ensureIsAlive();
        throw new UnsupportedRepositoryOperationException("TODO: Workspace.getObservationManager");
    }

    @Override
    public VersionManager getVersionManager() throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_VERSIONING_SUPPORTED);
        throw new UnsupportedRepositoryOperationException("TODO: Workspace.getVersionManager");
    }

    @Override
    public String[] getAccessibleWorkspaceNames() throws RepositoryException {
        ensureIsAlive();

        // TODO -> SPI
        return new String[] {getName()};
    }

    @SuppressWarnings("deprecation")
    @Override
    public ContentHandler getImportContentHandler(String parentAbsPath, int uuidBehavior) throws RepositoryException {
        ensureSupportedOption(Repository.LEVEL_2_SUPPORTED);
        ensureIsAlive();
        throw new UnsupportedRepositoryOperationException("TODO: Workspace.getImportContentHandler");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void importXML(String parentAbsPath, InputStream in, int uuidBehavior) throws IOException, RepositoryException {
        ensureSupportedOption(Repository.LEVEL_2_SUPPORTED);
        ensureIsAlive();

        // TODO -> SPI
    }

    @Override
    public void createWorkspace(String name) throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
    }

    @Override
    public void createWorkspace(String name, String srcWorkspace) throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
    }

    @Override
    public void deleteWorkspace(String name) throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
    }

    //------------------------------------------------< JackrabbitWorkspace >---

    @Override
    public void createWorkspace(String workspaceName, InputSource workspaceTemplate) throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
    }

    /**
     * @see org.apache.jackrabbit.api.JackrabbitWorkspace#getPrivilegeManager()
     */
    @Override
    public PrivilegeManager getPrivilegeManager() throws RepositoryException {
        return new PrivilegeManagerImpl(sessionContext);
    }


    //------------------------------------------------------------< private >---

    private void ensureIsAlive() throws RepositoryException {
        sessionContext.getSession().ensureIsAlive();
    }

    private void ensureSupportedOption(String option) throws RepositoryException {
        sessionContext.getSession().ensureSupportsOption(option);
    }

}
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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Connection;
import org.apache.jackrabbit.oak.api.NodeStateEditor;
import org.apache.jackrabbit.oak.jcr.query.QueryManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import javax.jcr.NamespaceRegistry;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Workspace;
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
public class WorkspaceImpl implements Workspace {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(WorkspaceImpl.class);

    private final SessionContext<SessionImpl> sessionContext;
    private QueryManagerImpl queryManager;

    public WorkspaceImpl(SessionContext<SessionImpl> sessionContext) {
        this.sessionContext = sessionContext;
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
        getSessionImpl().checkSupportedOption(Repository.LEVEL_2_SUPPORTED);
        getSessionImpl().checkIsAlive();

        try {
            Connection connection = sessionContext.getConnection();
            NodeStateEditor editor = connection.getNodeStateEditor(connection.getCurrentRoot());

            String srcPath = Paths.relativize("/", srcAbsPath);
            String destPath = Paths.relativize("/", destAbsPath);
            editor.copy(srcPath, destPath);

            connection.commit(editor);
        }
        catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void clone(String srcWorkspace, String srcAbsPath, String destAbsPath, boolean removeExisting) throws RepositoryException {
        getSessionImpl().checkSupportedOption(Repository.LEVEL_2_SUPPORTED);
        getSessionImpl().checkIsAlive();

        // TODO -> SPI

    }

    @SuppressWarnings("deprecation")
    @Override
    public void move(String srcAbsPath, String destAbsPath) throws RepositoryException {
        getSessionImpl().checkSupportedOption(Repository.LEVEL_2_SUPPORTED);
        getSessionImpl().checkIsAlive();

        try {
            Connection connection = sessionContext.getConnection();
            NodeStateEditor editor = connection.getNodeStateEditor(connection.getCurrentRoot());

            String srcPath = Paths.relativize("/", srcAbsPath);
            String destPath = Paths.relativize("/", destAbsPath);
            editor.move(srcPath, destPath);

            connection.commit(editor);
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
        getSessionImpl().checkIsAlive();
        getSessionImpl().checkSupportedOption(Repository.OPTION_LOCKING_SUPPORTED);

        // TODO
        return null;
    }

    @Override
    public QueryManager getQueryManager() throws RepositoryException {
        getSessionImpl().checkIsAlive();
        if (queryManager == null) {
            queryManager = new QueryManagerImpl(this, sessionContext);
        }
        return queryManager;
    }

    @Override
    public NamespaceRegistry getNamespaceRegistry() throws RepositoryException {
        getSessionImpl().checkIsAlive();

        // TODO
        return null;
    }

    @Override
    public NodeTypeManager getNodeTypeManager() throws RepositoryException {
        getSessionImpl().checkIsAlive();

        // TODO
        return null;
    }

    @Override
    public ObservationManager getObservationManager() throws RepositoryException {
        getSessionImpl().checkSupportedOption(Repository.OPTION_OBSERVATION_SUPPORTED);
        getSessionImpl().checkIsAlive();

        // TODO
        return null;
    }

    @Override
    public VersionManager getVersionManager() throws RepositoryException {
        getSessionImpl().checkIsAlive();
        getSessionImpl().checkSupportedOption(Repository.OPTION_VERSIONING_SUPPORTED);

        // TODO
        return null;
    }

    @Override
    public String[] getAccessibleWorkspaceNames() throws RepositoryException {
        getSessionImpl().checkIsAlive();

        // TODO -> SPI
        return null;
    }

    @SuppressWarnings("deprecation")
    @Override
    public ContentHandler getImportContentHandler(String parentAbsPath, int uuidBehavior) throws RepositoryException {
        getSessionImpl().checkSupportedOption(Repository.LEVEL_2_SUPPORTED);
        getSessionImpl().checkIsAlive();

        // TODO
        return null;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void importXML(String parentAbsPath, InputStream in, int uuidBehavior) throws IOException, RepositoryException {
        getSessionImpl().checkSupportedOption(Repository.LEVEL_2_SUPPORTED);
        getSessionImpl().checkIsAlive();

        // TODO -> SPI
    }

    @Override
    public void createWorkspace(String name) throws RepositoryException {
        getSessionImpl().checkIsAlive();
        getSessionImpl().checkSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
    }

    @Override
    public void createWorkspace(String name, String srcWorkspace) throws RepositoryException {
        getSessionImpl().checkIsAlive();
        getSessionImpl().checkSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
    }

    @Override
    public void deleteWorkspace(String name) throws RepositoryException {
        getSessionImpl().checkIsAlive();
        getSessionImpl().checkSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
    }

    //------------------------------------------------------------< private >---
    private SessionImpl getSessionImpl() {
        return sessionContext.getSession();
    }
}
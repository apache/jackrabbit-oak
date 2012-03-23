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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import javax.jcr.NamespaceRegistry;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Workspace;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.observation.ObservationManager;
import javax.jcr.query.QueryManager;
import javax.jcr.version.Version;
import javax.jcr.version.VersionManager;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code WorkspaceImpl}...
 */
public class WorkspaceImpl implements Workspace {
    public static final String DEFAULT_WORKSPACE_NAME = "default";

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(WorkspaceImpl.class);

    private final SessionContext<SessionImpl> sessionContext;

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
        getOakSession().checkSupportedOption(Repository.LEVEL_2_SUPPORTED);
        getOakSession().checkIsAlive();

        // TODO -> SPI

    }

    @SuppressWarnings("deprecation")
    @Override
    public void clone(String srcWorkspace, String srcAbsPath, String destAbsPath, boolean removeExisting) throws RepositoryException {
        getOakSession().checkSupportedOption(Repository.LEVEL_2_SUPPORTED);
        getOakSession().checkIsAlive();

        // TODO -> SPI

    }

    @SuppressWarnings("deprecation")
    @Override
    public void move(String srcAbsPath, String destAbsPath) throws RepositoryException {
        getOakSession().checkSupportedOption(Repository.LEVEL_2_SUPPORTED);
        getOakSession().checkIsAlive();

        // TODO -> SPI

    }

    @Override
    public void restore(Version[] versions, boolean removeExisting) throws RepositoryException {
        getVersionManager().restore(versions, removeExisting);
    }

    @Override
    public LockManager getLockManager() throws RepositoryException {
        getOakSession().checkIsAlive();
        getOakSession().checkSupportedOption(Repository.OPTION_LOCKING_SUPPORTED);

        // TODO
        return null;
    }

    @Override
    public QueryManager getQueryManager() throws RepositoryException {
        getOakSession().checkIsAlive();

        // TODO
        return null;
    }

    @Override
    public NamespaceRegistry getNamespaceRegistry() throws RepositoryException {
        getOakSession().checkIsAlive();

        // TODO
        return null;
    }

    @Override
    public NodeTypeManager getNodeTypeManager() throws RepositoryException {
        getOakSession().checkIsAlive();

        // TODO
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public ObservationManager getObservationManager() throws RepositoryException {
        getOakSession().checkSupportedOption(Repository.OPTION_OBSERVATION_SUPPORTED);
        getOakSession().checkIsAlive();

        // TODO
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public VersionManager getVersionManager() throws RepositoryException {
        getOakSession().checkIsAlive();
        getOakSession().checkSupportedOption(Repository.OPTION_VERSIONING_SUPPORTED);

        // TODO
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public String[] getAccessibleWorkspaceNames() throws RepositoryException {
        getOakSession().checkIsAlive();

        MicroKernel microKernel = sessionContext.getMicrokernel();
        String revision = sessionContext.getRevision();
        String json = microKernel.getNodes("/", revision, 0, 0, -1, null);

        JsopReader reader = new JsopTokenizer(json);
        reader.read('{');
        List<String> workspaces = new ArrayList<String>();
        do {
            String name = reader.readString();
            reader.read(':');
            if (name.startsWith(":")) {
                reader.read();
            } else if (reader.matches('{')) {
                reader.read('}');
                workspaces.add(name);
            } else {
                throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
            }
        } while (reader.matches(','));
        reader.read('}');
        reader.read(JsopTokenizer.END);
        
        return workspaces.toArray(new String[workspaces.size()]);
    }

    @SuppressWarnings("deprecation")
    @Override
    public ContentHandler getImportContentHandler(String parentAbsPath, int uuidBehavior) throws RepositoryException {
        getOakSession().checkSupportedOption(Repository.LEVEL_2_SUPPORTED);
        getOakSession().checkIsAlive();

        // TODO
        return null;
    }

    @SuppressWarnings("deprecation")
    @Override
    public void importXML(String parentAbsPath, InputStream in, int uuidBehavior) throws IOException, RepositoryException {
        getOakSession().checkSupportedOption(Repository.LEVEL_2_SUPPORTED);
        getOakSession().checkIsAlive();

        // TODO -> SPI
    }

    @Override
    public void createWorkspace(String name) throws RepositoryException {
        getOakSession().checkIsAlive();
        getOakSession().checkSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        createWorkspace(sessionContext.getMicrokernel(), name);
    }

    @Override
    public void createWorkspace(String name, String srcWorkspace) throws RepositoryException {
        getOakSession().checkIsAlive();
        getOakSession().checkSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
    }

    @Override
    public void deleteWorkspace(String name) throws RepositoryException {
        getOakSession().checkIsAlive();
        getOakSession().checkSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        MicroKernel microKernel = sessionContext.getMicrokernel();
        String revision = microKernel.getHeadRevision();
        microKernel.commit("/", "- \"" + name + '\"', revision, null);
    }

    //------------------------------------------------------------< private >---

    private SessionImpl getOakSession() {
        return sessionContext.getSession();
    }

    static void createWorkspace(MicroKernel microKernel,String name) {
        String revision = microKernel.getHeadRevision();
        microKernel.commit("/", "+ \"" + name + "\" : {}", revision, null);
    }

}
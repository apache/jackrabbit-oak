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
import org.apache.jackrabbit.oak.jcr.nodetype.NodeTypeManagerImpl;
import org.apache.jackrabbit.oak.jcr.query.QueryManagerImpl;
import org.apache.jackrabbit.oak.jcr.security.privileges.PrivilegeManagerImpl;
import org.apache.jackrabbit.oak.plugins.name.NamespaceRegistryImpl;
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

    private final SessionDelegate sessionDelegate;
    private final NodeTypeManager nodeTypeManager;
    private final QueryManagerImpl queryManager;

    public WorkspaceImpl(SessionDelegate sessionDelegate)
            throws RepositoryException {

        this.sessionDelegate = sessionDelegate;
        this.nodeTypeManager = new NodeTypeManagerImpl(sessionDelegate.getValueFactory(), sessionDelegate.getNamePathMapper());
        this.queryManager = new QueryManagerImpl(sessionDelegate);
    }

    //----------------------------------------------------------< Workspace >---
    @Override
    public Session getSession() {
        return sessionDelegate.getSession();
    }

    @Override
    public String getName() {
        return sessionDelegate.getWorkspaceName();
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

        sessionDelegate.copy(
                sessionDelegate.getOakPathOrThrowNotFound(srcAbsPath),
                sessionDelegate.getOakPathOrThrowNotFound(destAbsPath));
    }

    @SuppressWarnings("deprecation")
    @Override
    public void clone(String srcWorkspace, String srcAbsPath, String destAbsPath, boolean removeExisting) throws RepositoryException {
        ensureSupportedOption(Repository.LEVEL_2_SUPPORTED);
        ensureIsAlive();

        // TODO -> SPI
        throw new UnsupportedRepositoryOperationException("Not implemented.");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void move(String srcAbsPath, String destAbsPath) throws RepositoryException {
        ensureSupportedOption(Repository.LEVEL_2_SUPPORTED);
        ensureIsAlive();

        sessionDelegate.move(
                sessionDelegate.getOakPathOrThrowNotFound(srcAbsPath),
                sessionDelegate.getOakPathOrThrowNotFound(destAbsPath),
                false);
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
        return queryManager;
    }

    @Override
    public NamespaceRegistry getNamespaceRegistry() {
        return new NamespaceRegistryImpl(sessionDelegate.getContentSession());
    }

    @Override
    public NodeTypeManager getNodeTypeManager() {
        return nodeTypeManager;
    }

    @Override
    public ObservationManager getObservationManager() throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_OBSERVATION_SUPPORTED);

        // TODO
        throw new UnsupportedRepositoryOperationException("TODO: Workspace.getObservationManager");
    }

    @Override
    public VersionManager getVersionManager() throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_VERSIONING_SUPPORTED);

        // TODO
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
        ensureIsAlive();
        ensureSupportedOption(Repository.LEVEL_2_SUPPORTED);

        // TODO
        throw new UnsupportedRepositoryOperationException("TODO: Workspace.getImportContentHandler");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void importXML(String parentAbsPath, InputStream in, int uuidBehavior) throws IOException, RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.LEVEL_2_SUPPORTED);

        // TODO -> SPI
        throw new UnsupportedRepositoryOperationException("TODO: Workspace.importXML");
    }

    @Override
    public void createWorkspace(String name) throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
        throw new UnsupportedRepositoryOperationException("TODO: Workspace.createWorkspace");
    }

    @Override
    public void createWorkspace(String name, String srcWorkspace) throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
        throw new UnsupportedRepositoryOperationException("TODO: Workspace.createWorkspace");
    }

    @Override
    public void deleteWorkspace(String name) throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
        throw new UnsupportedRepositoryOperationException("TODO: Workspace.deleteWorkspace");
    }

    //------------------------------------------------< JackrabbitWorkspace >---

    @Override
    public void createWorkspace(String workspaceName, InputSource workspaceTemplate) throws RepositoryException {
        ensureIsAlive();
        ensureSupportedOption(Repository.OPTION_WORKSPACE_MANAGEMENT_SUPPORTED);

        // TODO -> SPI
        throw new UnsupportedRepositoryOperationException("TODO: Workspace.createWorkspace");
    }

    /**
     * @see org.apache.jackrabbit.api.JackrabbitWorkspace#getPrivilegeManager()
     */
    @Override
    public PrivilegeManager getPrivilegeManager() throws RepositoryException {
        return new PrivilegeManagerImpl(sessionDelegate);
    }

    //------------------------------------------------------------< private >---

    private void ensureIsAlive() throws RepositoryException {
        // check session status
        if (!sessionDelegate.isAlive()) {
            throw new RepositoryException("This session has been closed.");
        }
    }

    /**
     * Returns true if the repository supports the given option. False otherwise.
     *
     * @param option Any of the option constants defined by {@link Repository}
     * that either returns 'true' or 'false'. I.e.
     * <ul>
     * <li>{@link Repository#LEVEL_1_SUPPORTED}</li>
     * <li>{@link Repository#LEVEL_2_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_ACCESS_CONTROL_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_ACTIVITIES_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_BASELINES_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_JOURNALED_OBSERVATION_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_LIFECYCLE_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_LOCKING_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_NODE_AND_PROPERTY_WITH_SAME_NAME_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_NODE_TYPE_MANAGEMENT_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_OBSERVATION_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_QUERY_SQL_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_RETENTION_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_SHAREABLE_NODES_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_SIMPLE_VERSIONING_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_TRANSACTIONS_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_UNFILED_CONTENT_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_UPDATE_MIXIN_NODE_TYPES_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_UPDATE_PRIMARY_NODE_TYPE_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_VERSIONING_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_WORKSPACE_MANAGEMENT_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_XML_EXPORT_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_XML_IMPORT_SUPPORTED}</li>
     * <li>{@link Repository#WRITE_SUPPORTED}</li>
     * </ul>
     * @return true if the repository supports the given option. False otherwise.
     */
    private boolean isSupportedOption(String option) {
        String desc = sessionDelegate.getSession().getRepository().getDescriptor(option);
        // if the descriptors are not available return true. the missing
        // functionality of the given SPI impl will in this case be detected
        // upon the corresponding SPI call (see JCR-3143).
        return (desc == null) ? true : Boolean.valueOf(desc);
    }

    /**
     * Make sure the repository supports the option indicated by the given string
     * and throw an exception otherwise.
     *
     * @param option Any of the option constants defined by {@link Repository}
     * that either returns 'true' or 'false'. I.e.
     * <ul>
     * <li>{@link Repository#LEVEL_1_SUPPORTED}</li>
     * <li>{@link Repository#LEVEL_2_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_ACCESS_CONTROL_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_ACTIVITIES_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_BASELINES_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_JOURNALED_OBSERVATION_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_LIFECYCLE_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_LOCKING_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_NODE_AND_PROPERTY_WITH_SAME_NAME_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_NODE_TYPE_MANAGEMENT_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_OBSERVATION_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_QUERY_SQL_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_RETENTION_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_SHAREABLE_NODES_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_SIMPLE_VERSIONING_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_TRANSACTIONS_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_UNFILED_CONTENT_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_UPDATE_MIXIN_NODE_TYPES_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_UPDATE_PRIMARY_NODE_TYPE_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_VERSIONING_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_WORKSPACE_MANAGEMENT_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_XML_EXPORT_SUPPORTED}</li>
     * <li>{@link Repository#OPTION_XML_IMPORT_SUPPORTED}</li>
     * <li>{@link Repository#WRITE_SUPPORTED}</li>
     * </ul>
     * @throws UnsupportedRepositoryOperationException If the given option is
     * not supported.
     * @throws RepositoryException If another error occurs.
     * @see javax.jcr.Repository#getDescriptorKeys()
     */
    private void ensureSupportedOption(String option) throws RepositoryException {
        if (!isSupportedOption(option)) {
            throw new UnsupportedRepositoryOperationException(option + " is not supported by this repository.");
        }
    }
}

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

import org.apache.jackrabbit.commons.AbstractSession;
import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Connection;
import org.apache.jackrabbit.oak.api.NodeState;
import org.apache.jackrabbit.oak.api.NodeStateEditor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import javax.jcr.Credentials;
import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.Workspace;
import javax.jcr.retention.RetentionManager;
import javax.jcr.security.AccessControlManager;
import java.io.IOException;
import java.security.AccessControlException;

/**
 * {@code SessionImpl}...
 */
public class SessionImpl extends AbstractSession {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(SessionImpl.class);

    private final Repository repository;
    private final Workspace workspace;
    private final Connection connection;
    private final ValueFactory valueFactory;
    private final GlobalContext globalContext;
    private final SessionContext<SessionImpl> sessionContext = new Context();
    private boolean isAlive = true;

    private NodeStateEditor editor;
    private ItemStateProvider itemStateProvider;

    SessionImpl(GlobalContext globalContext, Repository repository, Connection connection) {

        this.globalContext = globalContext;
        this.repository = repository;
        this.connection = connection;
        this.valueFactory = new ValueFactoryImpl();

        this.editor = connection.getNodeStateEditor(connection.getCurrentRoot());
        this.itemStateProvider = new ItemStateProvider(editor.getTransientState());

        workspace = new WorkspaceImpl(sessionContext);
    }


    //------------------------------------------------------------< Session >---

    @Override
    public Repository getRepository() {
        return repository;
    }

    @Override
    public String getUserID() {
        return connection.getAuthInfo().getUserID();
    }

    @Override
    public String[] getAttributeNames() {
        return connection.getAuthInfo().getAttributeNames();
    }

    @Override
    public Object getAttribute(String name) {
        return connection.getAuthInfo().getAttribute(name);
    }

    @Override
    public Workspace getWorkspace() {
        return workspace;
    }

    /**
     * @see javax.jcr.Session#impersonate(Credentials)
     */
    @Override
    public Session impersonate(Credentials credentials) throws RepositoryException {
        checkIsAlive();

        // TODO
        return null;
    }

    @Override
    public ValueFactory getValueFactory() throws RepositoryException {
        checkIsAlive();
        return sessionContext.getValueFactory();
    }

    //------------------------------------------------------------< Reading >---

    @Override
    public Node getRootNode() throws RepositoryException {
        checkIsAlive();
        return new NodeImpl(sessionContext, itemStateProvider.getNodeState("/"));
    }

    @Override
    public Node getNodeByUUID(String uuid) throws RepositoryException {
        checkIsAlive();

        // TODO
        return null;
    }

    @Override
    public Node getNodeByIdentifier(String id) throws RepositoryException {
        checkIsAlive();

        // TODO
        return null;
    }

    //------------------------------------------------------------< Writing >---

    @Override
    public void move(String srcAbsPath, String destAbsPath) throws RepositoryException {
        checkIsAlive();

        String srcPath = PathUtils.relativize("/", srcAbsPath);
        String destPath = PathUtils.relativize("/", destAbsPath);
        editor.move(srcPath, destPath);
    }

    //------------------------------------------------------------< state >---

    @Override
    public void save() throws RepositoryException {
        checkIsAlive();
        try {
            NodeState newState = connection.commit(editor);
            editor = connection.getNodeStateEditor(newState);
            itemStateProvider = new ItemStateProvider(editor.getTransientState());
        } catch (CommitFailedException e) {
            throw new RepositoryException(e);
        }
    }

    @Override
    public void refresh(boolean keepChanges) throws RepositoryException {
        checkIsAlive();
        connection.refresh();
        if (!keepChanges) {
            editor = connection.getNodeStateEditor(connection.getCurrentRoot());
            itemStateProvider = new ItemStateProvider(editor.getTransientState());
        }
    }

    @Override
    public boolean hasPendingChanges() throws RepositoryException {
        checkIsAlive();

        // todo implement hasPendingChanges
        return false;
    }

    //----------------------------------------------------------< Lifecycle >---

    @Override
    public boolean isLive() {
        return isAlive;
    }


    @Override
    public void logout() {
        if (!isAlive) {
            // ignore
            return;
        }

        isAlive = false;
        // TODO

        try {
            connection.close();
        } catch (IOException e) {
            log.warn("Error while closing connection", e);
        }
    }

    //----------------------------------------------------< Import / Export >---

    @Override
    public ContentHandler getImportContentHandler(String parentAbsPath, int uuidBehavior) throws RepositoryException {
        checkIsAlive();

        // TODO
        return null;
    }

    //------------------------------------------------------------< Locking >---

    /**
     * @see javax.jcr.Session#addLockToken(String)
     */
    @Override
    public void addLockToken(String lt) {
        try {
            getWorkspace().getLockManager().addLockToken(lt);
        } catch (RepositoryException e) {
            log.warn("Unable to add lock token '{}' to this session: {}", lt, e.getMessage());
        }
    }

    /**
     * @see javax.jcr.Session#getLockTokens()
     */
    @Override
    public String[] getLockTokens() {
        try {
            return getWorkspace().getLockManager().getLockTokens();
        } catch (RepositoryException e) {
            log.warn("Unable to retrieve lock tokens for this session: {}", e.getMessage());
            return new String[0];        }
    }

    /**
     * @see javax.jcr.Session#removeLockToken(String)
     */
    @Override
    public void removeLockToken(String lt) {
        try {
            getWorkspace().getLockManager().addLockToken(lt);
        } catch (RepositoryException e) {
            log.warn("Unable to add lock token '{}' to this session: {}", lt, e.getMessage());
        }
    }

    //------------------------------------------------------< AccessControl >---

    @Override
    public boolean hasPermission(String absPath, String actions) throws RepositoryException {
        checkIsAlive();

        // TODO
        return false;
    }

    /**
     * @see javax.jcr.Session#checkPermission(String, String)
     */
    @Override
    public void checkPermission(String absPath, String actions) throws AccessControlException, RepositoryException {
        if (!hasPermission(absPath, actions)) {
            throw new AccessControlException("Access control violation: path = " + absPath + ", actions = " + actions);
        }
    }

    @Override
    public boolean hasCapability(String methodName, Object target, Object[] arguments) throws RepositoryException {
        checkIsAlive();

        // TODO
        return false;
    }

    @Override
    public AccessControlManager getAccessControlManager() throws RepositoryException {
        checkIsAlive();

        // TODO
        return null;
    }

    //----------------------------------------------------------< Retention >---

    @Override
    public RetentionManager getRetentionManager() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Retention Management is not supported.");
    }

    //--------------------------------------------------------------------------

    /**
     * Performs a sanity check on this session.
     *
     * @throws RepositoryException if this session has been rendered invalid
     * for some reason (e.g. if this session has been closed explicitly by logout)
     */
    void checkIsAlive() throws RepositoryException {
        // check session status
        if (!isAlive) {
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
    boolean isSupportedOption(String option) {
        String desc = getRepository().getDescriptor(option);
        // if the descriptors are not available return true. the missing
        // functionality of the given SPI impl will in this case be detected
        // upon the corresponding SPI call (see JCR-3143).
        return (desc == null) ? true : Boolean.valueOf(desc);
    }

    /**
     * Make sure the repository supports the option indicated by the given string.
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
    void checkSupportedOption(String option) throws RepositoryException {
        if (!isSupportedOption(option)) {
            throw new UnsupportedRepositoryOperationException(option + " is not supported by this repository.");
        }
    }

    /**
     * Checks if this session has pending changes.
     *
     * @throws InvalidItemStateException if this nodes session has pending changes
     * @throws RepositoryException
     */
    void checkHasPendingChanges() throws RepositoryException {
        // check for pending changes
        if (hasPendingChanges()) {
            String msg = "Unable to perform operation. Session has pending changes.";
            log.debug(msg);
            throw new InvalidItemStateException(msg);
        }
    }

    //--------------------------------------------------------------------------

    private class Context implements SessionContext<SessionImpl> {

        @Override
        public SessionImpl getSession() {
            return SessionImpl.this;
        }

        @Override
        public GlobalContext getGlobalContext() {
            return globalContext;
        }

        @Override
        public String getWorkspaceName() {
            return connection.getWorkspaceName();
        }

        @Override
        public Connection getConnection() {
            return connection;
        }

        @Override
        public ValueFactory getValueFactory() {
            return valueFactory;
        }

        @Override
        public ItemStateProvider getItemStateProvider() {
            return itemStateProvider;
        }
    }
}
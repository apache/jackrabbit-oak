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
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.jcr.security.Authenticator;
import org.apache.jackrabbit.oak.jcr.security.CredentialsInfo;
import org.apache.jackrabbit.oak.jcr.state.NodeStateProvider;
import org.apache.jackrabbit.oak.jcr.state.TransientNodeState;
import org.apache.jackrabbit.oak.jcr.state.TransientSpace;
import org.apache.jackrabbit.oak.jcr.util.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import javax.jcr.AccessDeniedException;
import javax.jcr.Credentials;
import javax.jcr.InvalidItemStateException;
import javax.jcr.ItemExistsException;
import javax.jcr.ItemNotFoundException;
import javax.jcr.LoginException;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.ReferentialIntegrityException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.Workspace;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.retention.RetentionManager;
import javax.jcr.security.AccessControlManager;
import javax.jcr.version.VersionException;
import java.security.AccessControlException;

/**
 * {@code SessionImpl}...
 */
public class SessionImpl extends AbstractSession {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(SessionImpl.class);

    private final Workspace workspace;
    private final GlobalContext globalContext;
    private final CredentialsInfo credentialsInfo;
    private final String workspaceName;
    private final MicroKernel microKernel;
    private final TransientSpace transientSpace;
    private final NodeStateProvider nodeStateProvider;

    private String revision;
    private boolean isAlive = true;

    public static final SessionFactory FACTORY = new SessionFactory() {
        @Override
        public Session createSession(GlobalContext globalContext, Credentials credentials,
                String workspaceName) throws LoginException, NoSuchWorkspaceException {

            Authenticator authenticator = globalContext.getInstance(Authenticator.class);
            CredentialsInfo credentialsInfo = authenticator.authenticate(credentials);
            MicroKernel microKernel = globalContext.getInstance(MicroKernel.class);
            String revision = microKernel.getHeadRevision();

            if (workspaceName == null) {
                workspaceName = WorkspaceImpl.DEFAULT_WORKSPACE_NAME;
            }

            if (!microKernel.nodeExists('/' + workspaceName, revision)) {
                if (WorkspaceImpl.DEFAULT_WORKSPACE_NAME.equals(workspaceName)) {
                    WorkspaceImpl.createWorkspace(microKernel, workspaceName);
                    revision = microKernel.getHeadRevision();
                }
                else {
                    throw new NoSuchWorkspaceException(workspaceName);
                }
            }

            return new SessionImpl(globalContext, credentialsInfo, workspaceName, revision);
        }
    };

    public interface Context extends SessionContext<SessionImpl>{}

    private final Context sessionContext = new Context() {
        @Override
        public SessionImpl getSession() {
            return SessionImpl.this;
        }

        @Override
        public GlobalContext getGlobalContext() {
            return globalContext;
        }

        @Override
        public CredentialsInfo getCredentialsInfo() {
            return credentialsInfo;
        }

        @Override
        public String getWorkspaceName() {
            return workspaceName;
        }

        @Override
        public MicroKernel getMicrokernel() {
            return microKernel;
        }

        @Override
        public String getRevision() {
            return revision;
        }

        @Override
        public ValueFactory getValueFactory() {
            return globalContext.getInstance(ValueFactory.class);
        }

        @Override
        public NodeStateProvider getNodeStateProvider() {
            return nodeStateProvider;
        }
    };

    private SessionImpl(GlobalContext globalContext, CredentialsInfo credentialsInfo, String workspaceName,
            String revision) {

        this.globalContext = globalContext;
        this.credentialsInfo = credentialsInfo;
        this.workspaceName = workspaceName;
        this.revision = revision;
        workspace = new WorkspaceImpl(sessionContext);
        microKernel = globalContext.getInstance(MicroKernel.class);
        transientSpace = new TransientSpace(workspaceName, microKernel, revision);
        nodeStateProvider = new NodeStateProvider(sessionContext, transientSpace);
    }


    //------------------------------------------------------------< Session >---
    @Override
    public Repository getRepository() {
        return globalContext.getInstance(Repository.class);
    }

    @Override
    public String getUserID() {
        return credentialsInfo.getUserId();
    }

    @Override
    public String[] getAttributeNames() {
        return credentialsInfo.getAttributeNames();
    }

    @Override
    public Object getAttribute(String name) {
        return credentialsInfo.getAttribute(name);
    }

    @Override
    public Workspace getWorkspace() {
        return workspace;
    }

    @Override
    public Node getRootNode() throws RepositoryException {
        checkIsAlive();
        return NodeImpl.create(sessionContext, Path.create(workspaceName));
    }

    @Override
    public Node getNodeByUUID(String uuid) throws ItemNotFoundException, RepositoryException {
        checkIsAlive();

        // TODO
        return null;
    }

    @Override
    public Node getNodeByIdentifier(String id) throws ItemNotFoundException, RepositoryException {
        checkIsAlive();

        // TODO
        return null;
    }

    @Override
    public void move(String srcAbsPath, String destAbsPath) throws ItemExistsException, PathNotFoundException, VersionException, ConstraintViolationException, LockException, RepositoryException {
        checkIsAlive();
        Path sourcePath = Path.create(workspaceName, srcAbsPath);
        TransientNodeState sourceParent = nodeStateProvider.getNodeState(sourcePath.getParent());
        if (sourceParent == null) {
            throw new PathNotFoundException(srcAbsPath);
        }

        sourceParent.move(sourcePath.getName(), Path.create(workspaceName, destAbsPath));
    }

    @Override
    public void save() throws AccessDeniedException, ItemExistsException, ReferentialIntegrityException, ConstraintViolationException, InvalidItemStateException, VersionException, LockException, NoSuchNodeTypeException, RepositoryException {
        checkIsAlive();
        revision = transientSpace.save();
        nodeStateProvider.clear();
    }

    @Override
    public void refresh(boolean keepChanges) throws RepositoryException {
        checkIsAlive();
        revision = transientSpace.refresh(keepChanges);
        nodeStateProvider.clear();
    }

    @Override
    public boolean hasPendingChanges() throws RepositoryException {
        checkIsAlive();
        return transientSpace.isDirty();
    }

    @Override
    public ValueFactory getValueFactory() throws UnsupportedRepositoryOperationException, RepositoryException {
        checkIsAlive();
        return sessionContext.getValueFactory();
    }

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
    public ContentHandler getImportContentHandler(String parentAbsPath, int uuidBehavior) throws PathNotFoundException, ConstraintViolationException, VersionException, LockException, RepositoryException {
        checkIsAlive();

        // TODO
        return null;
    }

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
    }

    /**
     * @see javax.jcr.Session#impersonate(Credentials)
     */
    @Override
    public Session impersonate(Credentials credentials) throws LoginException, RepositoryException {
        checkIsAlive();

        // TODO
        return null;
    }

    @Override
    public void addLockToken(String lt) {
        try {
            getWorkspace().getLockManager().addLockToken(lt);
        } catch (RepositoryException e) {
            log.warn("Unable to add lock token '" +lt+ "' to this session.", e);
        }
    }

    @Override
    public String[] getLockTokens() {
        try {
            return getWorkspace().getLockManager().getLockTokens();
        } catch (RepositoryException e) {
            log.warn("Unable to retrieve lock tokens for this session. (" + e.getMessage() + ")");
            return new String[0];        }
    }

    @Override
    public void removeLockToken(String lt) {
        try {
            getWorkspace().getLockManager().addLockToken(lt);
        } catch (RepositoryException e) {
            log.warn("Unable to add lock token '" +lt+ "' to this session.", e);
        }
    }

    @Override
    public AccessControlManager getAccessControlManager() throws UnsupportedRepositoryOperationException, RepositoryException {
        checkIsAlive();

        // TODO
        return null;
    }

    @Override
    public RetentionManager getRetentionManager() throws UnsupportedRepositoryOperationException, RepositoryException {
        throw new UnsupportedRepositoryOperationException("Retention Management is not supported.");
    }

    //------------------------------------------------------< check methods >---
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
     * @throws UnsupportedRepositoryOperationException
     * @throws RepositoryException
     * @see javax.jcr.Repository#getDescriptorKeys()
     */
    void checkSupportedOption(String option) throws UnsupportedRepositoryOperationException, RepositoryException {
        if (!isSupportedOption(option)) {
            throw new UnsupportedRepositoryOperationException(option + " is not supported by this repository.");
        }
    }

    /**
     * Checks if this nodes session has pending changes.
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

    //------------------------------------------------------------< private >---

}
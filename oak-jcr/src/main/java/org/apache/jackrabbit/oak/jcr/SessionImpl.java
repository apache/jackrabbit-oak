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

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.AbstractSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.Workspace;
import javax.jcr.retention.RetentionManager;
import javax.jcr.security.AccessControlManager;
import java.security.AccessControlException;

/**
 * {@code SessionImpl}...
 */
public class SessionImpl extends AbstractSession implements JackrabbitSession {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(SessionImpl.class);

    private final SessionDelegate dlg;

    SessionImpl(SessionDelegate dlg) {
        this.dlg = dlg;
    }

    //------------------------------------------------------------< Session >---

    @Override
    public Repository getRepository() {
        return dlg.getRepository();
    }

    @Override
    public String getUserID() {
        return dlg.getAuthInfo().getUserID();
    }

    @Override
    public String[] getAttributeNames() {
        return dlg.getAuthInfo().getAttributeNames();
    }

    @Override
    public Object getAttribute(String name) {
        return dlg.getAuthInfo().getAttribute(name);
    }

    @Override
    public Workspace getWorkspace() {
        return dlg.getWorkspace();
    }

    @Override
    public Session impersonate(Credentials credentials) throws RepositoryException {
        ensureIsAlive();

        // TODO
        throw new UnsupportedRepositoryOperationException("TODO: Session.impersonate");
    }

    @Override
    public ValueFactory getValueFactory() throws RepositoryException {
        ensureIsAlive();
        return dlg.getValueFactory();
    }

    @Override
    public Node getRootNode() throws RepositoryException {
        ensureIsAlive();
        return new NodeImpl(dlg.getRoot());
    }

    @Override
    public Node getNodeByUUID(String uuid) throws RepositoryException {
        ensureIsAlive();
        throw new UnsupportedRepositoryOperationException("TODO: Session.getNodeByUUID");
    }

    @Override
    public Node getNodeByIdentifier(String id) throws RepositoryException {
        ensureIsAlive();
        // TODO following line throws IndexOutOfBoundsException if id is empty
        if (id.charAt(0) == '/') {
            return getNode(id);
        } else {
            // TODO
            throw new UnsupportedRepositoryOperationException("TODO: Session.getNodeByIdentifier");
        }
    }

    @Override
    public void move(String srcAbsPath, String destAbsPath) throws RepositoryException {
        ensureIsAlive();
        dlg.move(dlg.getOakPath(srcAbsPath), dlg.getOakPath(destAbsPath), true);
    }

    @Override
    public void save() throws RepositoryException {
        ensureIsAlive();
        dlg.save();
    }

    @Override
    public void refresh(boolean keepChanges) throws RepositoryException {
        ensureIsAlive();
        dlg.refresh(keepChanges);
    }

    @Override
    public boolean hasPendingChanges() throws RepositoryException {
        ensureIsAlive();
        return dlg.hasPendingChanges();
    }

    @Override
    public boolean isLive() {
        return dlg.isAlive();
    }


    @Override
    public void logout() {
        dlg.logout();
    }

    @Override
    public ContentHandler getImportContentHandler(String parentAbsPath, int uuidBehavior) throws RepositoryException {
        ensureIsAlive();

        // TODO
        String internalPath = dlg.getOakPath(parentAbsPath);
        throw new UnsupportedRepositoryOperationException("TODO: Session.getImportContentHandler");
    }

    /**
     * @see javax.jcr.Session#addLockToken(String)
     */
    @Override
    public void addLockToken(String lt) {
        try {
            dlg.getLockManager().addLockToken(lt);
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
            return dlg.getLockManager().getLockTokens();
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
            dlg.getLockManager().addLockToken(lt);
        } catch (RepositoryException e) {
            log.warn("Unable to add lock token '{}' to this session: {}", lt, e.getMessage());
        }
    }

    @Override
    public boolean hasPermission(String absPath, String actions) throws RepositoryException {
        ensureIsAlive();
        String internalPath = dlg.getOakPath(absPath);

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
        ensureIsAlive();

        // TODO
        return false;
    }

    @Override
    public AccessControlManager getAccessControlManager() throws RepositoryException {
        ensureIsAlive();
        throw new UnsupportedRepositoryOperationException("TODO: Session.getAccessControlManager");
    }

    /**
     * @see javax.jcr.Session#getRetentionManager()
     */
    @Override
    public RetentionManager getRetentionManager() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Retention Management is not supported.");
    }

    //--------------------------------------------------< JackrabbitSession >---

    @Override
    public PrincipalManager getPrincipalManager() throws RepositoryException {
        // TODO
        throw new UnsupportedOperationException("Implementation missing");
    }

    @Override
    public UserManager getUserManager() throws RepositoryException {
        // TODO
        throw new UnsupportedOperationException("Implementation missing");
    }

    //------------------------------------------------------------< private >---

    /**
     * Ensure that this session is alive and throw an exception otherwise.
     *
     * @throws RepositoryException if this session has been rendered invalid
     * for some reason (e.g. if this session has been closed explicitly by logout)
     */
    private void ensureIsAlive() throws RepositoryException {
        // check session status
        if (!dlg.isAlive()) {
            throw new RepositoryException("This session has been closed.");
        }
    }
}
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
package org.apache.jackrabbit.oak.jcr.session;

import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.jcr.session.SessionImpl.checkIndexOnName;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;
import javax.jcr.InvalidSerializedDataException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.observation.ObservationManager;
import javax.jcr.query.QueryManager;
import javax.jcr.version.Version;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.commons.xml.ParsingContentHandler;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.WorkspaceDelegate;
import org.apache.jackrabbit.oak.jcr.lock.LockManagerImpl;
import org.apache.jackrabbit.oak.jcr.query.QueryManagerImpl;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.jcr.version.VersionManagerImpl;
import org.apache.jackrabbit.oak.jcr.xml.ImportHandler;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.plugins.nodetype.write.ReadWriteNodeTypeManager;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * TODO document
 */
public class WorkspaceImpl implements JackrabbitWorkspace {

    private final SessionContext sessionContext;
    private final SessionDelegate sessionDelegate;
    private final WorkspaceDelegate workspaceDelegate;
    private final QueryManagerImpl queryManager;
    private final VersionManagerImpl versionManager;
    private final ReadWriteNodeTypeManager nodeTypeManager;

    public WorkspaceImpl(final SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.sessionDelegate = sessionContext.getSessionDelegate();
        this.workspaceDelegate = new WorkspaceDelegate(sessionContext);
        this.queryManager = new QueryManagerImpl(sessionContext);
        this.versionManager = new VersionManagerImpl(sessionContext);
        this.nodeTypeManager = new ReadWriteNodeTypeManager() {
            @Override
            protected void refresh() throws RepositoryException {
                getSession().refresh(true);
            }

            @Override
            protected Tree getTypes() {
                return sessionDelegate.getRoot().getTree(NODE_TYPES_PATH);
            }

            @Nonnull
            @Override
            protected Root getWriteRoot() {
                return sessionDelegate.getContentSession().getLatestRoot();
            }

            @Override
            @Nonnull
            protected ValueFactory getValueFactory() {
                return sessionContext.getValueFactory();
            }

            @Nonnull
            @Override
            protected NamePathMapper getNamePathMapper() {
                return sessionContext;
            }
        };
    }

    //----------------------------------------------------------< Workspace >---
    @Override
    @Nonnull
    public Session getSession() {
        return sessionContext.getSession();
    }

    @Override
    public String getName() {
        return sessionDelegate.getWorkspaceName();
    }

    @Override
    public void copy(String srcAbsPath, String destAbsPath) throws RepositoryException {
        copy(getName(), srcAbsPath, destAbsPath);
    }

    private String getOakPathOrThrowNotFound(String srcAbsPath) throws PathNotFoundException {
        return sessionContext.getOakPathOrThrowNotFound(srcAbsPath);
    }

    @Override
    public void copy(String srcWorkspace,
                     String srcAbsPath,
                     final String destAbsPath) throws RepositoryException {
        final String srcOakPath = getOakPathOrThrowNotFound(srcAbsPath);
        final String destOakPath = getOakPathOrThrowNotFound(destAbsPath);

        if (!getName().equals(srcWorkspace)) {
            throw new UnsupportedRepositoryOperationException("Not implemented.");
        }

        sessionDelegate.performVoid(new SessionOperation<Void>("copy", true) {
            @Override
            public void checkPreconditions() throws RepositoryException {
                super.checkPreconditions();
                ensureIsAlive();
            }

            @Override
            public void performVoid() throws RepositoryException {
                sessionDelegate.checkProtectedNode(getParentPath(destOakPath));

                checkIndexOnName(destAbsPath);

                workspaceDelegate.copy(srcOakPath, destOakPath);
            }
        });

    }

    @Override
    public void clone(String srcWorkspace, String srcAbsPath, String destAbsPath, boolean removeExisting) throws RepositoryException {
        final String srcOakPath = getOakPathOrThrowNotFound(srcAbsPath);
        final String destOakPath = getOakPathOrThrowNotFound(destAbsPath);

        sessionDelegate.performVoid(new SessionOperation<Void>("clone", true) {

            @Override
            public void checkPreconditions() throws RepositoryException {
                super.checkPreconditions();
                ensureIsAlive();
            }

            @Override
            public void performVoid() throws RepositoryException {
                sessionDelegate.checkProtectedNode(getParentPath(srcOakPath));
                sessionDelegate.checkProtectedNode(getParentPath(destOakPath));
                throw new UnsupportedRepositoryOperationException("Not implemented.");
            }
        });
    }

    @Override
    public void move(String srcAbsPath, final String destAbsPath) throws RepositoryException {
        final String srcOakPath = getOakPathOrThrowNotFound(srcAbsPath);
        final String destOakPath = getOakPathOrThrowNotFound(destAbsPath);

        ensureIsAlive();
        sessionDelegate.checkProtectedNode(getParentPath(srcOakPath));
        sessionDelegate.checkProtectedNode(getParentPath(destOakPath));

        checkIndexOnName(destAbsPath);
        sessionDelegate.move(srcOakPath, destOakPath, false);
    }

    @Override
    public void restore(Version[] versions, boolean removeExisting) throws RepositoryException {
        getVersionManager().restore(versions, removeExisting);
    }

    @Override
    public LockManagerImpl getLockManager() {
        return new LockManagerImpl(sessionContext);
    }

    @Override
    public QueryManager getQueryManager() throws RepositoryException {
        ensureIsAlive();
        return queryManager;
    }

    @Override
    public NamespaceRegistry getNamespaceRegistry() {
        return new ReadWriteNamespaceRegistry(sessionDelegate.getRoot()) {
            @Override
            protected Root getWriteRoot() {
                return sessionDelegate.getContentSession().getLatestRoot();
            }
            @Override
            protected void refresh() throws RepositoryException {
                getSession().refresh(true);
            }
        };
    }

    @Override
    public ReadWriteNodeTypeManager getNodeTypeManager() {
        return nodeTypeManager;
    }

    @Override
    public ObservationManager getObservationManager() throws RepositoryException {
        ensureIsAlive();

        return sessionContext.getObservationManager();
    }

    @Override
    public VersionManager getVersionManager() throws RepositoryException {
        ensureIsAlive();
        return versionManager;
    }

    @Override
    public String[] getAccessibleWorkspaceNames() throws RepositoryException {
        ensureIsAlive();
        // FIXME: adjust implementation once OAK-118 is being addressed.
        return new String[]{getName()};
    }

    @Override
    public ContentHandler getImportContentHandler(String parentAbsPath, int uuidBehavior) throws RepositoryException {
        ensureIsAlive();

        return new ImportHandler(parentAbsPath, sessionContext, uuidBehavior, true);
    }

    @Override
    public void importXML(String parentAbsPath, InputStream in, int uuidBehavior) throws IOException, RepositoryException {
        ensureIsAlive();

        try {
            ContentHandler handler = getImportContentHandler(parentAbsPath, uuidBehavior);
            new ParsingContentHandler(handler).parse(in);
        } catch (SAXException e) {
            Throwable exception = e.getException();
            if (exception instanceof RepositoryException) {
                throw (RepositoryException) exception;
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
            } else if (exception instanceof CommitFailedException) {
                throw ((CommitFailedException) exception).asRepositoryException();
            } else {
                throw new InvalidSerializedDataException("XML parse error", e);
            }
        } finally {
            // JCR-2903
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    @Override
    public void createWorkspace(String name) throws RepositoryException {
        ensureIsAlive();
        throw new UnsupportedRepositoryOperationException("OAK-118: Workspace.createWorkspace");
    }

    @Override
    public void createWorkspace(String name, String srcWorkspace) throws RepositoryException {
        ensureIsAlive();
        throw new UnsupportedRepositoryOperationException("OAK-118: Workspace.createWorkspace");
    }

    @Override
    public void deleteWorkspace(String name) throws RepositoryException {
        ensureIsAlive();
        throw new UnsupportedRepositoryOperationException("OAK-118: Workspace.deleteWorkspace");
    }

    //------------------------------------------------< JackrabbitWorkspace >---

    @Override
    public void createWorkspace(String workspaceName, InputSource workspaceTemplate) throws RepositoryException {
        ensureIsAlive();
        throw new UnsupportedRepositoryOperationException("OAK-118: Workspace.createWorkspace");
    }

    /**
     * @see org.apache.jackrabbit.api.JackrabbitWorkspace#getPrivilegeManager()
     */
    @Override
    public PrivilegeManager getPrivilegeManager() throws RepositoryException {
        return sessionContext.getPrivilegeManager();
    }

    //------------------------------------------------------------< private >---

    private void ensureIsAlive() throws RepositoryException {
        // check session status
        if (!sessionDelegate.isAlive()) {
            throw new RepositoryException("This session has been closed.");
        }
    }

}

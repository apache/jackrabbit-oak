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

import static com.google.common.collect.Sets.newTreeSet;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.SESSION_COUNT;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.AccessControlException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.Credentials;
import javax.jcr.InvalidSerializedDataException;
import javax.jcr.Item;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.Workspace;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.retention.RetentionManager;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import org.apache.jackrabbit.commons.xml.DocumentViewExporter;
import org.apache.jackrabbit.commons.xml.Exporter;
import org.apache.jackrabbit.commons.xml.ParsingContentHandler;
import org.apache.jackrabbit.commons.xml.SystemViewExporter;
import org.apache.jackrabbit.commons.xml.ToXmlContentHandler;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.delegate.ItemDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.PropertyDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.jcr.xml.ImportHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * TODO document
 */
public class SessionImpl implements JackrabbitSession {
    private static final Logger log = LoggerFactory.getLogger(SessionImpl.class);

    private final SessionContext sessionContext;
    private final SessionDelegate sd;
    private final AtomicLong sessionCounter;

    public SessionImpl(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.sd = sessionContext.getSessionDelegate();
        this.sessionCounter = sessionContext.getCounter(SESSION_COUNT);
        sessionCounter.incrementAndGet();
        sessionContext.getCounter(Type.SESSION_LOGIN_COUNTER).incrementAndGet();
    }

    static void checkIndexOnName(SessionContext sessionContext, String path) throws RepositoryException {
        String oakPath = sessionContext.getOakPathKeepIndex(path);
        if (oakPath != null) {
            if (PathUtils.getName(oakPath).contains("[")) {
                throw new RepositoryException("Cannot create a new node using a name including an index");
            }
        }
    }

    private abstract class ReadOperation<T> extends SessionOperation<T> {
        @Override
        public void checkPreconditions() throws RepositoryException {
            sd.checkAlive();
        }
    }

    private abstract class WriteOperation<T> extends SessionOperation<T> {
        @Override
        public void checkPreconditions() throws RepositoryException {
            sd.checkAlive();
        }

        @Override
        public boolean isUpdate() {
            return true;
        }
    }

    @CheckForNull
    private <T> T perform(@Nonnull SessionOperation<T> op) throws RepositoryException {
        return sd.perform(op);
    }

    @CheckForNull
    private <T> T safePerform(@Nonnull SessionOperation<T> op) {
        return sd.safePerform(op);
    }

    @Nonnull
    private String getOakPathOrThrow(String absPath) throws RepositoryException {
        String p = sessionContext.getOakPathOrThrow(absPath);
        if (!PathUtils.isAbsolute(p)) {
            throw new RepositoryException("Not an absolute path: " + absPath);
        }
        return p;
    }

    @Nonnull
    private String getOakPathOrThrowNotFound(String absPath) throws PathNotFoundException {
        return sessionContext.getOakPathOrThrowNotFound(absPath);
    }

    @CheckForNull
    private ItemImpl<?> getItemInternal(@Nonnull String oakPath)
            throws RepositoryException {
        ItemDelegate item = sd.getItem(oakPath);
        if (item instanceof NodeDelegate) {
            return NodeImpl.createNodeOrNull((NodeDelegate) item, sessionContext);
        } else if (item instanceof PropertyDelegate) {
            return new PropertyImpl((PropertyDelegate) item, sessionContext);
        } else {
            return null;
        }
    }

    /**
     * Returns the node at the specified absolute path in the workspace or
     * {@code null} if no such node exists.
     *
     * @param absPath An absolute path.
     * @return the specified {@code Node} or {@code null}.
     * @throws RepositoryException If another error occurs.
     */
    @CheckForNull
    public Node getNodeOrNull(final String absPath) throws RepositoryException {
        return perform(new ReadOperation<Node>() {
            @Override
            public Node perform() throws RepositoryException {
                try {
                    return NodeImpl.createNodeOrNull(sd.getNode(getOakPathOrThrow(absPath)), sessionContext);
                } catch (PathNotFoundException e) {
                    return null;
                }
            }
        });
    }

    /**
     * Returns the property at the specified absolute path in the workspace or
     * {@code null} if no such node exists.
     *
     * @param absPath An absolute path.
     * @return the specified {@code Property} or {@code null}.
     * @throws RepositoryException if another error occurs.
     */
    @CheckForNull
    public Property getPropertyOrNull(final String absPath) throws RepositoryException {
        if (absPath.equals("/")) {
            return null;
        } else {
            final String oakPath;
            try {
                oakPath = getOakPathOrThrow(absPath);
            } catch (PathNotFoundException e) {
                return null;
            }
            return perform(new ReadOperation<Property>() {
                @Override
                public Property perform() throws RepositoryException {
                    PropertyDelegate pd = sd.getProperty(oakPath);
                    if (pd != null) {
                        return new PropertyImpl(pd, sessionContext);
                    } else {
                        return null;
                    }
                }
            });
        }
    }

    /**
     * Returns the node at the specified absolute path in the workspace. If no
     * such node exists, then it returns the property at the specified path.
     * If no such property exists, then it return {@code null}.
     *
     * @param absPath An absolute path.
     * @return the specified {@code Item} or {@code null}.
     * @throws RepositoryException if another error occurs.
     */
    @CheckForNull
    public Item getItemOrNull(final String absPath) throws RepositoryException {
        return perform(new ReadOperation<Item>() {
            @Override
            public Item perform() throws RepositoryException {
                return getItemInternal(getOakPathOrThrow(absPath));
            }
        });
    }

    //------------------------------------------------------------< Session >---

    @Override
    @Nonnull
    public Repository getRepository() {
        return sessionContext.getRepository();
    }

    @Override
    public String getUserID() {
        return sd.getAuthInfo().getUserID();
    }

    @Override
    public String[] getAttributeNames() {
        Set<String> names = newTreeSet(sessionContext.getAttributes().keySet());
        Collections.addAll(names, sd.getAuthInfo().getAttributeNames());
        return names.toArray(new String[names.size()]);
    }

    @Override
    public Object getAttribute(String name) {
        Object attribute = sd.getAuthInfo().getAttribute(name);
        if (attribute == null) {
            attribute = sessionContext.getAttributes().get(name);
        }
        return attribute;
    }

    @Override @Nonnull
    public Workspace getWorkspace() {
        return sessionContext.getWorkspace();
    }

    @Override
    @Nonnull
    public Session impersonate(Credentials credentials) throws RepositoryException {
        sd.checkAlive();

        ImpersonationCredentials impCreds = new ImpersonationCredentials(credentials, sd.getAuthInfo());
        return getRepository().login(impCreds, sd.getWorkspaceName());
    }

    @Override
    @Nonnull
    public ValueFactory getValueFactory() throws RepositoryException {
        sd.checkAlive();
        return sessionContext.getValueFactory();
    }

    @Override
    @Nonnull
    public Node getRootNode() throws RepositoryException {
        return perform(new ReadOperation<Node>() {
            @Override
            public Node perform() throws RepositoryException {
                NodeDelegate nd = sd.getRootNode();
                if (nd == null) {
                    throw new AccessDeniedException("Root node is not accessible.");
                }
                return NodeImpl.createNodeOrNull(nd, sessionContext);
            }
        });
    }

    @Override
    public Node getNode(String absPath) throws RepositoryException {
        Node node = getNodeOrNull(absPath);
        if (node == null) {
            throw new PathNotFoundException("Node with path " + absPath + " does not exist.");
        }
        return node;
    }

    @Override
    public boolean nodeExists(String absPath) throws RepositoryException {
        return getNodeOrNull(absPath) != null;
    }

    @Nonnull
    private Node getNodeById(final String id) throws RepositoryException {
        return perform(new ReadOperation<Node>() {
            @Override
            public Node perform() throws RepositoryException {
                NodeDelegate nd = sd.getNodeByIdentifier(id);
                if (nd == null) {
                    throw new ItemNotFoundException("Node with id " + id + " does not exist.");
                }
                return NodeImpl.createNodeOrNull(nd, sessionContext);
            }
        });
    }

    @Override
    @Nonnull
    public Node getNodeByUUID(String uuid) throws RepositoryException {
        return getNodeById(uuid);
    }

    @Override
    @Nonnull
    public Node getNodeByIdentifier(String id) throws RepositoryException {
        return getNodeById(id);
    }

    @Override
    public Property getProperty(String absPath) throws RepositoryException {
        Property property = getPropertyOrNull(absPath);
        if (property == null) {
            throw new PathNotFoundException(absPath);
        }
        return property;
    }

    @Override
    public boolean propertyExists(String absPath) throws RepositoryException {
        return getPropertyOrNull(absPath) != null;
    }

    @Override
    public Item getItem(String absPath) throws RepositoryException {
        Item item = getItemOrNull(absPath);
        if (item == null) {
            throw new PathNotFoundException(absPath);
        }
        return item;
    }

    @Override
    public boolean itemExists(String absPath) throws RepositoryException {
        return getItemOrNull(absPath) != null;
    }

    @Override
    public void move(String srcAbsPath, final String destAbsPath) throws RepositoryException {
        checkIndexOnName(sessionContext, destAbsPath);
        final String srcOakPath = getOakPathOrThrowNotFound(srcAbsPath);
        final String destOakPath = getOakPathOrThrowNotFound(destAbsPath);
        sd.perform(new WriteOperation<Void>() {
            @Override
            public void checkPreconditions() throws RepositoryException {
                super.checkPreconditions();
                sd.checkProtectedNode(getParentPath(srcOakPath));
                sd.checkProtectedNode(getParentPath(destOakPath));
            }

            @Override
            public Void perform() throws RepositoryException {
                sd.move(srcOakPath, destOakPath, true);
                return null;
            }
        });
    }

    @Override
    public void removeItem(final String absPath) throws RepositoryException {
        final String oakPath = getOakPathOrThrowNotFound(absPath);
        perform(new WriteOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                ItemDelegate item = sd.getItem(oakPath);
                if (item == null) {
                    throw new PathNotFoundException(absPath);
                } else if (item.isProtected()) {
                    throw new ConstraintViolationException(
                            item.getPath() + " is protected");
                } else if (item.remove()) {
                    return null;
                } else {
                    throw new RepositoryException(
                            item.getPath() + " could not be removed");
                }
            }
        });
    }

    @Override
    public void save() throws RepositoryException {
        perform(new WriteOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                sd.save();
                return null;
            }

            @Override
            public boolean isSave() {
                return true;
            }

            @Override
            public String description() {
                return "Session saved";
            }
        });
    }

    @Override
    public void refresh(final boolean keepChanges) throws RepositoryException {
        perform(new WriteOperation<Void>() {
            @Override
            public Void perform() {
                sd.refresh(keepChanges);
                return null;
            }

            @Override
            public boolean isRefresh() {
                return true;
            }
        });
    }

    @Override
    public boolean hasPendingChanges() throws RepositoryException {
        sd.checkAlive();
        return sd.hasPendingChanges();
    }

    @Override
    public boolean isLive() {
        return sd.isAlive();
    }


    @Override
    public void logout() {
        if (sd.isAlive()) {
            sessionCounter.decrementAndGet();
            safePerform(new SessionOperation<Void>() {
                @Override
                public Void perform() {
                    sessionContext.dispose();
                    sd.logout();
                    return null;
                }

                @Override
                public boolean isLogout() {
                    return true;
                }
            });
        }
    }

    @Override
    @Nonnull
    public ContentHandler getImportContentHandler(String parentAbsPath, int uuidBehavior)
            throws RepositoryException {
        return new ImportHandler(parentAbsPath, sessionContext, uuidBehavior, false);
    }

    @Override
    public void importXML(String parentAbsPath, InputStream in, int uuidBehavior)
            throws IOException, RepositoryException {
        try {
            ContentHandler handler = getImportContentHandler(parentAbsPath, uuidBehavior);
            new ParsingContentHandler(handler).parse(in);
        } catch (SAXException e) {
            Throwable exception = e.getException();
            if (exception instanceof RepositoryException) {
                throw (RepositoryException) exception;
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
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

    /**
     * Exports content at the given path using the given exporter.
     *
     * @param path     of the node to be exported
     * @param exporter document or system view exporter
     * @throws SAXException        if the SAX event handler failed
     * @throws RepositoryException if another error occurs
     */
    private synchronized void export(String path, Exporter exporter)
            throws SAXException, RepositoryException {
        Item item = getItem(path);
        if (item.isNode()) {
            exporter.export((Node) item);
        } else {
            throw new PathNotFoundException("XML export is not defined for properties: " + path);
        }
    }

    @Override
    public void exportSystemView(String absPath, ContentHandler contentHandler, boolean skipBinary, boolean noRecurse)
            throws SAXException, RepositoryException {
        export(absPath, new SystemViewExporter(this, contentHandler, !noRecurse, !skipBinary));
    }

    @Override
    public void exportSystemView(String absPath, OutputStream out, boolean skipBinary, boolean noRecurse)
            throws IOException, RepositoryException {
        try {
            ContentHandler handler = new ToXmlContentHandler(out);
            export(absPath, new SystemViewExporter(this, handler, !noRecurse, !skipBinary));
        } catch (SAXException e) {
            Exception exception = e.getException();
            if (exception instanceof RepositoryException) {
                throw (RepositoryException) exception;
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
            } else {
                throw new RepositoryException("Error serializing system view XML", e);
            }
        }
    }

    @Override
    public void exportDocumentView(String absPath, ContentHandler contentHandler, boolean skipBinary,
                                   boolean noRecurse) throws SAXException, RepositoryException {
        export(absPath, new DocumentViewExporter(this, contentHandler, !noRecurse, !skipBinary));
    }

    @Override
    public void exportDocumentView(String absPath, OutputStream out, boolean skipBinary, boolean noRecurse)
            throws IOException, RepositoryException {
        try {
            ContentHandler handler = new ToXmlContentHandler(out);
            export(absPath, new DocumentViewExporter(this, handler, !noRecurse, !skipBinary));
        } catch (SAXException e) {
            Exception exception = e.getException();
            if (exception instanceof RepositoryException) {
                throw (RepositoryException) exception;
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
            } else {
                throw new RepositoryException("Error serializing document view XML", e);
            }
        }
    }

    @Override
    public void addLockToken(String lt) {
        try {
            getWorkspace().getLockManager().addLockToken(lt);
        } catch (RepositoryException e) {
            log.warn("Unable to add lock token " + lt + " to session", e);
        }
    }

    @Override @Nonnull
    public String[] getLockTokens() {
        try {
            return getWorkspace().getLockManager().getLockTokens();
        } catch (RepositoryException e) {
            log.warn("Unable to retrieve lock tokens from session", e);
            return new String[0];
        }
    }

    @Override
    public void removeLockToken(String lt) {
        try {
            getWorkspace().getLockManager().removeLockToken(lt);
        } catch (RepositoryException e) {
            log.warn("Unable to remove lock token " + lt + " from session", e);
        }
    }

    @Override
    public boolean hasPermission(String absPath, final String actions) throws RepositoryException {
        final String oakPath = getOakPathOrThrow(absPath);
        return perform(new ReadOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return sessionContext.getAccessManager().hasPermissions(oakPath, actions);
            }
        });
    }

    @Override
    public void checkPermission(String absPath, String actions) throws RepositoryException {
        if (!hasPermission(absPath, actions)) {
            throw new AccessControlException("Access denied.");
        }
    }

    @Override
    public boolean hasCapability(String methodName, Object target, Object[] arguments) throws RepositoryException {
        sd.checkAlive();

        if (target instanceof ItemImpl) {
            ItemDelegate dlg = ((ItemImpl) target).dlg;
            if (dlg.isProtected()) {
                return false;
            }

            boolean isNode = ((ItemImpl) target).isNode();
            Node parent = (isNode) ? (Node) target : ((ItemImpl) target).getParent();
            if (!parent.isCheckedOut()) {
                return false;
            }
            if (parent.isLocked()) {
                return false;
            }

            AccessManager accessMgr = sessionContext.getAccessManager();
            long permission = Permissions.NO_PERMISSION;
            if (isNode) {
                Tree tree = ((NodeDelegate) dlg).getTree();
                if ("addNode".equals(methodName)) {
                    if (arguments != null && arguments.length > 0) {
                        // add-node needs to be checked on the (path of) the
                        // new node that has/will be added
                        String path = PathUtils.concat(tree.getPath(), sessionContext.getOakName(arguments[0].toString()));
                        return accessMgr.hasPermissions(path, Session.ACTION_ADD_NODE);
                    }
                } else if ("setPrimaryType".equals(methodName) || "addMixin".equals(methodName) || "removeMixin".equals(methodName)) {
                    permission = Permissions.NODE_TYPE_MANAGEMENT;
                } else if ("orderBefore".equals(methodName)) {
                    if (tree.isRoot()) {
                        return false;
                    } else {
                        permission = Permissions.MODIFY_CHILD_NODE_COLLECTION;
                        tree = tree.getParent();
                    }
                } else if ("setProperty".equals(methodName)) {
                    permission = Permissions.ADD_PROPERTY;
                } else if ("remove".equals(methodName)) {
                    permission = Permissions.REMOVE_NODE;
                }
                return accessMgr.hasPermissions(tree, null, permission);
            } else {
                if (methodName.equals("setValue")) {
                    permission = Permissions.MODIFY_PROPERTY;
                } else if ("remove".equals(methodName)) {
                    permission = Permissions.REMOVE_PROPERTY;
                }
                Tree tree = dlg.getParent().getTree();
                return accessMgr.hasPermissions(tree, ((PropertyDelegate) dlg).getPropertyState(), permission);
            }
        }
        // TODO: add more best-effort checks
        return true;
    }

    @Override
    @Nonnull
    public AccessControlManager getAccessControlManager() throws RepositoryException {
        return sessionContext.getAccessControlManager();
    }

    /**
     * @see javax.jcr.Session#getRetentionManager()
     */
    @Override
    @Nonnull
    public RetentionManager getRetentionManager() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Retention Management is not supported.");
    }

    //---------------------------------------------------------< Namespaces >---

    @Override
    public void setNamespacePrefix(String prefix, String uri) throws RepositoryException {
        sessionContext.getNamespaces().setNamespacePrefix(prefix, uri);
    }

    @Override
    public String[] getNamespacePrefixes() throws RepositoryException {
        return sessionContext.getNamespaces().getNamespacePrefixes();
    }

    @Override
    public String getNamespaceURI(String prefix) throws RepositoryException {
        return sessionContext.getNamespaces().getNamespaceURI(prefix);
    }

    @Override
    public String getNamespacePrefix(String uri) throws RepositoryException {
        return sessionContext.getNamespaces().getNamespacePrefix(uri);
    }

    //--------------------------------------------------< JackrabbitSession >---

    @Override
    @Nonnull
    public PrincipalManager getPrincipalManager() throws RepositoryException {
        return sessionContext.getPrincipalManager();
    }

    @Override
    @Nonnull
    public UserManager getUserManager() throws RepositoryException {
        return sessionContext.getUserManager();
    }

    @Override
    public String toString() {
        return sd.getContentSession().toString();
    }
}

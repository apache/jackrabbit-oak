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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.SESSION_COUNT;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.AccessControlException;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

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
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
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
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.jcr.xml.ImportHandler;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * TODO document
 */
public class SessionImpl implements JackrabbitSession {
    private static final Logger log = LoggerFactory.getLogger(SessionImpl.class);

    private SessionContext sessionContext;
    private SessionDelegate sd;
    private final CounterStats sessionCounter;
    private final Object logoutMonitor = new Object();

    public SessionImpl(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.sd = sessionContext.getSessionDelegate();
        this.sessionCounter = sessionContext.getCount(SESSION_COUNT);
        sessionCounter.inc();
        sessionContext.getMeter(Type.SESSION_LOGIN_COUNTER).mark();
    }

    static void checkIndexOnName(String jcrPath) throws RepositoryException {
        int pos = jcrPath.length() - 1;
        if (pos < 2 || jcrPath.charAt(pos) != ']') {
            return;
        }

        if ("0123456789".indexOf(jcrPath.charAt(--pos)) == -1) {
            return;
        }

        while (--pos >= 0) {
            char ch = jcrPath.charAt(pos);
            if (ch == '[') {
                throw new RepositoryException("Cannot create a new node using a name including an index");
            }
            if ("0123456789".indexOf(ch) == -1) {
                return;
            }
        }
    }

    private abstract class ReadOperation<T> extends SessionOperation<T> {
        protected ReadOperation(String name) {
            super(name);
        }

        @Override
        public void checkPreconditions() throws RepositoryException {
            checkAlive();
        }
    }

    private abstract class WriteOperation<T> extends SessionOperation<T> {
        protected WriteOperation(String name) {
            super(name, true);
        }

        @Override
        public void checkPreconditions() throws RepositoryException {
            checkAlive();
        }
    }

    private void checkAlive() throws RepositoryException {
        if (sd == null) {
            throw new RepositoryException("This session has been closed.");
        }
        sd.checkAlive();
    }

    @NotNull
    private String getOakPathOrThrow(@NotNull String absPath) throws RepositoryException {
        String p = sessionContext.getOakPathOrThrow(absPath);
        if (!PathUtils.isAbsolute(p)) {
            throw new RepositoryException("Not an absolute path: " + absPath);
        }
        return p;
    }

    @NotNull
    private String getOakPathOrThrowNotFound(@NotNull String absPath) throws PathNotFoundException {
        return sessionContext.getOakPathOrThrowNotFound(absPath);
    }

    @Nullable
    private ItemImpl<?> getItemInternal(@NotNull String oakPath) throws RepositoryException {
        checkAlive();
        ItemDelegate item = sd.getItem(oakPath);
        if (item instanceof NodeDelegate) {
            return NodeImpl.createNode((NodeDelegate) item, sessionContext);
        } else if (item instanceof PropertyDelegate) {
            return new PropertyImpl((PropertyDelegate) item, sessionContext);
        } else {
            return null;
        }
    }

    @Override
    @Nullable
    public Node getNodeOrNull(final String absPath) throws RepositoryException {
        requireNonNull(absPath, "parameter 'absPath' must not be null");
        checkAlive();
        return sd.performNullable(new ReadOperation<Node>("getNodeOrNull") {
            @Override
            public Node performNullable() throws RepositoryException {
                try {
                    return NodeImpl.createNodeOrNull(sd.getNode(getOakPathOrThrow(absPath)), sessionContext);
                } catch (PathNotFoundException e) {
                    return null;
                }
            }
        });
    }

    @Override
    @Nullable
    public Property getPropertyOrNull(final String absPath) throws RepositoryException {
        checkAlive();
        if (requireNonNull(absPath, "parameter 'absPath' must not be null").equals("/")) {
            return null;
        } else {
            final String oakPath;
            try {
                oakPath = getOakPathOrThrow(absPath);
            } catch (PathNotFoundException e) {
                return null;
            }
            return sd.performNullable(new ReadOperation<Property>("getPropertyOrNull") {
                @Override
                public Property performNullable() {
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

    @Override
    @Nullable
    public Item getItemOrNull(final String absPath) throws RepositoryException {
        requireNonNull(absPath, "parameter 'absPath' must not be null");
        checkAlive();
        return sd.performNullable(new ReadOperation<Item>("getItemOrNull") {
            @Override
            public Item performNullable() throws RepositoryException {
                return getItemInternal(getOakPathOrThrow(absPath));
            }
        });
    }

    @Override
    @Nullable
    public Node getParentOrNull(@NotNull Item item) throws RepositoryException {
        checkAlive();
        ItemImpl itemImpl = checkItemImpl(item);
        checkContext(itemImpl, sessionContext);
        return sd.performNullable(new ReadOperation<Node>("getParentOrNull") {
            @Override
            public Node performNullable() throws RepositoryException {
                return NodeImpl.createNodeOrNull(itemImpl.dlg.getParent(), sessionContext);
            }
        });
    }

    private static void checkContext(@NotNull ItemImpl item, @NotNull SessionContext context)
            throws RepositoryException {
        if (item.sessionContext != context) {
            throw new RepositoryException("Item '" + item + "' has been obtained through a different Session.");
        }
    }

    @NotNull
    private static ItemImpl checkItemImpl(@NotNull Item item) throws RepositoryException {
        if (item instanceof ItemImpl) {
            return (ItemImpl) item;
        } else {
            throw new RepositoryException("Invalid item implementation '" + item.getClass().getName() + "'. Expected "
                    + ItemImpl.class.getName());
        }
    }

    // ------------------------------------------------------------< Session >---

    @Override
    @NotNull
    public Repository getRepository() {
        return sessionContext.getRepository();
    }

    @Override
    public String getUserID() {
        return sd.getAuthInfo().getUserID();
    }

    @Override
    public String[] getAttributeNames() {
        Set<String> names = new TreeSet<>(sessionContext.getAttributes().keySet());
        Collections.addAll(names, sd.getAuthInfo().getAttributeNames());
        names.add(RepositoryImpl.BOUND_PRINCIPALS);
        return names.toArray(new String[names.size()]);
    }

    @Override
    public Object getAttribute(String name) {
        if (RepositoryImpl.BOUND_PRINCIPALS.equals(name)) {
            return sd.getAuthInfo().getPrincipals();
        }
        Object attribute = sd.getAuthInfo().getAttribute(name);
        if (attribute == null) {
            attribute = sessionContext.getAttributes().get(name);
        }
        return attribute;
    }

    @Override
    @NotNull
    public Workspace getWorkspace() {
        return sessionContext.getWorkspace();
    }

    @Override
    @NotNull
    public Session impersonate(Credentials credentials) throws RepositoryException {
        checkAlive();

        ImpersonationCredentials impCreds = new ImpersonationCredentials(
                requireNonNull(credentials, "parameter 'credentials' must not be null"), sd.getAuthInfo());
        return getRepository().login(impCreds, sd.getWorkspaceName());
    }

    @Override
    @NotNull
    public ValueFactory getValueFactory() throws RepositoryException {
        checkAlive();
        return sessionContext.getValueFactory();
    }

    @Override
    @NotNull
    public Node getRootNode() throws RepositoryException {
        checkAlive();
        return sd.perform(new ReadOperation<Node>("getRootNode") {
            @NotNull
            @Override
            public Node perform() throws RepositoryException {
                NodeDelegate nd = sd.getRootNode();
                if (nd == null) {
                    throw new AccessDeniedException("Root node is not accessible.");
                }
                return NodeImpl.createNode(nd, sessionContext);
            }
        });
    }

    @Override
    public Node getNode(String absPath) throws RepositoryException {
        Node node = getNodeOrNull(requireNonNull(absPath, "parameter 'absPath' must not be null"));
        if (node == null) {
            throw new PathNotFoundException("Node with path " + absPath + " does not exist.");
        }
        return node;
    }

    @Override
    public boolean nodeExists(String absPath) throws RepositoryException {
        return getNodeOrNull(requireNonNull(absPath, "parameter 'absPath' must not be null")) != null;
    }

    @NotNull
    private Node getNodeById(@NotNull final String id) throws RepositoryException {
        checkAlive();
        return sd.perform(new ReadOperation<Node>("getNodeById") {
            @NotNull
            @Override
            public Node perform() throws RepositoryException {
                try {
                    NodeDelegate nd = sd.getNodeByIdentifier(id);
                    if (nd == null) {
                        throw new ItemNotFoundException("Node with id " + id + " does not exist.");
                    }
                    return NodeImpl.createNode(nd, sessionContext);
                } catch (IllegalArgumentException ex) {
                    throw new RepositoryException(ex);
                }
            }
        });
    }

    @Override
    @NotNull
    public Node getNodeByUUID(String uuid) throws RepositoryException {
        return getNodeById(requireNonNull(uuid, "parameter 'uuid' must not be null"));
    }

    @Override
    @NotNull
    public Node getNodeByIdentifier(String id) throws RepositoryException {
        return getNodeById(requireNonNull(id, "parameter 'id' must not be null"));
    }

    @Override
    public Property getProperty(String absPath) throws RepositoryException {
        Property property = getPropertyOrNull(requireNonNull(absPath, "parameter 'absPath' must not be null"));
        if (property == null) {
            throw new PathNotFoundException(absPath);
        }
        return property;
    }

    @Override
    public boolean propertyExists(String absPath) throws RepositoryException {
        return getPropertyOrNull(requireNonNull(absPath, "parameter 'absPath' must not be null")) != null;
    }

    @Override
    public Item getItem(String absPath) throws RepositoryException {
        Item item = getItemOrNull(requireNonNull(absPath, "parameter 'absPath' must not be null"));
        if (item == null) {
            throw new PathNotFoundException(absPath);
        }
        return item;
    }

    @Override
    public boolean itemExists(String absPath) throws RepositoryException {
        return getItemOrNull(requireNonNull(absPath, "parameter 'absPath' must not be null")) != null;
    }

    @Override
    public void move(String srcAbsPath, final String destAbsPath) throws RepositoryException {
        checkAlive();
        checkIndexOnName(requireNonNull(destAbsPath, "parameter 'destAbsPath' must not be null"));
        final String srcOakPath = getOakPathOrThrowNotFound(requireNonNull(srcAbsPath, "parameter 'srcAbsPath' must not be null"));
        final String destOakPath = getOakPathOrThrowNotFound(destAbsPath);
        sd.performVoid(new WriteOperation<Void>("move") {
            @Override
            public void checkPreconditions() throws RepositoryException {
                super.checkPreconditions();
                sd.checkProtectedNode(getParentPath(srcOakPath));
                sd.checkProtectedNode(getParentPath(destOakPath));
            }

            @Override
            public void performVoid() throws RepositoryException {
                sd.move(srcOakPath, destOakPath, true);
            }
        });
    }

    @Override
    public void removeItem(final String absPath) throws RepositoryException {
        checkAlive();
        final String oakPath = getOakPathOrThrowNotFound(requireNonNull(absPath, "parameter 'absPath' must not be null"));
        sd.performVoid(new WriteOperation<Void>("removeItem") {
            @Override
            public void performVoid() throws RepositoryException {
                ItemDelegate item = sd.getItem(oakPath);
                if (item == null) {
                    throw new PathNotFoundException(absPath);
                } else if (item.isProtected()) {
                    throw new ConstraintViolationException(item.getPath() + " is protected");
                } else if (!item.remove()) {
                    throw new RepositoryException(item.getPath() + " could not be removed");
                }
            }
        });
    }

    @Override
    public void save() throws RepositoryException {
        checkAlive();
        sd.performVoid(new WriteOperation<Void>("save") {
            @Override
            public void performVoid() throws RepositoryException {
                sd.save(null);
            }

            @Override
            public boolean isSave() {
                return true;
            }
        });
    }

    @Override
    public void refresh(final boolean keepChanges) throws RepositoryException {
        checkAlive();
        sd.performVoid(new WriteOperation<Void>("refresh") {
            @Override
            public void performVoid() {
                sd.refresh(keepChanges);
            }

            @Override
            public boolean isRefresh() {
                return true;
            }
        });
    }

    @Override
    public boolean hasPendingChanges() throws RepositoryException {
        checkAlive();
        return sd.hasPendingChanges();
    }

    @Override
    public boolean isLive() {
        return sd != null && sd.isAlive();
    }

    @Override
    public void logout() {
        synchronized (logoutMonitor) {
            if (isLive()) {
                sessionCounter.dec();
                try {
                    sd.performVoid(new SessionOperation<Void>("logout") {
                        @Override
                        public void performVoid() {
                            sessionContext.dispose();
                            sd.logout();
                        }

                        @Override
                        public boolean isLogout() {
                            return true;
                        }
                    });
                } catch (RepositoryException e) {
                    throw new RuntimeException("Unexpected exception thrown by operation 'logout'", e);
                } finally {
                    sd = null;
                    sessionContext = null;
                }
            }
        }
    }

    @Override
    @NotNull
    public ContentHandler getImportContentHandler(String parentAbsPath, int uuidBehavior) throws RepositoryException {
        return new ImportHandler(requireNonNull(parentAbsPath, "parameter 'parentAbsPath' must not be null"), sessionContext,
                uuidBehavior, false);
    }

    @Override
    public void importXML(String parentAbsPath, InputStream in, int uuidBehavior)
            throws IOException, RepositoryException {
        try {
            ContentHandler handler = getImportContentHandler(
                    requireNonNull(parentAbsPath, "parameter 'parentAbsPath' must not be null"), uuidBehavior);
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
    private synchronized void export(String path, Exporter exporter) throws SAXException, RepositoryException {
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
        export(requireNonNull(absPath, "parameter 'absPath' must not be null"), new SystemViewExporter(this,
                requireNonNull(contentHandler, "parameter 'contentHandler' must not be null"), !noRecurse, !skipBinary));
    }

    @Override
    public void exportSystemView(String absPath, OutputStream out, boolean skipBinary, boolean noRecurse)
            throws IOException, RepositoryException {
        try {
            ContentHandler handler = new ToXmlContentHandler(requireNonNull(out, "parameter 'out' must not be null"));
            export(requireNonNull(absPath, "parameter 'absPath' must not be null"),
                    new SystemViewExporter(this, handler, !noRecurse, !skipBinary));
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
    public void exportDocumentView(String absPath, ContentHandler contentHandler, boolean skipBinary, boolean noRecurse)
            throws SAXException, RepositoryException {
        export(requireNonNull(absPath, "parameter 'absPath' must not be null"), new DocumentViewExporter(this,
                requireNonNull(contentHandler, "parameter 'contentHandler' must not be null"), !noRecurse, !skipBinary));
    }

    @Override
    public void exportDocumentView(String absPath, OutputStream out, boolean skipBinary, boolean noRecurse)
            throws IOException, RepositoryException {
        try {
            ContentHandler handler = new ToXmlContentHandler(requireNonNull(out, "parameter 'out' must not be null"));
            export(requireNonNull(absPath, "parameter 'absPath' must not be null"),
                    new DocumentViewExporter(this, handler, !noRecurse, !skipBinary));
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
            getWorkspace().getLockManager().addLockToken(requireNonNull(lt, "parameter 'lt' must not be null"));
        } catch (RepositoryException e) {
            log.warn("Unable to add lock token " + lt + " to session", e);
        }
    }

    @Override
    @NotNull
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
            getWorkspace().getLockManager().removeLockToken(requireNonNull(lt, "parameter 'lt' must not be null"));
        } catch (RepositoryException e) {
            log.warn("Unable to remove lock token " + lt + " from session", e);
        }
    }

    @Override
    public boolean hasPermission(String absPath, final String actions) throws RepositoryException {
        checkAlive();
        final String oakPath = getOakPathOrThrow(requireNonNull(absPath, "parameter 'absPath' must not be null"));
        requireNonNull(actions, "parameter 'actions' must not be null");
        return sd.perform(new ReadOperation<Boolean>("hasPermission") {
            @NotNull
            @Override
            public Boolean perform() throws RepositoryException {
                return sessionContext.getAccessManager().hasPermissions(oakPath, actions);
            }
        });
    }

    @Override
    public void checkPermission(String absPath, String actions) throws RepositoryException {
        if (!hasPermission(requireNonNull(absPath, "parameter 'absPath' must not be null"),
                requireNonNull(actions, "parameter 'actions' must not be null"))) {
            throw new AccessControlException("Access denied.");
        }
    }

    @Override
    public boolean hasCapability(String methodName, Object target, Object[] arguments) throws RepositoryException {
        requireNonNull(methodName, "parameter 'methodName' must not be null");
        requireNonNull(target, "parameter 'target' must not be null");
        checkAlive();

        AccessManager accessMgr = sessionContext.getAccessManager();
        if (target instanceof ItemImpl) {
            ItemDelegate dlg = ((ItemImpl<?>) target).dlg;
            if (dlg.isProtected()) {
                return false;
            }

            boolean isNode = ((ItemImpl<?>) target).isNode();
            NodeImpl parent = (NodeImpl) ((isNode) ? target : ((ItemImpl<?>) target).getParent());
            if (!parent.internalIsCheckedOut()) {
                return false;
            }
            boolean hasLocking = sessionContext.getRepository().getDescriptorValue(Repository.OPTION_LOCKING_SUPPORTED)
                    .getBoolean();
            if (hasLocking && parent.isLocked()) {
                return false;
            }

            long permission = Permissions.NO_PERMISSION;
            if (isNode) {
                Tree tree = ((NodeDelegate) dlg).getTree();
                if ("addNode".equals(methodName)) {
                    String relPath = getFirstArgument(arguments);
                    if (relPath != null) {
                        // add-node needs to be checked on the (path of) the
                        // new node that has/will be added
                        String path = PathUtils.concat(tree.getPath(), sessionContext.getOakPathOrThrow(relPath));
                        return accessMgr.hasPermissions(path, Session.ACTION_ADD_NODE) && !isMountedReadOnly(path);
                    } else {
                        // invalid arguments -> cannot verify
                        log.warn("Cannot verify capability to '{}' due to missing or invalid arguments, required a valid relative path.", methodName);
                        return false;
                    }
                } else if ("setPrimaryType".equals(methodName) || "addMixin".equals(methodName)
                        || "removeMixin".equals(methodName)) {
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
                return accessMgr.hasPermissions(tree, null, permission) && !isMountedReadOnly(tree.getPath());
            } else {
                if ("setValue".equals(methodName)) {
                    permission = Permissions.MODIFY_PROPERTY;
                } else if ("remove".equals(methodName)) {
                    permission = Permissions.REMOVE_PROPERTY;
                }
                NodeDelegate parentDelegate = dlg.getParent();
                if (parentDelegate != null) {
                    return accessMgr.hasPermissions(parentDelegate.getTree(),
                            ((PropertyDelegate) dlg).getPropertyState(), permission)
                            && !isMountedReadOnly(parentDelegate.getPath());
                } else {
                    return accessMgr.hasPermissions(dlg.getPath(),
                            (permission == Permissions.MODIFY_PROPERTY) ? Session.ACTION_SET_PROPERTY
                                    : Session.ACTION_REMOVE)
                            && !isMountedReadOnly(dlg.getPath());
                }
            }
        } else if (target instanceof AccessControlManager && isPolicyWriteMethod(methodName)) {
            if (!hasArguments(arguments)) {
                log.warn("Cannot verify capability to '{}' due to missing arguments.", methodName);
                return false;
            }
            String path = getFirstArgument(arguments);
            if (path == null) {
                return getAccessControlManager().hasPrivileges(null, AccessControlUtils.privilegesFromNames(this, PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL));
            } else {
                String oakPath = getOakPathOrThrow(path);
                return !isMountedReadOnly(oakPath) && accessMgr.hasPermissions(oakPath, JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL);
            }
        }
        // TODO: add more best-effort checks
        return true;
    }

    private static boolean hasArguments(@Nullable Object[] arguments) {
        return arguments != null && arguments.length > 0;
    }
    
    @Nullable
    private static String getFirstArgument(@Nullable Object[] arguments) {
        if (arguments != null && arguments.length > 0) {
            Object arg = arguments[0];
            return (arg == null) ? null : arg.toString();
        } else {
            return null;
        }
    }
    
    private static boolean isPolicyWriteMethod(@NotNull String methodName) {
        return "setPolicy".equals(methodName) || "removePolicy".equals(methodName);
    }

    private boolean isMountedReadOnly(String path) {
        MountInfoProvider mip = sessionContext.getMountInfoProvider();
        return mip != null && mip.getMountByPath(path).isReadOnly();
    }

    @Override
    @NotNull
    public AccessControlManager getAccessControlManager() throws RepositoryException {
        return sessionContext.getAccessControlManager();
    }

    /**
     * @see javax.jcr.Session#getRetentionManager()
     */
    @Override
    @NotNull
    public RetentionManager getRetentionManager() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("Retention Management is not supported.");
    }

    // ---------------------------------------------------------< Namespaces >---

    @Override
    public void setNamespacePrefix(String prefix, String uri) throws RepositoryException {
        sessionContext.getNamespaces().setNamespacePrefix(requireNonNull(prefix, "parameter 'prefix' must not be null"),
                requireNonNull(uri, "parameter 'uri' must not be null"));
    }

    @Override
    public String[] getNamespacePrefixes() throws RepositoryException {
        return sessionContext.getNamespaces().getNamespacePrefixes();
    }

    @Override
    public String getNamespaceURI(String prefix) throws RepositoryException {
        return sessionContext.getNamespaces().getNamespaceURI(requireNonNull(prefix, "parameter 'prefix' must not be null"));
    }

    @Override
    public String getNamespacePrefix(String uri) throws RepositoryException {
        return sessionContext.getNamespaces().getNamespacePrefix(requireNonNull(uri, "parameter 'uri' must not be null"));
    }

    // --------------------------------------------------< JackrabbitSession >---

    @Override
    public boolean hasPermission(@NotNull String absPath, @NotNull String... actions) throws RepositoryException {
        return hasPermission(absPath, Text.implode(actions, ","));
    }

    @Override
    @NotNull
    public PrincipalManager getPrincipalManager() throws RepositoryException {
        return sessionContext.getPrincipalManager();
    }

    @Override
    @NotNull
    public UserManager getUserManager() throws RepositoryException {
        return sessionContext.getUserManager();
    }

    @Override
    public String toString() {
        if (isLive()) {
            return sd.getContentSession().toString();
        }
        return "null";
    }
}

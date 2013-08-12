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

import static com.google.common.base.Preconditions.checkNotNull;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PathNotFoundException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.Workspace;
import javax.jcr.observation.ObservationManager;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.observation.ObservationManagerImpl;
import org.apache.jackrabbit.oak.jcr.operation.SessionOperation;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.namepath.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation.Observable;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

/**
 * Instances of this class are passed to all JCR implementation classes
 * (e.g. {@code SessionImpl}, {@code NodeImpl}, etc.) and provide access to
 * the session scoped instances generally needed (e.g. {@code NamePathMapper},
 * {@code ValueFactory}, etc.).
 */
public class SessionContext implements NamePathMapper {

    private final RepositoryImpl repository;
    private final Whiteboard whiteboard;
    private final Map<String, Object> attributes;
    private final SessionDelegate delegate;

    private final SessionNamespaces namespaces;
    private final NamePathMapper namePathMapper;
    private final ValueFactory valueFactory;

    private SessionImpl session = null;
    private WorkspaceImpl workspace = null;

    private AccessControlManager accessControlManager;
    private PermissionProvider permissionProvider;
    private PrincipalManager principalManager;
    private UserManager userManager;
    private PrivilegeManager privilegeManager;
    private ObservationManagerImpl observationManager;

    SessionContext(
            @Nonnull RepositoryImpl repository, @Nonnull Whiteboard whiteboard,
            Map<String, Object> attributes, @Nonnull final SessionDelegate delegate) {
        this.repository = checkNotNull(repository);
        this.whiteboard = checkNotNull(whiteboard);
        this.attributes = attributes;
        this.delegate = checkNotNull(delegate);

        this.namespaces = new SessionNamespaces(this);
        LocalNameMapper nameMapper = new LocalNameMapper() {
            @Override
            protected Map<String, String> getNamespaceMap() {
                return Namespaces.getNamespaceMap(delegate.getRoot().getTree("/"));
            }

            @Override
            protected Map<String, String> getSessionLocalMappings() {
                return namespaces.getSessionLocalMappings();
            }
        };
        this.namePathMapper = new NamePathMapperImpl(
                nameMapper, delegate.getIdManager());
        this.valueFactory = new ValueFactoryImpl(
                delegate.getRoot().getBlobFactory(), namePathMapper);
    }

    public final Map<String, Object> getAttributes() {
        return attributes;
    }

    public final synchronized SessionImpl getSession() {
        if (session == null) {
            session = createSession();
        }
        return session;
    }

    public final synchronized WorkspaceImpl getWorkspace() {
        if (workspace == null) {
            workspace = createWorkspace();
        }
        return workspace;
    }

    /**
     * Factory method for creating the {@link Session} instance for this
     * context. Called by {@link #getSession()} when first accessed. Can be
     * overridden by subclasses to customize the session implementation.
     *
     * @return session instance
     */
    protected SessionImpl createSession() {
        return new SessionImpl(this);
    }

    /**
     * Factory method for creating the {@link Workspace} instance for this
     * context. Called by {@link #getWorkspace()} when first accessed. Can be
     * overridden by subclasses to customize the workspace implementation.
     *
     * @return session instance
     */
    protected WorkspaceImpl createWorkspace() {
        return new WorkspaceImpl(this);
    }

    @Nonnull
    public Repository getRepository() {
        return repository;
    }

    @Nonnull
    public SessionDelegate getSessionDelegate() {
        return delegate;
    }

    SessionNamespaces getNamespaces() {
        return namespaces;
    }

    public ValueFactory getValueFactory() {
        return valueFactory;
    }

    @Nonnull
    public AccessControlManager getAccessControlManager() throws RepositoryException {
        if (accessControlManager == null) {
            accessControlManager = getConfig(AuthorizationConfiguration.class).getAccessControlManager(delegate.getRoot(), namePathMapper);
        }
        return accessControlManager;
    }

    @Nonnull
    public PrincipalManager getPrincipalManager() {
        if (principalManager == null) {
            principalManager = getConfig(PrincipalConfiguration.class)
                    .getPrincipalManager(delegate.getRoot(), namePathMapper);
        }
        return principalManager;
    }

    @Nonnull
    public UserManager getUserManager() {
        // TODO Move to top level class (OAK-938), make UserManager from core the delegate and pass root through argument of perform
        if (userManager == null) {
            final UserManager uMgr = getConfig(UserConfiguration.class).getUserManager(delegate.getRoot(), namePathMapper);
            userManager = new UserManager() {
                @Override
                public Authorizable getAuthorizable(final String id) throws RepositoryException {
                    return delegate.perform(new SessionOperation<Authorizable>() {
                        @Override
                        public Authorizable perform() throws RepositoryException {
                            return uMgr.getAuthorizable(id);
                        }
                    });
                }

                @Override
                public Authorizable getAuthorizable(final Principal principal) throws RepositoryException {
                    return delegate.perform(new SessionOperation<Authorizable>() {
                        @Override
                        public Authorizable perform() throws RepositoryException {
                            return uMgr.getAuthorizable(principal);
                        }
                    });
                }

                @Override
                public Authorizable getAuthorizableByPath(final String path) throws UnsupportedRepositoryOperationException, RepositoryException {
                    return delegate.perform(new SessionOperation<Authorizable>() {
                        @Override
                        public Authorizable perform() throws RepositoryException {
                            return uMgr.getAuthorizableByPath(path);
                        }
                    });
                }

                @Override
                public Iterator<Authorizable> findAuthorizables(final String relPath, final String value) throws RepositoryException {
                    return delegate.perform(new SessionOperation<Iterator<Authorizable>>() {
                        @Override
                        public Iterator<Authorizable> perform() throws RepositoryException {
                            return uMgr.findAuthorizables(relPath, value);
                        }
                    });
                }

                @Override
                public Iterator<Authorizable> findAuthorizables(final String relPath, final String value, final int searchType) throws RepositoryException {
                    return delegate.perform(new SessionOperation<Iterator<Authorizable>>() {
                        @Override
                        public Iterator<Authorizable> perform() throws RepositoryException {
                            return uMgr.findAuthorizables(relPath, value, searchType);
                        }
                    });
                }

                @Override
                public Iterator<Authorizable> findAuthorizables(final Query query) throws RepositoryException {
                    return delegate.perform(new SessionOperation<Iterator<Authorizable>>() {
                        @Override
                        public Iterator<Authorizable> perform() throws RepositoryException {
                            return uMgr.findAuthorizables(query);
                        }
                    });
                }

                @Override
                public User createUser(final String userID, final String password) throws AuthorizableExistsException, RepositoryException {
                    return delegate.perform(new SessionOperation<User>() {
                        @Override
                        public User perform() throws RepositoryException {
                            return uMgr.createUser(userID, password);
                        }
                    });
                }

                @Override
                public User createUser(final String userID, final String password, final Principal principal, final String intermediatePath) throws AuthorizableExistsException, RepositoryException {
                    return delegate.perform(new SessionOperation<User>() {
                        @Override
                        public User perform() throws RepositoryException {
                            return uMgr.createUser(userID, password, principal, intermediatePath);
                        }
                    });
                }

                @Override
                public Group createGroup(final String groupID) throws AuthorizableExistsException, RepositoryException {
                    return delegate.perform(new SessionOperation<Group>() {
                        @Override
                        public Group perform() throws RepositoryException {
                            return uMgr.createGroup(groupID);
                        }
                    });
                }

                @Override
                public Group createGroup(final Principal principal) throws AuthorizableExistsException, RepositoryException {
                    return delegate.perform(new SessionOperation<Group>() {
                        @Override
                        public Group perform() throws RepositoryException {
                            return uMgr.createGroup(principal);
                        }
                    });
                }

                @Override
                public Group createGroup(final Principal principal, final String intermediatePath) throws AuthorizableExistsException, RepositoryException {
                    return delegate.perform(new SessionOperation<Group>() {
                        @Override
                        public Group perform() throws RepositoryException {
                            return uMgr.createGroup(principal, intermediatePath);
                        }
                    });
                }

                @Override
                public Group createGroup(final String groupID, final Principal principal, final String intermediatePath) throws AuthorizableExistsException, RepositoryException {
                    return delegate.perform(new SessionOperation<Group>() {
                        @Override
                        public Group perform() throws RepositoryException {
                            return uMgr.createGroup(groupID, principal, intermediatePath);
                        }
                    });
                }

                @Override
                public boolean isAutoSave() {
                    try {
                        return delegate.perform(new SessionOperation<Boolean>() {
                            @Override
                            public Boolean perform() throws RepositoryException {
                                return uMgr.isAutoSave();
                            }
                        });
                    } catch (RepositoryException e) {
                        assert false : "Unexpected exception: " + e;
                        return false;
                    }
                }

                @Override
                public void autoSave(final boolean enable) throws UnsupportedRepositoryOperationException, RepositoryException {
                    delegate.perform(new SessionOperation<Void>() {
                        @Override
                        public Void perform() throws RepositoryException {
                            uMgr.autoSave(enable);
                            return null;
                        }
                    });
                }
            };

        }
        return userManager;
    }

    @Nonnull
    public PrivilegeManager getPrivilegeManager() {
        if (privilegeManager == null) {
            privilegeManager = getConfig(PrivilegeConfiguration.class).getPrivilegeManager(delegate.getRoot(), namePathMapper);
        }
        return privilegeManager;
    }

    @Nonnull
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        // TODO: take non-security related importers into account as well (proper configuration)
        List<ProtectedItemImporter> importers = new ArrayList<ProtectedItemImporter>();
        for (SecurityConfiguration sc : repository.getSecurityProvider().getConfigurations()) {
            importers.addAll(sc.getProtectedItemImporters());
        }
        return importers;
    }


    @Nonnull
    public ObservationManager getObservationManager() throws UnsupportedRepositoryOperationException {
        if (observationManager == null) {
            ContentSession contentSession = getSessionDelegate().getContentSession();
            if (!(contentSession instanceof Observable)) {
                throw new UnsupportedRepositoryOperationException("Observation not supported for session " + contentSession);
            }

            observationManager = new ObservationManagerImpl(
                delegate,
                ReadOnlyNodeTypeManager.getInstance(delegate.getRoot(), namePathMapper),
                namePathMapper, whiteboard);
        }
        return observationManager;
    }

    //-----------------------------------------------------< NamePathMapper >---

    @Override
    @Nonnull
    public String getOakName(String jcrName) throws RepositoryException {
        return namePathMapper.getOakName(jcrName);
    }

    @Override
    @CheckForNull
    public String getOakNameOrNull(@Nonnull String jcrName) {
        return namePathMapper.getOakNameOrNull(jcrName);
    }

    @Override
    public boolean hasSessionLocalMappings() {
        return !namespaces.getSessionLocalMappings().isEmpty();
    }

    @Override
    public String getJcrName(@Nonnull String oakName) {
        return namePathMapper.getJcrName(oakName);
    }

    @Override
    @CheckForNull
    public String getOakPath(String jcrPath) {
        return namePathMapper.getOakPath(jcrPath);
    }

    @Override
    @CheckForNull
    public String getOakPathKeepIndex(String jcrPath) {
        return namePathMapper.getOakPathKeepIndex(jcrPath);
    }

    @Override
    @Nonnull
    public String getJcrPath(String oakPath) {
        return namePathMapper.getJcrPath(oakPath);
    }

    /**
     * Returns the Oak path for the given JCR path, or throws a
     * {@link javax.jcr.RepositoryException} if the path can not be mapped.
     *
     * @param jcrPath JCR path
     * @return Oak path
     * @throws javax.jcr.RepositoryException if the path can not be mapped
     */
    @Nonnull
    public String getOakPathOrThrow(String jcrPath) throws RepositoryException {
        String oakPath = getOakPath(jcrPath);
        if (oakPath != null) {
            return oakPath;
        } else {
            throw new RepositoryException("Invalid name or path: " + jcrPath);
        }
    }

    /**
     * Returns the Oak path for the given JCR path, or throws a
     * {@link javax.jcr.PathNotFoundException} if the path can not be mapped.
     *
     * @param jcrPath JCR path
     * @return Oak path
     * @throws javax.jcr.PathNotFoundException if the path can not be mapped
     */
    @Nonnull
    public String getOakPathOrThrowNotFound(String jcrPath) throws PathNotFoundException {
        String oakPath = getOakPath(jcrPath);
        if (oakPath != null) {
            return oakPath;
        } else {
            throw new PathNotFoundException(jcrPath);
        }
    }

    public void refresh(boolean includeRoot) {
        if (includeRoot) {
            getSessionDelegate().getRoot().refresh();
        }
        if (permissionProvider != null) {
            permissionProvider.refresh();
        }
    }

    @Nonnull
    public AccessManager getAccessManager() throws RepositoryException {
        return new AccessManager(getPermissionProvider());
    }

    @Nonnull
    public SecurityProvider getSecurityProvider() {
        return repository.getSecurityProvider();
    }

    //-----------------------------------------------------------< internal >---

    void dispose() {
        if (observationManager != null) {
            observationManager.dispose();
        }
        namespaces.clear();
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private PermissionProvider getPermissionProvider() {
        if (permissionProvider == null) {
            permissionProvider = getConfig(AuthorizationConfiguration.class).getPermissionProvider(delegate.getRoot(), delegate.getAuthInfo().getPrincipals());
        }
        return permissionProvider;
    }

    @Nonnull
    private <T> T getConfig(Class<T> clss) {
        return repository.getSecurityProvider().getConfiguration(clss);
    }
}

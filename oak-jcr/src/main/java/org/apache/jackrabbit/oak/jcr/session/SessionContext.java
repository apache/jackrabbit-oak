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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newTreeSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import org.apache.jackrabbit.oak.jcr.delegate.AccessControlManagerDelegator;
import org.apache.jackrabbit.oak.jcr.delegate.JackrabbitAccessControlManagerDelegator;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.PrincipalManagerDelegator;
import org.apache.jackrabbit.oak.jcr.delegate.PrivilegeManagerDelegator;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.UserManagerDelegator;
import org.apache.jackrabbit.oak.jcr.observation.ObservationManagerImpl;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
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
import org.apache.jackrabbit.oak.stats.StatisticManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instances of this class are passed to all JCR implementation classes
 * (e.g. {@code SessionImpl}, {@code NodeImpl}, etc.) and provide access to
 * the session scoped instances generally needed (e.g. {@code NamePathMapper},
 * {@code ValueFactory}, etc.).
 */
public class SessionContext implements NamePathMapper {

    private static final Logger log = LoggerFactory.getLogger(SessionContext.class);

    private final Repository repository;
    private final StatisticManager statisticManager;
    private final SecurityProvider securityProvider;
    private final Whiteboard whiteboard;
    private final Map<String, Object> attributes;
    private final SessionDelegate delegate;
    private final int observationQueueLength;
    private final CommitRateLimiter commitRateLimiter;

    private final SessionNamespaces namespaces;
    private final NamePathMapper namePathMapper;
    private final ValueFactory valueFactory;

    private SessionImpl session = null;
    private WorkspaceImpl workspace = null;

    private PermissionProvider permissionProvider;
    private AccessControlManager accessControlManager;
    private AccessManager accessManager;
    private PrincipalManager principalManager;
    private UserManager userManager;
    private PrivilegeManager privilegeManager;
    private ObservationManagerImpl observationManager;

    /** Paths (tokens) of all open scoped locks held by this session. */
    private final Set<String> openScopedLocks = newTreeSet();

    /** Paths of all session scoped locks held by this session. */
    private final Set<String> sessionScopedLocks = newHashSet();

    public SessionContext(
            @Nonnull Repository repository, @Nonnull StatisticManager statisticManager,
            @Nonnull SecurityProvider securityProvider, @Nonnull Whiteboard whiteboard,
            @Nonnull Map<String, Object> attributes, @Nonnull final SessionDelegate delegate,
            int observationQueueLength, CommitRateLimiter commitRateLimiter) {
        this.repository = checkNotNull(repository);
        this.statisticManager = statisticManager;
        this.securityProvider = checkNotNull(securityProvider);
        this.whiteboard = checkNotNull(whiteboard);
        this.attributes = checkNotNull(attributes);
        this.delegate = checkNotNull(delegate);
        this.observationQueueLength = observationQueueLength;
        this.commitRateLimiter = commitRateLimiter;
        SessionStats sessionStats = delegate.getSessionStats();
        sessionStats.setAttributes(attributes);

        this.namespaces = new SessionNamespaces(delegate.getRoot());
        this.namePathMapper = new NamePathMapperImpl(
                namespaces, delegate.getIdManager());
        this.valueFactory = new ValueFactoryImpl(
                delegate.getRoot(), namePathMapper);
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
    public StatisticManager getStatisticManager() {
        return statisticManager;
    }

    @Nonnull
    public AtomicLong getCounter(Type type) {
        return statisticManager.getCounter(type);
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

    @Override
    @Nonnull
    public Map<String, String> getSessionLocalMappings() {
        return namespaces.getSessionLocalMappings();
    }

    public ValueFactory getValueFactory() {
        return valueFactory;
    }

    @Nonnull
    public AccessControlManager getAccessControlManager() throws RepositoryException {
        if (accessControlManager == null) {
            AccessControlManager acm = getConfig(AuthorizationConfiguration.class)
                    .getAccessControlManager(delegate.getRoot(), namePathMapper);
            if (acm instanceof JackrabbitAccessControlManager) {
                accessControlManager = new JackrabbitAccessControlManagerDelegator(
                        delegate, (JackrabbitAccessControlManager) acm);
            } else {
                accessControlManager = new AccessControlManagerDelegator(delegate, acm);
            }
        }
        return accessControlManager;
    }

    @Nonnull
    public PrincipalManager getPrincipalManager() {
        if (principalManager == null) {
            principalManager = new PrincipalManagerDelegator(delegate,
                    getConfig(PrincipalConfiguration.class)
                            .getPrincipalManager(delegate.getRoot(), namePathMapper));
        }
        return principalManager;
    }

    @Nonnull
    public UserManager getUserManager() {
        if (userManager == null) {
            userManager = new UserManagerDelegator(delegate, getConfig(UserConfiguration.class)
                    .getUserManager(delegate.getRoot(), namePathMapper));
        }
        return userManager;
    }

    @Nonnull
    public PrivilegeManager getPrivilegeManager() {
        if (privilegeManager == null) {
            privilegeManager = new PrivilegeManagerDelegator(delegate,
                    getConfig(PrivilegeConfiguration.class)
                            .getPrivilegeManager(delegate.getRoot(), namePathMapper));
        }
        return privilegeManager;
    }

    @Nonnull
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        // TODO: take non-security related importers into account as well (proper configuration)
        List<ProtectedItemImporter> importers = new ArrayList<ProtectedItemImporter>();
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            importers.addAll(sc.getProtectedItemImporters());
        }
        return importers;
    }


    @Nonnull
    public ObservationManager getObservationManager() throws UnsupportedRepositoryOperationException {
        if (observationManager == null) {
            observationManager = new ObservationManagerImpl(
                this,
                ReadOnlyNodeTypeManager.getInstance(delegate.getRoot(), namePathMapper),
                getPermissionProvider(), whiteboard, observationQueueLength, commitRateLimiter);
        }
        return observationManager;
    }

    public Set<String> getOpenScopedLocks() {
        return openScopedLocks;
    }

    public Set<String> getSessionScopedLocks() {
        return sessionScopedLocks;
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
            // check if the path is an SNS path with an index > 1 and throw a PathNotFoundException instead (see OAK-1216)
            if (getOakPathKeepIndex(jcrPath) != null) {
                throw new PathNotFoundException(jcrPath);
            } else {
                throw new RepositoryException("Invalid name or path: " + jcrPath);
            }

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

    @Nonnull
    public AccessManager getAccessManager() throws RepositoryException {
        if (accessManager == null) {
            accessManager = new AccessManager(delegate, getPermissionProvider());
        }
        return accessManager;
    }

    @Nonnull
    public SecurityProvider getSecurityProvider() {
        return securityProvider;
    }

    //-----------------------------------------------------------< internal >---

    void dispose() {
        try {
            unlockAllSessionScopedLocks();
        } catch (RepositoryException e) {
            throw new RuntimeException("Unexpected repository error", e);
        }
        if (observationManager != null) {
            observationManager.dispose();
        }
        namespaces.clear();
    }

    /**
     * Unlocks all existing session-scoped locks (if any). Used for cleanup
     * when a session is being closed.
     *
     * @throws RepositoryException if an unexpected problem occurs
     */
    // TODO: should this be in SessionImpl?
    private void unlockAllSessionScopedLocks() throws RepositoryException {
        delegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                Iterator<String> iterator = sessionScopedLocks.iterator();
                while (iterator.hasNext()) {
                    NodeDelegate node = delegate.getNode(iterator.next());
                    if (node != null) {
                        try {
                            node.unlock(); // TODO: use a single commit
                        } catch (RepositoryException e) {
                            log.warn("Failed to unlock a session scoped lock", e);
                        }
                    }
                    iterator.remove();
                }
                return null;
            }
        });
    }

    @Nonnull
    private <T> T getConfig(Class<T> clss) {
        return securityProvider.getConfiguration(clss);
    }

    private PermissionProvider getPermissionProvider() {
        // FIXME: review whether 'auto-refresh' should rather be made on a wrapping
        //        permission provider instead of doing this in the access manager
        //        since this permission provider is also passed to the observation manager.
        if (permissionProvider == null) {
            permissionProvider = checkNotNull(securityProvider)
                    .getConfiguration(AuthorizationConfiguration.class)
                    .getPermissionProvider(delegate.getRoot(), delegate.getWorkspaceName(), delegate.getAuthInfo().getPrincipals());
        }
        return permissionProvider;
    }

}

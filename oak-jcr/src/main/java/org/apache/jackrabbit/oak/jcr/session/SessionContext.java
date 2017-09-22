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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PathNotFoundException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
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
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.stats.StatisticManager;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.TimerStats;
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
    private MountInfoProvider mountInfoProvider;

    private final NamePathMapper namePathMapper;
    private final ValueFactory valueFactory;

    private SessionImpl session = null;
    private WorkspaceImpl workspace = null;

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
    
    private final boolean fastQueryResultSize;

    public SessionContext(
             @Nonnull Repository repository, @Nonnull StatisticManager statisticManager,
             @Nonnull SecurityProvider securityProvider, @Nonnull Whiteboard whiteboard,
             @Nonnull Map<String, Object> attributes, @Nonnull final SessionDelegate delegate,
             int observationQueueLength, CommitRateLimiter commitRateLimiter) {
        
        this(repository, statisticManager, securityProvider, whiteboard, attributes, delegate,
            observationQueueLength, commitRateLimiter, null, false);
    }

    public SessionContext(
            @Nonnull Repository repository, @Nonnull StatisticManager statisticManager,
            @Nonnull SecurityProvider securityProvider, @Nonnull Whiteboard whiteboard,
            @Nonnull Map<String, Object> attributes, @Nonnull final SessionDelegate delegate,
            int observationQueueLength, CommitRateLimiter commitRateLimiter,
            MountInfoProvider mountInfoProvider, boolean fastQueryResultSize) {
        this.repository = checkNotNull(repository);
        this.statisticManager = statisticManager;
        this.securityProvider = checkNotNull(securityProvider);
        this.whiteboard = checkNotNull(whiteboard);
        this.attributes = checkNotNull(attributes);
        this.delegate = checkNotNull(delegate);
        this.observationQueueLength = observationQueueLength;
        this.commitRateLimiter = commitRateLimiter;
        this.mountInfoProvider = mountInfoProvider;
        SessionStats sessionStats = delegate.getSessionStats();
        sessionStats.setAttributes(attributes);

        this.namePathMapper = new NamePathMapperImpl(
                delegate.getNamespaces(), delegate.getIdManager());
        this.valueFactory = new ValueFactoryImpl(
                delegate.getRoot(), namePathMapper);
        this.fastQueryResultSize = fastQueryResultSize;
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
     * Factory method for creating the {@link javax.jcr.Session} instance for this
     * context. Called by {@link #getSession()} when first accessed. Can be
     * overridden by subclasses to customize the session implementation.
     *
     * @return session instance
     */
    protected SessionImpl createSession() {
        return new SessionImpl(this);
    }

    /**
     * Factory method for creating the {@link javax.jcr.Workspace} instance for this
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
    public MeterStats getMeter(Type type){
        return statisticManager.getMeter(type);
    }

    @Nonnull
    public TimerStats getTimer(Type type) {
        return statisticManager.getTimer(type);
    }

    @Nonnull
    public CounterStats getCount(Type type) {
        return statisticManager.getStatsCounter(type);
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
        return delegate.getNamespaces();
    }

    @Override
    @Nonnull
    public Map<String, String> getSessionLocalMappings() {
        return getNamespaces().getSessionLocalMappings();
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
                whiteboard, observationQueueLength, commitRateLimiter);
        }
        return observationManager;
    }

    public boolean hasEventListeners(){
        if (observationManager != null){
            return observationManager.getRegisteredEventListeners().hasNext();
        }
        return false;
    }

    public Set<String> getOpenScopedLocks() {
        return openScopedLocks;
    }

    public Set<String> getSessionScopedLocks() {
        return sessionScopedLocks;
    }
    
    public boolean getFastQueryResultSize() {
        if (System.getProperty("oak.fastQuerySize") != null) {
            return Boolean.getBoolean("oak.fastQuerySize");
        }
        return fastQueryResultSize;
    }

    @Nullable
    public MountInfoProvider getMountInfoProvider() {
        return mountInfoProvider;
    }

    //-----------------------------------------------------< NamePathMapper >---

    @Override
    @Nonnull
    public String getOakName(@Nonnull String jcrName) throws RepositoryException {
        return namePathMapper.getOakName(jcrName);
    }

    @Override
    @CheckForNull
    public String getOakNameOrNull(@Nonnull String jcrName) {
        return namePathMapper.getOakNameOrNull(jcrName);
    }

    @Nonnull
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

    @Nonnull
    public AccessManager getAccessManager() throws RepositoryException {
        if (accessManager == null) {
            accessManager = new AccessManager(delegate, delegate.getPermissionProvider());
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
        getNamespaces().clear();
    }

    /**
     * Unlocks all existing session-scoped locks (if any). Used for cleanup
     * when a session is being closed.
     *
     * @throws RepositoryException if an unexpected problem occurs
     */
    // TODO: should this be in SessionImpl?
    private void unlockAllSessionScopedLocks() throws RepositoryException {
        delegate.performVoid(new SessionOperation<Void>("unlockAllSessionScopedLocks") {
            @Override
            public void performVoid() {
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
            }
        });
    }

    @Nonnull
    private <T> T getConfig(Class<T> clss) {
        return securityProvider.getConfiguration(clss);
    }

}

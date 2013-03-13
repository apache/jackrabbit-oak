package org.apache.jackrabbit.oak.jcr;

import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.observation.ObservationManager;
import javax.jcr.security.AccessControlManager;
import javax.jcr.version.VersionManager;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.namepath.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeTypeProvider;
import org.apache.jackrabbit.oak.plugins.observation.ObservationManagerImpl;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;

import static com.google.common.base.Preconditions.checkNotNull;

public final class SessionContextProvider {
    private static final Map<SessionDelegate, SessionContext> CONTEXTS = Maps.newConcurrentMap();

    private SessionContextProvider() { }

    @Nonnull
    public static Session newSession(@Nonnull SessionDelegate delegate, @Nonnull RepositoryImpl repository) {
        Map<String, String> namespaces = Maps.newHashMap();
        final Root root = delegate.getRoot();

        NamePathMapperImpl namePathMapper = new NamePathMapperImpl(new LocalNameMapper(namespaces) {
            @Override
            protected Map<String, String> getNamespaceMap() {
                return Namespaces.getNamespaceMap(root.getTree("/"));
            }
        }, delegate.getIdManager());
        ValueFactoryImpl valueFactory = new ValueFactoryImpl(root.getBlobFactory(), namePathMapper);
        SessionImpl session = new SessionImpl(checkNotNull(delegate), namespaces);
        WorkspaceImpl workspace = new WorkspaceImpl(delegate);

        CONTEXTS.put(delegate, new SessionContext(
                checkNotNull(repository), session, workspace, namePathMapper, valueFactory));
        return session;
    }

    public static void remove(SessionDelegate delegate) {
        SessionContext context = CONTEXTS.remove(delegate);
        if (context == null) {
            throw new IllegalStateException();
        }
        context.dispose();
    }

    @Nonnull
    public static SessionImpl getSession(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).session;
    }

    @Nonnull
    public static RepositoryImpl getRepository(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).repository;
    }

    @Nonnull
    public static WorkspaceImpl getWorkspace(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).workspace;
    }

    @Nonnull
    public static LockManager getLockManager(@Nonnull SessionDelegate delegate) {
        return getWorkspace(delegate).getLockManager();
    }

    @Nonnull
    public static NodeTypeManager getNodeTypeManager(@Nonnull SessionDelegate delegate) {
        return getWorkspace(delegate).getNodeTypeManager();
    }

    @Nonnull
    public static VersionManager getVersionManager(@Nonnull SessionDelegate delegate)
            throws RepositoryException {
        return getWorkspace(delegate).getVersionManager();
    }

    @Nonnull
    public static EffectiveNodeTypeProvider getEffectiveNodeTypeProvider(@Nonnull SessionDelegate delegate) {
        return getWorkspace(delegate).getReadWriteNodeTypeManager();
    }

    @Nonnull
    public static DefinitionProvider getDefinitionProvider(@Nonnull SessionDelegate delegate) {
        return getWorkspace(delegate).getReadWriteNodeTypeManager();
    }

    @Nonnull
    public static NamePathMapperImpl getNamePathMapper(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).namePathMapper;
    }

    @Nonnull
    public static ValueFactoryImpl getValueFactory(@Nonnull SessionDelegate delegate) {
        return getSessionContext(delegate).valueFactory;
    }

    @Nonnull
    public static String getOakName(@Nonnull SessionDelegate delegate, String jcrName)
            throws RepositoryException {
        return getSessionContext(delegate).getOakName(jcrName);
    }

    @CheckForNull
    public static String getOakPathOrNull(@Nonnull SessionDelegate delegate, String jcrPath) {
        return getSessionContext(delegate).getOakPathOrNull(jcrPath);
    }

    @Nonnull
    public static String getOakPathKeepIndexOrThrowNotFound(@Nonnull SessionDelegate delegate, String jcrPath)
            throws PathNotFoundException {
        return getSessionContext(delegate).getOakPathKeepIndexOrThrowNotFound(jcrPath);
    }

    @Nonnull
    public static String getOakPathOrThrowNotFound(@Nonnull SessionDelegate delegate, String jcrPath)
            throws PathNotFoundException {
        return getSessionContext(delegate).getOakPathOrThrowNotFound(jcrPath);
    }

    @Nonnull
    public static String getOakPath(@Nonnull SessionDelegate delegate, String jcrPath)
            throws RepositoryException {
        return getSessionContext(delegate).getOakPath(jcrPath);
    }

    @Nonnull
    public static SecurityProvider getSecurityProvider(SessionDelegate delegate) {
        return getRepository(delegate).getSecurityProvider();
    }

    @Nonnull
    public static AccessControlManager getAccessControlManager(SessionDelegate delegate) {
        SessionContext context = getSessionContext(delegate);
        return context.getAccessControlManager(delegate);
    }

    @Nonnull
    public static PermissionProvider getPermissionProvider(SessionDelegate delegate) {
        SecurityProvider securityProvider = getSecurityProvider(delegate);

        // TODO
        return securityProvider.getAccessControlConfiguration()
                .getPermissionProvider(delegate.getRoot(), delegate.getAuthInfo().getPrincipals());
    }

    @Nonnull
    public static PrincipalManager getPrincipalManager(SessionDelegate delegate) {
        SessionContext context = getSessionContext(delegate);
        return context.getPrincipalManager(delegate);
    }

    @Nonnull
    public static UserManager getUserManager(SessionDelegate delegate) {
        SessionContext context = getSessionContext(delegate);
        return context.getUserManager(delegate);
    }

    @Nonnull
    public static PrivilegeManager getPrivilegeManager(SessionDelegate delegate) {
        SessionContext context = getSessionContext(delegate);
        return context.getPrivilegeManager(delegate);
    }

    @Nonnull
    public static UserConfiguration getUserConfiguration(SessionDelegate delegate) {
        SessionContext context = getSessionContext(delegate);
        return context.getUserConfiguration();
    }

    @Nonnull
    public static AccessControlConfiguration getAccessControlConfiguration(SessionDelegate delegate) {
        SessionContext context = getSessionContext(delegate);
        return context.getAccessControlConfiguration();
    }

    @Nonnull
    public static ObservationManager getObservationManager(SessionDelegate delegate) {
        SessionContext context = getSessionContext(delegate);
        return context.getObservationManager(delegate);
    }

    public static boolean hasPendingEvents(SessionDelegate delegate) {
        SessionContext context = getSessionContext(delegate);
        return context.hasPendingEvents();
    }

    //------------------------------------------------------------< private >---

    @Nonnull
    private static SessionContext getSessionContext(SessionDelegate delegate) {
        SessionContext context = CONTEXTS.get(checkNotNull(delegate));
        if (context == null) {
            throw new IllegalStateException();
        }
        return context;
    }

    private static class SessionContext {
        final RepositoryImpl repository;
        final SessionImpl session;
        final WorkspaceImpl workspace;
        final NamePathMapperImpl namePathMapper;
        final ValueFactoryImpl valueFactory;

        private AccessControlManager accessControlManager;
        private PrincipalManager principalManager;
        private UserManager userManager;
        private PrivilegeManager privilegeManager;
        private UserConfiguration userConfiguration;
        private AccessControlConfiguration accessControlConfiguration;
        private ObservationManagerImpl observationManager;

        SessionContext(RepositoryImpl repository, SessionImpl session, WorkspaceImpl workspace,
                NamePathMapperImpl namePathMapper, ValueFactoryImpl valueFactory) {
            this.repository = repository;
            this.session = session;
            this.workspace = workspace;
            this.namePathMapper = namePathMapper;
            this.valueFactory = valueFactory;
        }

        /**
         * Returns the Oak name for the given JCR name, or throws a
         * {@link RepositoryException} if the name is invalid or can
         * otherwise not be mapped.
         *
         * @param jcrName JCR name
         * @return Oak name
         * @throws RepositoryException if the name is invalid
         */
        @Nonnull
        String getOakName(String jcrName) throws RepositoryException {
            return namePathMapper.getOakName(jcrName);
        }

        /**
         * Shortcut for {@code SessionDelegate.getNamePathMapper().getOakPath(jcrPath)}.
         *
         * @param jcrPath JCR path
         * @return Oak path, or {@code null}
         */
        @CheckForNull
        String getOakPathOrNull(String jcrPath) {
            return namePathMapper.getOakPath(jcrPath);
        }

        /**
         * Shortcut for {@code SessionDelegate.getOakPathKeepIndex(jcrPath)}.
         *
         * @param jcrPath JCR path
         * @return Oak path, or {@code null}, with indexes left intact
         * @throws javax.jcr.PathNotFoundException
         */
        @Nonnull
        String getOakPathKeepIndexOrThrowNotFound(String jcrPath) throws PathNotFoundException {
            String oakPath = namePathMapper.getOakPathKeepIndex(jcrPath);
            if (oakPath != null) {
                return oakPath;
            } else {
                throw new PathNotFoundException(jcrPath);
            }
        }

        /**
         * Returns the Oak path for the given JCR path, or throws a
         * {@link PathNotFoundException} if the path can not be mapped.
         *
         * @param jcrPath JCR path
         * @return Oak path
         * @throws PathNotFoundException if the path can not be mapped
         */
        @Nonnull
        String getOakPathOrThrowNotFound(String jcrPath) throws PathNotFoundException {
            String oakPath = getOakPathOrNull(jcrPath);
            if (oakPath != null) {
                return oakPath;
            } else {
                throw new PathNotFoundException(jcrPath);
            }
        }

        /**
         * Returns the Oak path for the given JCR path, or throws a
         * {@link RepositoryException} if the path can not be mapped.
         *
         * @param jcrPath JCR path
         * @return Oak path
         * @throws RepositoryException if the path can not be mapped
         */
        @Nonnull
        String getOakPath(String jcrPath) throws RepositoryException {
            String oakPath = getOakPathOrNull(jcrPath);
            if (oakPath != null) {
                return oakPath;
            } else {
                throw new RepositoryException("Invalid name or path: " + jcrPath);
            }
        }

        @Nonnull
        AccessControlManager getAccessControlManager(SessionDelegate delegate) {
            if (accessControlManager == null) {
                SecurityProvider securityProvider = repository.getSecurityProvider();
                accessControlManager = securityProvider.getAccessControlConfiguration()
                        .getAccessControlManager(delegate.getRoot(), namePathMapper);
            }
            return accessControlManager;
        }

        @Nonnull
        PrincipalManager getPrincipalManager(SessionDelegate delegate) {
            if (principalManager == null) {
                SecurityProvider securityProvider = repository.getSecurityProvider();
                principalManager = securityProvider.getPrincipalConfiguration()
                        .getPrincipalManager(delegate.getRoot(), namePathMapper);
            }
            return principalManager;
        }

        @Nonnull
        UserManager getUserManager(SessionDelegate delegate) {
            if (userManager == null) {
                SecurityProvider securityProvider = repository.getSecurityProvider();
                userManager = securityProvider.getUserConfiguration().getUserManager(delegate.getRoot(), namePathMapper);
            }
            return userManager;
        }

        @Nonnull
        PrivilegeManager getPrivilegeManager(SessionDelegate delegate) {
            if (privilegeManager == null) {
                SecurityProvider securityProvider = repository.getSecurityProvider();
                privilegeManager = securityProvider.getPrivilegeConfiguration().getPrivilegeManager(delegate.getRoot(), namePathMapper);
            }
            return privilegeManager;
        }

        @Nonnull
        UserConfiguration getUserConfiguration() {
            if (userConfiguration == null) {
                SecurityProvider securityProvider = repository.getSecurityProvider();
                userConfiguration = securityProvider.getUserConfiguration();
            }
            return userConfiguration;
        }

        @Nonnull
        AccessControlConfiguration getAccessControlConfiguration() {
            if (accessControlConfiguration == null) {
                SecurityProvider securityProvider = repository.getSecurityProvider();
                accessControlConfiguration = securityProvider.getAccessControlConfiguration();
            }
            return accessControlConfiguration;
        }

        @Nonnull
        ObservationManager getObservationManager(SessionDelegate delegate) {
            if (observationManager == null) {
                observationManager = new ObservationManagerImpl(
                        delegate.getRoot(), namePathMapper, repository.getObservationExecutor());
            }
            return observationManager;
        }

        void dispose() {
            if (observationManager != null) {
                observationManager.dispose();
            }
        }

        public boolean hasPendingEvents() {
            return observationManager != null && observationManager.hasEvents();
        }
    }
}

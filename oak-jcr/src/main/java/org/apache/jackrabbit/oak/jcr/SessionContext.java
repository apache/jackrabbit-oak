package org.apache.jackrabbit.oak.jcr;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.PathNotFoundException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;
import javax.jcr.Workspace;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.observation.ObservationManager;
import javax.jcr.security.AccessControlManager;
import javax.jcr.version.VersionManager;
import javax.security.auth.Subject;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.namepath.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeTypeProvider;
import org.apache.jackrabbit.oak.plugins.observation.ObservationManagerImpl;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * TODO doc
 */
public abstract class SessionContext implements NamePathMapper {
    private final RepositoryImpl repository;
    private final SessionDelegate delegate;
    private final NamePathMapper namePathMapper;
    private final ValueFactory valueFactory;

    private AccessControlManager accessControlManager;
    private PermissionProvider permissionProvider;
    private PrincipalManager principalManager;
    private UserManager userManager;
    private PrivilegeManager privilegeManager;
    private ObservationManagerImpl observationManager;

    private SessionContext(RepositoryImpl repository, SessionDelegate delegate,
            NamePathMapper namePathMapper, ValueFactory valueFactory) {
        this.delegate = delegate;
        this.repository = repository;
        this.namePathMapper = namePathMapper;
        this.valueFactory = valueFactory;
    }

    public static SessionContext create(final SessionDelegate delegate, RepositoryImpl repository) {
        final Map<String, String> namespaces = Maps.newHashMap();
        final Root root = checkNotNull(delegate).getRoot();

        LocalNameMapper nameMapper = new LocalNameMapper(namespaces) {
            @Override
            protected Map<String, String> getNamespaceMap() {
                return Namespaces.getNamespaceMap(root.getTree("/"));
            }
        };

        NamePathMapperImpl namePathMapper = new NamePathMapperImpl(nameMapper, delegate.getIdManager());
        ValueFactoryImpl valueFactory = new ValueFactoryImpl(root.getBlobFactory(), namePathMapper);

        return new SessionContext(checkNotNull(repository), delegate, namePathMapper, valueFactory){
            private final SessionImpl session = new SessionImpl(this, namespaces);
            private final WorkspaceImpl workspace = new WorkspaceImpl(this);

            @Override
            public Session getSession() {
                return session;
            }

            @Override
            public WorkspaceImpl getWorkspaceInternal() {
                return workspace;
            }
        };
    }

    public Repository getRepository() {
        return repository;
    }

    public SessionDelegate getSessionDelegate() {
        return delegate;
    }

    public abstract Session getSession();

    protected abstract WorkspaceImpl getWorkspaceInternal();

    public Workspace getWorkspace() {
        return getWorkspaceInternal();
    }

    public LockManager getLockManager() {
        return getWorkspaceInternal().getLockManager();
    }

    public NodeTypeManager getNodeTypeManager() {
        return getWorkspaceInternal().getNodeTypeManager();
    }

    public VersionManager getVersionManager() throws RepositoryException {
        return getWorkspaceInternal().getVersionManager();
    }

    public EffectiveNodeTypeProvider getEffectiveNodeTypeProvider() {
        return getWorkspaceInternal().getReadWriteNodeTypeManager();
    }

    public DefinitionProvider getDefinitionProvider() {
        return getWorkspaceInternal().getReadWriteNodeTypeManager();
    }

    public ValueFactory getValueFactory() {
        return valueFactory;
    }

    @Nonnull
    public AccessControlManager getAccessControlManager() throws RepositoryException {
        if (accessControlManager == null) {
            // TODO
            Subject subject = new Subject(true, delegate.getAuthInfo().getPrincipals(), Collections.singleton(getPermissionProvider()), Collections.<Object>emptySet());
            accessControlManager = Subject.doAs(subject, new PrivilegedAction<AccessControlManager>() {
                @Override
                public AccessControlManager run() {
                    SecurityProvider securityProvider = repository.getSecurityProvider();
                    return securityProvider.getAccessControlConfiguration().getAccessControlManager(delegate.getRoot(), namePathMapper);
                }
            });
        }
        return accessControlManager;
    }

    @Nonnull
    public PermissionProvider getPermissionProvider() throws RepositoryException {
        if (permissionProvider == null) {
            SecurityProvider securityProvider = repository.getSecurityProvider();
            permissionProvider = securityProvider.getAccessControlConfiguration().getPermissionProvider(delegate.getRoot(), delegate.getAuthInfo().getPrincipals());
        } else {
            permissionProvider.refresh();
        }
        return permissionProvider;
    }

    @Nonnull
    public PrincipalManager getPrincipalManager() {
        if (principalManager == null) {
            SecurityProvider securityProvider = repository.getSecurityProvider();
            principalManager = securityProvider.getPrincipalConfiguration()
                    .getPrincipalManager(delegate.getRoot(), namePathMapper);
        }
        return principalManager;
    }

    @Nonnull
    public UserManager getUserManager() {
        if (userManager == null) {
            SecurityProvider securityProvider = repository.getSecurityProvider();
            userManager = securityProvider.getUserConfiguration().getUserManager(delegate.getRoot(), namePathMapper);
        }
        return userManager;
    }

    @Nonnull
    public PrivilegeManager getPrivilegeManager() {
        if (privilegeManager == null) {
            SecurityProvider securityProvider = repository.getSecurityProvider();
            privilegeManager = securityProvider.getPrivilegeConfiguration().getPrivilegeManager(delegate.getRoot(), namePathMapper);
        }
        return privilegeManager;
    }

    @Nonnull
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        // TODO: take non-security related importers into account as well (proper configuration)
        List<ProtectedItemImporter> importers = new ArrayList<ProtectedItemImporter>();
        for (SecurityConfiguration sc : repository.getSecurityProvider().getSecurityConfigurations()) {
            importers.addAll(sc.getProtectedItemImporters());
        }
        return importers;
    }


    @Nonnull
    public ObservationManager getObservationManager() {
        if (observationManager == null) {
            observationManager = new ObservationManagerImpl(
                    delegate.getRoot(), namePathMapper, repository.getObservationExecutor());
        }
        return observationManager;
    }

    public boolean hasPendingEvents() {
        return observationManager != null && observationManager.hasEvents();
    }

    //------------------------------------------------------------< NamePathMapper >---

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
        return namePathMapper.hasSessionLocalMappings();
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

    /**
     * Shortcut for {@code SessionDelegate.getOakPathKeepIndex(jcrPath)}.
     *
     * @param jcrPath JCR path
     * @return Oak path, or {@code null}, with indexes left intact
     * @throws javax.jcr.PathNotFoundException
     */
    @Nonnull
    public String getOakPathKeepIndexOrThrowNotFound(String jcrPath) throws PathNotFoundException {
        String oakPath = namePathMapper.getOakPathKeepIndex(jcrPath);
        if (oakPath != null) {
            return oakPath;
        } else {
            throw new PathNotFoundException(jcrPath);
        }
    }

    //------------------------------------------------------------< internal >---

    void dispose() {
        if (observationManager != null) {
            observationManager.dispose();
        }
    }
}

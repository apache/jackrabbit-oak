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

import java.util.ArrayList;
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
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

/**
 * Instances of this class are passed to all JCR implementation classes
 * (e.g. {@code SessionImpl}, {@code NodeImpl}, etc.) and provide access to
 * the session scoped instances generally needed (e.g. {@code NamePathMapper},
 * {@code ValueFactory}, etc.).
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
        // FIXME don't rely on a naked map. See OAK-715
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
            public Workspace getWorkspace() {
                return workspace;
            }

            @Override
            public LockManager getLockManager() {
                return workspace.getLockManager();
            }

            @Override
            public NodeTypeManager getNodeTypeManager() {
                return workspace.getNodeTypeManager();
            }

            @Override
            public VersionManager getVersionManager() throws RepositoryException {
                return workspace.getVersionManager();
            }

            @Override
            public EffectiveNodeTypeProvider getEffectiveNodeTypeProvider() {
                return workspace.getReadWriteNodeTypeManager();
            }

            @Override
            public DefinitionProvider getDefinitionProvider() {
                return workspace.getReadWriteNodeTypeManager();
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

    public abstract Workspace getWorkspace();

    public abstract LockManager getLockManager();

    public abstract NodeTypeManager getNodeTypeManager();

    public abstract VersionManager getVersionManager() throws RepositoryException;

    public abstract EffectiveNodeTypeProvider getEffectiveNodeTypeProvider();

    public abstract DefinitionProvider getDefinitionProvider();

    public ValueFactory getValueFactory() {
        return valueFactory;
    }

    @Nonnull
    public AccessControlManager getAccessControlManager() throws RepositoryException {
        if (accessControlManager == null) {
            SecurityProvider securityProvider = repository.getSecurityProvider();
            accessControlManager = securityProvider.getAccessControlConfiguration().getAccessControlManager(delegate.getRoot(), namePathMapper, getPermissionProvider());
        }
        return accessControlManager;
    }

    @Nonnull
    public PermissionProvider getPermissionProvider() throws RepositoryException {
        if (permissionProvider == null) {
            SecurityProvider securityProvider = repository.getSecurityProvider();
            permissionProvider = securityProvider.getAccessControlConfiguration().getPermissionProvider(delegate.getRoot(), delegate.getAuthInfo().getPrincipals());
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

    void refresh() {
        if (permissionProvider != null) {
            permissionProvider.refresh();
        }
    }
}

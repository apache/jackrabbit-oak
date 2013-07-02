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
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFactory;
import javax.jcr.Workspace;
import javax.jcr.lock.LockManager;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.observation.ObservationManager;
import javax.jcr.security.AccessControlManager;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.PropertyDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.VersionManagerDelegate;
import org.apache.jackrabbit.oak.jcr.observation.ObservationManagerImpl;
import org.apache.jackrabbit.oak.jcr.security.AccessManager;
import org.apache.jackrabbit.oak.jcr.version.VersionHistoryImpl;
import org.apache.jackrabbit.oak.jcr.version.VersionImpl;
import org.apache.jackrabbit.oak.namepath.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.nodetype.DefinitionProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.EffectiveNodeTypeProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation.Observable;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
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
    private final SessionDelegate delegate;
    private final SessionNamespaces namespaces;
    private final NamePathMapper namePathMapper;
    private final ValueFactory valueFactory;
    private final SessionImpl session;
    private final WorkspaceImpl workspace;

    private AccessControlManager accessControlManager;
    private PermissionProvider permissionProvider;
    private PrincipalManager principalManager;
    private UserManager userManager;
    private PrivilegeManager privilegeManager;
    private ObservationManagerImpl observationManager;

    SessionContext(
            RepositoryImpl repository, Whiteboard whiteboard,
            final SessionDelegate delegate) {
        this.repository = checkNotNull(repository);
        this.whiteboard = checkNotNull(whiteboard);
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

        this.session = new SessionImpl(this);
        this.workspace = new WorkspaceImpl(this);
    }

    public Session getSession() {
        return session;
    }

    public Workspace getWorkspace() {
        return workspace;
    }

    public LockManager getLockManager() {
        return workspace.getLockManager();
    }

    public NodeTypeManager getNodeTypeManager() {
        return workspace.getNodeTypeManager();
    }

    public VersionManager getVersionManager() throws RepositoryException {
        return workspace.getVersionManager();
    }

    public EffectiveNodeTypeProvider getEffectiveNodeTypeProvider() {
        return workspace.getReadWriteNodeTypeManager();
    }

    public DefinitionProvider getDefinitionProvider() {
        return workspace.getReadWriteNodeTypeManager();
    }

    public Repository getRepository() {
        return repository;
    }

    public SessionDelegate getSessionDelegate() {
        return delegate;
    }

    SessionNamespaces getNamespaces() {
        return namespaces;
    }

    public NodeImpl createNodeOrNull(NodeDelegate nd) throws RepositoryException {
        if (nd == null) {
            return null;
        }
        PropertyDelegate pd = nd.getPropertyOrNull(JcrConstants.JCR_PRIMARYTYPE);
        String type = pd != null ? pd.getString() : null;
        if (JcrConstants.NT_VERSION.equals(type)) {
            VersionManagerDelegate delegate = VersionManagerDelegate.create(getSessionDelegate());
            return new VersionImpl(delegate.createVersion(nd), this);
        } else if (JcrConstants.NT_VERSIONHISTORY.equals(type)) {
            VersionManagerDelegate delegate = VersionManagerDelegate.create(getSessionDelegate());
            return new VersionHistoryImpl(delegate.createVersionHistory(nd), this);
        } else {
            return new NodeImpl<NodeDelegate>(nd, this);
        }
    }

    public ValueFactory getValueFactory() {
        return valueFactory;
    }

    @Nonnull
    public AccessControlManager getAccessControlManager() throws RepositoryException {
        if (accessControlManager == null) {
            accessControlManager = getConfig(AccessControlConfiguration.class).getAccessControlManager(delegate.getRoot(), namePathMapper);
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
        if (userManager == null) {
            userManager = getConfig(UserConfiguration.class).getUserManager(delegate.getRoot(), namePathMapper);
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

    //-----------------------------------------------------------< internal >---
    @Nonnull
    AccessManager getAccessManager() throws RepositoryException {
        return new AccessManager(getPermissionProvider());
    }

    void dispose() {
        if (observationManager != null) {
            observationManager.dispose();
        }
        namespaces.clear();
    }

    void refresh() {
        if (permissionProvider != null) {
            permissionProvider.refresh();
        }
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private PermissionProvider getPermissionProvider() {
        if (permissionProvider == null) {
            permissionProvider = getConfig(AccessControlConfiguration.class).getPermissionProvider(delegate.getRoot(), delegate.getAuthInfo().getPrincipals());
        }
        return permissionProvider;
    }

    @Nonnull
    private <T> T getConfig(Class<T> clss) {
        return repository.getSecurityProvider().getConfiguration(clss);
    }
}

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
package org.apache.jackrabbit.oak.exercise.security.authorization.models.readonly;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.osgi.service.component.annotations.Component;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

/**
 * TODO ADD DESCRIPTION
 */
@Component(
        service = {AuthorizationConfiguration.class, SecurityConfiguration.class},
        immediate = true,
        property = OAK_SECURITY_NAME + "=org.apache.jackrabbit.oak.exercise.security.authorization.models.readonly.ReadOnlyAuthorizationConfiguration")
public final class ReadOnlyAuthorizationConfiguration extends ConfigurationBase implements AuthorizationConfiguration {

    private static final long READ_PERMISSIONS = Permissions.READ | Permissions.READ_ACCESS_CONTROL;
    private static final Set<String> READ_PRIVILEGE_NAMES = ImmutableSet.of(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL, PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.REP_READ_PROPERTIES);

    @Nonnull
    @Override
    public AccessControlManager getAccessControlManager(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper) {
        return new AbstractAccessControlManager(root, namePathMapper, getSecurityProvider()) {

            @Override
            public AccessControlPolicy[] getPolicies(String absPath) {
                return new AccessControlPolicy[0];
            }

            @Override
            public AccessControlPolicy[] getEffectivePolicies(String absPath) {
                return new AccessControlPolicy[0];
            }

            @Override
            public AccessControlPolicyIterator getApplicablePolicies(String absPath) {
                return new AccessControlPolicyIteratorAdapter(Iterators.emptyIterator());
            }

            @Override
            public void setPolicy(String absPath, AccessControlPolicy policy) throws UnsupportedRepositoryOperationException {
                throw new UnsupportedRepositoryOperationException();
            }

            @Override
            public void removePolicy(String absPath, AccessControlPolicy policy) throws UnsupportedRepositoryOperationException {
                throw new UnsupportedRepositoryOperationException();
            }

            @Override
            public JackrabbitAccessControlPolicy[] getApplicablePolicies(Principal principal) {
                return new JackrabbitAccessControlPolicy[0];
            }

            @Override
            public JackrabbitAccessControlPolicy[] getPolicies(Principal principal) {
                return new JackrabbitAccessControlPolicy[0];
            }

            @Override
            public AccessControlPolicy[] getEffectivePolicies(Set<Principal> set) {
                return new AccessControlPolicy[0];
            }
        };
    }

    @Nonnull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        return RestrictionProvider.EMPTY;
    }

    @Nonnull
    @Override
    public PermissionProvider getPermissionProvider(@Nonnull Root root, @Nonnull String workspaceName, @Nonnull Set<Principal> principals) {
        return new AggregatedPermissionProvider() {

            private Root immutableRoot = getRootProvider().createReadOnlyRoot(root);

            @Nonnull
            @Override
            public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
                return (privilegeBits != null) ? privilegeBits : new PrivilegeBitsProvider(immutableRoot).getBits(PrivilegeConstants.JCR_ALL);
            }

            @Override
            public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
                return permissions;
            }

            @Override
            public long supportedPermissions(@Nonnull TreeLocation location, long permissions) {
                return permissions;
            }

            @Override
            public long supportedPermissions(@Nonnull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
                return permissions;
            }

            @Override
            public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
                return onlyReadPermissions(permissions);
            }

            @Nonnull
            @Override
            public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull TreePermission parentPermission) {
                return new ReadOnlyPermissions();
            }

            @Override
            public void refresh() {
                immutableRoot = getRootProvider().createReadOnlyRoot(root);
            }

            @Nonnull
            @Override
            public Set<String> getPrivileges(@Nullable Tree tree) {
                return READ_PRIVILEGE_NAMES;
            }

            @Override
            public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
                Set<String> privs = Sets.newHashSet(privilegeNames);
                privs.removeAll(READ_PRIVILEGE_NAMES);

                return privs.isEmpty();
            }

            @Nonnull
            @Override
            public RepositoryPermission getRepositoryPermission() {
                return RepositoryPermission.EMPTY;
            }

            @Nonnull
            @Override
            public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
                return ReadOnlyPermissions.INSTANCE;
            }

            @Override
            public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
                return onlyReadPermissions(permissions);
            }

            @Override
            public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
                return onlyReadPermissions(Permissions.getPermissions(jcrActions, TreeLocation.create(root, oakPath), false));
            }
        };
    }

    private static final boolean onlyReadPermissions(long permissions) {
        return Permissions.diff(permissions, READ_PERMISSIONS) == Permissions.NO_PERMISSION;
    }

    @Nonnull
    @Override
    public String getName() {
        return AuthorizationConfiguration.NAME;
    }

    @Nonnull
    @Override
    public ConfigurationParameters getParameters() {
        return ConfigurationParameters.EMPTY;
    }

    @Nonnull
    @Override
    public WorkspaceInitializer getWorkspaceInitializer() {
        return WorkspaceInitializer.DEFAULT;
    }

    @Nonnull
    @Override
    public RepositoryInitializer getRepositoryInitializer() {
        return RepositoryInitializer.DEFAULT;
    }

    @Nonnull
    @Override
    public List<? extends CommitHook> getCommitHooks(@Nonnull String workspaceName) {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public List<? extends ValidatorProvider> getValidators(@Nonnull String workspaceName, @Nonnull Set<Principal> principals, @Nonnull MoveTracker moveTracker) {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public List<ThreeWayConflictHandler> getConflictHandlers() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public Context getContext() {
        return Context.DEFAULT;
    }

    private static final class ReadOnlyPermissions implements TreePermission {

        private static final TreePermission INSTANCE = new ReadOnlyPermissions();

        @Nonnull
        @Override
        public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
            return this;
        }

        @Override
        public boolean canRead() {
            return true;
        }

        @Override
        public boolean canRead(@Nonnull PropertyState property) {
            return true;
        }

        @Override
        public boolean canReadAll() {
            return true;
        }

        @Override
        public boolean canReadProperties() {
            return true;
        }

        @Override
        public boolean isGranted(long permissions) {
            return onlyReadPermissions(permissions);
        }

        @Override
        public boolean isGranted(long permissions, @Nonnull PropertyState property) {
            return onlyReadPermissions(permissions);
        }
    }
}
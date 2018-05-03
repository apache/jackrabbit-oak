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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.NamedAccessControlPolicy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
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
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

/**
 * <h1>Read Only Authorization Model</h1>
 *
 * This authorization module forms part of the training material provided by the
 * <i>oak-exercise</i> module and must not be used in a productive environment!
 *
 * <h2>Overview</h2>
 * This simplistic authorization model is limited to permission evaluation and
 * doesn't support access control management.
 *
 * The permission evaluation is hardcoded to only allow read access to every single
 * item in the repository (even access control content). All other permissions are
 * denied for every set of principals.
 *
 * There exists a single exception to that rule: For the internal {@link SystemPrincipal}
 * permission evaluation is not enforced by this module i.e. this module is skipped.
 *
 * <h2>Intended Usage</h2>
 * This authorization model is intended to be used in 'AND' combination with the
 * default authorization setup defined by Oak (and optionally additional models
 * such as e.g. <i>oak-authorization-cug</i>.
 *
 * It is not intended to be used as standalone model as it would grant full read
 * access to everyone.
 *
 * <h2>Limitations</h2>
 * Experimental model for training purpose and not intended for usage in production.
 *
 * <h2>Key Features</h2>
 *
 * <h3>Access Control Management</h3>
 *
 * <table align="left" summary="">
 *     <tr><th align="left">Feature</th><th align="left">Description</th></tr>
 *     <tr><td>Supported Privileges</td><td>all</td></tr>
 *     <tr><td>Supports Custom Privileges</td><td>yes</td></tr>
 *     <tr><td>Management by Path</td><td>not supported</td></tr>
 *     <tr><td>Management by Principals</td><td>not supported</td></tr>
 *     <tr><td>Owned Policies</td><td>None</td></tr>
 *     <tr><td>Effective Policies by Path</td><td>for every path a single effective policy of type {@link NamedAccessControlPolicy}</td></tr>
 *     <tr><td>Effective Policies by Principals</td><td>for every set of principals a single effective policy of type {@link NamedAccessControlPolicy}</td></tr>
 * </table>
 *
 * <h3>Permission Evaluation</h3>
 *
 * <table summary="">
 *     <tr><th align="left">Feature</th><th align="left">Description</th></tr>
 *     <tr><td>Supported Permissions</td><td>all</td></tr>
 *     <tr><td>Aggregated Permission Provider</td><td>yes</td></tr>
 * </table>
 *
 * <h2>Representation in the Repository</h2>
 *
 * There exists no dedicated access control or permission content for this
 * authorization model as it doesn't persist any information into the repository.
 * {@link SecurityConfiguration#getContext()} therefore returns the {@link Context#DEFAULT default}.
 *
 * <h2>Configuration</h2>
 *
 * This model comes with a single mandatory configurable property:
 *
 * - configurationRanking : {@link  CompositeConfiguration#PARAM_RANKING}, no default value.
 *
 *
 * <h2>Installation Instructions</h2>
 *
 * The following steps are required to install this authorization model in an OSGi based Oak setup.
 *
 * <ul>
 *     <li>Upload the oak-exercise bundle</li>
 *     <li>Edit configuration of 'ReadOnlyAuthorizationConfiguration' specifying the mandatory ranking property</li>
 *     <li>Edit configuration of {@link org.apache.jackrabbit.oak.security.internal.SecurityProviderRegistration}
 *     <ul>
 *         <li>add {@code org.apache.jackrabbit.oak.exercise.security.authorization.models.readonly.ReadOnlyAuthorizationConfiguration}
 *             to the list of required service IDs</li>
 *         <li>make sure the 'Authorization Composition Type' is set to AND</li>
 *     </ul>
 *     </li>
 *     <li>Wait for the {@link org.apache.jackrabbit.oak.spi.security.SecurityProvider} to be successfully registered again.</li>
 * </ul>
 *
 */
@org.apache.felix.scr.annotations.Component(metatype = true, policy = org.apache.felix.scr.annotations.ConfigurationPolicy.REQUIRE)
@Service({AuthorizationConfiguration.class, org.apache.jackrabbit.oak.spi.security.SecurityConfiguration.class})
@Properties({
        @Property(name = CompositeConfiguration.PARAM_RANKING,
                label = "Ranking",
                description = "Ranking of this configuration in a setup with multiple authorization configurations.",
                intValue = 300),
        @Property(name = OAK_SECURITY_NAME,
                propertyPrivate = true,
                value = "org.apache.jackrabbit.oak.exercise.security.authorization.models.readonly.ReadOnlyAuthorizationConfiguration")
})
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
                return new AccessControlPolicy[] {ReadOnlyPolicy.INSTANCE};
            }

            @Override
            public AccessControlPolicyIterator getApplicablePolicies(String absPath) {
                return new AccessControlPolicyIteratorAdapter(Collections.emptyIterator());
            }

            @Override
            public void setPolicy(String absPath, AccessControlPolicy policy) throws AccessControlException {
                throw new AccessControlException();
            }

            @Override
            public void removePolicy(String absPath, AccessControlPolicy policy) throws AccessControlException {
                throw new AccessControlException();
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
                return new AccessControlPolicy[] {ReadOnlyPolicy.INSTANCE};
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
        if (principals.contains(SystemPrincipal.INSTANCE)) {
            return EmptyPermissionProvider.getInstance();
        } else {
            return new AggregatedPermissionProvider() {

                @Nonnull
                @Override
                public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
                    return (privilegeBits != null) ? privilegeBits : new PrivilegeBitsProvider(root).getBits(PrivilegeConstants.JCR_ALL);
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

    private static final class ReadOnlyPolicy implements NamedAccessControlPolicy {

        private static final NamedAccessControlPolicy INSTANCE = new ReadOnlyPolicy();

        @Override
        public String getName() {
            return "Read-only Policy defined by 'ReadOnlyAuthorizationConfiguration'";
        }
    }
}
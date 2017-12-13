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
package org.apache.jackrabbit.oak.security.authorization;

import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.version.VersionablePathHook;
import org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlImporter;
import org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlManagerImpl;
import org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlValidatorProvider;
import org.apache.jackrabbit.oak.security.authorization.permission.MountPermissionProvider;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionHook;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionProviderImpl;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionStoreValidatorProvider;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionValidatorProvider;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.Option;

import static org.apache.jackrabbit.oak.spi.security.RegistrationConstants.OAK_SECURITY_NAME;

/**
 * Default implementation of the {@code AccessControlConfiguration}.
 */
@Component(
        service = {AuthorizationConfiguration.class, SecurityConfiguration.class},
        property = OAK_SECURITY_NAME + "=org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl")
@Designate(ocd = AuthorizationConfigurationImpl.Configuration.class)
public class AuthorizationConfigurationImpl extends ConfigurationBase implements AuthorizationConfiguration {
    
    @ObjectClassDefinition(name = "Apache Jackrabbit Oak AuthorizationConfiguration")
    @interface Configuration {
        @AttributeDefinition(
                name = "Jackrabbit 2.x Permissions",
                description = "Enforce backwards compatible permission validation with respect to the configurable options.",
                cardinality = 2,
                options = {
                        @Option(label = "USER_MANAGEMENT", value = "USER_MANAGEMENT"),
                        @Option(label = "REMOVE_NODE", value = "REMOVE_NODE")
                })
        String permissionsJr2();
        @AttributeDefinition(
                name = "Import Behavior",
                description = "Behavior for access control related items upon XML import.",
                options = {
                        @Option(label = ImportBehavior.NAME_ABORT, value = ImportBehavior.NAME_ABORT),
                        @Option(label = ImportBehavior.NAME_BESTEFFORT, value = ImportBehavior.NAME_BESTEFFORT),
                        @Option(label = ImportBehavior.NAME_IGNORE, value = ImportBehavior.NAME_IGNORE)
                })
        String importBehavior() default ImportBehavior.NAME_ABORT;

        @AttributeDefinition(
                name = "Readable Paths",
                description = "Enable full read access to regular nodes and properties at the specified paths irrespective of other policies that may take effective.")
        String[] readPaths() default {
                NamespaceConstants.NAMESPACES_PATH,
                NodeTypeConstants.NODE_TYPES_PATH,
                PrivilegeConstants.PRIVILEGES_PATH };

        @AttributeDefinition(
                name = "Administrative Principals",
                description = "Allows to specify principals that should be granted full permissions on the complete repository content.",
                cardinality = 10)
        String[] administrativePrincipals();

        @AttributeDefinition(
                name = "Ranking",
                description = "Ranking of this configuration in a setup with multiple authorization configurations.")
        int configurationRanking() default 100;
    }

    @Reference
    private MountInfoProvider mountInfoProvider = Mounts.defaultMountInfoProvider();

    public AuthorizationConfigurationImpl() {
        super();
    }

    public AuthorizationConfigurationImpl(@Nonnull SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    // reference to @Configuration class needed for correct DS xml generation
    private void activate(Configuration configuration, Map<String, Object> properties) {
        setParameters(ConfigurationParameters.of(properties));
    }

    //----------------------------------------------< SecurityConfiguration >---
    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Nonnull
    @Override
    public Context getContext() {
        return AuthorizationContext.getInstance();
    }

    @Nonnull
    @Override
    public WorkspaceInitializer getWorkspaceInitializer() {
        return new AuthorizationInitializer(mountInfoProvider);
    }

    @Nonnull
    @Override
    public List<? extends CommitHook> getCommitHooks(@Nonnull String workspaceName) {
        return ImmutableList.of(
                new VersionablePathHook(workspaceName),
                new PermissionHook(workspaceName, getRestrictionProvider(), mountInfoProvider, getRootProvider(), getTreeProvider()));
    }

    @Nonnull
    @Override
    public List<ValidatorProvider> getValidators(@Nonnull String workspaceName, @Nonnull Set<Principal> principals, @Nonnull MoveTracker moveTracker) {
        return ImmutableList.of(
                new PermissionStoreValidatorProvider(),
                new PermissionValidatorProvider(getSecurityProvider(), workspaceName, principals, moveTracker, getRootProvider(), getTreeProvider()),
                new AccessControlValidatorProvider(getSecurityProvider(), getRootProvider(), getTreeProvider()));
    }

    @Nonnull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return ImmutableList.of(new AccessControlImporter());
    }

    //-----------------------------------------< AccessControlConfiguration >---
    @Nonnull
    @Override
    public AccessControlManager getAccessControlManager(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper) {
        return new AccessControlManagerImpl(root, namePathMapper, getSecurityProvider());
    }

    @Nonnull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        RestrictionProvider restrictionProvider = getParameters().getConfigValue(AccessControlConstants.PARAM_RESTRICTION_PROVIDER, null, RestrictionProvider.class);
        if (restrictionProvider == null) {
            // default
            restrictionProvider = new RestrictionProviderImpl();
        }
        return restrictionProvider;
    }

    @Nonnull
    @Override
    public PermissionProvider getPermissionProvider(@Nonnull Root root, @Nonnull String workspaceName,
            @Nonnull Set<Principal> principals) {
        Context ctx = getSecurityProvider().getConfiguration(AuthorizationConfiguration.class).getContext();

        if (mountInfoProvider.hasNonDefaultMounts()) {
            return new MountPermissionProvider(root, workspaceName, principals, getRestrictionProvider(),
                    getParameters(), ctx, mountInfoProvider, getRootProvider());
        } else {
            return new PermissionProviderImpl(root, workspaceName, principals, getRestrictionProvider(),
                    getParameters(), ctx, getRootProvider());
        }
    }

    public void bindMountInfoProvider(MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
    }

    public void unbindMountInfoProvider(MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = null;
    }
}

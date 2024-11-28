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
package org.apache.jackrabbit.oak.security.user;


import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.value.jcr.PartialValueFactory;
import org.apache.jackrabbit.oak.security.user.autosave.AutoSaveEnabledManager;
import org.apache.jackrabbit.oak.security.user.monitor.UserMonitor;
import org.apache.jackrabbit.oak.security.user.monitor.UserMonitorImpl;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.cache.CachePrincipalFactory;
import org.apache.jackrabbit.oak.spi.security.user.cache.CachedMembershipReader;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAware;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.stats.Monitor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.AttributeType;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.Option;

import static org.apache.jackrabbit.oak.plugins.value.jcr.PartialValueFactory.DEFAULT_BLOB_ACCESS_PROVIDER;

/**
 * Default implementation of the {@link UserConfiguration}.
 */
@Component(service = {UserConfiguration.class, SecurityConfiguration.class})
@Designate(ocd = UserConfigurationImpl.Configuration.class)
public class UserConfigurationImpl extends ConfigurationBase implements UserConfiguration, SecurityConfiguration {

    @ObjectClassDefinition(name = "Apache Jackrabbit Oak UserConfiguration")
    @interface Configuration {
        @AttributeDefinition(
                name = "User Path",
                description = "Path underneath which user nodes are being created.")
        String usersPath() default UserConstants.DEFAULT_USER_PATH;

        @AttributeDefinition(
                name = "Impersonator principals",
                description = "List of users who can impersonate and groups whose members can impersonate any user.",
                type = AttributeType.STRING)
        String[] impersonatorPrincipals() default {};

        @AttributeDefinition(
                name = "Group Path",
                description = "Path underneath which group nodes are being created.")
        String groupsPath() default UserConstants.DEFAULT_GROUP_PATH;

        @AttributeDefinition(
                name = "System User Relative Path",
                description = "Path relative to the user root path underneath which system user nodes are being " +
                        "created. The default value is 'system'.")
        String systemRelativePath() default UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH;

        @AttributeDefinition(
                name = "Default Depth",
                description = "Number of levels that are used by default to store authorizable nodes")
        int defaultDepth() default UserConstants.DEFAULT_DEPTH;

        @AttributeDefinition(
                name = "Import Behavior",
                description = "Behavior for user/group related items upon XML import.",
                options = {
                        @Option(label = ImportBehavior.NAME_ABORT, value = ImportBehavior.NAME_ABORT),
                        @Option(label = ImportBehavior.NAME_BESTEFFORT, value = ImportBehavior.NAME_BESTEFFORT),
                        @Option(label = ImportBehavior.NAME_IGNORE, value = ImportBehavior.NAME_IGNORE)
                })
        String importBehavior() default ImportBehavior.NAME_IGNORE;

        @AttributeDefinition(
                name = "Hash Algorithm",
                description = "Name of the algorithm used to generate the password hash.")
        String passwordHashAlgorithm() default PasswordUtil.DEFAULT_ALGORITHM;

        @AttributeDefinition(
                name = "Hash Iterations",
                description = "Number of iterations to generate the password hash.")
        int passwordHashIterations() default PasswordUtil.DEFAULT_ITERATIONS;

        @AttributeDefinition(
                name = "Hash Salt Size",
                description = "Salt size to generate the password hash.")
        int passwordSaltSize() default PasswordUtil.DEFAULT_SALT_SIZE;

        @AttributeDefinition(
                name = "Omit Admin Password",
                description = "Boolean flag to prevent the administrator account to be created with a password " +
                        "upon repository initialization. Please note that changing this option after the initial " +
                        "repository setup will have no effect.")
        boolean omitAdminPw() default false;

        @AttributeDefinition(
                name = "Autosave Support",
                description = "Configuration option to enable autosave behavior. Note: this config option is " +
                        "present for backwards compatibility with Jackrabbit 2.x and should only be used for " +
                        "broken code that doesn't properly verify the autosave behavior (see Jackrabbit API). " +
                        "If this option is turned on autosave will be enabled by default; otherwise autosave is " +
                        "not supported.")
        boolean supportAutoSave() default false;

        @AttributeDefinition(
                name = "Maximum Password Age",
                description = "Maximum age in days a password may have. Values greater 0 will implicitly enable " +
                        "password expiry. A value of 0 indicates unlimited password age.")
        int passwordMaxAge() default UserConstants.DEFAULT_PASSWORD_MAX_AGE;

        @AttributeDefinition(
                name = "Change Password On First Login",
                description = "When enabled, forces users to change their password upon first login.")
        boolean initialPasswordChange() default UserConstants.DEFAULT_PASSWORD_INITIAL_CHANGE;

        @AttributeDefinition(
                name = "Maximum Password History Size",
                description = "Maximum number of passwords recorded for a user after changing her password (NOTE: " +
                        "upper limit is 1000). When changing the password the new password must not be present in the " +
                        "password history. A value of 0 indicates no password history is recorded.")
        int passwordHistorySize() default UserConstants.PASSWORD_HISTORY_DISABLED_SIZE;

        @AttributeDefinition(
                name = "Enable Password Expiry for Admin User",
                description = "When enabled, the admin user will also be subject to password expiry. The default value is false for backwards compatibility."
        )
        boolean passwordExpiryForAdmin() default false;

        @AttributeDefinition(
                name = "Principal Cache Expiration",
                description = "Optional configuration defining the number of milliseconds " +
                        "until the principal cache expires (NOTE: currently only respected for principal resolution " +
                        "with the internal system session such as used for login). If not set or equal/lower than zero " +
                        "no caches are created/evaluated.")
        long cacheExpiration() default CacheConfiguration.EXPIRATION_NO_CACHE;
        
        @AttributeDefinition(
                name = "Principal Cache Stale Time",
                description = "Optional configuration defining the number of milliseconds " +
                        "for which a stale principal cache can be served if another thread is already writing the cache. If not set or " +
                        "zero no stale cache is returned and group principals are read from the persistence without being cached. " +
                        "This configuration option only takes effect if the principal cache is enabled with a " +
                        "'Principal Cache Expiration' value greater than zero.")
        long cacheMaxStale() default CacheConfiguration.NO_STALE_CACHE;

        @AttributeDefinition(
                name = "RFC7613 Username Comparison Profile",
                description = "Enable the UsercaseMappedProfile defined in RFC7613 for username comparison.")
        boolean enableRFC7613UsercaseMappedProfile() default false;

        @AttributeDefinition(
                name = "Allow Disable Anonymous User",
                description = "By default the anonymous user can be disabled. By changing this option to false trying "
                        + "to disable the anonymous user will throw an exception.")
        boolean allowDisableAnonymous() default true;
    }

    private static final UserAuthenticationFactory DEFAULT_AUTH_FACTORY = new UserAuthenticationFactoryImpl();

    private UserMonitor monitor = UserMonitor.NOOP;

    private BlobAccessProvider blobAccessProvider;

    private final DynamicMembershipTracker dynamicMembership = new DynamicMembershipTracker();
    
    public UserConfigurationImpl() {
        super();
    }

    public UserConfigurationImpl(SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    public static UserAuthenticationFactory getDefaultAuthenticationFactory() {
        return DEFAULT_AUTH_FACTORY;
    }

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    // reference to @Configuration class needed for correct DS xml generation
    private void activate(Configuration configuration, BundleContext bundleContext, Map<String, Object> properties) {
        setParameters(ConfigurationParameters.of(properties));
        dynamicMembership.start(new OsgiWhiteboard(bundleContext));
    }
    
    @Deactivate
    private void deactivate() {
        dynamicMembership.stop();
    }
    
    @Reference(cardinality = ReferenceCardinality.OPTIONAL, policy = ReferencePolicy.DYNAMIC)
    void bindBlobAccessProvider(BlobAccessProvider bap) {
        blobAccessProvider = bap;
    }

    void unbindBlobAccessProvider(BlobAccessProvider bap) {
        blobAccessProvider = DEFAULT_BLOB_ACCESS_PROVIDER;
    }

    //----------------------------------------------< SecurityConfiguration >---
    @NotNull
    @Override
    public String getName() {
        return NAME;
    }

    @NotNull
    @Override
    public ConfigurationParameters getParameters() {
        ConfigurationParameters params = super.getParameters();
        if (!params.containsKey(UserConstants.PARAM_USER_AUTHENTICATION_FACTORY)) {
            return ConfigurationParameters.of(
                    params,
                    ConfigurationParameters.of(UserConstants.PARAM_USER_AUTHENTICATION_FACTORY, DEFAULT_AUTH_FACTORY));
        } else {
            return params;
        }
    }

    @NotNull
    @Override
    public WorkspaceInitializer getWorkspaceInitializer() {
        return new UserInitializer(getSecurityProvider());
    }

    @NotNull
    @Override
    public List<? extends ValidatorProvider> getValidators(@NotNull String workspaceName, @NotNull Set<Principal> principals, @NotNull MoveTracker moveTracker) {
        return ImmutableList.of(new UserValidatorProvider(getParameters(), getRootProvider(), getTreeProvider()), new CacheValidatorProvider(principals, getTreeProvider()));
    }

    @NotNull
    @Override
    public List<ThreeWayConflictHandler> getConflictHandlers() {
        return ImmutableList.of(new RepMembersConflictHandler(), new CacheConflictHandler());
    }

    @NotNull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return Collections.singletonList(new UserImporter(getParameters()));
    }

    @NotNull
    @Override
    public Context getContext() {
        return UserContext.getInstance();
    }

    @Override
    public @NotNull Iterable<Monitor<?>> getMonitors(@NotNull StatisticsProvider statisticsProvider) {
        monitor = new UserMonitorImpl(statisticsProvider);
        return Collections.singleton(monitor);
    }

    //--------------------------------------------------< UserConfiguration >---
    @NotNull
    @Override
    public UserManager getUserManager(Root root, NamePathMapper namePathMapper) {
        PartialValueFactory vf = new PartialValueFactory(namePathMapper, getBlobAccessProvider());
        UserManagerImpl umgr = new UserManagerImpl(root, vf, getSecurityProvider(), monitor, dynamicMembership);
        if (getParameters().getConfigValue(UserConstants.PARAM_SUPPORT_AUTOSAVE, false)) {
            return new AutoSaveEnabledManager(umgr, root);
        } else {
            return umgr;
        }
    }

    @Nullable
    @Override
    public PrincipalProvider getUserPrincipalProvider(@NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        return new UserPrincipalProvider(root, this, namePathMapper);
    }

    @Override
    public CachedMembershipReader getCachedMembershipReader(@NotNull Root root,
            @NotNull CachePrincipalFactory cachePrincipalFactory, @NotNull String propName) {
        CacheConfiguration cacheConfig = CacheConfiguration.fromUserConfiguration(this, propName);
        if (cacheConfig.isCacheEnabled()) {
            return new CachedPrincipalMembershipReader(cacheConfig, root, cachePrincipalFactory);
        } else {
            return null;
        }
    }

    @NotNull
    private BlobAccessProvider getBlobAccessProvider() {
        BlobAccessProvider provider = blobAccessProvider;
        if (provider == null) {
            SecurityProvider securityProvider = getSecurityProvider();
            if (securityProvider instanceof WhiteboardAware) {
                Whiteboard wb = ((WhiteboardAware) securityProvider).getWhiteboard();
                if (wb != null) {
                    provider = WhiteboardUtils.getService(wb, BlobAccessProvider.class);
                }
            }
        }
        if (provider == null) {
            provider = DEFAULT_BLOB_ACCESS_PROVIDER;
        }
        blobAccessProvider = provider;
        return provider;
    }
}

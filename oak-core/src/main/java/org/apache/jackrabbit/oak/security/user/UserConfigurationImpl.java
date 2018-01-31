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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.user.autosave.AutoSaveEnabledManager;
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
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.Option;

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
                name = "Principal Cache Expiration",
                description = "Optional configuration defining the number of milliseconds " +
                        "until the principal cache expires (NOTE: currently only respected for principal resolution " +
                        "with the internal system session such as used for login). If not set or equal/lower than zero " +
                        "no caches are created/evaluated.")
        long cacheExpiration() default UserPrincipalProvider.EXPIRATION_NO_CACHE;

        @AttributeDefinition(
                name = "RFC7613 Username Comparison Profile",
                description = "Enable the UsercaseMappedProfile defined in RFC7613 for username comparison.")
        boolean enableRFC7613UsercaseMappedProfile() default false;
    }

    private static final UserAuthenticationFactory DEFAULT_AUTH_FACTORY = new UserAuthenticationFactoryImpl();

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

    @Nonnull
    @Override
    public WorkspaceInitializer getWorkspaceInitializer() {
        return new UserInitializer(getSecurityProvider());
    }

    @Nonnull
    @Override
    public List<? extends ValidatorProvider> getValidators(@Nonnull String workspaceName, @Nonnull Set<Principal> principals, @Nonnull MoveTracker moveTracker) {
        return ImmutableList.of(new UserValidatorProvider(getParameters(), getRootProvider(), getTreeProvider()), new CacheValidatorProvider(principals, getTreeProvider()));
    }

    @Nonnull
    @Override
    public List<ThreeWayConflictHandler> getConflictHandlers() {
        return ImmutableList.of(new RepMembersConflictHandler());
    }

    @Nonnull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return Collections.<ProtectedItemImporter>singletonList(new UserImporter(getParameters()));
    }

    @Nonnull
    @Override
    public Context getContext() {
        return UserContext.getInstance();
    }

    //--------------------------------------------------< UserConfiguration >---
    @Nonnull
    @Override
    public UserManager getUserManager(Root root, NamePathMapper namePathMapper) {
        UserManager umgr = new UserManagerImpl(root, namePathMapper, getSecurityProvider());
        if (getParameters().getConfigValue(UserConstants.PARAM_SUPPORT_AUTOSAVE, false)) {
            return new AutoSaveEnabledManager(umgr, root);
        } else {
            return umgr;
        }
    }

    @Nullable
    @Override
    public PrincipalProvider getUserPrincipalProvider(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper) {
        return new UserPrincipalProvider(root, this, namePathMapper);
    }
}

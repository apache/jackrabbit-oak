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
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Properties;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyOption;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.user.autosave.AutoSaveEnabledManager;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
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

/**
 * Default implementation of the {@link UserConfiguration}.
 */
@Component(metatype = true, label = "Apache Jackrabbit Oak UserConfiguration")
@Service({UserConfiguration.class, SecurityConfiguration.class})
@Properties({
        @Property(name = UserConstants.PARAM_USER_PATH,
                label = "User Path",
                description = "Path underneath which user nodes are being created.",
                value = UserConstants.DEFAULT_USER_PATH),
        @Property(name = UserConstants.PARAM_GROUP_PATH,
                label = "Group Path",
                description = "Path underneath which group nodes are being created.",
                value = UserConstants.DEFAULT_GROUP_PATH),
        @Property(name = UserConstants.PARAM_SYSTEM_RELATIVE_PATH,
                label = "System User Relative Path",
                description = "Path relative to the user root path underneath which system user nodes are being created. The default value is 'system'.",
                value = UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH),
        @Property(name = UserConstants.PARAM_DEFAULT_DEPTH,
                label = "Default Depth",
                description = "Number of levels that are used by default to store authorizable nodes",
                intValue = UserConstants.DEFAULT_DEPTH),
        @Property(name = ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR,
                label = "Import Behavior",
                description = "Behavior for user/group related items upon XML import.",
                options = {
                        @PropertyOption(name = ImportBehavior.NAME_ABORT, value = ImportBehavior.NAME_ABORT),
                        @PropertyOption(name = ImportBehavior.NAME_BESTEFFORT, value = ImportBehavior.NAME_BESTEFFORT),
                        @PropertyOption(name = ImportBehavior.NAME_IGNORE, value = ImportBehavior.NAME_IGNORE)
                },
                value = ImportBehavior.NAME_IGNORE),
        @Property(name = UserConstants.PARAM_PASSWORD_HASH_ALGORITHM,
                label = "Hash Algorithm",
                description = "Name of the algorithm used to generate the password hash.",
                value = PasswordUtil.DEFAULT_ALGORITHM),
        @Property(name = UserConstants.PARAM_PASSWORD_HASH_ITERATIONS,
                label = "Hash Iterations",
                description = "Number of iterations to generate the password hash.",
                intValue = PasswordUtil.DEFAULT_ITERATIONS),
        @Property(name = UserConstants.PARAM_PASSWORD_SALT_SIZE,
                label = "Hash Salt Size",
                description = "Salt size to generate the password hash.",
                intValue = PasswordUtil.DEFAULT_SALT_SIZE),
        @Property(name = UserConstants.PARAM_OMIT_ADMIN_PW,
                label = "Omit Admin Password",
                description = "Boolean flag to prevent the administrator account to be created with a password upon repository initialization. Please note that changing this option after the initial repository setup will have no effect.",
                boolValue = false),
        @Property(name = UserConstants.PARAM_SUPPORT_AUTOSAVE,
                label = "Autosave Support",
                description = "Configuration option to enable autosave behavior. Note: this config option is present for backwards compatibility with Jackrabbit 2.x and should only be used for broken code that doesn't properly verify the autosave behavior (see Jackrabbit API). If this option is turned on autosave will be enabled by default; otherwise autosave is not supported.",
                boolValue = false),
        @Property(name = UserConstants.PARAM_PASSWORD_MAX_AGE,
                label = "Maximum Password Age",
                description = "Maximum age in days a password may have. Values greater 0 will implicitly enable password expiry. A value of 0 indicates unlimited password age.",
                intValue = UserConstants.DEFAULT_PASSWORD_MAX_AGE),
        @Property(name = UserConstants.PARAM_PASSWORD_INITIAL_CHANGE,
                label = "Change Password On First Login",
                description = "When enabled, forces users to change their password upon first login.",
                boolValue = UserConstants.DEFAULT_PASSWORD_INITIAL_CHANGE),
        @Property(name = UserConstants.PARAM_PASSWORD_HISTORY_SIZE,
                label = "Maximum Password History Size",
                description = "Maximum number of passwords recorded for a user after changing her password (NOTE: upper limit is 1000). When changing the password the new password must not be present in the password history. A value of 0 indicates no password history is recorded.",
                intValue = UserConstants.PASSWORD_HISTORY_DISABLED_SIZE),
        @Property(name = UserPrincipalProvider.PARAM_CACHE_EXPIRATION,
                label = "Principal Cache Expiration",
                description = "Optional configuration defining the number of milliseconds " +
                        "until the principal cache expires (NOTE: currently only respected for principal resolution with the internal system session such as used for login). " +
                        "If not set or equal/lower than zero no caches are created/evaluated.",
                longValue = UserPrincipalProvider.EXPIRATION_NO_CACHE),
        @Property(name = UserConstants.PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE,
                label = "RFC7613 Username Comparison Profile",
                description = "Enable the UsercaseMappedProfile defined in RFC7613 for username comparison.",
                boolValue = false)
})
public class UserConfigurationImpl extends ConfigurationBase implements UserConfiguration, SecurityConfiguration {

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
    private void activate(Map<String, Object> properties) {
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
        return ImmutableList.of(new UserValidatorProvider(getParameters()), new CacheValidatorProvider(principals));
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

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
        @Property(name = UserConstants.PARAM_SUPPORT_AUTOSAVE,
                label = "Autosave Support",
                description = "Configuration option to enable autosave behavior. Note: this config option is present for backwards compatibility with Jackrabbit 2.x and should only be used for broken code that doesn't properly verify the autosave behavior (see Jackrabbit API). If this option is turned on autosave will be enabled by default; otherwise autosave is not supported.",
                boolValue = false)
})
public class UserConfigurationImpl extends ConfigurationBase implements UserConfiguration, SecurityConfiguration {

    public UserConfigurationImpl() {
        super();
    }

    public UserConfigurationImpl(SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

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
    public WorkspaceInitializer getWorkspaceInitializer() {
        return new UserInitializer(getSecurityProvider());
    }

    @Nonnull
    @Override
    public List<? extends ValidatorProvider> getValidators(String workspaceName, Set<Principal> principals, MoveTracker moveTracker) {
        return Collections.singletonList(new UserValidatorProvider(getParameters()));
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
}

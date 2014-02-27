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
import java.util.Dictionary;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
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
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

/**
 * Default implementation of the {@link UserConfiguration}.
 */
@Component()
@Service({UserConfiguration.class, SecurityConfiguration.class})
public class UserConfigurationImpl extends ConfigurationBase implements UserConfiguration, SecurityConfiguration {

    public UserConfigurationImpl() {
        super();
    }

    public UserConfigurationImpl(SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    @Activate
    private void activate(Dictionary<String, Object> properties) {
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

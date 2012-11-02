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

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.Session;

import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;

/**
 * UserConfigurationImpl... TODO
 */
public class UserConfigurationImpl implements UserConfiguration {

    private final ConfigurationParameters config;
    private final SecurityProvider securityProvider;

    public UserConfigurationImpl(ConfigurationParameters config,
                                 SecurityProvider securityProvider) {
        this.config = config;
        this.securityProvider = securityProvider;
    }

    @Nonnull
    @Override
    public ConfigurationParameters getConfigurationParameters() {
        return config;
    }

    @Override
    public List<ValidatorProvider> getValidatorProviders() {
        ValidatorProvider vp = new UserValidatorProvider(getConfigurationParameters());
        return Collections.singletonList(vp);
    }

    @Nonnull
    @Override
    public List<AuthorizableAction> getAuthorizableActions() {
        // TODO: create authorizable actions from configuration
        return Collections.emptyList();
    }

    @Override
    public UserManager getUserManager(Root root, NamePathMapper namePathMapper, Session session) {
        return new UserManagerImpl(session, root, namePathMapper, securityProvider);
    }

    @Override
    public UserManager getUserManager(Root root, NamePathMapper namePathMapper) {
        return new UserManagerImpl(null, root, namePathMapper, securityProvider);
    }
}
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
package org.apache.jackrabbit.oak.security.privilege;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;

/**
 * Configuration for the privilege management component.
 */
@Component(service = {PrivilegeConfiguration.class, SecurityConfiguration.class})
public class PrivilegeConfigurationImpl extends ConfigurationBase implements PrivilegeConfiguration {

    //---------------------------------------------< PrivilegeConfiguration >---
    @NotNull
    @Override
    public PrivilegeManager getPrivilegeManager(@NotNull Root root, @NotNull NamePathMapper namePathMapper) {
        return new PrivilegeManagerImpl(root, namePathMapper);
    }

    @SuppressWarnings("UnusedDeclaration")
    @Activate
    private void activate(Map<String, Object> properties) {
        setParameters(ConfigurationParameters.of(properties));
    }

    //----------------------------------------------< SecurityConfiguration >---
    @NotNull
    @Override
    public String getName() {
        return NAME;
    }

    @NotNull
    @Override
    public RepositoryInitializer getRepositoryInitializer() {
        return new PrivilegeInitializer(getRootProvider());
    }

    @NotNull
    @Override
    public List<? extends CommitHook> getCommitHooks(@NotNull String workspaceName) {
        return Collections.singletonList(new JcrAllCommitHook());
    }

    @NotNull
    @Override
    public List<? extends ValidatorProvider> getValidators(@NotNull String workspaceName, @NotNull Set<Principal> principals, @NotNull MoveTracker moveTracker) {
        return Collections.singletonList(new PrivilegeValidatorProvider(getRootProvider(), getTreeProvider()));
    }

    @NotNull
    @Override
    public Context getContext() {
        return PrivilegeContext.getInstance();
    }
}

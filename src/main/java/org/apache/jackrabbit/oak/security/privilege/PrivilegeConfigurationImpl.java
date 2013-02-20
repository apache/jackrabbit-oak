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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitHookProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;

/**
 * Configuration for the privilege management component.
 */
public class PrivilegeConfigurationImpl extends SecurityConfiguration.Default implements PrivilegeConfiguration {

    //---------------------------------------------< PrivilegeConfiguration >---
    @Nonnull
    @Override
    public PrivilegeManager getPrivilegeManager(Root root, NamePathMapper namePathMapper) {
        return new PrivilegeManagerImpl(root, namePathMapper);
    }

    @Nonnull
    @Override
    public RepositoryInitializer getPrivilegeInitializer() {
        return new PrivilegeInitializer();
    }

    //----------------------------------------------< SecurityConfiguration >---
    @Nonnull
    @Override
    public CommitHookProvider getSecurityHooks() {
        return new CommitHookProvider() {
            @Override
            public CommitHook getCommitHook(String workspaceName) {
                return new JcrAllCommitHook();
            }
        };
    }

    @Nonnull
    @Override
    public CommitHookProvider getValidators() {
        return new CommitHookProvider() {
            @Nonnull
            @Override
            public CommitHook getCommitHook(@Nonnull String workspaceName) {
                return new ValidatingHook(new PrivilegeValidatorProvider());
            }
        };
    }

    @Nonnull
    @Override
    public Context getContext() {
        return PrivilegeContext.getInstance();
    }
}

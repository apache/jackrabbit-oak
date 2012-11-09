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

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;

/**
 * PrivilegeConfigurationImpl... TODO
 */
public class PrivilegeConfigurationImpl extends SecurityConfiguration.Default implements PrivilegeConfiguration {

    @Nonnull
    @Override
    public PrivilegeManager getPrivilegeManager(Root root, NamePathMapper namePathMapper) {
        return new ReadOnlyPrivilegeManager(root, namePathMapper);
    }

    @Nonnull
    @Override
    public PrivilegeManager getPrivilegeManager(ContentSession contentSession, Root root, NamePathMapper namePathMapper) {
        return new PrivilegeManagerImpl(root, namePathMapper, contentSession);
    }

    @Nonnull
    @Override
    public RepositoryInitializer getRepositoryInitializer() {
        return new PrivilegeInitializer();
    }

    @Nonnull
    @Override
    public List<ValidatorProvider> getValidatorProviders() {
        ValidatorProvider vp = new PrivilegeValidatorProvider();
        return Collections.singletonList(vp);
    }
}

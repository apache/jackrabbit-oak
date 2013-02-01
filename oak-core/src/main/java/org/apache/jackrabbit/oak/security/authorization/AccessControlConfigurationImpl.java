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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.version.VersionablePathHook;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

/**
 * {@code AccessControlConfigurationImpl} ... TODO
 */
public class AccessControlConfigurationImpl extends SecurityConfiguration.Default implements AccessControlConfiguration {

    private final SecurityProvider securityProvider;

    public AccessControlConfigurationImpl(SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
    }

    //----------------------------------------------< SecurityConfiguration >---

    @Override
    public Context getContext() {
        return AccessControlContext.getInstance();
    }

    @Nonnull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return Collections.<ProtectedItemImporter>singletonList(new AccessControlImporter(securityProvider));
    }

    //-----------------------------------------< AccessControlConfiguration >---
    @Override
    public AccessControlManager getAccessControlManager(Root root, NamePathMapper namePathMapper) {
        // TODO OAK-51
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Nonnull
    @Override
    public RestrictionProvider getRestrictionProvider(NamePathMapper namePathMapper) {
        return new RestrictionProviderImpl(namePathMapper);
    }

    @Nonnull
    @Override
    public PermissionProvider getPermissionProvider(Root root, Set<Principal> principals) {
        // TODO OAK-51
        return new TmpPermissionProvider(root, principals, securityProvider);
    }

    @Nonnull
    @Override
    public RepositoryInitializer getRepositoryInitializer() {
        return new AccessControlInitializer();
    }

    @Nonnull
    @Override
    public List<CommitHook> getCommitHooks() {
        // TODO: review if VersionablePathHook should be included here
        return ImmutableList.of(new PermissionHook(), new VersionablePathHook());
    }

    @Override
    public List<ValidatorProvider> getValidatorProviders() {
        List<ValidatorProvider> vps = new ArrayList<ValidatorProvider>();
        vps.add(new PermissionValidatorProvider(securityProvider));
        vps.add(new AccessControlValidatorProvider(securityProvider));
        return Collections.unmodifiableList(vps);
    }
}

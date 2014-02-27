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
import java.util.Collections;
import java.util.Dictionary;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlManager;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.version.VersionablePathHook;
import org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlImporter;
import org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlManagerImpl;
import org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlValidatorProvider;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionEntryCache;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionHook;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionProviderImpl;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionStoreValidatorProvider;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionValidatorProvider;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

import com.google.common.collect.ImmutableList;

/**
 * Default implementation of the {@code AccessControlConfiguration}.
 */
@Component()
@Service({AuthorizationConfiguration.class, SecurityConfiguration.class})
public class AuthorizationConfigurationImpl extends ConfigurationBase implements AuthorizationConfiguration {

    private final PermissionEntryCache permissionEntryCache = new PermissionEntryCache();

    public AuthorizationConfigurationImpl() {
        super();
    }

    @Activate
    private void activate(Dictionary<String, Object> properties) {
        setParameters(ConfigurationParameters.of(properties));
    }


    public AuthorizationConfigurationImpl(SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    //----------------------------------------------< SecurityConfiguration >---
    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return AuthorizationContext.getInstance();
    }

    @Nonnull
    @Override
    public WorkspaceInitializer getWorkspaceInitializer() {
        return new AuthorizationInitializer();
    }

    @Nonnull
    @Override
    public List<? extends CommitHook> getCommitHooks(String workspaceName) {
        return ImmutableList.of(
                new VersionablePathHook(workspaceName),
                new PermissionHook(workspaceName, getRestrictionProvider(), permissionEntryCache));
    }

    @Override
    public List<ValidatorProvider> getValidators(String workspaceName, Set<Principal> principals, MoveTracker moveTracker) {
        return ImmutableList.of(
                new PermissionStoreValidatorProvider(),
                new PermissionValidatorProvider(getSecurityProvider(), workspaceName, principals, moveTracker),
                new AccessControlValidatorProvider(getSecurityProvider()));
    }

    @Nonnull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return Collections.<ProtectedItemImporter>singletonList(new AccessControlImporter());
    }

    //-----------------------------------------< AccessControlConfiguration >---
    @Override
    public AccessControlManager getAccessControlManager(Root root, NamePathMapper namePathMapper) {
        return new AccessControlManagerImpl(root, namePathMapper, getSecurityProvider());
    }

    @Nonnull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        RestrictionProvider restrictionProvider = getParameters().getConfigValue(AccessControlConstants.PARAM_RESTRICTION_PROVIDER, null, RestrictionProvider.class);
        if (restrictionProvider == null) {
            // default
            restrictionProvider = new RestrictionProviderImpl();
        }
        return restrictionProvider;
    }

    @Nonnull
    @Override
    public PermissionProvider getPermissionProvider(Root root, String workspaceName, Set<Principal> principals) {
        return new PermissionProviderImpl(root, workspaceName, principals, this, permissionEntryCache.createLocalCache());
    }

}

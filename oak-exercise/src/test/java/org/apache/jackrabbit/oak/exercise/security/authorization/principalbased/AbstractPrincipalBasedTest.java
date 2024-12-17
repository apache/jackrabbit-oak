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
package org.apache.jackrabbit.oak.exercise.security.authorization.principalbased;

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.composite.MountInfoProviderService;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderHelper;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregationFilter;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.FilterProviderImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.PrincipalBasedAuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;

import javax.security.auth.Subject;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

abstract class AbstractPrincipalBasedTest extends AbstractSecurityTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private Group gr;
    private final Map<String, User> systemUsers = new HashMap<>();

    private final PrincipalBasedAuthorizationConfiguration principalBasedAuthorizationConfiguration = new PrincipalBasedAuthorizationConfiguration();

    @Override
    public void after() throws Exception {
        try {
            if (gr != null) {
                gr.remove();
            }
            for (User u : systemUsers.values()) {
                u.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    @Override
    protected SecurityProvider initSecurityProvider() {
        // principal-based authorization is designed for osgi-setup.
        // injecting the configuration into the security provider needs a some workarounds....
        FilterProvider filterProvider = getFilterProvider();
        context.registerInjectActivateService(filterProvider, Collections.singletonMap("path", getSupportedPath()));
        context.registerInjectActivateService(new MountInfoProviderService());
        ConfigurationParameters params = ConfigurationParameters.of(CompositeConfiguration.PARAM_RANKING, 500, "enableAggregationFilter", enableAggregationFilter());
        context.registerInjectActivateService(principalBasedAuthorizationConfiguration, params);

        SecurityProvider sp = super.initSecurityProvider();
        SecurityProviderHelper.updateConfig(sp, principalBasedAuthorizationConfiguration, AuthorizationConfiguration.class);
        if (enableAggregationFilter()) {
            AggregationFilter aggregationFilter = context.getService(AggregationFilter.class);
            ((CompositeAuthorizationConfiguration) sp.getConfiguration(AuthorizationConfiguration.class)).withAggregationFilter(aggregationFilter);
        }
        return sp;
    }

    @NotNull
    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of("authorizationCompositionType", getCompositionType().toString());
    }

    @NotNull
    CompositeAuthorizationConfiguration.CompositionType getCompositionType() {
        return CompositeAuthorizationConfiguration.CompositionType.AND;
    }

    @NotNull
    FilterProvider getFilterProvider() {
        return new FilterProviderImpl();
    }

    boolean enableAggregationFilter() {
        return true;
    }

    @NotNull
    String getSupportedPath() {
        return PathUtils.concat(UserConstants.DEFAULT_USER_PATH, getSupportedIntermediatePath());
    }

    @NotNull
    String getSupportedIntermediatePath() {
        return PathUtils.concatRelativePaths(UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH, "supported");
    }

    @NotNull
    Principal getRegularUserPrincipal() throws Exception {
        return getTestUser().getPrincipal();
    }

    @NotNull
    Principal getGroupPrincipal() throws Exception {
        if (gr == null) {
            gr = getUserManager(root).createGroup("groupId");
            root.commit();
        }
        return gr.getPrincipal();
    }

    @NotNull
    Principal getSystemUserPrincipal(@NotNull String name, @Nullable String intermediatePath) throws Exception {
        User su;
        if (systemUsers.containsKey(name)) {
            su = systemUsers.get(name);
        } else {
            su = getUserManager(root).createSystemUser(name, intermediatePath);
            root.commit();
            systemUsers.put(name, su);
        }
        return su.getPrincipal();
    }

    @NotNull
    PrincipalBasedAuthorizationConfiguration getPrincipalBasedAuthorizationConfiguration() {
        return principalBasedAuthorizationConfiguration;
    }

    @Nullable
    static PrincipalAccessControlList getApplicablePrincipalAccessControlList(@NotNull JackrabbitAccessControlManager acMgr, @NotNull Principal principal) throws Exception {
        Set<JackrabbitAccessControlPolicy> applicable = ImmutableSet.copyOf(acMgr.getApplicablePolicies(principal));
        PrincipalAccessControlList acl = (PrincipalAccessControlList) Iterables.find(applicable, accessControlPolicy -> accessControlPolicy instanceof PrincipalAccessControlList, null);
        return acl;
    }

    @NotNull
    ContentSession getTestSession(@NotNull Principal... principals) throws Exception {
        Subject subject = new Subject(true, ImmutableSet.copyOf(principals), Set.of(), Set.of());
        return Java23Subject.doAsPrivileged(subject, (PrivilegedExceptionAction<ContentSession>) () -> getContentRepository().login(null, null), null);
    }
}
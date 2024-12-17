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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.composite.MountInfoProviderService;
import org.apache.jackrabbit.oak.plugins.tree.impl.RootProviderService;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeProviderService;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.apache.sling.testing.mock.osgi.ReferenceViolationException;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CugConfigurationOsgiTest extends AbstractSecurityTest {

    private static final String EXCLUDED_PRINCIPAL_NAME = "excludedPrincipal";
    private static final String ANY_PRINCIPAL_NAME = "anyPrincipal";

    private static final Map<String, Object> PROPERTIES = Map.of(
            CugConstants.PARAM_CUG_ENABLED, true,
            CugConstants.PARAM_CUG_SUPPORTED_PATHS, new String[] {"/"});

    @Rule
    public final OsgiContext context = new OsgiContext();

    private CugConfiguration cugConfiguration;
    private CugExcludeImpl cugExclude;
    private String wspName;

    @Before
    public void before() throws Exception {
        super.before();

        wspName = root.getContentSession().getWorkspaceName();

        cugConfiguration = new CugConfiguration(getSecurityProvider());
        cugConfiguration.setRootProvider(new RootProviderService());
        cugConfiguration.setTreeProvider(new TreeProviderService());

        cugExclude = new CugExcludeImpl();

        MountInfoProviderService mip = new MountInfoProviderService();
        context.registerInjectActivateService(mip);
    }

    @Test(expected = ReferenceViolationException.class)
    public void testMissingCugExclude() {
        context.registerInjectActivateService(cugConfiguration, PROPERTIES);
    }

    @Test
    public void testCugExcludeExcludedDefault() {
        context.registerInjectActivateService(cugExclude);
        context.registerInjectActivateService(cugConfiguration, PROPERTIES);

        // default exclusion
        AdminPrincipal admin = () -> "name";
        SystemUserPrincipal suPrincipal = () -> "name";

        AuthorizationConfiguration config = context.getService(AuthorizationConfiguration.class);
        for (Principal p : new Principal[] {SystemPrincipal.INSTANCE, admin, suPrincipal}) {
            PermissionProvider permissionProvider = config.getPermissionProvider(root, wspName, Set.of(p));
            assertSame(EmptyPermissionProvider.getInstance(), permissionProvider);
        }

        // however, other principals must not be excluded
        PermissionProvider permissionProvider = config.getPermissionProvider(root, wspName, Set.of(new PrincipalImpl(EXCLUDED_PRINCIPAL_NAME)));
        assertTrue(permissionProvider instanceof CugPermissionProvider);
    }

    @Test
    public void testCugExcludeExcludedPrincipal() {
        context.registerInjectActivateService(cugExclude, Map.of("principalNames", new String[] {EXCLUDED_PRINCIPAL_NAME}));
        context.registerInjectActivateService(cugConfiguration, PROPERTIES);

        AuthorizationConfiguration config = context.getService(AuthorizationConfiguration.class);
        PermissionProvider permissionProvider = config.getPermissionProvider(root, wspName, Set.of(new PrincipalImpl(EXCLUDED_PRINCIPAL_NAME)));
        assertSame(EmptyPermissionProvider.getInstance(), permissionProvider);
    }

    @Test
    public void testCugExcludeAnyPrincipal() {
        context.registerInjectActivateService(cugExclude, Map.of("principalNames", new String[] {EXCLUDED_PRINCIPAL_NAME}));
        context.registerInjectActivateService(cugConfiguration, PROPERTIES);

        AuthorizationConfiguration config = context.getService(AuthorizationConfiguration.class);
        PermissionProvider permissionProvider = config.getPermissionProvider(root, wspName, Set.of(new PrincipalImpl(ANY_PRINCIPAL_NAME)));
        assertTrue(permissionProvider instanceof CugPermissionProvider);
    }

    @Test
    public void testNotEnabled() {
        context.registerInjectActivateService(cugExclude, Map.of("principalNames", new String[] {ANY_PRINCIPAL_NAME}));
        context.registerInjectActivateService(cugConfiguration, Map.of(
                CugConstants.PARAM_CUG_ENABLED, false,
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, new String[]{"/"}));

        AuthorizationConfiguration config = context.getService(AuthorizationConfiguration.class);
        PermissionProvider permissionProvider = config.getPermissionProvider(root, wspName, Set.of(new PrincipalImpl(ANY_PRINCIPAL_NAME)));
        assertSame(EmptyPermissionProvider.getInstance(), permissionProvider);
    }

    @Test
    public void testNoSupportedPaths() {
        context.registerInjectActivateService(cugExclude, Map.of("principalNames", new String[] {ANY_PRINCIPAL_NAME}));
        context.registerInjectActivateService(cugConfiguration, Map.of(
                CugConstants.PARAM_CUG_ENABLED, true,
                CugConstants.PARAM_CUG_SUPPORTED_PATHS, new String[0]));

        AuthorizationConfiguration config = context.getService(AuthorizationConfiguration.class);
        PermissionProvider permissionProvider = config.getPermissionProvider(root, wspName, Set.of(new PrincipalImpl(ANY_PRINCIPAL_NAME)));
        assertSame(EmptyPermissionProvider.getInstance(), permissionProvider);
    }
}
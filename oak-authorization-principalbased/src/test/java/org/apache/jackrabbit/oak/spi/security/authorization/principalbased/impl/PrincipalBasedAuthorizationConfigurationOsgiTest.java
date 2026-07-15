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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.oak.composite.MountInfoProviderService;
import org.apache.jackrabbit.oak.plugins.tree.impl.RootProviderService;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeProviderService;
import org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionProviderImpl;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderRegistration;
import org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregationFilter;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.util.Text;
import org.apache.sling.testing.mock.osgi.ReferenceViolationException;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PrincipalBasedAuthorizationConfigurationOsgiTest extends AbstractPrincipalBasedTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final PrincipalBasedAuthorizationConfiguration pbac = new PrincipalBasedAuthorizationConfiguration();

    @Test(expected = ReferenceViolationException.class)
    public void testMissingMandatoryReferences() {
        context.registerInjectActivateService(pbac, Map.of());
    }

    @Test(expected = ReferenceViolationException.class)
    public void testMissingMountInfoProviderReference() {
        context.registerInjectActivateService(new FilterProviderImpl(), Map.of("path", SUPPORTED_PATH));
        context.registerInjectActivateService(pbac);
    }

    @Test(expected = ReferenceViolationException.class)
    public void testMissingFilterProviderReference() {
        context.registerInjectActivateService(new MountInfoProviderService());
        context.registerInjectActivateService(pbac, Map.of());
    }

    @Test
    public void testMountCollidingWithFilterRoot() {
        FilterProviderImpl fp = new FilterProviderImpl();
        context.registerInjectActivateService(fp, Map.of("path", SUPPORTED_PATH));

        MountInfoProviderService mipService = new MountInfoProviderService();
        context.registerInjectActivateService(mipService, Map.of("mountedPaths", new String[] {SUPPORTED_PATH + "/some/subtree", "/etc"}));

        try {
            context.registerInjectActivateService(pbac, Map.of());
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void testMountMatchingFilterRoot() {
        FilterProviderImpl fp = new FilterProviderImpl();
        context.registerInjectActivateService(fp, Map.of("path", SUPPORTED_PATH));

        MountInfoProviderService mipService = new MountInfoProviderService();
        context.registerInjectActivateService(mipService, Map.of("mountedPaths", new String[] {SUPPORTED_PATH}));

        context.registerInjectActivateService(pbac, Map.of());
    }

    @Test
    public void testMountAboveFilterRoot() {
        FilterProviderImpl fp = new FilterProviderImpl();
        context.registerInjectActivateService(fp, Map.of("path", SUPPORTED_PATH));

        MountInfoProviderService mipService = new MountInfoProviderService();
        context.registerInjectActivateService(mipService, Map.of("mountedPaths", new String[] {Text.getRelativeParent(SUPPORTED_PATH, 1)}));

        context.registerInjectActivateService(pbac, Map.of());
    }

    @Test
    public void testMountsElsewhere() {
        FilterProviderImpl fp = new FilterProviderImpl();
        context.registerInjectActivateService(fp, Map.of("path", SUPPORTED_PATH));

        MountInfoProviderService mipService = new MountInfoProviderService();
        context.registerInjectActivateService(mipService, Map.of("mountedPaths", new String[] {"/etc", "/var/some/mount", UserConstants.DEFAULT_GROUP_PATH}));

        context.registerInjectActivateService(pbac, Map.of());
    }

    @Test
    public void testEnableAggregationFilter() throws Exception {
        context.registerInjectActivateService(new FilterProviderImpl(), Map.of("path", SUPPORTED_PATH));
        context.registerInjectActivateService(new MountInfoProviderService(), Map.of("mountedPaths", new String[] {"/etc", "/var/some/mount", UserConstants.DEFAULT_GROUP_PATH}));

        context.registerInjectActivateService(pbac, Map.of(Constants.PARAM_ENABLE_AGGREGATION_FILTER, true));
        assertNotNull(context.getService(AggregationFilter.class));

        context.registerInjectActivateService(new AuthorizationConfigurationImpl());
        context.registerInjectActivateService(new AuthenticationConfigurationImpl());
        context.registerInjectActivateService(new UserConfigurationImpl());
        context.registerInjectActivateService(new PrivilegeConfigurationImpl());
        context.registerInjectActivateService(new PrincipalConfigurationImpl());
        context.registerInjectActivateService(new RootProviderService());
        context.registerInjectActivateService(new TreeProviderService());
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);

        context.registerInjectActivateService(new SecurityProviderRegistration(), Map.of("requiredServicePids", new String[0]));
        SecurityProvider securityProvider = context.getService(SecurityProvider.class);
        assertNotNull(securityProvider);

        AuthorizationConfiguration ac = securityProvider.getConfiguration(AuthorizationConfiguration.class);
        assertTrue(ac instanceof CompositeAuthorizationConfiguration);
        assertEquals(2, ((CompositeAuthorizationConfiguration) ac).getConfigurations().size());

        PermissionProvider pp = ac.getPermissionProvider(root, adminSession.getWorkspaceName(), Set.of(getTestSystemUser().getPrincipal()));
        assertTrue(pp instanceof PrincipalBasedPermissionProvider);

        pp = ac.getPermissionProvider(root, adminSession.getWorkspaceName(), Set.of(getTestUser().getPrincipal()));
        assertTrue(pp instanceof PermissionProviderImpl);
    }
}
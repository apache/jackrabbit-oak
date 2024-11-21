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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlImporter;
import org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlValidatorProvider;
import org.apache.jackrabbit.oak.security.authorization.permission.MountPermissionProvider;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionHook;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionProviderImpl;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionStoreValidatorProvider;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionValidatorProvider;
import org.apache.jackrabbit.oak.security.authorization.permission.VersionablePathHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.PARAM_ADMINISTRATIVE_PRINCIPALS;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.PARAM_READ_PATHS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuthorizationConfigurationImplOSGiTest extends AbstractSecurityTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private AuthorizationConfigurationImpl authorizationConfiguration;

    private MountInfoProvider mip;

    @Override
    public void before() throws Exception {
        super.before();

        authorizationConfiguration =  new AuthorizationConfigurationImpl(getSecurityProvider());
        authorizationConfiguration.setTreeProvider(getTreeProvider());
        authorizationConfiguration.setRootProvider(getRootProvider());

        mip = when(mock(MountInfoProvider.class).hasNonDefaultMounts()).thenReturn(true).getMock();
        context.registerService(MountInfoProvider.class, mip);

        Map options = Map.of(PARAM_ADMINISTRATIVE_PRINCIPALS, "administrators");
        context.registerInjectActivateService(authorizationConfiguration, options);
    }

    @Test
    public void testGetParameters() {
        ConfigurationParameters params = authorizationConfiguration.getParameters();
        assertEquals("administrators", params.getConfigValue(PARAM_ADMINISTRATIVE_PRINCIPALS, "undefined"));
        assertEquals(PermissionConstants.DEFAULT_READ_PATHS, params.getConfigValue(PARAM_READ_PATHS, Set.of()));
    }

    @Test
    public void testGetName() {
        assertEquals(AuthorizationConfiguration.NAME, authorizationConfiguration.getName());
    }

    @Test
    public void testGetContext() {
        assertSame(AuthorizationContext.getInstance(), authorizationConfiguration.getContext());
    }

    @Test
    public void testGetWorkspaceInitializer() {
        assertTrue(authorizationConfiguration.getWorkspaceInitializer() instanceof AuthorizationInitializer);
    }

    @Test
    public void testGetCommitHooks() {
        List<Class> expected = ImmutableList.of(VersionablePathHook.class, PermissionHook.class);
        assertTrue(Iterables.elementsEqual(expected, Iterables.transform(authorizationConfiguration.getCommitHooks(adminSession.getWorkspaceName()), commitHook -> commitHook.getClass())));
    }

    @Test
    public void testGetValidators() {
        List<Class> expected = ImmutableList.of(PermissionStoreValidatorProvider.class, PermissionValidatorProvider.class, AccessControlValidatorProvider.class);
        assertTrue(Iterables.elementsEqual(expected, Iterables.transform(authorizationConfiguration.getValidators(adminSession.getWorkspaceName(), Set.of(), new MoveTracker()), commitHook -> commitHook.getClass())));
    }

    @Test
    public void testGetProtectedItemImporters() {
        List<ProtectedItemImporter> importers = authorizationConfiguration.getProtectedItemImporters();
        assertEquals(1, importers.size());
        assertTrue(importers.get(0) instanceof AccessControlImporter);
    }

    @Test
    public void testDefaultMountInfoProvider() {
        AuthorizationConfigurationImpl ac = new AuthorizationConfigurationImpl(getSecurityProvider());
        ac.setRootProvider(getRootProvider());
        ac.setTreeProvider(getTreeProvider());

        PermissionProvider pp = ac.getPermissionProvider(root, adminSession.getWorkspaceName(), Set.of(EveryonePrincipal.getInstance()));
        assertTrue(pp instanceof PermissionProviderImpl);
    }

    @Test
    public void testBindMountInfoProvider() {
        PermissionProvider pp = authorizationConfiguration.getPermissionProvider(root, adminSession.getWorkspaceName(), Set.of(EveryonePrincipal.getInstance()));
        assertTrue(pp instanceof MountPermissionProvider);
    }

    @Test
    public void testUnbindMountInfoProvider() throws Exception {
        authorizationConfiguration.unbindMountInfoProvider(mip);

        Field f = AuthorizationConfigurationImpl.class.getDeclaredField("mountInfoProvider");
        f.setAccessible(true);

        assertNull(f.get(authorizationConfiguration));
    }
}
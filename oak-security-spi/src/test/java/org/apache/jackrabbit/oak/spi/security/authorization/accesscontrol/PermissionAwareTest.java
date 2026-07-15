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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionAware;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class PermissionAwareTest extends AbstractAccessControlTest {

    private final PermissionProvider permissionProvider = mock(PermissionProvider.class);

    private Root awareRoot;
    private Root unawareRoot;

    private SecurityProvider securityProvider;

    @Before
    public void before() throws Exception {
        ContentSession cs = when(mock(ContentSession.class).getAuthInfo()).thenReturn(AuthInfo.EMPTY).getMock();
        when(cs.getWorkspaceName()).thenReturn("wsp");

        awareRoot = when(mock(Root.class, withSettings().extraInterfaces(PermissionAware.class)).getContentSession()).thenReturn(cs).getMock();
        when(((PermissionAware) awareRoot).getPermissionProvider()).thenReturn(permissionProvider);

        unawareRoot = when(mock(Root.class).getContentSession()).thenReturn(cs).getMock();

        PrivilegeManager privilegeManager = mock(PrivilegeManager.class);
        PrivilegeConfiguration privilegeConfiguration = Mockito.mock(PrivilegeConfiguration.class);
        when(privilegeConfiguration.getPrivilegeManager(any(Root.class), any(NamePathMapper.class))).thenReturn(privilegeManager);

        AuthorizationConfiguration authorizationConfiguration = mock(AuthorizationConfiguration.class);
        when(authorizationConfiguration.getPermissionProvider(unawareRoot, "wsp", Collections.emptySet())).thenReturn(permissionProvider);

        securityProvider = mock(SecurityProvider.class);
        when(securityProvider.getConfiguration(PrivilegeConfiguration.class)).thenReturn(privilegeConfiguration);
        when(securityProvider.getConfiguration(AuthorizationConfiguration.class)).thenReturn(authorizationConfiguration);
    }

    @Test
    public void testGetPermissionProviderRootAware() {
        PermissionAware pa = (PermissionAware) awareRoot;
        AbstractAccessControlManager acMgr = mock(AbstractAccessControlManager.class, withSettings().useConstructor(awareRoot, getNamePathMapper(), securityProvider).defaultAnswer(CALLS_REAL_METHODS));
        verify(pa, never()).getPermissionProvider();

        assertSame(permissionProvider, acMgr.getPermissionProvider());
        assertSame(permissionProvider, acMgr.getPermissionProvider());

        verify(permissionProvider, never()).refresh();
        verify(pa, times(1)).getPermissionProvider();
    }

    @Test
    public void testGetPermissionProviderRootNotAware() {
        AbstractAccessControlManager acMgr = mock(AbstractAccessControlManager.class, withSettings().useConstructor(unawareRoot, getNamePathMapper(), securityProvider).defaultAnswer(CALLS_REAL_METHODS));
        assertSame(permissionProvider, acMgr.getPermissionProvider());
        verify(permissionProvider, never()).refresh();

        assertSame(permissionProvider, acMgr.getPermissionProvider());
        assertSame(permissionProvider, acMgr.getPermissionProvider());
        verify(permissionProvider, times(2)).refresh();
    }
}
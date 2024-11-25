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

import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.DEFAULT_USER_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FilterImplTest extends AbstractPrincipalBasedTest {

    private Filter filter;
    private String supportedPath;

    @Before
    public void before() throws Exception {
        super.before();
        FilterProvider fp = getFilterProvider();
        filter = fp.getFilter(getSecurityProvider(), root, getNamePathMapper());
        supportedPath = fp.getFilterRoot();
    }

    @Test
    public void testCanHandleEmptySet()  {
        assertFalse(filter.canHandle(Collections.emptySet()));
    }

    @Test
    public void testCanHandleGroupPrincipal() throws Exception {
        assertFalse(filter.canHandle(singleton(getUserManager(root).createGroup("group").getPrincipal())));
    }

    @Test
    public void testCanHandleUserPrincipal() throws Exception {
        assertFalse(filter.canHandle(singleton(getTestUser().getPrincipal())));
    }

    @Test
    public void testCanHandleUnknownSystemUserPrincipal() {
        SystemUserPrincipal principal = () -> "systemUserPrincipal";
        assertFalse(filter.canHandle(singleton(principal)));
    }

    @Test
    public void testCanHandleRandomSystemUserPrincipal() throws Exception {
        Principal principal = getUserManager(root).createSystemUser("anySystemUser", null).getPrincipal();
        assertFalse(filter.canHandle(singleton(principal)));
    }

    @Test
    public void testCanHandleValidSystemUserPrincipal() throws Exception {
        assertTrue(filter.canHandle(singleton(getTestSystemUser().getPrincipal())));
    }

    @Test
    public void testCanHandleValidSystemUserPrincipal2() throws Exception {
        Principal principal = getTestSystemUser().getPrincipal();
        assertTrue(filter.canHandle(singleton((SystemUserPrincipal) () -> principal.getName())));
    }

    @Test
    public void testCanHandleWrongPrincipalClass() throws Exception {
        Principal principal = getTestSystemUser().getPrincipal();
        assertFalse(filter.canHandle(singleton((AdminPrincipal) () -> principal.getName())));
        assertFalse(filter.canHandle(singleton((new ItemBasedPrincipal() {
            @NotNull
            @Override
            public String getPath() throws RepositoryException {
                return ((ItemBasedPrincipal) principal).getPath();
            }

            @Override
            public String getName() {
                return principal.getName();
            }
        }))));
    }

    @Test
    public void testCanHandleItemBasedSystemUserPrincipalUnsupportedPath() throws Exception {
        // make sure supported path exists
        User tu = getTestSystemUser();
        assertTrue(root.getTree(supportedPath).exists());
        Principal principal = new TestPrincipal("name", PathUtils.getParentPath(supportedPath));
        assertFalse(filter.canHandle(singleton(principal)));
    }

    @Test
    public void testCanHandleMovedItemBasedSystemUserPrincipal() throws Exception {
        Principal principal = getTestSystemUser().getPrincipal();
        assertTrue(filter.canHandle(singleton(principal)));

        String oakPath = filter.getOakPath(principal);
        assertEquals(getNamePathMapper().getOakPath(getTestSystemUser().getPath()), oakPath);

        String destPath = oakPath + "_moved";
        root.move(oakPath, destPath);

        Principal movedPrincipal = new TestPrincipal(principal.getName(), destPath);
        assertTrue(filter.canHandle(singleton(movedPrincipal)));
        assertEquals(destPath, filter.getOakPath(movedPrincipal));
    }

    @Test
    public void testCanHandleMovedUnsupportedItemBasedSystemUserPrincipal() throws Exception {
        Principal principal = getTestSystemUser().getPrincipal();
        String srcPath = getNamePathMapper().getOakPath(getTestSystemUser().getPath());
        String destPath = PathUtils.concat(DEFAULT_USER_PATH, DEFAULT_SYSTEM_RELATIVE_PATH, "moved");
        Principal movedPrincipal = new TestPrincipal(principal.getName(), destPath);

        root.move(srcPath, destPath);
        assertFalse(filter.canHandle(singleton(movedPrincipal)));

        root.move(destPath, srcPath);
        assertTrue(filter.canHandle(singleton(principal)));
        assertEquals(srcPath, filter.getOakPath(movedPrincipal));
    }

    @Test
    public void testCanHandleRemoved() throws Exception {
        Principal principal = getTestSystemUser().getPrincipal();
        assertTrue(filter.canHandle(singleton(principal)));
        root.getTree(filter.getOakPath(principal)).remove();

        // using original principal again -> obtaining from cache without further validation
        assertTrue(filter.canHandle(singleton(principal)));
        // using SystemUserPrincipal without path -> obtaining from cache without further validation
        assertTrue(filter.canHandle(singleton((SystemUserPrincipal) () -> principal.getName())));

        // path mismatch -> lookup will reveal removed tree and adjust caches
        assertFalse(filter.canHandle(singleton(new TestPrincipal(principal.getName(), null))));
        // using original principal will no longer work
        assertFalse(filter.canHandle(singleton(principal)));
    }

    @Test
    public void testCanHandleGetPathThrows() {
        Principal principal = new TestPrincipal("name", null);
        assertFalse(filter.canHandle(singleton(principal)));
    }

    /**
     * Test that the filter can deal with principals that have been accessed with a different {@code NamePathMapper}.
     * This might occur with {@code AbstractAccessControlManager#hasPrivilege} and {@code AbstractAccessControlManager#getPrivileges},
     * when a {@code PermissionProvider} is built from the principal set passed to the Jackrabbit API methods (and not from
     * principals obtained on the system level when populating the {@code Subject}.
     */
    @Test
    public void testCanHandlePathMapperMismatch() throws Exception {
        Principal principal = getTestSystemUser().getPrincipal();

        // create filter with a different NamePathMapper than was used to build the principal
        Filter f =  getFilterProvider().getFilter(getSecurityProvider(), root, NamePathMapper.DEFAULT);
        assertTrue(f.canHandle(singleton(principal)));
    }

    @Test
    public void testCanHandlePathMapperMismatchUnknownPrincipal() {
        Principal principal = new TestPrincipal("name", PathUtils.concat(supportedPath, "oak:path/to/oak:principal"));

        // create filter with a different NamePathMapper than was used to build the principal
        // since the principal is not known to the PrincipalManager, the extra lookup doesn't reveal a valid principal.
        Filter f =  getFilterProvider().getFilter(getSecurityProvider(), root, NamePathMapper.DEFAULT);
        assertFalse(f.canHandle(singleton(principal)));
    }

    @Test
    public void testCanHandleCombination() throws Exception {
        assertFalse(filter.canHandle(Set.of(getTestSystemUser().getPrincipal(), getTestUser().getPrincipal())));
    }

    @Test
    public void testCanHandlePopulatesCache() throws Exception  {
        Principal principal = getTestSystemUser().getPrincipal();
        PrincipalProvider pp = when(mock(PrincipalProvider.class).getPrincipal(principal.getName())).thenReturn(principal).getMock();
        PrincipalConfiguration pc = when(mock(PrincipalConfiguration.class).getPrincipalProvider(root, getNamePathMapper())).thenReturn(pp).getMock();
        SecurityProvider sp = when(mock(SecurityProvider.class).getConfiguration(PrincipalConfiguration.class)).thenReturn(pc).getMock();

        Filter filter = getFilterProvider().getFilter(sp, root, getNamePathMapper());

        // call 'canHandle' twice
        Set<Principal> set = singleton((SystemUserPrincipal) () -> principal.getName());
        assertTrue(filter.canHandle(set));
        assertTrue(filter.canHandle(set));

        // principalprovider must only be hit once
        verify(pp, times(1)).getPrincipal(principal.getName());
    }

    @Test
    public void testCanHandlePopulatesCacheUnsupportedSystemUser() {
        Principal unsupported = (SystemUserPrincipal) () -> "unsupported";

        PrincipalProvider pp = when(mock(PrincipalProvider.class).getPrincipal(unsupported.getName())).thenReturn(unsupported).getMock();
        PrincipalConfiguration pc = when(mock(PrincipalConfiguration.class).getPrincipalProvider(root, getNamePathMapper())).thenReturn(pp).getMock();
        SecurityProvider sp = when(mock(SecurityProvider.class).getConfiguration(PrincipalConfiguration.class)).thenReturn(pc).getMock();

        Filter filter = getFilterProvider().getFilter(sp, root, getNamePathMapper());

        // call 'canHandle' twice
        Set<Principal> set = singleton(unsupported);
        assertFalse(filter.canHandle(set));
        assertFalse(filter.canHandle(set));

        // principalprovider must only be hit once
        verify(pp, times(1)).getPrincipal(unsupported.getName());
    }

    @Test
    public void testCanHandleUserPrincipalDoesntHitProvider() throws Exception  {
        Principal userPrincipal = getTestUser().getPrincipal();

        PrincipalProvider pp = when(mock(PrincipalProvider.class).getPrincipal(userPrincipal.getName())).thenReturn(userPrincipal).getMock();
        PrincipalConfiguration pc = when(mock(PrincipalConfiguration.class).getPrincipalProvider(root, getNamePathMapper())).thenReturn(pp).getMock();
        SecurityProvider sp = when(mock(SecurityProvider.class).getConfiguration(PrincipalConfiguration.class)).thenReturn(pc).getMock();

        Filter filter = getFilterProvider().getFilter(sp, root, getNamePathMapper());

        // call 'canHandle' twice
        Set<Principal> set = singleton(userPrincipal);
        assertFalse(filter.canHandle(set));
        assertFalse(filter.canHandle(set));

        // for a non-system-user principal the principal resolution must never be triggered
        verify(pp, never()).getPrincipal(userPrincipal.getName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPathUserPrincipal() throws Exception {
        filter.getOakPath(getTestUser().getPrincipal());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPathInvalidSystemUserPrincipal() {
        filter.getOakPath((SystemUserPrincipal) () -> "name");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPathValidSystemUserPrincipalNotValidated() throws Exception {
        filter.getOakPath(getTestSystemUser().getPrincipal());
    }

    @Test
    public void testGetPathValidatedSystemUserPrincipal() throws Exception {
        ItemBasedPrincipal principal = (ItemBasedPrincipal) getTestSystemUser().getPrincipal();
        filter.canHandle(singleton(principal));

        assertNotEquals(principal.getPath(), filter.getOakPath(principal));
        assertEquals(getNamePathMapper().getOakPath(principal.getPath()), filter.getOakPath(principal));
    }

    @Test
    public void testGetPathAfterGetValidUserPrincipal() throws Exception {
        ItemBasedPrincipal principal = (ItemBasedPrincipal) getTestSystemUser().getPrincipal();
        filter.getValidPrincipal(getNamePathMapper().getOakPath(principal.getPath()));

        assertNotEquals(principal.getPath(), filter.getOakPath(principal));
        assertEquals(getNamePathMapper().getOakPath(principal.getPath()), filter.getOakPath(principal));
    }

    @Test
    public void testGetPrincipalUserPath() throws Exception {
        assertNull(filter.getValidPrincipal(getNamePathMapper().getOakPath(getTestUser().getPath())));
    }

    @Test
    public void testGetPrincipalJcrPath() throws Exception {
        assertNull(filter.getValidPrincipal(getTestSystemUser().getPath()));
    }

    @Test
    public void testGetPrincipalSystemUserPath() throws Exception {
        User user = getTestSystemUser();
        Principal principal = user.getPrincipal();
        assertEquals(principal, filter.getValidPrincipal(getNamePathMapper().getOakPath(user.getPath())));
    }

    @Test
    public void testGetPrincipalSupportedRootPath() {
        assertNull(filter.getValidPrincipal(supportedPath));
    }

    @Test
    public void testGetPrincipalMockedItemBasedProvider() throws Exception {
        ItemBasedPrincipal principal = (ItemBasedPrincipal) getTestSystemUser().getPrincipal();
        String oakPath = getNamePathMapper().getOakPath(principal.getPath());

        PrincipalProvider pp = when(mock(PrincipalProvider.class).getItemBasedPrincipal(oakPath)).thenReturn(principal).getMock();
        PrincipalConfiguration pc = when(mock(PrincipalConfiguration.class).getPrincipalProvider(root, getNamePathMapper())).thenReturn(pp).getMock();
        SecurityProvider sp = when(mock(SecurityProvider.class).getConfiguration(PrincipalConfiguration.class)).thenReturn(pc).getMock();

        Filter filter = getFilterProvider().getFilter(sp, root, getNamePathMapper());

        // call 'getValidPrincipal' twice
        Principal p = filter.getValidPrincipal(oakPath);
        assertEquals(principal, p);
        assertEquals(principal.getPath(), ((ItemBasedPrincipal) p).getPath());
        assertEquals(principal, filter.getValidPrincipal(oakPath));

        verify(pp, times(2)).getItemBasedPrincipal(oakPath);
        verify(pp, never()).getPrincipal(principal.getName());
    }

    @Test
    public void testGetPrincipalMockedPrincipalProvider() throws Exception {
        ItemBasedPrincipal principal = (ItemBasedPrincipal) getTestSystemUser().getPrincipal();
        String oakPath = getNamePathMapper().getOakPath(principal.getPath());

        PrincipalProvider pp = mock(PrincipalProvider.class);
        PrincipalConfiguration pc = when(mock(PrincipalConfiguration.class).getPrincipalProvider(root, getNamePathMapper())).thenReturn(pp).getMock();
        SecurityProvider sp = when(mock(SecurityProvider.class).getConfiguration(PrincipalConfiguration.class)).thenReturn(pc).getMock();

        Filter filter = getFilterProvider().getFilter(sp, root, getNamePathMapper());

        assertNull(filter.getValidPrincipal(oakPath));
        verify(pp, never()).getPrincipal(principal.getName());
    }

    private final class TestPrincipal implements SystemUserPrincipal, ItemBasedPrincipal {

        private final String jcrPath;
        private final String name;

        private TestPrincipal(@NotNull String name, @Nullable String oakPath) {
            if (oakPath == null) {
                jcrPath = null;
            } else {
                jcrPath = getNamePathMapper().getJcrPath(oakPath);
                assertNotNull(jcrPath);
            }
            this.name = name;
        }

        @NotNull
        @Override
        public String getPath() throws RepositoryException {
            if (jcrPath != null) {
                return jcrPath;
            } else {
                throw new RepositoryException();
            }
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
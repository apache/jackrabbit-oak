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
package org.apache.jackrabbit.oak.security.principal;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.jcr.RepositoryException;

import java.security.Principal;
import java.util.Iterator;

import static org.apache.jackrabbit.oak.namepath.NamePathMapper.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class PrincipalProviderImplErrorTest extends AbstractSecurityTest {

    private User testUser;
    private UserManager umMock;
    private PrincipalProviderImpl principalProvider;

    @Before
    public void before() throws Exception {
        super.before();

        testUser = getTestUser();

        Answer throwingAnswer = (Answer<Object>) invocation -> {
            throw new RepositoryException();
        };
        umMock = mock(UserManager.class, throwingAnswer);
        principalProvider = createPrincipalProvider(umMock);
    }

    private PrincipalProviderImpl createPrincipalProvider(@NotNull UserManager um) {
        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(any(Root.class), any(NamePathMapper.class))).thenReturn(um).getMock();
        return new PrincipalProviderImpl(root, uc, DEFAULT);
    }

    @Test
    public void testGetPrincipal() throws Exception {
        String principalName = testUser.getPrincipal().getName();
        assertNull(principalProvider.getPrincipal(getTestUser().getPrincipal().getName()));

        verify(umMock, times(1)).getAuthorizable(new PrincipalImpl(principalName));
    }

    @Test
    public void testGetPrincipalFailsOnUser() throws Exception {
        String principalName = testUser.getPrincipal().getName();
        Principal p = new PrincipalImpl(principalName);

        User userMock = when(mock(User.class).getPrincipal()).thenThrow(new RepositoryException()).getMock();
        UserManager um = when(mock(UserManager.class).getAuthorizable(p)).thenReturn(userMock).getMock();

        assertNull(createPrincipalProvider(um).getPrincipal(getTestUser().getPrincipal().getName()));
        verify(um, times(1)).getAuthorizable(p);
        verify(userMock, times(1)).getPrincipal();
    }

    @Test
    public void testGetEveryone() throws Exception {
        assertSame(EveryonePrincipal.getInstance(), principalProvider.getPrincipal(EveryonePrincipal.NAME));

        verify(umMock, times(1)).getAuthorizable(new PrincipalImpl(EveryonePrincipal.NAME));
    }

    @Test
    public void testGetItemBasedPrincipal() throws Exception {
        String jcrPath = getTestUser().getPath();
        String oakPath = getNamePathMapper().getOakPath(jcrPath);
        assertNotNull(oakPath);
        assertNull(principalProvider.getItemBasedPrincipal(oakPath));

        verify(umMock, times(1)).getAuthorizableByPath(jcrPath);
    }

    @Test
    public void testGetMembershipPrincipals() throws Exception {
        assertTrue(principalProvider.getMembershipPrincipals(testUser.getPrincipal()).isEmpty());

        verify(umMock, times(1)).getAuthorizable(testUser.getPrincipal());
    }

    @Test
    public void testGetMembershipPrincipalsFailsOnUser() throws Exception {
        Principal p = testUser.getPrincipal();

        User userMock = when(mock(User.class).memberOf()).thenThrow(new RepositoryException()).getMock();
        UserManager um = when(mock(UserManager.class).getAuthorizable(p)).thenReturn(userMock).getMock();

        assertEquals(Sets.newHashSet(EveryonePrincipal.getInstance()), createPrincipalProvider(um).getMembershipPrincipals(p));

        verify(um, times(1)).getAuthorizable(p);
        verify(userMock, times(1)).memberOf();
    }

    @Test
    public void testGetPrincipals() throws Exception {
        assertTrue(principalProvider.getPrincipals(testUser.getID()).isEmpty());

        verify(umMock, times(1)).getAuthorizable(testUser.getID());
    }

    @Test
    public void testFindByTypeUser() throws Exception {
        assertFalse(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP).hasNext());

        verify(umMock, times(1)).findAuthorizables(any(Query.class));
    }

    @Test
    public void testFindByTypeGroup() throws Exception {
        assertFalse(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_GROUP).hasNext());

        verify(umMock, times(1)).findAuthorizables(any(Query.class));
    }

    @Test
    public void testFind() throws Exception {
        assertFalse(principalProvider.findPrincipals(testUser.getPrincipal().getName(), false, PrincipalManager.SEARCH_TYPE_NOT_GROUP, 0, Long.MAX_VALUE).hasNext());

        verify(umMock, times(1)).findAuthorizables(any(Query.class));
    }

    @Test
    public void testFindFailsOnUser() throws Exception {
        Principal p = testUser.getPrincipal();

        User userMock = when(mock(User.class).getPrincipal()).thenThrow(new RepositoryException()).getMock();
        UserManager um = when(mock(UserManager.class).findAuthorizables(any(Query.class))).thenReturn(Iterators.singletonIterator(userMock)).getMock();

        Iterator it = createPrincipalProvider(um).findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        assertFalse(it.hasNext());

        verify(userMock, times(1)).getPrincipal();
    }
}
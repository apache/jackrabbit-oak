/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;
import javax.jcr.RepositoryException;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.junit.Before;
import org.junit.Test;

public class CachedGroupPrincipalTest {

    private UserManager userManager;

    @Before
    public void before() throws Exception {
        userManager = mock(UserManager.class);
    }

    @Test
    public void testIsMemberWithAuthorizableNotFound() throws RepositoryException {
        CachedGroupPrincipal cachedGroupPrincipal = new CachedGroupPrincipal("ignored", userManager);
        PrincipalImpl principal = new PrincipalImpl("test");
        when(userManager.getAuthorizable(principal)).thenReturn(null);
        assertFalse(cachedGroupPrincipal.isMember(principal));
    }

    @Test
    public void testIsMemberWithRepositoryException() throws RepositoryException {
        CachedGroupPrincipal cachedGroupPrincipal = new CachedGroupPrincipal("ignored", userManager);
        PrincipalImpl principal = new PrincipalImpl("test");
        when(userManager.getAuthorizable(principal)).thenThrow(new RepositoryException("Mocked exception"));
        assertFalse(cachedGroupPrincipal.isMember(principal));
    }

    @Test
    public void testIsMemberWithCachedPrincipalIsNotAUser() throws RepositoryException {
        CachedGroupPrincipal cachedGroupPrincipal = new CachedGroupPrincipal("testUser", userManager);
        PrincipalImpl principal = new PrincipalImpl("test");
        when(userManager.getAuthorizable(principal)).thenReturn(mock(Authorizable.class));
        Group mockedGroup = mock(Group.class);
        when(userManager.getAuthorizable(new PrincipalImpl("testUser"))).thenReturn(mockedGroup);

        when(mockedGroup.isGroup()).thenReturn(true);
        when(mockedGroup.isMember(any())).thenReturn(false);
        assertFalse(cachedGroupPrincipal.isMember(principal));
    }

    @Test
    public void testIsMemberWithCachedPrincipalIsAUser() throws RepositoryException {
        CachedGroupPrincipal cachedGroupPrincipal = new CachedGroupPrincipal("testUser", userManager);
        PrincipalImpl principal = new PrincipalImpl("test");
        when(userManager.getAuthorizable(principal)).thenReturn(mock(Authorizable.class));
        Authorizable mockedTestUserAuth = mock(Authorizable.class);
        when(userManager.getAuthorizable(new PrincipalImpl("testUser"))).thenReturn(mockedTestUserAuth);

        when(mockedTestUserAuth.isGroup()).thenReturn(false);
        assertFalse(cachedGroupPrincipal.isMember(principal));
    }

    @Test
    public void testIsMemberWithCachedPrincipalNotFound() throws RepositoryException {
        CachedGroupPrincipal cachedGroupPrincipal = new CachedGroupPrincipal("testUser", userManager);
        PrincipalImpl principal = new PrincipalImpl("test");
        when(userManager.getAuthorizable(principal)).thenReturn(mock(Authorizable.class));
        when(userManager.getAuthorizable(new PrincipalImpl("testUser"))).thenReturn(null);
        assertFalse(cachedGroupPrincipal.isMember(principal));
    }

    @Test
    public void testIsMemberWhenPrincipalBelongsToCachedGroup() throws RepositoryException {
        CachedGroupPrincipal cachedGroupPrincipal = new CachedGroupPrincipal("testGroup", userManager);
        PrincipalImpl principal = new PrincipalImpl("test");
        when(userManager.getAuthorizable(principal)).thenReturn(mock(Authorizable.class));
        Group mockedTestUserAuth = mock(Group.class);
        when(userManager.getAuthorizable(new PrincipalImpl("testGroup"))).thenReturn(mockedTestUserAuth);

        when(mockedTestUserAuth.isGroup()).thenReturn(true);
        when(mockedTestUserAuth.isMember(any())).thenReturn(true);
        assertTrue(cachedGroupPrincipal.isMember(principal));
    }

    @Test
    public void testMembersCornerCases() throws RepositoryException {
        CachedGroupPrincipal cachedGroupPrincipal = new CachedGroupPrincipal("test", userManager);
        assertNotNull(cachedGroupPrincipal.members());

        when(userManager.getAuthorizable(new PrincipalImpl("test"))).thenThrow(new RepositoryException("Mocked exception"));
        assertThrows(IllegalStateException.class, cachedGroupPrincipal::members);

        // Test when group members contains null values
        CachedGroupPrincipal anotherGroupPrincipal = new CachedGroupPrincipal("testNullAuth", userManager);
        Group mockedTestUserAuth = mock(Group.class);
        when(userManager.getAuthorizable(new PrincipalImpl("testNullAuth"))).thenReturn(mockedTestUserAuth);
        when(mockedTestUserAuth.isGroup()).thenReturn(true);
        when(mockedTestUserAuth.getMembers()).thenReturn(Collections.singletonList((Authorizable) null).iterator());
        assertFalse(anotherGroupPrincipal.members().hasMoreElements());
        assertFalse(anotherGroupPrincipal.members().hasMoreElements()); //Testing group internal var is not modified

        // Test when group members authorizables cannnot be transformed to principals
        CachedGroupPrincipal failedGroupPrincipal = new CachedGroupPrincipal("testNotPrincipal", userManager);
        Group mockedWrongPrincipal = mock(Group.class);
        when(userManager.getAuthorizable(new PrincipalImpl("testNotPrincipal"))).thenReturn(mockedWrongPrincipal);
        when(mockedWrongPrincipal.isGroup()).thenReturn(true);
        Authorizable mockedAuthorizable = mock(Authorizable.class);
        when(mockedAuthorizable.getPrincipal()).thenThrow(new RepositoryException("Mocked exception"));
        when(mockedWrongPrincipal.getMembers()).thenReturn(Collections.singletonList(mockedAuthorizable).iterator());
        assertThrows(IllegalStateException.class, () -> failedGroupPrincipal.members().hasMoreElements());

    }

    @Test
    public void testMembersWithEmptyMembers() throws RepositoryException {
        CachedGroupPrincipal cachedGroupPrincipal = new CachedGroupPrincipal("testGroup", userManager);
        PrincipalImpl principal = new PrincipalImpl("test");
        when(userManager.getAuthorizable(principal)).thenReturn(mock(Authorizable.class));
        Group mockedTestUserAuth = mock(Group.class);
        when(userManager.getAuthorizable(new PrincipalImpl("testGroup"))).thenReturn(mockedTestUserAuth);

        when(mockedTestUserAuth.isGroup()).thenReturn(true);
        when(mockedTestUserAuth.getMembers()).thenReturn(Collections.emptyIterator());
        assertFalse(cachedGroupPrincipal.members().hasMoreElements());
    }

    @Test
    public void testMembers() throws RepositoryException {
        CachedGroupPrincipal cachedGroupPrincipal = new CachedGroupPrincipal("testGroup", userManager);
        PrincipalImpl principal = new PrincipalImpl("test");
        when(userManager.getAuthorizable(principal)).thenReturn(mock(Authorizable.class));
        Group mockedTestUserAuth = mock(Group.class);
        when(userManager.getAuthorizable(new PrincipalImpl("testGroup"))).thenReturn(mockedTestUserAuth);

        when(mockedTestUserAuth.isGroup()).thenReturn(true);

        Set<Authorizable> authorizables = Set.of(mockAuthorizables("test1"),mockAuthorizables("test2"));
        when(mockedTestUserAuth.getMembers()).thenReturn(authorizables.iterator());
        assertTrue(cachedGroupPrincipal.members().hasMoreElements());
    }

    @Test
    public void testGetPath() throws RepositoryException {
        CachedGroupPrincipal cachedGroupPrincipal = new CachedGroupPrincipal("testGroup", userManager);
        PrincipalImpl principal = new PrincipalImpl("test");
        when(userManager.getAuthorizable(principal)).thenReturn(mock(Authorizable.class));
        Group mockedTestUserAuth = mock(Group.class);
        when(userManager.getAuthorizable(new PrincipalImpl("testGroup"))).thenReturn(mockedTestUserAuth);
        when(mockedTestUserAuth.isGroup()).thenReturn(true);
        when(mockedTestUserAuth.getPath()).thenReturn("/path/to/group");

        assertEquals("/path/to/group", cachedGroupPrincipal.getPath());

    }

    private Authorizable mockAuthorizables(String principalName) throws RepositoryException {
        Authorizable authorizable = mock(Authorizable.class);
        PrincipalImpl principal = new PrincipalImpl(principalName);
        when(authorizable.getPrincipal()).thenReturn(principal);
        return authorizable;
    }
}
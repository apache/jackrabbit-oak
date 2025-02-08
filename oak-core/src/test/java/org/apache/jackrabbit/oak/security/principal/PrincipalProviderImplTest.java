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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.jackrabbit.oak.namepath.NamePathMapper.DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrincipalProviderImplTest extends AbstractPrincipalProviderTest {

    @NotNull
    @Override
    protected PrincipalProvider createPrincipalProvider() {
        return new PrincipalProviderImpl(root, getUserConfiguration(), namePathMapper);
    }

    private PrincipalProviderImpl createPrincipalProvider(@NotNull UserManager um) {
        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(any(Root.class), any(NamePathMapper.class))).thenReturn(um).getMock();
        return new PrincipalProviderImpl(root, uc, DEFAULT);
    }

    @Test
    public void testEveryoneMembers() throws Exception {
        Principal everyone = principalProvider.getPrincipal(EveryonePrincipal.NAME);
        assertTrue(everyone instanceof EveryonePrincipal);

        Group everyoneGroup = null;
        try {
            UserManager userMgr = getUserManager(root);
            everyoneGroup = userMgr.createGroup(EveryonePrincipal.NAME);
            root.commit();

            Principal ep = principalProvider.getPrincipal(EveryonePrincipal.NAME);
            Set<? extends Principal> everyoneMembers = Set.copyOf(Collections.list(((GroupPrincipal) ep).members()));

            Iterator<? extends Principal> all = principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_ALL);
            while (all.hasNext()) {
                Principal p = all.next();
                if (everyone.equals(p)) {
                    assertFalse(everyoneMembers.contains(p));
                } else {
                    assertTrue(everyoneMembers.contains(p));
                }
            }

        } finally {
            if (everyoneGroup != null) {
                everyoneGroup.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testGetGroupMembershipNonGroupPrincipal() throws Exception {
        // Group.getPrincipal doesn't return a GroupPrincipal
        Group gr = when(mock(Group.class).getPrincipal()).thenReturn(new PrincipalImpl("group")).getMock();
        Authorizable mockAuthorizable = when(mock(User.class).memberOf()).thenReturn(Iterators.singletonIterator(gr)).getMock();
        UserManager umMock = when(mock(UserManager.class).getAuthorizable(any(Principal.class))).thenReturn(mockAuthorizable).getMock();

        Set<Principal> membership = createPrincipalProvider(umMock).getMembershipPrincipals(new PrincipalImpl("userPrincipal"));
        assertEquals(Set.of(EveryonePrincipal.getInstance()), membership);
    }

    @Test
    public void testGetItemBasedPrincipalNotItemBased() throws Exception {
        Authorizable mockUser = when(mock(User.class).getPrincipal()).thenReturn(new PrincipalImpl("noPath")).getMock();
        UserManager umMock = mock(UserManager.class);
        when(umMock.getAuthorizableByPath(anyString())).thenReturn(mockUser);

        createPrincipalProvider(umMock).getItemBasedPrincipal("/path/to/authorizable");
        verify(umMock, times(1)).getAuthorizableByPath("/path/to/authorizable");
    }

    @Test
    public void testFindWithUnexpectedNullAuthorizable() throws Exception {
        List<Authorizable> l = new ArrayList<>();
        l.add(null);
        UserManager umMock = mock(UserManager.class);
        when(umMock.findAuthorizables(any(Query.class))).thenReturn(l.iterator());

        Iterator<? extends Principal> result = createPrincipalProvider(umMock).findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        assertFalse(result.hasNext());

        result = createPrincipalProvider(umMock).findPrincipals(PrincipalManager.SEARCH_TYPE_GROUP);
        assertTrue(Iterators.elementsEqual(Iterators.singletonIterator(EveryonePrincipal.getInstance()), result));
    }

    @Test
    public void testFindWithUnexpectedNullPrincipal() throws Exception {
        Authorizable userMock = when(mock(Authorizable.class).getPrincipal()).thenReturn(null).getMock();
        UserManager umMock = mock(UserManager.class);
        when(umMock.findAuthorizables(any(Query.class))).thenReturn(Iterators.singletonIterator(userMock));

        Iterator<? extends Principal> result = createPrincipalProvider(umMock).findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        assertFalse(result.hasNext());

        result = createPrincipalProvider(umMock).findPrincipals(PrincipalManager.SEARCH_TYPE_GROUP);
        assertTrue(Iterators.elementsEqual(Iterators.singletonIterator(EveryonePrincipal.getInstance()), result));
    }
}
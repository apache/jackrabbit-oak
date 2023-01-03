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
package org.apache.jackrabbit.oak.jcr.delegate;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.session.operation.UserManagerOperation;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.security.Principal;
import java.util.Collections;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class UserManagerDelegatorTest extends AbstractDelegatorTest {

    private final SessionDelegate sessionDelegate = mockSessionDelegate();
    private final UserManager um = mock(UserManager.class);
    private final Authorizable user = when(mock(Authorizable.class, withSettings().extraInterfaces(User.class)).isGroup()).thenReturn(false).getMock();
    private final Authorizable group = when(mock(Authorizable.class, withSettings().extraInterfaces(Group.class)).isGroup()).thenReturn(true).getMock();

    private final UserManagerDelegator delegator = new UserManagerDelegator(sessionDelegate, um);
    
    private static void verifySessionDelegatePerform(@NotNull SessionDelegate sessionDelegate, int times) throws Exception {
        verify(sessionDelegate, times(times)).checkAlive();
        verify(sessionDelegate, times(times)).isAlive();
        verify(sessionDelegate, times(times)).perform(any(UserManagerOperation.class));
    }

    private static void verifySessionDelegatePerformNullable(@NotNull SessionDelegate sessionDelegate,  int times) throws Exception {
        verify(sessionDelegate, times(times)).checkAlive();
        verify(sessionDelegate, times(times)).isAlive();
        verify(sessionDelegate, times(times)).performNullable(any(UserManagerOperation.class));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateFromDelegator() {
        new UserManagerDelegator(sessionDelegate, delegator);
    }
    
    @Test
    public void testGetAuthorizableById() throws Exception {
        assertNull(delegator.getAuthorizable("id"));
        
        doReturn(user).when(um).getAuthorizable("id");
        Authorizable a = delegator.getAuthorizable("id");
        assertTrue(a instanceof UserDelegator);
        
        verify(um, times(2)).getAuthorizable("id");
        verify(user).isGroup();
        verifySessionDelegatePerformNullable(sessionDelegate, 2);
        verifyNoMoreInteractions(um, user, sessionDelegate);
        verifyNoMoreInteractions(group);
    }

    @Test
    public void testGetAuthorizableByPrincipal() throws Exception {
        Principal principal = mock(Principal.class);
        assertNull(delegator.getAuthorizable(principal));

        doReturn(group).when(um).getAuthorizable(principal);
        Authorizable a = delegator.getAuthorizable(principal);
        assertTrue(a instanceof GroupDelegator);

        verify(um, times(2)).getAuthorizable(principal);
        verify(group).isGroup();
        verifySessionDelegatePerformNullable(sessionDelegate, 2);
        verifyNoMoreInteractions(um, group, sessionDelegate);
        verifyNoMoreInteractions(user);
    }

    @Test
    public void testGetAuthorizableByPath() throws Exception {
        String path = "/path";
        assertNull(delegator.getAuthorizableByPath(path));

        doReturn(user).when(um).getAuthorizableByPath(path);
        Authorizable a = delegator.getAuthorizableByPath(path);
        assertTrue(a instanceof UserDelegator);

        verify(um, times(2)).getAuthorizableByPath(path);
        verify(user).isGroup();
        verifySessionDelegatePerformNullable(sessionDelegate, 2);
        verifyNoMoreInteractions(um, user, sessionDelegate);
        verifyNoMoreInteractions(group);
    }
    
    @Test
    public void testFindAuthorizables() throws Exception {
        Query query = mock(Query.class);
        doReturn(Collections.emptyIterator()).when(um).findAuthorizables(query);
        doReturn(Collections.emptyIterator()).when(um).findAuthorizables(anyString(), anyString());
        doReturn(Collections.emptyIterator()).when(um).findAuthorizables(anyString(), anyString(), anyInt());
        
        delegator.findAuthorizables(query);
        delegator.findAuthorizables("rel/path", "value");
        delegator.findAuthorizables("rel/path", "value", UserManager.SEARCH_TYPE_AUTHORIZABLE);
        
        verify(um).findAuthorizables(query);
        verify(um).findAuthorizables("rel/path", "value");
        verify(um).findAuthorizables("rel/path", "value", UserManager.SEARCH_TYPE_AUTHORIZABLE);
        
        verifySessionDelegatePerform(sessionDelegate, 3);
        verifyNoMoreInteractions(um, sessionDelegate);
    }
    
    @Test
    public void testCreateUser() throws Exception {
        doReturn(user).when(um).createUser(anyString(), anyString());
        doReturn(user).when(um).createUser(anyString(), anyString(), any(Principal.class), anyString());
        
        Principal p = mock(Principal.class);
        User u1 = delegator.createUser("uid", "pw");
        User u2 = delegator.createUser("uid", "pw", p, "rel/path");
        assertTrue(u1 instanceof UserDelegator);
        assertTrue(u2 instanceof UserDelegator);
        
        verify(um).createUser("uid", "pw");
        verify(um).createUser("uid", "pw", p, "rel/path");

        verifySessionDelegatePerform(sessionDelegate, 2);
        verifyNoMoreInteractions(um, sessionDelegate);
        verifyNoInteractions(user);
    }

    @Test
    public void testCreateSystemUser() throws Exception {
        doReturn(user).when(um).createSystemUser(anyString(), anyString());

        User u = delegator.createSystemUser("sys-uid", "rel/path");
        assertTrue(u instanceof UserDelegator);

        verify(um).createSystemUser("sys-uid", "rel/path");
        verifySessionDelegatePerform(sessionDelegate, 1);
        verifyNoMoreInteractions(um, sessionDelegate);
        verifyNoInteractions(user);
    }
    
    @Test
    public void testCreateGroup() throws Exception {
        doReturn(group).when(um).createGroup(anyString());
        doReturn(group).when(um).createGroup(anyString(), any(Principal.class), anyString());
        doReturn(group).when(um).createGroup(any(Principal.class));
        doReturn(group).when(um).createGroup(any(Principal.class), anyString());

        Principal p = mock(Principal.class);
        assertTrue(delegator.createGroup("groupId") instanceof GroupDelegator);
        assertTrue(delegator.createGroup("groupId", p, "rel/path") instanceof GroupDelegator);
        assertTrue(delegator.createGroup(p) instanceof GroupDelegator);
        assertTrue(delegator.createGroup(p, "rel/path") instanceof GroupDelegator);
        
        verify(um).createGroup("groupId");
        verify(um).createGroup("groupId", p, "rel/path");
        verify(um).createGroup(p);
        verify(um).createGroup(p, "rel/path");
        
        verifySessionDelegatePerform(sessionDelegate, 4);
        verifyNoMoreInteractions(um, sessionDelegate);
        verifyNoInteractions(group);
    }
    
    
    @Test
    public void testIsAutoSave() throws Exception {
        delegator.isAutoSave();

        verify(um).isAutoSave();
        verify(sessionDelegate).checkAlive();
        verify(sessionDelegate).isAlive();
        verify(sessionDelegate).safePerform(any(UserManagerOperation.class));
        verify(sessionDelegate).perform(any(UserManagerOperation.class));
        verifyNoMoreInteractions(um, sessionDelegate);
    }
    
    @Test
    public void testAutoSave() throws Exception {
        delegator.autoSave(true);
        delegator.autoSave(false);
        
        verify(um, times(2)).autoSave(anyBoolean());
        verify(sessionDelegate, times(2)).checkAlive();
        verify(sessionDelegate, times(2)).isAlive();
        verify(sessionDelegate, times(2)).performVoid(any(UserManagerOperation.class));
        verifyNoMoreInteractions(um, sessionDelegate);
    }
}
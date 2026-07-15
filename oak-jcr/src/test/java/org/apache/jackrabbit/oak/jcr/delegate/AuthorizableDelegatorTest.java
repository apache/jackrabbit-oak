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
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.Value;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class AuthorizableDelegatorTest extends AbstractDelegatorTest {
    
    private final SessionDelegate sessionDelegate = mockSessionDelegate();
    private final Authorizable authorizable = mock(Authorizable.class);
    private final Authorizable user = when(mock(Authorizable.class, withSettings().extraInterfaces(User.class)).isGroup()).thenReturn(false).getMock();
    private final Authorizable group = when(mock(Authorizable.class, withSettings().extraInterfaces(Group.class)).isGroup()).thenReturn(true).getMock();
    private final AuthorizableDelegator ad = mockDelegator(sessionDelegate, authorizable);

    private static AuthorizableDelegator mockDelegator(@NotNull SessionDelegate sd, @NotNull Authorizable a) {
        return mock(AuthorizableDelegator.class, withSettings().useConstructor(sd, a).defaultAnswer(CALLS_REAL_METHODS));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testCreateFromDelegator() {
        new AuthorizableDelegator(sessionDelegate, ad) {};
    }
    
    @Test
    public void testWrap() {
        assertNull(AuthorizableDelegator.wrap(sessionDelegate, null));
        
        Authorizable a = AuthorizableDelegator.wrap(sessionDelegate, group);
        assertTrue(a instanceof GroupDelegator);

        a = AuthorizableDelegator.wrap(sessionDelegate, user);
        assertTrue(a instanceof UserDelegator);
        
        verify(user).isGroup();
        verify(group).isGroup();
        verifyNoMoreInteractions(user, group);
        verifyNoInteractions(sessionDelegate);
    }

    @Test
    public void testUnwrap() {
        Authorizable a = AuthorizableDelegator.unwrap(group);
        assertSame(group, a);

        a = AuthorizableDelegator.unwrap(user);
        assertSame(user, a);

        verify(user).isGroup();
        verify(group).isGroup();
        verifyNoMoreInteractions(user, group, sessionDelegate);
    }

    @Test
    public void testUnwrapDelegator() throws Exception {
        Authorizable gDelegator = AuthorizableDelegator.wrap(sessionDelegate, group);
        Authorizable a = AuthorizableDelegator.unwrap(gDelegator);
        assertSame(group, a);

        Authorizable uDelegator = AuthorizableDelegator.wrap(sessionDelegate, user);
        a = AuthorizableDelegator.unwrap(uDelegator);
        assertSame(user, a);

        verify(user, times(2)).isGroup();
        verify(group, times(2)).isGroup();
        verify(sessionDelegate, times(2)).safePerform(any(SessionOperation.class));
        verify(sessionDelegate, times(2)).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(user, group, sessionDelegate);
    }
    
    @Test
    public void testIsGroup() throws Exception {
        ad.isGroup();
        
        verify(authorizable).isGroup();
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verify(sessionDelegate).safePerform(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }

    @Test
    public void testGetId() throws Exception {
        ad.getID();

        verify(authorizable).getID();
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }

    @Test
    public void testGetPrincipal() throws Exception {
        ad.getPrincipal();

        verify(authorizable).getPrincipal();
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }
    
    @Test
    public void testMemberOf() throws Exception {
        doReturn(Collections.emptyIterator()).when(authorizable).memberOf();
        ad.memberOf();

        verify(authorizable).memberOf();
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }

    @Test
    public void testDeclaredMemberOf() throws Exception {
        doReturn(Collections.emptyIterator()).when(authorizable).declaredMemberOf();
        ad.declaredMemberOf();

        verify(authorizable).declaredMemberOf();
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }

    @Test
    public void testRemove() throws Exception {
        ad.remove();

        verify(authorizable).remove();
        verify(sessionDelegate).performVoid(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }
    
    @Test
    public void testGetPropertyNames() throws Exception {
        ad.getPropertyNames();
        ad.getPropertyNames("rel/path");
        
        verify(authorizable).getPropertyNames();
        verify(authorizable).getPropertyNames("rel/path");
        verify(sessionDelegate, times(2)).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }
    
    @Test
    public void testHasProperty() throws Exception {
        ad.hasProperty("rel/path");
        verify(authorizable).hasProperty("rel/path");
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }

    @Test
    public void testGetProperty() throws Exception {
        ad.getProperty("rel/path");
        verify(authorizable).getProperty("rel/path");
        verify(sessionDelegate).performNullable(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }

    @Test
    public void testSetProperty() throws Exception {
        Value v = mock(Value.class);
        ad.setProperty("rel/path", v);
        ad.setProperty("rel/path", new Value[] {v});
        verify(authorizable).setProperty("rel/path", v);
        verify(authorizable).setProperty("rel/path", new Value[] {v});
        verify(sessionDelegate, times(2)).performVoid(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }

    @Test
    public void testRemoveProperty() throws Exception {
        ad.removeProperty("rel/path");
        verify(authorizable).removeProperty("rel/path");
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }

    @Test
    public void testGetPath() throws Exception {
        ad.getPath();

        verify(authorizable).getPath();
        verify(sessionDelegate).perform(any(SessionOperation.class));
        verifyNoMoreInteractions(authorizable, sessionDelegate);
    }
    
    @Test
    public void testEquals() {
        AuthorizableDelegator delegator = (AuthorizableDelegator) AuthorizableDelegator.wrap(sessionDelegate, user);
        assertTrue(delegator.equals(delegator));
        assertNotEquals(delegator, user);
        
        AuthorizableDelegator uDelegator = (AuthorizableDelegator) AuthorizableDelegator.wrap(sessionDelegate, user);
        assertEquals(delegator, uDelegator);

        AuthorizableDelegator gDelegator = (AuthorizableDelegator) AuthorizableDelegator.wrap(sessionDelegate, group);
        assertNotEquals(delegator, gDelegator);
    }
}
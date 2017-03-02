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
package org.apache.jackrabbit.oak.spi.security.user;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AuthorizableTypeTest {

    private User user;
    private Group gr;

    @Before
    public void before() {
        user = Mockito.mock(User.class);
        Mockito.when(user.isGroup()).thenReturn(false);

        gr = Mockito.mock(Group.class);
        Mockito.when(gr.isGroup()).thenReturn(true);
    }

    @Test
    public void testGetType() throws Exception {
        assertSame(AuthorizableType.USER, AuthorizableType.getType(UserManager.SEARCH_TYPE_USER));
        assertSame(AuthorizableType.GROUP, AuthorizableType.getType(UserManager.SEARCH_TYPE_GROUP));
        assertSame(AuthorizableType.AUTHORIZABLE, AuthorizableType.getType(UserManager.SEARCH_TYPE_AUTHORIZABLE));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTypeIllegalSearchType() throws Exception {
        AuthorizableType.getType(0);
    }

    @Test
    public void testIsTypeUser() throws Exception {
        assertFalse(AuthorizableType.USER.isType(null));
        assertTrue(AuthorizableType.USER.isType(user));
        assertFalse(AuthorizableType.USER.isType(gr));
    }

    @Test
    public void testIsTypeGroup() throws Exception {
        assertFalse(AuthorizableType.GROUP.isType(null));
        assertFalse(AuthorizableType.GROUP.isType(user));
        assertTrue(AuthorizableType.GROUP.isType(gr));

    }

    @Test
    public void testIsTypeAuthorizable() throws Exception {
        assertFalse(AuthorizableType.AUTHORIZABLE.isType(null));
        assertTrue(AuthorizableType.AUTHORIZABLE.isType(user));
        assertTrue(AuthorizableType.AUTHORIZABLE.isType(gr));
    }

    @Test
    public void testGetAuthorizableClass() {
        assertEquals(User.class, AuthorizableType.USER.getAuthorizableClass());
        assertEquals(Group.class, AuthorizableType.GROUP.getAuthorizableClass());
        assertEquals(Authorizable.class, AuthorizableType.AUTHORIZABLE.getAuthorizableClass());
    }
}
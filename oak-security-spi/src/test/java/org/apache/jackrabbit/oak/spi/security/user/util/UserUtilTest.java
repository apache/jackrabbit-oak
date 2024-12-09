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
package org.apache.jackrabbit.oak.spi.security.user.util;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableTypeException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class UserUtilTest {

    private final User u = mock(User.class);
    private final Group g = mock(Group.class);

    @NotNull
    private static Tree createTree(@Nullable String ntName) {
        return createTree(ntName, null, null);
    }

    @NotNull
    private static Tree createTree(@Nullable String ntName, @Nullable String id) {
        return createTree(ntName, id, null);
    }

    @NotNull
    private static Tree createTree(@Nullable String ntName, @Nullable String id, @Nullable String nodeName) {
        Tree t = mock(Tree.class);
        if (ntName != null) {
            when(t.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, ntName, Type.NAME));
        }
        if (id != null) {
            when(t.getProperty(UserConstants.REP_AUTHORIZABLE_ID)).thenReturn(PropertyStates.createProperty(UserConstants.REP_AUTHORIZABLE_ID, id, Type.STRING));
        }
        if (nodeName != null) {
            when(t.getName()).thenReturn(nodeName);
        }
        return t;
    }

    @Test
    public void testIsAdmin() {
        String altAdminId = "test";
        ConfigurationParameters config = ConfigurationParameters.of(UserConstants.PARAM_ADMIN_ID, altAdminId);

        assertTrue(UserUtil.isAdmin(ConfigurationParameters.EMPTY, UserConstants.DEFAULT_ADMIN_ID));
        assertFalse(UserUtil.isAdmin(ConfigurationParameters.EMPTY, altAdminId));

        assertFalse(UserUtil.isAdmin(config, UserConstants.DEFAULT_ADMIN_ID));
        assertTrue(UserUtil.isAdmin(config, altAdminId));
    }

    @Test
    public void testGetAdminId() {
        String altAdminId = "test";
        ConfigurationParameters config = ConfigurationParameters.of(UserConstants.PARAM_ADMIN_ID, altAdminId);

        assertEquals(UserConstants.DEFAULT_ADMIN_ID, UserUtil.getAdminId(ConfigurationParameters.EMPTY));
        assertEquals(altAdminId, UserUtil.getAdminId(config));
    }

    @Test
    public void testGetAnonymousId() {
        String altAnonymousId = "test";
        ConfigurationParameters config = ConfigurationParameters.of(UserConstants.PARAM_ANONYMOUS_ID, altAnonymousId);

        assertEquals(UserConstants.DEFAULT_ANONYMOUS_ID, UserUtil.getAnonymousId(ConfigurationParameters.EMPTY));
        assertEquals(altAnonymousId, UserUtil.getAnonymousId(config));
    }

    @Test
    public void testIsTypeNullTree() {
        assertFalse(UserUtil.isType(null, AuthorizableType.GROUP));
        assertFalse(UserUtil.isType(null, AuthorizableType.USER));
        assertFalse(UserUtil.isType(null, AuthorizableType.AUTHORIZABLE));
    }

    @Test
    public void testIsTypeGroupFromTree() {
        Map<String, Boolean> test = Map.of(
                UserConstants.NT_REP_GROUP, true,
                UserConstants.NT_REP_USER, false,
                UserConstants.NT_REP_SYSTEM_USER, false,
                UserConstants.NT_REP_AUTHORIZABLE, false,
                JcrConstants.NT_FILE, false
        );

        test.forEach((key, value) -> assertEquals(value, UserUtil.isType(createTree(key), AuthorizableType.GROUP)));
    }

    @Test
    public void testIsTypeUserFromTree() {
        Map<String, Boolean> test = Map.of(
                UserConstants.NT_REP_GROUP, false,
                UserConstants.NT_REP_USER, true,
                UserConstants.NT_REP_SYSTEM_USER, true,
                UserConstants.NT_REP_AUTHORIZABLE, false,
                JcrConstants.NT_FILE, false
        );

        test.forEach((ntName, value) -> {
            boolean expected = value;
            assertEquals(ntName, expected, UserUtil.isType(createTree(ntName), AuthorizableType.USER));
        });
    }

    @Test
    public void testIsTypeAuthorizableFromTree() {
        Map<String, Boolean> test = Map.of(
                UserConstants.NT_REP_GROUP, true,
                UserConstants.NT_REP_USER, true,
                UserConstants.NT_REP_SYSTEM_USER, true,
                UserConstants.NT_REP_AUTHORIZABLE, false,
                JcrConstants.NT_FILE, false
        );

        test.forEach((ntName, value) -> {
            boolean expected = value;
            assertEquals(ntName, expected, UserUtil.isType(createTree(ntName), AuthorizableType.AUTHORIZABLE));
        });
    }

    @Test
    public void testGetTypeFromTree() {
        Map<String, AuthorizableType> test = Map.of(
                UserConstants.NT_REP_GROUP, AuthorizableType.GROUP,
                UserConstants.NT_REP_USER, AuthorizableType.USER,
                UserConstants.NT_REP_SYSTEM_USER, AuthorizableType.USER
        );

        test.forEach((ntName, expected) -> assertEquals(ntName, expected, UserUtil.getType(createTree(ntName))));
    }

    @Test
    public void testGetTypeFromTree2() {
        List<String> test = ImmutableList.of(
                UserConstants.NT_REP_AUTHORIZABLE,
                JcrConstants.NT_FILE
        );

        for (String ntName : test) {
            assertNull(UserUtil.getType(createTree(ntName)));
        }
    }

    @Test
    public void testGetTypeFromNt() {
        assertEquals(AuthorizableType.GROUP, UserUtil.getType(UserConstants.NT_REP_GROUP));
        assertEquals(AuthorizableType.USER, UserUtil.getType(UserConstants.NT_REP_USER));
        assertEquals(AuthorizableType.USER, UserUtil.getType(UserConstants.NT_REP_SYSTEM_USER));
    }

    @Test
    public void testGetTypeFromNtReturnsNull() {
        // abstract primary type 'rep:Authorizable'
        assertNull(UserUtil.getType(UserConstants.NT_REP_AUTHORIZABLE));

        // another node type name
        assertNull(UserUtil.getType(JcrConstants.NT_FILE));

        // null or empty name
        assertNull(UserUtil.getType((String) null));
        assertNull(UserUtil.getType(""));
    }

    @Test
    public void testIsSystemUserNullTree() {
        assertFalse(UserUtil.isSystemUser(null));
    }

    @Test
    public void testIsSystemUser() {
        Map<String, Boolean> test = Map.of(
                UserConstants.NT_REP_GROUP, false,
                UserConstants.NT_REP_USER, false,
                UserConstants.NT_REP_SYSTEM_USER, true,
                UserConstants.NT_REP_AUTHORIZABLE, false,
                JcrConstants.NT_FILE, false
        );

        test.forEach((ntName, value) -> {
            boolean expected = value;
            assertEquals(ntName, expected, UserUtil.isSystemUser(createTree(ntName)));
        });
    }

    @Test
    public void testGetAuthorizableRootPathDefault() {
        assertEquals(UserConstants.DEFAULT_GROUP_PATH, UserUtil.getAuthorizableRootPath(ConfigurationParameters.EMPTY, AuthorizableType.GROUP));
        assertEquals(UserConstants.DEFAULT_USER_PATH, UserUtil.getAuthorizableRootPath(ConfigurationParameters.EMPTY, AuthorizableType.USER));
        assertEquals("/rep:security/rep:authorizables", UserUtil.getAuthorizableRootPath(ConfigurationParameters.EMPTY, AuthorizableType.AUTHORIZABLE));
    }

    @Test
    public void testGetAuthorizableRootPath() {
        ConfigurationParameters config = ConfigurationParameters.of(
                UserConstants.PARAM_GROUP_PATH, "/groups",
                UserConstants.PARAM_USER_PATH, "/users");

        assertEquals("/groups", UserUtil.getAuthorizableRootPath(config, AuthorizableType.GROUP));
        assertEquals("/users", UserUtil.getAuthorizableRootPath(config, AuthorizableType.USER));
        assertEquals("/", UserUtil.getAuthorizableRootPath(config, AuthorizableType.AUTHORIZABLE));
    }

    @Test
    public void testGetAuthorizableRootPathNullType() {
        assertNull(UserUtil.getAuthorizableRootPath(ConfigurationParameters.EMPTY, null));
    }


    @Test(expected = NullPointerException.class)
    public void testGetAuthorizableIdNullTree() {
        UserUtil.getAuthorizableId(null);
    }

    @Test
    public void testGetAuthorizableId() {
        List<String> test = ImmutableList.of(UserConstants.NT_REP_GROUP, UserConstants.NT_REP_SYSTEM_USER, UserConstants.NT_REP_USER);
        for (String ntName : test) {
            assertEquals("id", UserUtil.getAuthorizableId(createTree(ntName, "id")));
        }
    }

    @Test
    public void testGetAuthorizableIdFallback() {
        List<String> test = ImmutableList.of(UserConstants.NT_REP_GROUP, UserConstants.NT_REP_SYSTEM_USER, UserConstants.NT_REP_USER);
        for (String ntName : test) {
            assertEquals("nName", UserUtil.getAuthorizableId(createTree(ntName, null, "nName")));
        }
    }

    @Test
    public void testGetAuthorizableIdNoAuthorizableType() {
        List<String> test = ImmutableList.of(UserConstants.NT_REP_AUTHORIZABLE, JcrConstants.NT_UNSTRUCTURED);
        for (String ntName : test) {
            assertNull(UserUtil.getAuthorizableId(createTree(ntName, "id")));
        }
    }

    @Test
    public void testGetAuthorizableIdWithType() {
        Map<AuthorizableType, String[]> test = Map.of(
                AuthorizableType.USER, new String[] {UserConstants.NT_REP_USER, UserConstants.NT_REP_SYSTEM_USER},
                AuthorizableType.AUTHORIZABLE, new String[] {UserConstants.NT_REP_USER, UserConstants.NT_REP_SYSTEM_USER, UserConstants.NT_REP_GROUP},
                AuthorizableType.GROUP, new String[] {UserConstants.NT_REP_GROUP});

        test.forEach((key, value) -> {
            for (String ntName : value) {
                assertEquals("id", UserUtil.getAuthorizableId(createTree(ntName, "id"), key));
            }
        });
    }

    @Test
    public void testGetAuthorizableIdWithTypeFallback() {
        Map<AuthorizableType, String[]> test = Map.of(
                AuthorizableType.USER, new String[]{UserConstants.NT_REP_USER, UserConstants.NT_REP_SYSTEM_USER},
                AuthorizableType.AUTHORIZABLE, new String[]{UserConstants.NT_REP_USER, UserConstants.NT_REP_SYSTEM_USER, UserConstants.NT_REP_GROUP},
                AuthorizableType.GROUP, new String[]{UserConstants.NT_REP_GROUP});

        test.forEach((key, value) -> {
            for (String ntName : value) {
                assertEquals("nodeName", UserUtil.getAuthorizableId(createTree(ntName, null, "nodeName"), key));
            }
        });
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetAuthorizableIdTypeNotGroup() {
        UserUtil.getAuthorizableId(createTree(UserConstants.NT_REP_USER, "id"), AuthorizableType.GROUP);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetAuthorizableIdWithTypeNotGrou() {
        UserUtil.getAuthorizableId(createTree(UserConstants.NT_REP_SYSTEM_USER, "id"), AuthorizableType.GROUP);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetAuthorizableIdWithTypeNotUser() {
        UserUtil.getAuthorizableId(createTree(UserConstants.NT_REP_GROUP, "id"), AuthorizableType.USER);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testGetAuthorizableIdTypeNotUser() {
        UserUtil.getAuthorizableId(createTree(JcrConstants.NT_UNSTRUCTURED, "id"), AuthorizableType.USER);
    }

    @Test
    public void testCastNullAuthorizable() throws Exception {
        assertNull(UserUtil.castAuthorizable(null, null));
        assertNull(UserUtil.castAuthorizable(null, User.class));
        assertNull(UserUtil.castAuthorizable(null, Group.class));
    }

    @Test(expected = AuthorizableTypeException.class)
    public void testCastNullClass() throws Exception {
        UserUtil.castAuthorizable(u, null);
    }

    @Test(expected = AuthorizableTypeException.class)
    public void testCastUserToGroup() throws Exception {
        UserUtil.castAuthorizable(u, Group.class);
    }

    @Test(expected = AuthorizableTypeException.class)
    public void testCastGroupToUser() throws Exception {
        UserUtil.castAuthorizable(g, User.class);
    }

    @Test(expected = AuthorizableTypeException.class)
    public void testCastAuthorizableToUser() throws Exception {
        UserUtil.castAuthorizable(mock(Authorizable.class), User.class);
    }

    @Test(expected = AuthorizableTypeException.class)
    public void testCastAuthorizableToGroup() throws Exception {
        UserUtil.castAuthorizable(mock(Authorizable.class), Group.class);
    }

    @Test
    public void testCastUserToUser() throws Exception {
        UserUtil.castAuthorizable(u, User.class);
        verifyNoInteractions(u);
    }

    @Test
    public void testCastUserToAuthorizable() throws Exception {
        UserUtil.castAuthorizable(u, Authorizable.class);
        verifyNoInteractions(u);
    }

    @Test
    public void testCastGroupToGroup() throws Exception {
        UserUtil.castAuthorizable(g, Group.class);
        verifyNoInteractions(g);
    }

    @Test
    public void testCastGroupToAuthorizable() throws Exception {
        UserUtil.castAuthorizable(g, Authorizable.class);
        verifyNoInteractions(g);
    }

    @Test
    public void testGetImportBehavior() {
        Map<ConfigurationParameters, Integer> testMap = Map.of(
                ConfigurationParameters.EMPTY, ImportBehavior.IGNORE,
                ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, "anyString"), ImportBehavior.ABORT,
                ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.BESTEFFORT), ImportBehavior.ABORT,
                ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT), ImportBehavior.BESTEFFORT
        );

        testMap.forEach((key, value) -> assertEquals(value.intValue(), UserUtil.getImportBehavior(key)));
    }
}

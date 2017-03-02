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

import java.util.List;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class UserUtilTest {

    @Nonnull
    private static Tree createTree(@CheckForNull String ntName) {
        return createTree(ntName, null, null);
    }

    @Nonnull
    private static Tree createTree(@CheckForNull String ntName, @CheckForNull String id) {
        return createTree(ntName, id, null);
    }

    @Nonnull
    private static Tree createTree(@CheckForNull String ntName, @CheckForNull String id, @CheckForNull String nodeName) {
        Tree t = Mockito.mock(Tree.class);
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
        Map<String, Boolean> test = ImmutableMap.of(
                UserConstants.NT_REP_GROUP, true,
                UserConstants.NT_REP_USER, false,
                UserConstants.NT_REP_SYSTEM_USER, false,
                UserConstants.NT_REP_AUTHORIZABLE, false,
                JcrConstants.NT_FILE, false
        );

        for (String key : test.keySet()) {
            assertEquals(test.get(key), UserUtil.isType(createTree(key), AuthorizableType.GROUP));
        }
    }

    @Test
    public void testIsTypeUserFromTree() {
        Map<String, Boolean> test = ImmutableMap.of(
                UserConstants.NT_REP_GROUP, false,
                UserConstants.NT_REP_USER, true,
                UserConstants.NT_REP_SYSTEM_USER, true,
                UserConstants.NT_REP_AUTHORIZABLE, false,
                JcrConstants.NT_FILE, false
        );

        for (String ntName : test.keySet()) {
            boolean expected = test.get(ntName);
            assertEquals(ntName, expected, UserUtil.isType(createTree(ntName), AuthorizableType.USER));
        }
    }

    @Test
    public void testIsTypeAuthorizableFromTree() {
        Map<String, Boolean> test = ImmutableMap.of(
                UserConstants.NT_REP_GROUP, true,
                UserConstants.NT_REP_USER, true,
                UserConstants.NT_REP_SYSTEM_USER, true,
                UserConstants.NT_REP_AUTHORIZABLE, false,
                JcrConstants.NT_FILE, false
        );

        for (String ntName : test.keySet()) {
            boolean expected = test.get(ntName);
            assertEquals(ntName, expected, UserUtil.isType(createTree(ntName), AuthorizableType.AUTHORIZABLE));
        }
    }

    @Test
    public void testGetTypeFromTree() {
        Map<String, AuthorizableType> test = ImmutableMap.of(
                UserConstants.NT_REP_GROUP, AuthorizableType.GROUP,
                UserConstants.NT_REP_USER, AuthorizableType.USER,
                UserConstants.NT_REP_SYSTEM_USER, AuthorizableType.USER
        );

        for (String ntName : test.keySet()) {
            AuthorizableType expected = test.get(ntName);
            assertEquals(ntName, expected, UserUtil.getType(createTree(ntName)));
        }
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
        Map<String, Boolean> test = ImmutableMap.of(
                UserConstants.NT_REP_GROUP, false,
                UserConstants.NT_REP_USER, false,
                UserConstants.NT_REP_SYSTEM_USER, true,
                UserConstants.NT_REP_AUTHORIZABLE, false,
                JcrConstants.NT_FILE, false
        );

        for (String ntName : test.keySet()) {
            boolean expected = test.get(ntName);
            assertEquals(ntName, expected, UserUtil.isSystemUser(createTree(ntName)));
        }
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
        Map<AuthorizableType, String[]> test = ImmutableMap.<AuthorizableType,String[]>builder().
                put(AuthorizableType.USER, new String[] {UserConstants.NT_REP_USER, UserConstants.NT_REP_SYSTEM_USER}).
                put(AuthorizableType.AUTHORIZABLE, new String[] {UserConstants.NT_REP_USER, UserConstants.NT_REP_SYSTEM_USER, UserConstants.NT_REP_GROUP}).
                put(AuthorizableType.GROUP, new String[] {UserConstants.NT_REP_GROUP}).build();

        for (AuthorizableType type : test.keySet()) {
            for (String ntName : test.get(type)) {
                assertEquals("id", UserUtil.getAuthorizableId(createTree(ntName, "id"), type));
            }
        }
    }

    @Test
    public void testGetAuthorizableIdWithTypeFallback() {
        Map<AuthorizableType, String[]> test = ImmutableMap.<AuthorizableType,String[]>builder().
                put(AuthorizableType.USER, new String[]{UserConstants.NT_REP_USER, UserConstants.NT_REP_SYSTEM_USER}).
                put(AuthorizableType.AUTHORIZABLE, new String[]{UserConstants.NT_REP_USER, UserConstants.NT_REP_SYSTEM_USER, UserConstants.NT_REP_GROUP}).
                put(AuthorizableType.GROUP, new String[]{UserConstants.NT_REP_GROUP}).build();

        for (AuthorizableType type : test.keySet()) {
            for (String ntName : test.get(type)) {
                assertEquals("nodeName", UserUtil.getAuthorizableId(createTree(ntName, null, "nodeName"), type));
            }
        }
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
        UserUtil.castAuthorizable(Mockito.mock(User.class), null);
    }

    @Test(expected = AuthorizableTypeException.class)
    public void testCastUserToGroup() throws Exception {
        UserUtil.castAuthorizable(Mockito.mock(User.class), Group.class);
    }

    @Test(expected = AuthorizableTypeException.class)
    public void testCastGroupToUser() throws Exception {
        UserUtil.castAuthorizable(Mockito.mock(Group.class), User.class);
    }

    @Test(expected = AuthorizableTypeException.class)
    public void testCastAuthorizableToUser() throws Exception {
        UserUtil.castAuthorizable(Mockito.mock(Authorizable.class), User.class);
    }

    @Test(expected = AuthorizableTypeException.class)
    public void testCastAuthorizableToGroup() throws Exception {
        UserUtil.castAuthorizable(Mockito.mock(Authorizable.class), Group.class);
    }

    @Test
    public void testCastUserToUser() throws Exception {
        UserUtil.castAuthorizable(Mockito.mock(User.class), User.class);
    }

    @Test
    public void testCastUserToAuthorizable() throws Exception {
        UserUtil.castAuthorizable(Mockito.mock(User.class), Authorizable.class);
    }

    @Test
    public void testCastGroupToGroup() throws Exception {
        UserUtil.castAuthorizable(Mockito.mock(Group.class), Group.class);
    }

    @Test
    public void testCastGroupToAuthorizable() throws Exception {
        UserUtil.castAuthorizable(Mockito.mock(Group.class), Authorizable.class);
    }

    @Test
    public void testGetImportBehavior() {
        Map<ConfigurationParameters, Integer> testMap = ImmutableMap.of(
                ConfigurationParameters.EMPTY, ImportBehavior.IGNORE,
                ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, "anyString"), ImportBehavior.ABORT,
                ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.BESTEFFORT), ImportBehavior.ABORT,
                ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT), ImportBehavior.BESTEFFORT
        );

        for (Map.Entry<ConfigurationParameters, Integer> entry : testMap.entrySet()) {
            assertEquals(entry.getValue().intValue(), UserUtil.getImportBehavior(entry.getKey()));
        }
    }
}
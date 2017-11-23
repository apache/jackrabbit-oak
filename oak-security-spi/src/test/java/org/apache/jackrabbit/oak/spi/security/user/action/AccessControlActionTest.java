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
package org.apache.jackrabbit.oak.spi.security.user.action;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

/**
 * Unit tests for {@code AccessControlAction}.
 *
 * @see {@link org.apache.jackrabbit.oak.security.user.action.AccessControlActionTest} for integration tests include complete setup.
 */
public class AccessControlActionTest implements UserConstants {

    private final Root root = Mockito.mock(Root.class);
    private final SecurityProvider securityProvider = Mockito.mock(SecurityProvider.class);
    private final UserConfiguration userConfiguration = Mockito.mock(UserConfiguration.class);
    private final AuthorizationConfiguration authorizationConfiguration = Mockito.mock(AuthorizationConfiguration.class);

    private void initSecurityProvider(@Nonnull String adminId, @Nonnull String anonymousId, @Nonnull String... adminPrincipalNames) {
        when(userConfiguration.getParameters()).thenReturn(ConfigurationParameters.of(
                PARAM_ADMIN_ID, adminId,
                PARAM_ANONYMOUS_ID, anonymousId));

        when(authorizationConfiguration.getParameters()).thenReturn(ConfigurationParameters.of(PermissionConstants.PARAM_ADMINISTRATIVE_PRINCIPALS, adminPrincipalNames));

        when(securityProvider.getConfiguration(UserConfiguration.class)).thenReturn(userConfiguration);
        when(securityProvider.getConfiguration(AuthorizationConfiguration.class)).thenReturn(authorizationConfiguration);

    }

    private AccessControlAction createAction(@Nonnull String... privNames) {
        AccessControlAction action = new AccessControlAction();
        action.init(securityProvider, ConfigurationParameters.of(
                AccessControlAction.USER_PRIVILEGE_NAMES, privNames,
                AccessControlAction.GROUP_PRIVILEGE_NAMES, privNames));
        return action;
    }

    private AccessControlAction createAction(@Nonnull String[] userPrivNames, @Nonnull String[] groupPrivNames) {
        AccessControlAction action = new AccessControlAction();
        action.init(securityProvider, ConfigurationParameters.of(
                AccessControlAction.USER_PRIVILEGE_NAMES, userPrivNames,
                AccessControlAction.GROUP_PRIVILEGE_NAMES, groupPrivNames));
        return action;
    }

    private static void mockAuthorizable(@Nonnull Authorizable a, @Nonnull String id, @CheckForNull String principalName, @CheckForNull String path) throws RepositoryException {
        when(a.getID()).thenReturn(id);
        if (principalName != null) {
            when(a.getPrincipal()).thenReturn(new PrincipalImpl(principalName));
        } else {
            when(a.getPrincipal()).thenThrow(new RepositoryException());
        }
        if (path != null) {
            when(a.getPath()).thenReturn(path);
        } else {
            when(a.getPath()).thenThrow(new RepositoryException());
        }
    }

    private static User mockUser(@Nonnull String id, @CheckForNull String principalName, @CheckForNull String path) throws RepositoryException {
        User user = Mockito.mock(User.class);
        when(user.isGroup()).thenReturn(false);
        mockAuthorizable(user, id, principalName, path);
        return user;
    }

    private static Group mockGroup(@Nonnull String id, @CheckForNull String principalName, @CheckForNull String path) throws RepositoryException {
        Group gr = Mockito.mock(Group.class);
        when(gr.isGroup()).thenReturn(true);
        mockAuthorizable(gr, id, principalName, path);
        return gr;
    }

    @Test(expected = IllegalStateException.class)
    public void testOnCreateUserMissingSecurityProvider() throws Exception {
        new AccessControlAction().onCreate(Mockito.mock(User.class), null, root, NamePathMapper.DEFAULT);
    }

    @Test(expected = IllegalStateException.class)
    public void testOnCreateGroupMissingSecurityProvider() throws Exception {
        new AccessControlAction().onCreate(Mockito.mock(Group.class), root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testOnCreateBuiltinUser() throws Exception {
        initSecurityProvider("adminId", "anonymousId");
        AccessControlAction action = createAction(PrivilegeConstants.JCR_READ);

        String[] buildinIds = new String[] {"adminId", "anonymousId"};
        for (String id : buildinIds) {
            // throw upon getPrincipal as onCreate for builtin users must not reach that statement
            User user = mockUser(id, null, null);
            action.onCreate(user, null, root, NamePathMapper.DEFAULT);
        }
    }

    @Test(expected = RepositoryException.class)
    public void testOnCreateBuiltinIsGroup() throws Exception {
        initSecurityProvider("adminIdIsUsedByGroup", "anonymousId");
        AccessControlAction action = createAction(PrivilegeConstants.JCR_READ);

        // the check for built-in user must ignore groups
        Group gr = mockGroup("adminIdIsUsedByGroup", null, null);
        action.onCreate(gr, root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testOnCreateUserEmptyPrivs() throws Exception {
        initSecurityProvider(DEFAULT_ADMIN_ID, DEFAULT_ANONYMOUS_ID);
        AccessControlAction action = createAction(new String[0], new String[] {PrivilegeConstants.JCR_READ});

        // throw upon getPrincipal as onCreate without configured privileges call must not reach that statement
        User user = mockUser("id", null, null);
        action.onCreate(user, null, root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testOnCreateGroupEmptyPrivs() throws Exception {
        initSecurityProvider(DEFAULT_ADMIN_ID, DEFAULT_ANONYMOUS_ID);
        AccessControlAction action = createAction(new String[] {PrivilegeConstants.JCR_READ}, new String[0]);

        // throw upon getPrincipal as onCreate without configured privileges call must not reach that statement
        Group gr = mockGroup("id", null, null);
        action.onCreate(gr, root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testOnCreateAdminUser() throws Exception {
        initSecurityProvider(DEFAULT_ADMIN_ID, DEFAULT_ANONYMOUS_ID, "administrativePrincipal");
        AccessControlAction action = createAction(PrivilegeConstants.JCR_READ);

        // throw upon getPath as onCreate for administrative principal call must not reach that statement
        User user = mockUser("id", "administrativePrincipal", null);
        action.onCreate(user, null, root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testOnCreateAdminGroup() throws Exception {
        initSecurityProvider(DEFAULT_ADMIN_ID, DEFAULT_ANONYMOUS_ID, "administrativePrincipal");
        AccessControlAction action = createAction(PrivilegeConstants.JCR_READ);

        // throw upon getPath as onCreate for administrative principal call must not reach that statement
        Group gr = mockGroup("id", "administrativePrincipal", null);
        action.onCreate(gr, root, NamePathMapper.DEFAULT);
    }


    @Test(expected = RepositoryException.class)
    public void testOnCreateUserWithoutPath() throws Exception {
        initSecurityProvider(DEFAULT_ADMIN_ID, DEFAULT_ANONYMOUS_ID);
        AccessControlAction action = createAction(PrivilegeConstants.JCR_READ);

        // throw upon getPath
        User user = mockUser("id", "principalName", null);
        action.onCreate(user, null, root, NamePathMapper.DEFAULT);
    }

    @Test(expected = RepositoryException.class)
    public void testOnCreateGroupWithoutPath() throws Exception {
        initSecurityProvider(DEFAULT_ADMIN_ID, DEFAULT_ANONYMOUS_ID);
        AccessControlAction action = createAction(PrivilegeConstants.JCR_READ);

        // throw upon getPath as onCreate for administrative principal call must not reach that statement
        Group gr = mockGroup("id", "principal", null);
        action.onCreate(gr, root, NamePathMapper.DEFAULT);
    }

}
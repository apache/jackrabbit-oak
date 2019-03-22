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

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.iterator.AccessControlPolicyIteratorAdapter;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.mockito.Mockito;

import java.security.Principal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@code AccessControlAction}.
 *
 * @see {@code org.apache.jackrabbit.oak.security.user.action.AccessControlActionTest} for integration tests include complete setup.
 */
public class AccessControlActionTest implements UserConstants {

    private final Root root = mock(Root.class);
    private final SecurityProvider securityProvider = mock(SecurityProvider.class);
    private final UserConfiguration userConfiguration = mock(UserConfiguration.class);
    private final AuthorizationConfiguration authorizationConfiguration = mock(AuthorizationConfiguration.class);

    private void initSecurityProvider(@NotNull String adminId, @NotNull String anonymousId, @NotNull String... adminPrincipalNames) throws Exception {
        initSecurityProvider(null, adminId, anonymousId, adminPrincipalNames);
    }

    private void initSecurityProvider(@Nullable AccessControlManager acMgr, @NotNull String adminId, @NotNull String anonymousId, @NotNull String... adminPrincipalNames) throws Exception {
        when(userConfiguration.getParameters()).thenReturn(ConfigurationParameters.of(
                PARAM_ADMIN_ID, adminId,
                PARAM_ANONYMOUS_ID, anonymousId));

        when(authorizationConfiguration.getParameters()).thenReturn(ConfigurationParameters.of(PermissionConstants.PARAM_ADMINISTRATIVE_PRINCIPALS, adminPrincipalNames));
        if (acMgr != null) {
            when(authorizationConfiguration.getAccessControlManager(root, NamePathMapper.DEFAULT)).thenReturn(acMgr);
        }
        when(securityProvider.getConfiguration(UserConfiguration.class)).thenReturn(userConfiguration);
        when(securityProvider.getConfiguration(AuthorizationConfiguration.class)).thenReturn(authorizationConfiguration);

    }

    private AccessControlManager mockAccessControlManager(boolean addEntrySuccess) throws Exception {
        AccessControlManager acMgr = mock(AccessControlManager.class);
        when(acMgr.getApplicablePolicies("/none")).thenReturn(AccessControlPolicyIteratorAdapter.EMPTY);
        AccessControlPolicy policy = mock(AccessControlPolicy.class);
        when(acMgr.getApplicablePolicies("/nonACL")).thenReturn(new AccessControlPolicyIteratorAdapter(ImmutableList.of(policy)));
        JackrabbitAccessControlList acl = mock(JackrabbitAccessControlList.class);
        if (addEntrySuccess) {
            when(acl.addAccessControlEntry(any(Principal.class), any(Privilege[].class))).thenReturn(true);
        }
        when(acMgr.getApplicablePolicies("/acl")).thenReturn(new AccessControlPolicyIteratorAdapter(ImmutableList.of(acl)));
        return acMgr;

    }

    private AccessControlAction createAction(@NotNull String... privNames) {
        AccessControlAction action = new AccessControlAction();
        action.init(securityProvider, ConfigurationParameters.of(
                AccessControlAction.USER_PRIVILEGE_NAMES, privNames,
                AccessControlAction.GROUP_PRIVILEGE_NAMES, privNames));
        return action;
    }

    private AccessControlAction createAction(@NotNull String[] userPrivNames, @NotNull String[] groupPrivNames) {
        AccessControlAction action = new AccessControlAction();
        action.init(securityProvider, ConfigurationParameters.of(
                AccessControlAction.USER_PRIVILEGE_NAMES, userPrivNames,
                AccessControlAction.GROUP_PRIVILEGE_NAMES, groupPrivNames));
        return action;
    }

    private static void mockAuthorizable(@NotNull Authorizable a, @NotNull String id, @Nullable String principalName, @Nullable String path) throws RepositoryException {
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

    private static User mockUser(@NotNull String id, @Nullable String principalName, @Nullable String path) throws RepositoryException {
        User user = mock(User.class);
        when(user.isGroup()).thenReturn(false);
        mockAuthorizable(user, id, principalName, path);
        return user;
    }

    private static Group mockGroup(@NotNull String id, @Nullable String principalName, @Nullable String path) throws RepositoryException {
        Group gr = mock(Group.class);
        when(gr.isGroup()).thenReturn(true);
        mockAuthorizable(gr, id, principalName, path);
        return gr;
    }

    @Test(expected = IllegalStateException.class)
    public void testOnCreateUserMissingSecurityProvider() throws Exception {
        new AccessControlAction().onCreate(mock(User.class), null, root, NamePathMapper.DEFAULT);
    }

    @Test(expected = IllegalStateException.class)
    public void testOnCreateGroupMissingSecurityProvider() throws Exception {
        new AccessControlAction().onCreate(mock(Group.class), root, NamePathMapper.DEFAULT);
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

    @Test
    public void testOnCreateNoApplicablePolicy() throws Exception {
        initSecurityProvider(mockAccessControlManager(false), DEFAULT_ADMIN_ID, DEFAULT_ANONYMOUS_ID);
        AccessControlAction action = createAction(PrivilegeConstants.JCR_READ);

        action.onCreate(mockUser("userId", "pName", "/none"), "pw", root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testOnCreateNoApplicableAclPolicy() throws Exception {
        initSecurityProvider(mockAccessControlManager(false), DEFAULT_ADMIN_ID, DEFAULT_ANONYMOUS_ID);
        AccessControlAction action = createAction(PrivilegeConstants.JCR_READ);

        action.onCreate(mockGroup("grId", "pName", "/nonACL"), root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testOnCreateApplicableAclPolicyForGroup() throws Exception {
        initSecurityProvider(mockAccessControlManager(false), DEFAULT_ADMIN_ID, DEFAULT_ANONYMOUS_ID);
        AccessControlAction action = createAction(PrivilegeConstants.JCR_READ);

        action.onCreate(mockGroup("grId", "pName", "/acl"), root, NamePathMapper.DEFAULT);
    }

    @Test
    public void testOnCreateApplicableAclPolicyForUser() throws Exception {
        initSecurityProvider(mockAccessControlManager(true), DEFAULT_ADMIN_ID, DEFAULT_ANONYMOUS_ID);
        AccessControlAction action = createAction(PrivilegeConstants.JCR_READ);

        action.onCreate(mockUser("userId", "pName", "/acl"), "pw", root, NamePathMapper.DEFAULT);
    }
}

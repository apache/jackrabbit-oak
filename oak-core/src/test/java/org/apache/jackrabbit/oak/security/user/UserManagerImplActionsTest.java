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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.user.monitor.UserMonitor;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.GroupAction;
import org.apache.jackrabbit.oak.spi.security.user.action.UserAction;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class UserManagerImplActionsTest extends AbstractUserTest {

    private final AuthorizableActionProvider actionProvider = mock(AuthorizableActionProvider.class);
    private final AuthorizableAction action = mock(AuthorizableAction.class, withSettings().extraInterfaces(GroupAction.class, UserAction.class));

    private UserManagerImpl userMgr;

    @Before
    public void before() throws Exception {
        super.before();
        userMgr = createUserManagerImpl(root);
        reset(action);
    }

    @After
    public void after() throws Exception {
        try {
            reset(action, actionProvider);
            root.refresh();
        } finally {
            super.after();
        }
    }

    @Override
    protected UserMonitor getUserMonitor() {
        return UserMonitor.NOOP;
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        List actions = Collections.singletonList(action);
        when(actionProvider.getAuthorizableActions(any(SecurityProvider.class))).thenReturn(actions);
        return ConfigurationParameters.of(UserConfiguration.NAME,
                ConfigurationParameters.of(
                        PARAM_AUTHORIZABLE_ACTION_PROVIDER, actionProvider,
                        ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT));
    }

    @Test
    public void testCreateUser() throws Exception {
        User u1 = userMgr.createUser("uid", "pw");
        User u2 = userMgr.createUser("uid2", null, new PrincipalImpl("name2"), "relpath");
        verify(action, times(1)).onCreate(u1, "pw", root, getNamePathMapper());
        verify(action, times(1)).onCreate(u2, null, root, getNamePathMapper());
        verifyNoMoreInteractions(action);
    }

    @Test
    public void testOnUserCreateWithSystemUser() throws RepositoryException {
        User user = when(mock(User.class).isSystemUser()).thenReturn(true).getMock();
        userMgr.onCreate(user, null);
        verifyNoInteractions(action);
    }

    @Test
    public void testCreateSystemUser() throws Exception {
        User su = userMgr.createSystemUser("sid", "system/relpath");
        assertNotNull(su);
        verify(action, times(1)).onCreate(su, root, getNamePathMapper());
        verifyNoMoreInteractions(action);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOnSystemUserCreateWithRegularUser() throws RepositoryException {
        User user = when(mock(User.class).isSystemUser()).thenReturn(false).getMock();
        userMgr.onCreate(user);
        verifyNoInteractions(action);
    }

    @Test
    public void testCreateGroup() throws Exception {
        Group gr1 = userMgr.createGroup("gId");
        Group gr2 = userMgr.createGroup(new PrincipalImpl("grName"));
        Group gr3 = userMgr.createGroup(new PrincipalImpl("grName2"), "relPath");
        verify(action, times(3)).onCreate(any(Group.class), any(Root.class), any(NamePathMapper.class));
        verifyNoMoreInteractions(action);
    }

    @Test
    public void testChangePw() throws Exception {
        User u1 = userMgr.createUser("uid", null);
        u1.changePassword("new");
        u1.changePassword("changedAgain", "new");

        verify(action).onCreate(u1, null, root, getNamePathMapper());
        verify(action, times(2)).onPasswordChange(any(User.class), any(String.class), any(Root.class), any(NamePathMapper.class));
        verifyNoMoreInteractions(action);
    }

    @Test
    public void testDisable() throws Exception {
        User u1 = userMgr.createUser("uid", "pw");
        u1.disable("disable");
        u1.disable(null);

        verify(action).onCreate(u1, "pw", root, getNamePathMapper());
        verify((UserAction) action).onDisable(u1, "disable", root, getNamePathMapper());
        verify((UserAction) action).onDisable(u1, null, root, getNamePathMapper());
        verifyNoMoreInteractions(action);
    }

    @Test
    public void testAddMembers() throws Exception {
        User testUser = getTestUser();
        Group gr1 = userMgr.createGroup("gId");
        gr1.addMember(testUser);
        gr1.addMembers("memberId1", "memberId2", gr1.getID());

        gr1.removeMember(testUser);
        gr1.removeMembers("memberId1");

        verify(((GroupAction) action)).onMemberAdded(gr1, testUser, root, getNamePathMapper());
        verify(((GroupAction) action)).onMembersAdded(gr1, Set.of("memberId1", "memberId2"), Collections.singleton(gr1.getID()), root, getNamePathMapper());
        verify(((GroupAction) action), never()).onMembersAddedContentId(any(Group.class), any(Iterable.class), any(Iterable.class), any(Root.class), any(NamePathMapper.class));

        verify(((GroupAction) action)).onMemberRemoved(gr1, testUser, root, getNamePathMapper());
        verify(((GroupAction) action)).onMembersRemoved(gr1, Collections.singleton("memberId1"), Collections.emptySet(), root, getNamePathMapper());
    }

    @Test
    public void testOnMembersAddedByContentId() throws Exception {
        Group testGroup = mock(Group.class);
        Set<String> membersIds = Set.of(UUIDUtils.generateUUID());

        userMgr.onGroupUpdate(testGroup, false, true, membersIds, Collections.emptySet());
        verify(((GroupAction) action), times(1)).onMembersAddedContentId(testGroup, membersIds, Collections.emptySet(), root, getNamePathMapper());
    }
}
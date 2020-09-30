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
package org.apache.jackrabbit.oak.security.user.action;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AbstractAuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.UserAction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;
import java.security.Principal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class UserActionTest extends AbstractSecurityTest {

    private UserAction userAction = mock(UserAction.class);
    private ClearProfileAction clearProfileAction = new ClearProfileAction();

    private final AuthorizableActionProvider actionProvider = securityProvider -> ImmutableList.of(userAction, clearProfileAction);

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters userParams = ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, actionProvider);
        return ConfigurationParameters.of(UserConfiguration.NAME, userParams);
    }

    @Test
    public void testDisableUserCnt() throws Exception {
        User user = getTestUser();
        user.disable("disabled");

        verify(userAction, times(1)).onDisable(user, "disabled", root, getNamePathMapper());
        verify(userAction, never()).onGrantImpersonation(any(User.class), any(Principal.class), any(Root.class), any(NamePathMapper.class));
        verify(userAction, never()).onRevokeImpersonation(any(User.class), any(Principal.class), any(Root.class), any(NamePathMapper.class));

        user.disable(null);
        verify(userAction, times(1)).onDisable(user, null, root, getNamePathMapper());
        verify(userAction, never()).onGrantImpersonation(any(User.class), any(Principal.class), any(Root.class), any(NamePathMapper.class));
        verify(userAction, never()).onRevokeImpersonation(any(User.class), any(Principal.class), any(Root.class), any(NamePathMapper.class));
    }

    @Test
    public void testGrantImpCnt() throws Exception {
        User user = getTestUser();
        Principal p2 = getUserManager(root).createUser("tmpUser", null).getPrincipal();

        user.getImpersonation().grantImpersonation(p2);

        verify(userAction, never()).onDisable(any(User.class), anyString(), any(Root.class), any(NamePathMapper.class));
        verify(userAction, times(1)).onGrantImpersonation(any(User.class), any(Principal.class), any(Root.class), any(NamePathMapper.class));
        verify(userAction, never()).onRevokeImpersonation(any(User.class), any(Principal.class), any(Root.class), any(NamePathMapper.class));
    }

    @Test
    public void testRevokeImpCnt() throws Exception {
        User user = getTestUser();
        Principal p2 = getUserManager(root).createUser("tmpUser", null).getPrincipal();

        user.getImpersonation().revokeImpersonation(p2);

        verify(userAction, never()).onDisable(any(User.class), anyString(), any(Root.class), any(NamePathMapper.class));
        verify(userAction, never()).onGrantImpersonation(any(User.class), any(Principal.class), any(Root.class), any(NamePathMapper.class));
        verify(userAction, times(1)).onRevokeImpersonation(any(User.class), any(Principal.class), any(Root.class), any(NamePathMapper.class));
    }

    @Test
    public void testDisableRemovesProfiles() throws Exception {
        User user = getTestUser();
        ValueFactory vf = getValueFactory();
        user.setProperty("any", vf.createValue("value"));
        user.setProperty("profiles/public/nickname", vf.createValue("amal"));
        user.setProperty("profiles/private/age", vf.createValue(14));
        root.commit();

        user.disable("disabled");

        assertTrue(user.hasProperty("any"));
        assertFalse(user.hasProperty("profiles/public/nickname"));
        assertFalse(user.hasProperty("profiles/private/age"));

        Tree t = root.getTree(user.getPath());
        assertTrue(t.hasProperty(UserConstants.REP_DISABLED));
        assertFalse(t.hasChild("profiles"));

        // it's transient:
        root.refresh();
        t = root.getTree(user.getPath());
        assertFalse(t.hasProperty(UserConstants.REP_DISABLED));
        assertTrue(t.hasChild("profiles"));
    }

    class ClearProfileAction extends AbstractAuthorizableAction implements UserAction {

        @Override
        public void onDisable(@NotNull User user, @Nullable String disableReason, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
            if (disableReason != null) {
                Tree t = root.getTree(user.getPath());
                if (t.exists() && t.hasChild("profiles")) {
                    t.getChild("profiles").remove();
                }
            }
        }

        @Override
        public void onGrantImpersonation(@NotNull User user, @NotNull Principal principal, @NotNull Root root, @NotNull NamePathMapper namePathMapper) {
            // nothing to do
        }

        @Override
        public void onRevokeImpersonation(@NotNull User user, @NotNull Principal principal, @NotNull Root root, @NotNull NamePathMapper namePathMapper) {
            // nothing to do
        }
    }
}
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

import java.security.Principal;
import java.util.List;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AbstractAuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.UserAction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UserActionTest extends AbstractSecurityTest {

    private CountingUserAction cntAction = new CountingUserAction();
    private ClearProfileAction clearProfileAction = new ClearProfileAction();

    private final AuthorizableActionProvider actionProvider = new AuthorizableActionProvider() {
        @Override
        public @NotNull List<? extends AuthorizableAction> getAuthorizableActions(@NotNull SecurityProvider securityProvider) {
            return ImmutableList.of(cntAction, clearProfileAction);
        }
    };

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters userParams = ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, actionProvider);
        return ConfigurationParameters.of(UserConfiguration.NAME, userParams);
    }

    @Test
    public void testDisableUserCnt() throws Exception {
        User user = getTestUser();
        user.disable("disabled");

        assertEquals(1, cntAction.onDisabledCnt);
        assertEquals(0, cntAction.onGrantImpCnt);
        assertEquals(0, cntAction.onRevokeImpCnt);

        user.disable(null);
        assertEquals(2, cntAction.onDisabledCnt);
        assertEquals(0, cntAction.onGrantImpCnt);
        assertEquals(0, cntAction.onRevokeImpCnt);
    }

    @Test
    public void testGrantImpCnt() throws Exception {
        User user = getTestUser();
        Principal p2 = getUserManager(root).createUser("tmpUser", null).getPrincipal();

        user.getImpersonation().grantImpersonation(p2);

        assertEquals(0, cntAction.onDisabledCnt);
        assertEquals(1, cntAction.onGrantImpCnt);
        assertEquals(0, cntAction.onRevokeImpCnt);
    }

    @Test
    public void testRevokeImpCnt() throws Exception {
        User user = getTestUser();
        Principal p2 = getUserManager(root).createUser("tmpUser", null).getPrincipal();

        user.getImpersonation().revokeImpersonation(p2);

        assertEquals(0, cntAction.onDisabledCnt);
        assertEquals(0, cntAction.onGrantImpCnt);
        assertEquals(1, cntAction.onRevokeImpCnt);
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


    class CountingUserAction extends AbstractAuthorizableAction implements UserAction  {

        int onDisabledCnt = 0;
        int onGrantImpCnt = 0;
        int onRevokeImpCnt = 0;

        @Override
        public void onDisable(@NotNull User user, @Nullable String disableReason, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
            onDisabledCnt++;
        }

        @Override
        public void onGrantImpersonation(@NotNull User user, @NotNull Principal principal, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
            onGrantImpCnt++;
        }

        @Override
        public void onRevokeImpersonation(@NotNull User user, @NotNull Principal principal, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
            onRevokeImpCnt++;
        }
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
        public void onGrantImpersonation(@NotNull User user, @NotNull Principal principal, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
            // nothing to do
        }

        @Override
        public void onRevokeImpersonation(@NotNull User user, @NotNull Principal principal, @NotNull Root root, @NotNull NamePathMapper namePathMapper) throws RepositoryException {
            // nothing to do
        }
    }
}
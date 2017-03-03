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

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AbstractAuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.PasswordValidationAction;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PasswordValidationActionTest extends AbstractSecurityTest {

    private final PasswordValidationAction pwAction = new PasswordValidationAction();
    private final TestAction testAction = new TestAction();
    private final AuthorizableActionProvider actionProvider = new AuthorizableActionProvider() {
        @Nonnull
        @Override
        public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
            return ImmutableList.of(pwAction, testAction);
        }
    };

    private User user;

    @Before
    public void before() throws Exception {
        super.before();
        testAction.reset();
        pwAction.init(getSecurityProvider(), ConfigurationParameters.of(
                PasswordValidationAction.CONSTRAINT, "^.*(?=.{8,})(?=.*[a-z])(?=.*[A-Z]).*"));

    }

    @After
    public void after() throws Exception {
        if (user != null) {
            user.remove();
            root.commit();
        }
        root = null;
        super.after();
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters userParams = ConfigurationParameters.of(
                UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, actionProvider
        );
        return ConfigurationParameters.of(UserConfiguration.NAME, userParams);
    }

    @Test
    public void testActionIsCalled() throws Exception {
        user = getUserManager(root).createUser("testUser", "testUser12345");
        root.commit();
        assertEquals(1, testAction.onCreateCalled);

        user.changePassword("pW12345678");
        assertEquals(1, testAction.onPasswordChangeCalled);

        user.changePassword("pW1234567890", "pW12345678");
        assertEquals(2, testAction.onPasswordChangeCalled);
    }

    @Test
    public void testPasswordValidationActionOnCreate() throws Exception {
        String hashed = PasswordUtil.buildPasswordHash("DWkej32H");
        user = getUserManager(root).createUser("testuser", hashed);
        root.commit();

        String pwValue = root.getTree(user.getPath()).getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
        assertFalse(PasswordUtil.isPlainTextPassword(pwValue));
        assertTrue(PasswordUtil.isSame(pwValue, hashed));
    }

    @Test
    public void testPasswordValidationActionOnChange() throws Exception {
        user = getUserManager(root).createUser("testuser", "testPw123456");
        root.commit();
        try {
            pwAction.init(getSecurityProvider(), ConfigurationParameters.of(PasswordValidationAction.CONSTRAINT, "abc"));

            String hashed = PasswordUtil.buildPasswordHash("abc");
            user.changePassword(hashed);

            fail("Password change must always enforce password validation.");

        } catch (ConstraintViolationException e) {
            // success
        }
    }

    //--------------------------------------------------------------------------
    private class TestAction extends AbstractAuthorizableAction {

        private int onCreateCalled = 0;
        private int onPasswordChangeCalled = 0;

        void reset() {
            onCreateCalled = 0;
            onPasswordChangeCalled = 0;
        }

        @Override
        public void onCreate(@Nonnull User user, @Nullable String password, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            onCreateCalled++;
        }

        @Override
        public void onPasswordChange(@Nonnull User user, @Nullable String newPassword, @Nonnull Root root, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
            onPasswordChangeCalled++;
        }
    }
}

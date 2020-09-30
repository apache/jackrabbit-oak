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
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.PasswordValidationAction;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.nodetype.ConstraintViolationException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PasswordValidationActionTest extends AbstractSecurityTest {

    private final PasswordValidationAction pwAction = new PasswordValidationAction();
    private final AuthorizableAction testAction = mock(AuthorizableAction.class);
    private final AuthorizableActionProvider actionProvider = securityProvider -> ImmutableList.of(pwAction, testAction);

    @Before
    public void before() throws Exception {
        super.before();
        pwAction.init(getSecurityProvider(), ConfigurationParameters.of(
                PasswordValidationAction.CONSTRAINT, "^.*(?=.{8,})(?=.*[a-z])(?=.*[A-Z]).*"));

    }

    @After
    public void after() throws Exception {
        root.refresh();
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
        User user = getUserManager(root).createUser("testUser", "testUser12345");
        verify(testAction, times(1)).onCreate(user, "testUser12345", root, getNamePathMapper());

        user.changePassword("pW12345678");
        verify(testAction, times(1)).onPasswordChange(user, "pW12345678", root, getNamePathMapper());

        user.changePassword("pW1234567890", "pW12345678");
        verify(testAction, times(1)).onPasswordChange(user, "pW12345678", root, getNamePathMapper());
    }

    @Test
    public void testPasswordValidationActionOnCreate() throws Exception {
        String hashed = PasswordUtil.buildPasswordHash("DWkej32H");
        User user = getUserManager(root).createUser("testuser", hashed);

        String pwValue = root.getTree(user.getPath()).getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
        assertFalse(PasswordUtil.isPlainTextPassword(pwValue));
        assertTrue(PasswordUtil.isSame(pwValue, hashed));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testPasswordValidationActionOnChange() throws Exception {
        User user = getUserManager(root).createUser("testuser", "testPw123456");
        String hashed = PasswordUtil.buildPasswordHash("abc");
        try {
            pwAction.init(getSecurityProvider(), ConfigurationParameters.of(PasswordValidationAction.CONSTRAINT, "abc"));
            user.changePassword(hashed);
        } finally {
            verify(testAction, times(1)).onCreate(user, "testPw123456", root, getNamePathMapper());
            verify(testAction, never()).onPasswordChange(user, hashed, root, getNamePathMapper());
        }
    }
}

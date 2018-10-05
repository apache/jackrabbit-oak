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

import java.util.UUID;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.CredentialExpiredException;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @see <a href="https://issues.apache.org/jira/browse/OAK-1922">OAK-1922</a>
 */
public class PasswordExpiryAndForceInitialChangeTest extends AbstractSecurityTest {

    private String userId;

    @Before
    public void before() throws Exception {
        super.before();
        userId = getTestUser().getID();
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters parameters = ConfigurationParameters.of(
                UserConstants.PARAM_PASSWORD_MAX_AGE, 10,
                UserConstants.PARAM_PASSWORD_INITIAL_CHANGE, true);
        return ConfigurationParameters.of(ImmutableMap.of(UserConfiguration.NAME, parameters));
    }

    @Test
    public void testCreateUser() throws Exception {
        String newUserId = "newuser" + UUID.randomUUID();
        User user = null;
        try {
            user = getUserManager(root).createUser(newUserId, newUserId);
            root.commit();

            assertFalse(root.getTree(user.getPath()).hasChild(UserConstants.REP_PWD));
            assertFalse(user.hasProperty(UserConstants.REP_PWD + "/" + UserConstants.REP_PASSWORD_LAST_MODIFIED));
        } finally {
            if (user != null) {
                user.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testAuthenticateMustChangePassword() throws Exception {
        Authentication a = new UserAuthentication(getUserConfiguration(), root, userId);
        try {
            // the user should need to change the password on first login
            a.authenticate(new SimpleCredentials(userId, userId.toCharArray()));
            fail("Credentials should be expired");
        } catch (CredentialExpiredException e) {
            // success
        }
    }

    @Test
    public void testChangePasswordReset() throws Exception {
        // once the user changes the password, the login should succeed
        User user = getTestUser();
        user.changePassword(userId);
        root.commit();
        PropertyState p = root.getTree(user.getPath()).getChild(UserConstants.REP_PWD).getProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED);
        long newModTime = p.getValue(Type.LONG);
        assertTrue(newModTime > 0);

        Authentication a = new UserAuthentication(getUserConfiguration(), root, userId);
        // during user creation pw last modified is set, thus it shouldn't expire
        a.authenticate(new SimpleCredentials(userId, userId.toCharArray()));
    }
}

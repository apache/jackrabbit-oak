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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction;
import org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableActionProvider;
import org.apache.jackrabbit.oak.spi.security.user.action.PasswordValidationAction;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.CredentialExpiredException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * @see <a href="https://issues.apache.org/jira/browse/OAK-3463">OAK-3463</a>
 */
public class PasswordExpiryHistoryTest extends AbstractSecurityTest {

    private String userId;

    @Before
    public void before() throws Exception {
        super.before();
        userId = getTestUser().getID();
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        final PasswordValidationAction pwAction = new PasswordValidationAction();
        pwAction.init(null, ConfigurationParameters.of(
                PasswordValidationAction.CONSTRAINT, "^.*(?=.{4,}).*"
        ));
        final AuthorizableActionProvider actionProvider = new AuthorizableActionProvider() {
            @Nonnull
            @Override
            public List<? extends AuthorizableAction> getAuthorizableActions(@Nonnull SecurityProvider securityProvider) {
                return ImmutableList.of(pwAction);
            }
        };

        ConfigurationParameters userConfig = ConfigurationParameters.of(
                ImmutableMap.of(
                        UserConstants.PARAM_AUTHORIZABLE_ACTION_PROVIDER, actionProvider,
                        UserConstants.PARAM_PASSWORD_MAX_AGE, 10,
                        UserConstants.PARAM_PASSWORD_HISTORY_SIZE, 10
                )
        );
        return ConfigurationParameters.of(UserConfiguration.NAME, userConfig);
    }

    @Test
    public void testAuthenticatePasswordExpiredAndSame() throws Exception {
        User user = getTestUser();
        Authentication a = new UserAuthentication(getUserConfiguration(), root, userId);
        // set password last modified to beginning of epoch
        root.getTree(user.getPath()).getChild(UserConstants.REP_PWD).setProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED, 0);
        root.commit();
        try {
            a.authenticate(new SimpleCredentials(userId, userId.toCharArray()));
            fail("Credentials should be expired");
        } catch (CredentialExpiredException e) {
            // success, credentials are expired

            // try to change password to the same one, this should fail due pw history
            SimpleCredentials pwChangeCreds = new SimpleCredentials(userId, userId.toCharArray());
            try {
                pwChangeCreds.setAttribute(UserConstants.CREDENTIALS_ATTRIBUTE_NEWPASSWORD, user.getID());
                a.authenticate(pwChangeCreds);
                fail("User password changed in spite of enabled pw history");
            } catch (CredentialExpiredException c) {
                // success, pw found in history
                Object attr = pwChangeCreds.getAttribute(PasswordHistoryException.class.getSimpleName());
                assertEquals(
                        "credentials should contain pw change failure reason",
                        "New password is identical to the current password.",
                        attr);
            }
        }
    }

    @Test
    public void testAuthenticatePasswordExpiredAndInHistory() throws Exception {
        User user = getTestUser();
        user.changePassword("pw12345678");
        Authentication a = new UserAuthentication(getUserConfiguration(), root, userId);
        // set password last modified to beginning of epoch
        root.getTree(user.getPath()).getChild(UserConstants.REP_PWD).setProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED, 0);
        root.commit();
        try {
            a.authenticate(new SimpleCredentials(userId, "pw12345678".toCharArray()));
            fail("Credentials should be expired");
        } catch (CredentialExpiredException e) {
            // success, credentials are expired

            // try to change password to the same one, this should fail due pw history
            SimpleCredentials pwChangeCreds = new SimpleCredentials(userId, "pw12345678".toCharArray());
            try {
                pwChangeCreds.setAttribute(UserConstants.CREDENTIALS_ATTRIBUTE_NEWPASSWORD, user.getID());
                a.authenticate(pwChangeCreds);
                fail("User password changed in spite of enabled pw history");
            } catch (CredentialExpiredException c) {
                // success, pw found in history
                Object attr = pwChangeCreds.getAttribute(PasswordHistoryException.class.getSimpleName());
                assertEquals(
                        "credentials should contain pw change failure reason",
                        "New password was found in password history.",
                        attr);
            }
        }
    }

    @Test
    public void testAuthenticatePasswordExpiredAndValidationFailure() throws Exception {
        User user = getTestUser();
        Authentication a = new UserAuthentication(getUserConfiguration(), root, userId);
        // set password last modified to beginning of epoch
        root.getTree(user.getPath()).getChild(UserConstants.REP_PWD).setProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED, 0);
        root.commit();
        try {
            a.authenticate(new SimpleCredentials(userId, userId.toCharArray()));
            fail("Credentials should be expired");
        } catch (CredentialExpiredException e) {
            // success, credentials are expired

            // try to change password to the same one, this should fail due pw history
            SimpleCredentials pwChangeCreds = new SimpleCredentials(userId, userId.toCharArray());
            try {
                pwChangeCreds.setAttribute(UserConstants.CREDENTIALS_ATTRIBUTE_NEWPASSWORD, "2");
                a.authenticate(pwChangeCreds);
                fail("User password changed in spite of expected validation failure");
            } catch (CredentialExpiredException c) {
                // success, pw found in history
                assertNull(pwChangeCreds.getAttribute(PasswordHistoryException.class.getSimpleName()));
            }
        }
    }
}

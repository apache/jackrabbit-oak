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

import javax.jcr.SimpleCredentials;
import javax.security.auth.login.CredentialExpiredException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @see <a href="https://issues.apache.org/jira/browse/OAK-2156">OAK-2156</a>
 */
public class ResetExpiredPasswordTest extends AbstractSecurityTest implements UserConstants {

    private String userId;

    @Before
    public void before() throws Exception {
        super.before();

        User testuser = getTestUser();
        userId = testuser.getID();

        // set password last modified to beginning of epoch
        root.getTree(testuser.getPath()).getChild(REP_PWD).setProperty(REP_PASSWORD_LAST_MODIFIED, 0);
        root.commit();
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters userConfig = ConfigurationParameters.of(UserConstants.PARAM_PASSWORD_MAX_AGE, 10);
        return ConfigurationParameters.of(UserConfiguration.NAME, userConfig);
    }

    private void authenticate(String expiredPw, Object newPw) throws LoginException {
        SimpleCredentials creds = new SimpleCredentials(userId, expiredPw.toCharArray());
        creds.setAttribute(UserConstants.CREDENTIALS_ATTRIBUTE_NEWPASSWORD, newPw);

        Authentication a = new UserAuthentication(getUserConfiguration(), root, userId);
        a.authenticate(creds);
    }

    @Test
    public void testPasswordChangePersisted() throws Exception {
        authenticate(userId, "newPw");

        // check that the password has been persisted and has the value of the new password
        Root rootBasedOnSeparateSession = login(getAdminCredentials()).getLatestRoot();
        Tree userTree = rootBasedOnSeparateSession.getTree(getTestUser().getPath());
        assertTrue(PasswordUtil.isSame(userTree.getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING), "newPw"));
    }

    @Test
    public void testAuthenticatePasswordExpiredThenChanged() throws Exception {
        authenticate(userId, userId);
    }

    @Test
    public void testChangeWithWrongPw() throws Exception {
        try {
            authenticate("wrongPw", "newPw");
            fail("Authentication with wrong expired password should fail and should not reset pw.");
        } catch (LoginException e) {
            // success
        } finally {
            Tree userTree = root.getTree(getTestUser().getPath());
            assertTrue(PasswordUtil.isSame(userTree.getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING), userId));
            assertEquals(0, userTree.getChild(UserConstants.REP_PWD).getProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED).getValue(Type.LONG).longValue());
        }
    }

    @Test
    public void testChangeWithNonStringAttribute() throws Exception {
        try {
            authenticate(userId, new Long(1));
            fail("Authentication with non-string attribute should fail.");
        } catch (CredentialExpiredException e) {
            // success
        } finally {
            Tree userTree = root.getTree(getTestUser().getPath());
            assertTrue(PasswordUtil.isSame(userTree.getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING), userId));
            assertEquals(0, userTree.getChild(UserConstants.REP_PWD).getProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED).getValue(Type.LONG).longValue());
        }
    }
}

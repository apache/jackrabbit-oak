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

import java.util.Map;
import java.util.UUID;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.CredentialExpiredException;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.CREDENTIALS_ATTRIBUTE_NEWPASSWORD;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.REP_PWD;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
        return ConfigurationParameters.of(Map.of(UserConfiguration.NAME, parameters));
    }

    @NotNull
    private Tree getUserTree(@NotNull User user) throws RepositoryException {
        return root.getTree(user.getPath());
    }

    @Test
    public void testCreateUser() throws Exception {
        String newUserId = "newuser" + UUID.randomUUID();
        User user = null;
        try {
            user = getUserManager(root).createUser(newUserId, newUserId);
            root.commit();

            assertFalse(getUserTree(user).hasChild(UserConstants.REP_PWD));
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
        PropertyState p = getUserTree(user).getChild(UserConstants.REP_PWD).getProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED);
        long newModTime = p.getValue(Type.LONG);
        assertTrue(newModTime > 0);

        Authentication a = new UserAuthentication(getUserConfiguration(), root, userId);
        // during user creation pw last modified is set, thus it shouldn't expire
        a.authenticate(new SimpleCredentials(userId, userId.toCharArray()));
    }

    /**
     * rep:passwordLastModified must NOT be created otherwise the user might never be forced to change pw upon first login.
     */
    @Test
    public void testSetPasswordImportExistingUser() throws Exception {
        UserManagerImpl userManager = (UserManagerImpl) getUserManager(root);
        Tree userTree = getUserTree(getTestUser());
        assertFalse(userTree.hasChild(REP_PWD));

        userManager.setPassword(userTree, getTestUser().getID(), "pwd", true);
        assertFalse(userTree.hasChild(REP_PWD));
    }

    /**
     * rep:passwordLastModified must NOT be created in accordance to UserManager.createUser
     */
    @Test
    public void testSetPasswordImportNewUser() throws Exception {
        UserManagerImpl userManager = (UserManagerImpl) getUserManager(root);
        User u = userManager.createUser("uNew", null);
        Tree userTree = getUserTree(u);
        assertFalse(userTree.hasChild(REP_PWD));

        userManager.setPassword(userTree, "uNew", "pwd", true);
        assertFalse(userTree.hasChild(REP_PWD));
    }

    @Test
    public void testAuthenticateWithNewPasswordAttribute() throws Exception {
        Authentication a = new UserAuthentication(getUserConfiguration(), root, userId);
        SimpleCredentials sc = new SimpleCredentials(userId, userId.toCharArray());
        sc.setAttribute(CREDENTIALS_ATTRIBUTE_NEWPASSWORD, "SureChangedMyPassword!");
        try {
            assertTrue(a.authenticate(sc));
        } finally {
            assertNull(sc.getAttribute(CREDENTIALS_ATTRIBUTE_NEWPASSWORD));
        }
    }
}

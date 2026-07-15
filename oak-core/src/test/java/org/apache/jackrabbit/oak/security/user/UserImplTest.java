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

import static javax.jcr.Property.JCR_PRIMARY_TYPE;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.NT_REP_GROUP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.jcr.Credentials;
import javax.jcr.RepositoryException;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserIdCredentials;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UserImplTest extends AbstractSecurityTest {

    private User user;
    private boolean allowDisableAnonymous = true;

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        user = getTestUser();
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    @NotNull
    private User getAdminUser() throws Exception {
        User admin = getUserManager(root).getAuthorizable(UserConstants.DEFAULT_ADMIN_ID, User.class);
        assertNotNull(admin);
        return admin;
    }

    @NotNull
    private User getAnonymousUser() throws Exception {
        User anonymous = getUserManager(root).getAuthorizable(UserConstants.DEFAULT_ANONYMOUS_ID, User.class);
        assertNotNull(anonymous);
        return anonymous;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateFromInvalidTree() throws Exception {
        Tree t = when(mock(Tree.class).getProperty(JCR_PRIMARY_TYPE)).thenReturn(PropertyStates.createProperty(
                JCR_PRIMARY_TYPE,
                NT_REP_GROUP,
                Type.NAME)).getMock();
        User u = new UserImpl("uid", t, (UserManagerImpl) getUserManager(root));
    }

    @Test
    public void testIsAdmin() {
        assertFalse(user.isAdmin());
    }

    @Test
    public void testAdministratorIsAdmin() throws Exception {
        assertTrue(getAdminUser().isAdmin());
    }

    @Test
    public void testIsSystemUser() {
        assertFalse(user.isSystemUser());
    }

    @Test
    public void testIsGroup() {
        assertFalse(user.isGroup());
    }

    @Test
    public void testRemove() throws Exception {
        String id = user.getID();
        user.remove();
        assertNull(getUserManager(root).getAuthorizable(id, User.class));
    }

    @Test(expected = RepositoryException.class)
    public void testRemoveAdmin() throws Exception {
        getAdminUser().remove();
    }

    @Test(expected = RepositoryException.class)
    public void testChangePasswordToNull() throws Exception {
        user.changePassword(null);
    }

    @Test
    public void testChangePassword() throws Exception {
        String pwHash = root.getTree(user.getPath()).getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
        assertTrue(PasswordUtil.isSame(pwHash, user.getID()));

        user.changePassword("different");
        String pwHash2 = root.getTree(user.getPath()).getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
        assertTrue(PasswordUtil.isSame(pwHash2, "different"));
    }

    @Test(expected = RepositoryException.class)
    public void testChangePasswordWithOldMismatch() throws Exception {
        user.changePassword("different", "wrongOldPassword");
    }

    @Test
    public void testChangePasswordWithOld() throws Exception {
        String pwHash = root.getTree(user.getPath()).getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
        assertTrue(PasswordUtil.isSame(pwHash, user.getID()));

        user.changePassword("different", user.getID());

        String pwHash2 = root.getTree(user.getPath()).getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
        assertTrue(PasswordUtil.isSame(pwHash2, "different"));
    }

    @Test
    public void testDisable() throws Exception {
        assertNull(user.getDisabledReason());
        assertFalse(user.isDisabled());

        user.disable("reason");
        assertEquals("reason", user.getDisabledReason());
        assertTrue(user.isDisabled());

        user.disable(null);
        assertNull(user.getDisabledReason());
        assertFalse(user.isDisabled());
    }

    @Test
    public void testDisableNullReason() throws Exception {
        assertNull(user.getDisabledReason());
        assertFalse(user.isDisabled());

        user.disable(null);

        assertNull(user.getDisabledReason());
        assertFalse(user.isDisabled());
    }

    @Test
    public void testDisableAnonymous() throws Exception {
        User anonymous = getAnonymousUser();
        assertFalse(anonymous.isDisabled());

        anonymous.disable("Test anonymous disable");

        assertNotNull(anonymous.getDisabledReason());
        assertEquals("Test anonymous disable", anonymous.getDisabledReason());
        assertTrue(anonymous.isDisabled());
    }

    @Test(expected = RepositoryException.class)
    public void testDisableAnonymousNotAllowed() throws Exception {
        // Dirty hack to prevent anonymous user from being disabled
        // and initialize the repo again
        allowDisableAnonymous = false;
        securityProvider = null;

        cleanUserManager();
        before();

        User anonymous = getAnonymousUser();
        assertFalse(anonymous.isDisabled());

        anonymous.disable("Test anonymous disable");

        fail("Shouldn't have reached this point");
    }

    @Test(expected = RepositoryException.class)
    public void testDisableAdministrator() throws Exception {
        getAdminUser().disable("reason");
    }

    @Test
    public void testGetCredentials() throws Exception {
        Credentials creds = user.getCredentials();
        assertTrue(creds instanceof CredentialsImpl);

        CredentialsImpl cImpl = (CredentialsImpl) creds;
        assertEquals(user.getID(), cImpl.getUserId());
        assertTrue(PasswordUtil.isSame(cImpl.getPasswordHash(), user.getID()));
    }

    @Test
    public void testGetCredentialsUserWithoutPassword() throws Exception {
        User u = getUserManager(root).createUser("u" + UUIDUtils.generateUUID(), null);

        Credentials creds = u.getCredentials();
        assertTrue(creds instanceof UserIdCredentials);
        assertEquals(u.getID(), ((UserIdCredentials) creds).getUserId());
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        if (allowDisableAnonymous) {
            return super.getSecurityConfigParameters();
        } else {
            ConfigurationParameters userConfig = ConfigurationParameters.of(
                    UserConstants.PARAM_ALLOW_DISABLE_ANONYMOUS, false);
            return ConfigurationParameters.of(UserConfiguration.NAME, userConfig);
        }
    }
}
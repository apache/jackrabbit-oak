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

import java.lang.reflect.Field;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @see OAK-2445
 */
public class PasswordHistoryTest extends AbstractSecurityTest implements UserConstants {

    private static final String[] PASSWORDS = {
            "abc", "def", "ghi", "jkl", "mno", "pqr", "stu", "vwx", "yz0", "123", "456", "789"
    };

    private static final ConfigurationParameters CONFIG = ConfigurationParameters.of(PARAM_PASSWORD_HISTORY_SIZE, 10);

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(ImmutableMap.of(UserConfiguration.NAME, CONFIG));
    }

    @Nonnull
    private List<String> getHistory(@Nonnull User user) throws RepositoryException {
        return ImmutableList.copyOf(TreeUtil.getStrings(
                root.getTree(user.getPath()).getChild(REP_PWD),
                REP_PWD_HISTORY)).reverse();
    }

    /**
     * Use reflection to access the private fields stored in the PasswordHistory
     */
    private static Integer getMaxSize(@Nonnull PasswordHistory history) throws Exception {
        Field maxSize = history.getClass().getDeclaredField("maxSize");
        maxSize.setAccessible(true);
        return (Integer) maxSize.get(history);
    }

    /**
     * Use reflection to access the private fields stored in the PasswordHistory
     */
    private static boolean isEnabled(@Nonnull PasswordHistory history) throws Exception {
        Field isEnabled = history.getClass().getDeclaredField("isEnabled");
        isEnabled.setAccessible(true);
        return (Boolean) isEnabled.get(history);
    }

    @Test
    public void testNoPwdTreeOnUserCreation() throws Exception {
        User user = getTestUser();
        assertFalse(root.getTree(user.getPath()).hasChild(REP_PWD));
    }

    @Test
    public void testHistoryEmptyOnUserCreationWithPassword() throws Exception {
        User user = getTestUser(); // the user is created with a password set

        // the rep:pwd child must not exist. without the rep:pwd child no password history can exist.
        assertFalse(root.getTree(user.getPath()).hasChild(REP_PWD));
    }

    @Test
    public void testHistoryWithSinglePasswordChange() throws Exception {
        // the user must be able to change the password
        User user = getTestUser();
        String oldPassword = TreeUtil.getString(root.getTree(user.getPath()), REP_PASSWORD);
        user.changePassword("newPwd");
        root.commit();

        // after changing the password, 1 password history entry should be present and the
        // recorded password should be equal to the user's initial password
        // however, the user's current password must not match the old password.
        assertTrue(root.getTree(user.getPath()).hasChild(REP_PWD));

        Tree pwTree = root.getTree(user.getPath()).getChild(REP_PWD);
        assertTrue(pwTree.hasProperty(REP_PWD_HISTORY));

        List<String> history = getHistory(user);
        assertEquals(1, history.size());
        assertEquals(oldPassword, history.iterator().next());

        String currentPw = TreeUtil.getString(root.getTree(user.getPath()), REP_PASSWORD);
        assertNotSame(currentPw, oldPassword);
    }

    @Test
    public void testHistoryMaxSize() throws Exception {
        User user = getTestUser();

        // we're changing the password 12 times, history max is 10
        for (String pw : PASSWORDS) {
            user.changePassword(pw);
            root.commit();
        }

        assertEquals(10, getHistory(user).size());
    }

    @Test
    public void testHistoryOrder() throws Exception {
        User user = getTestUser();

        // we're changing the password 12 times, history max is 10
        for (String pw : PASSWORDS) {
            user.changePassword(pw);
        }

        // we skip the first entry in the password list as it was shifted out
        // due to max history size = 10.
        int i = 1;
        for (String pwHash : getHistory(user)) {
            assertTrue(PasswordUtil.isSame(pwHash, PASSWORDS[i++]));
        }
    }

    @Test
    public void testRepeatedPwAfterHistorySizeReached() throws Exception {
        User user = getTestUser();
        for (String pw : PASSWORDS) {
            user.changePassword(pw);
        }

        // changing pw back to the original value (as used for creation) must succeed
        user.changePassword(user.getID());
        // now, using all old passwords must also succeed as they get shifted out
        for (String pw : PASSWORDS) {
            user.changePassword(pw);
        }
    }

    @Test(expected = ConstraintViolationException.class)
    public void testHistoryViolationAtFirstChange() throws Exception {
        User user = getTestUser();
        user.changePassword(user.getID());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testHistoryViolation() throws Exception {
        User user = getTestUser();
        user.changePassword("abc");
        user.changePassword("def");
        user.changePassword("abc");
    }

    @Test
    public void testNoHistoryUpdateOnViolation() throws Exception {
        User user = getTestUser();
        try {
            user.changePassword("abc");
            user.changePassword("def");
            user.changePassword("abc");
            fail("history violation not detected");
        } catch (ConstraintViolationException e) {
            String[] expected = new String[] {user.getID(), "abc"};
            int i = 0;
            for (String pwHash : getHistory(user)) {
                assertTrue(PasswordUtil.isSame(pwHash, expected[i++]));
            }
        }
    }

    @Test
    public void testEnabledPasswordHistory() throws Exception {
        PasswordHistory history = new PasswordHistory(CONFIG);
        assertTrue(isEnabled(history));
        assertEquals(10, getMaxSize(history).longValue());
    }

    @Test
    public void testHistoryUpperLimit() throws Exception {
        PasswordHistory history = new PasswordHistory(ConfigurationParameters.of(PARAM_PASSWORD_HISTORY_SIZE, Integer.MAX_VALUE));

        assertTrue(isEnabled(history));
        assertEquals(1000, getMaxSize(history).longValue());
    }

    @Test
    public void testDisabledPasswordHistory() throws Exception {
        User user = getTestUser();
        Tree userTree = root.getTree(user.getPath());

        List<ConfigurationParameters> configs = ImmutableList.of(
                ConfigurationParameters.EMPTY,
                ConfigurationParameters.of(PARAM_PASSWORD_HISTORY_SIZE, PASSWORD_HISTORY_DISABLED_SIZE),
                ConfigurationParameters.of(PARAM_PASSWORD_HISTORY_SIZE, -1),
                ConfigurationParameters.of(PARAM_PASSWORD_HISTORY_SIZE, Integer.MIN_VALUE)
        );

        for (ConfigurationParameters config : configs) {
            PasswordHistory disabledHistory = new PasswordHistory(config);

            assertFalse(isEnabled(disabledHistory));
            assertFalse(disabledHistory.updatePasswordHistory(userTree, user.getID()));
        }
    }

    @Test(expected = ConstraintViolationException.class)
    public void testCheckPasswordHistory() throws Exception {
        Tree userTree = root.getTree(getTestUser().getPath());

        PasswordHistory history = new PasswordHistory(CONFIG);
        assertTrue(isEnabled(history));
        assertEquals(10, getMaxSize(history).longValue());

        history.updatePasswordHistory(userTree, getTestUser().getID());
    }

    @Test
    public void testConfigurationChange() throws Exception {
        User user = getTestUser();
        Tree userTree = root.getTree(user.getPath());

        PasswordHistory history = new PasswordHistory(CONFIG);
        for (String pw : PASSWORDS) {
            assertTrue(history.updatePasswordHistory(userTree, pw));
        }
        assertEquals(10, getHistory(user).size());

        // change configuration to a smaller size
        history = new PasswordHistory(ConfigurationParameters.of(PARAM_PASSWORD_HISTORY_SIZE, 5));
        List<String> oldPwds = getHistory(user);
        assertEquals(10, oldPwds.size());

        // only the configured max-size number of entries in the history must be
        // checked. additional entries in the history must be ignored
        Iterables.skip(oldPwds, 6);
        history.updatePasswordHistory(userTree, oldPwds.iterator().next());

        // after chaning the pwd again however the rep:pwdHistory property must
        // only contain the max-size number of passwords
        assertEquals(5, getHistory(user).size());

        history = new PasswordHistory(CONFIG);
        history.updatePasswordHistory(userTree, "newPwd");
        assertEquals(6, getHistory(user).size());
    }

    @Test
    public void testEnableDisable() throws Exception {
        User user = getTestUser();
        Tree userTree = root.getTree(user.getPath());

        PasswordHistory history = new PasswordHistory(CONFIG);
        for (String pw : PASSWORDS) {
            assertTrue(history.updatePasswordHistory(userTree, pw));
        }
        assertEquals(10, getHistory(user).size());

        // disable the password history : changing the pw now must not
        // modify the rep:pwdHistory property.
        history = new PasswordHistory(ConfigurationParameters.EMPTY);
        history.updatePasswordHistory(userTree, PASSWORDS[8]);

        assertEquals(10, getHistory(user).size());
    }

    @Test
    public void testSingleTypeHistoryProperty() throws Exception {
        Tree userTree = root.getTree(getTestUser().getPath());
        Tree pwdNode = new NodeUtil(userTree).getOrAddChild(REP_PWD, NT_REP_PASSWORD).getTree();

        pwdNode.setProperty(REP_PWD_HISTORY, "singleValuedProperty");
        assertFalse(pwdNode.getProperty(REP_PWD_HISTORY).isArray());
        assertFalse(pwdNode.getProperty(REP_PWD_HISTORY).getType().isArray());

        PasswordHistory history = new PasswordHistory(CONFIG);
        assertTrue(history.updatePasswordHistory(userTree, "anyOtherPassword"));

        assertTrue(pwdNode.getProperty(REP_PWD_HISTORY).isArray());
        assertTrue(pwdNode.getProperty(REP_PWD_HISTORY).getType().isArray());
    }
}

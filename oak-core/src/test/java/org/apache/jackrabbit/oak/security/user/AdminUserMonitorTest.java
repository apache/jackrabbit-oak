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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AccountLockedException;
import javax.security.auth.login.AccountNotFoundException;
import javax.security.auth.login.CredentialExpiredException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.security.user.monitor.UserMonitor;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests for the monitoring of operations related to the admin user (OAK-12495).
 */
public class AdminUserMonitorTest extends AbstractSecurityTest {

    private static final String LOGIN_SUCCEEDED = "security.user.admin.login.succeeded";
    private static final String LOGIN_FAILED = "security.user.admin.login.failed";
    private static final String IMPERSONATION_SUCCEEDED = "security.user.admin.impersonation.succeeded";
    private static final String IMPERSONATION_FAILED = "security.user.admin.impersonation.failed";
    private static final String PASSWORD_ADDED = "security.user.admin.password.added";
    private static final String PASSWORD_CHANGED = "security.user.admin.password.changed";
    private static final String IMPERSONATORS_GRANTED = "security.user.admin.impersonators.granted";
    private static final String IMPERSONATORS_REVOKED = "security.user.admin.impersonators.revoked";

    private final Map<String, MeterStats> meters = new HashMap<>();

    private String adminId;
    private User admin;

    @Before
    public void before() throws Exception {
        super.before();

        StatisticsProvider statisticsProvider = mock(StatisticsProvider.class);
        when(statisticsProvider.getMeter(anyString(), any(StatsOptions.class))).thenAnswer(
                invocation -> meters.computeIfAbsent(invocation.getArgument(0), name -> mock(MeterStats.class)));
        when(statisticsProvider.getTimer(anyString(), any(StatsOptions.class))).thenReturn(mock(TimerStats.class));
        getUserConfigurationImpl().getMonitors(statisticsProvider);

        adminId = UserUtil.getAdminId(getUserConfiguration().getParameters());
        admin = getUserManager(root).getAuthorizable(adminId, User.class);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters userConfig = ConfigurationParameters.of(
                UserConstants.PARAM_PASSWORD_MAX_AGE, 10,
                UserAuthentication.PARAM_PASSWORD_EXPIRY_FOR_ADMIN, true);
        return ConfigurationParameters.of(UserConfiguration.NAME, userConfig);
    }

    @NotNull
    private UserConfigurationImpl getUserConfigurationImpl() {
        UserConfiguration uc = getUserConfiguration();
        assertTrue(uc instanceof UserConfigurationImpl);
        return (UserConfigurationImpl) uc;
    }

    @NotNull
    private MeterStats meter(@NotNull String name) {
        return meters.get(name);
    }

    private void assertNoAdminMetrics() {
        meters.values().forEach(m -> {
            verify(m, never()).mark();
            verify(m, never()).mark(anyLong());
        });
    }

    private boolean authenticate(@NotNull String loginId, @NotNull Credentials credentials) throws Exception {
        return new UserAuthentication(getUserConfiguration(), root, loginId).authenticate(credentials);
    }

    private boolean authenticate(@NotNull String loginId, @NotNull Credentials credentials, @NotNull UserMonitor monitor) throws Exception {
        UserConfigurationImpl uc = spy(getUserConfigurationImpl());
        doReturn(monitor).when(uc).getMonitor();
        return new UserAuthentication(uc, root, loginId).authenticate(credentials);
    }

    @NotNull
    private ImpersonationCredentials impersonationCredentials(@NotNull String targetId, @NotNull User impersonator) throws Exception {
        AuthInfoImpl info = new AuthInfoImpl(impersonator.getID(), null, Set.of(impersonator.getPrincipal()));
        return new ImpersonationCredentials(new SimpleCredentials(targetId, new char[0]), info);
    }

    private void expireAdminPassword() throws Exception {
        root.getTree(admin.getPath()).getChild(UserConstants.REP_PWD).setProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED, 0);
        root.commit();
    }

    //-----------------------------------------------------------< login >---

    @Test
    public void testAdminLoginSucceeded() throws Exception {
        assertTrue(authenticate(adminId, new SimpleCredentials(adminId, adminId.toCharArray())));

        verify(meter(LOGIN_SUCCEEDED), times(1)).mark();
        verify(meter(LOGIN_FAILED), never()).mark();
    }

    @Test
    public void testAdminLoginFailed() throws Exception {
        try {
            authenticate(adminId, new SimpleCredentials(adminId, "wrong".toCharArray()));
            fail("FailedLoginException expected");
        } catch (FailedLoginException e) {
            verify(meter(LOGIN_FAILED), times(1)).mark();
            verify(meter(LOGIN_SUCCEEDED), never()).mark();
        }
    }

    @Test
    public void testAdminLoginPasswordExpired() throws Exception {
        expireAdminPassword();
        try {
            authenticate(adminId, new SimpleCredentials(adminId, adminId.toCharArray()));
            fail("CredentialExpiredException expected");
        } catch (CredentialExpiredException e) {
            verify(meter(LOGIN_FAILED), times(1)).mark();
            verify(meter(LOGIN_SUCCEEDED), never()).mark();
            verify(meter(PASSWORD_CHANGED), never()).mark();
        }
    }

    @Test
    public void testAdminLoginPasswordExpiredWithChange() throws Exception {
        expireAdminPassword();
        SimpleCredentials sc = new SimpleCredentials(adminId, adminId.toCharArray());
        sc.setAttribute(UserConstants.CREDENTIALS_ATTRIBUTE_NEWPASSWORD, "newPassword");

        assertTrue(authenticate(adminId, sc));
        verify(meter(LOGIN_SUCCEEDED), times(1)).mark();
        verify(meter(PASSWORD_CHANGED), times(1)).mark();
    }

    @Test
    public void testNonAdminLogin() throws Exception {
        String userId = getTestUser().getID();
        assertTrue(authenticate(userId, new SimpleCredentials(userId, userId.toCharArray())));
        try {
            authenticate(userId, new SimpleCredentials(userId, "wrong".toCharArray()));
            fail("FailedLoginException expected");
        } catch (FailedLoginException e) {
            assertNoAdminMetrics();
        }
    }

    @Test
    public void testAdminLoginOtherUserConfiguration() throws Exception {
        UserConfiguration uc = mock(UserConfiguration.class, delegatesTo(getUserConfiguration()));
        UserAuthentication authentication = new UserAuthentication(uc, root, adminId);

        assertTrue(authentication.authenticate(new SimpleCredentials(adminId, adminId.toCharArray())));
        assertNoAdminMetrics();
    }

    @Test
    public void testLoginReportsIsAdmin() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        String userId = getTestUser().getID();

        assertTrue(authenticate(userId, new SimpleCredentials(userId, userId.toCharArray()), monitor));
        verify(monitor, times(1)).login(true, false);
        assertTrue(authenticate(adminId, new SimpleCredentials(adminId, adminId.toCharArray()), monitor));
        verify(monitor, times(1)).login(true, true);
    }

    @Test
    public void testFailedLoginReportsIsAdmin() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        String userId = getTestUser().getID();

        assertFailedLogin(userId, new SimpleCredentials(userId, "wrong".toCharArray()), monitor);
        verify(monitor, times(1)).login(false, false);
        assertFailedLogin(adminId, new SimpleCredentials(adminId, "wrong".toCharArray()), monitor);
        verify(monitor, times(1)).login(false, true);
    }

    @Test
    public void testImpersonationReportsIsAdmin() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        User testUser = getTestUser();

        assertTrue(authenticate(testUser.getID(), impersonationCredentials(testUser.getID(), admin), monitor));
        verify(monitor, times(1)).impersonated(true, false);
        assertTrue(authenticate(adminId, impersonationCredentials(adminId, admin), monitor));
        verify(monitor, times(1)).impersonated(true, true);
    }

    @Test
    public void testFailedImpersonationReportsIsAdmin() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        User testUser = getTestUser();
        User other = getUserManager(root).createUser("other", "pw");
        root.commit();

        assertFailedLogin(testUser.getID(), impersonationCredentials(testUser.getID(), other), monitor);
        verify(monitor, times(1)).impersonated(false, false);
        assertFailedLogin(adminId, impersonationCredentials(adminId, testUser), monitor);
        verify(monitor, times(1)).impersonated(false, true);
    }

    @Test
    public void testDisabledUserLoginReportsFailure() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        User testUser = getTestUser();
        testUser.disable("disabled");
        root.commit();

        assertAccountLocked(testUser.getID(), new SimpleCredentials(testUser.getID(), testUser.getID().toCharArray()), monitor);
        verify(monitor, times(1)).login(false, false);
        verify(monitor, never()).impersonated(anyBoolean(), anyBoolean());
    }

    @Test
    public void testDisabledUserImpersonationReportsFailure() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        User testUser = getTestUser();
        testUser.disable("disabled");
        root.commit();

        assertAccountLocked(testUser.getID(), impersonationCredentials(testUser.getID(), admin), monitor);
        verify(monitor, times(1)).impersonated(false, false);
        verify(monitor, never()).login(anyBoolean(), anyBoolean());
    }

    @Test
    public void testDisabledUserGuestOrPreAuthenticatedNotMonitored() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        User testUser = getTestUser();
        testUser.disable("disabled");
        root.commit();

        assertAccountLocked(testUser.getID(), new GuestCredentials(), monitor);
        assertAccountLocked(testUser.getID(), PreAuthenticatedLogin.PRE_AUTHENTICATED, monitor);
        verifyNoInteractions(monitor);
    }

    @Test
    public void testGuestLoginNotMonitored() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        String anonymousId = UserUtil.getAnonymousId(getUserConfiguration().getParameters());

        assertTrue(authenticate(anonymousId, new GuestCredentials(), monitor));
        verifyNoInteractions(monitor);
    }

    @Test
    public void testPreAuthenticatedLoginNotMonitored() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);

        assertTrue(authenticate(adminId, PreAuthenticatedLogin.PRE_AUTHENTICATED, monitor));
        verifyNoInteractions(monitor);
    }

    @Test
    public void testUnknownUserNotMonitored() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);

        assertFalse(authenticate("unknown", new SimpleCredentials("unknown", "pw".toCharArray()), monitor));
        assertFalse(authenticate("unknown", impersonationCredentials("unknown", admin), monitor));
        verifyNoInteractions(monitor);
    }

    @Test
    public void testGroupNotMonitored() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        String groupId = getUserManager(root).createGroup("group").getID();
        root.commit();

        for (Credentials creds : new Credentials[] {new SimpleCredentials(groupId, "pw".toCharArray()), impersonationCredentials(groupId, admin)}) {
            try {
                authenticate(groupId, creds, monitor);
                fail("AccountNotFoundException expected");
            } catch (AccountNotFoundException e) {
                // success
            }
        }
        verifyNoInteractions(monitor);
    }

    @Test
    public void testRepositoryExceptionAfterVerificationReportsFailure() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        User user = spy(admin);
        doThrow(new RepositoryException()).when(user).getPrincipal();
        UserManager userManager = mock(UserManager.class);
        when(userManager.getAuthorizable(adminId)).thenReturn(user);
        UserConfigurationImpl uc = spy(getUserConfigurationImpl());
        doReturn(monitor).when(uc).getMonitor();
        doReturn(userManager).when(uc).getUserManager(any(Root.class), any(NamePathMapper.class));

        try {
            new UserAuthentication(uc, root, adminId).authenticate(new SimpleCredentials(adminId, adminId.toCharArray()));
            fail("LoginException expected");
        } catch (LoginException e) {
            verify(monitor, times(1)).login(false, true);
            verify(monitor, never()).login(true, true);
        }
    }

    private void assertAccountLocked(@NotNull String loginId, @NotNull Credentials credentials, @NotNull UserMonitor monitor) throws Exception {
        try {
            authenticate(loginId, credentials, monitor);
            fail("AccountLockedException expected");
        } catch (AccountLockedException e) {
            // success
        }
    }

    private void assertFailedLogin(@NotNull String loginId, @NotNull Credentials credentials, @NotNull UserMonitor monitor) throws Exception {
        try {
            authenticate(loginId, credentials, monitor);
            fail("FailedLoginException expected");
        } catch (FailedLoginException e) {
            // success
        }
    }

    //---------------------------------------------------< impersonation >---

    @Test
    public void testAdminImpersonationFailed() throws Exception {
        try {
            authenticate(adminId, impersonationCredentials(adminId, getTestUser()));
            fail("FailedLoginException expected");
        } catch (FailedLoginException e) {
            verify(meter(IMPERSONATION_FAILED), times(1)).mark();
            verify(meter(IMPERSONATION_SUCCEEDED), never()).mark();
        }
    }

    @Test
    public void testAdminImpersonationSucceeded() throws Exception {
        User testUser = getTestUser();
        admin.getImpersonation().grantImpersonation(testUser.getPrincipal());
        root.commit();

        assertTrue(authenticate(adminId, impersonationCredentials(adminId, testUser)));
        verify(meter(IMPERSONATION_SUCCEEDED), times(1)).mark();
        verify(meter(IMPERSONATION_FAILED), never()).mark();
    }

    @Test
    public void testAdminSelfImpersonation() throws Exception {
        assertTrue(authenticate(adminId, impersonationCredentials(adminId, admin)));
        verify(meter(IMPERSONATION_SUCCEEDED), times(1)).mark();
        verify(meter(IMPERSONATION_FAILED), never()).mark();
    }

    @Test
    public void testImpersonationOfNonAdmin() throws Exception {
        User testUser = getTestUser();
        assertTrue(authenticate(testUser.getID(), impersonationCredentials(testUser.getID(), admin)));
        assertNoAdminMetrics();
    }

    //-------------------------------------------------< password change >---

    @Test
    public void testAdminPasswordChange() throws Exception {
        admin.changePassword("newPassword");
        root.commit();

        verify(meter(PASSWORD_CHANGED), times(1)).mark();
        verify(meter(PASSWORD_ADDED), never()).mark();
    }

    @Test
    public void testAdminPasswordChangeOakApi() throws Exception {
        root.getTree(admin.getPath()).setProperty(UserConstants.REP_PASSWORD, PasswordUtil.buildPasswordHash("newPassword"));
        root.commit();

        verify(meter(PASSWORD_CHANGED), times(1)).mark();
        verify(meter(PASSWORD_ADDED), never()).mark();
    }

    @Test
    public void testAdminPasswordChangeNotCommitted() throws Exception {
        admin.changePassword("newPassword");
        root.refresh();

        assertNoAdminMetrics();
    }

    @Test
    public void testAdminPasswordChangeRejected() throws Exception {
        root.getTree(admin.getPath()).setProperty(UserConstants.REP_PASSWORD, "plaintext");
        try {
            root.commit();
            fail("CommitFailedException expected");
        } catch (CommitFailedException e) {
            assertNoAdminMetrics();
        } finally {
            root.refresh();
        }
    }

    /**
     * Documents the accepted limitation that recording is best-effort: a change observed during commit validation is
     * recorded even if the commit subsequently fails.
     */
    @Test
    public void testAdminPasswordChangeCommitFailsAfterRecording() throws Exception {
        Tree adminTree = root.getTree(admin.getPath());
        String originalHash = adminTree.getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
        adminTree.setProperty(UserConstants.REP_PASSWORD, PasswordUtil.buildPasswordHash("newPassword"));

        String childId = "childUser";
        Tree child = adminTree.addChild(childId);
        child.setProperty(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_USER, Type.NAME);
        child.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, getUserConfiguration().getParameters()).getContentID(childId));
        child.setProperty(UserConstants.REP_AUTHORIZABLE_ID, childId);
        child.setProperty(UserConstants.REP_PRINCIPAL_NAME, childId);
        try {
            root.commit();
            fail("CommitFailedException expected");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(29, e.getCode());
            verify(meter(PASSWORD_CHANGED), times(1)).mark();
        } finally {
            root.refresh();
        }
        assertEquals(originalHash, root.getTree(admin.getPath()).getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING));
    }

    @Test
    public void testNonAdminPasswordChange() throws Exception {
        getTestUser().changePassword("newPassword");
        root.commit();

        assertNoAdminMetrics();
    }

    @Test
    public void testOtherAdminPropertyChange() throws Exception {
        root.getTree(admin.getPath()).setProperty("prop", "value");
        root.commit();

        assertNoAdminMetrics();
    }

    @Test
    public void testUserCreation() throws Exception {
        UserManager umgr = getUserManager(root);
        User u = umgr.createUser("newUser", "pw");
        u.getImpersonation().grantImpersonation(getTestUser().getPrincipal());
        root.commit();

        assertNoAdminMetrics();
    }

    //--------------------------------------------------< impersonators >---

    @Test
    public void testAdminImpersonatorsChanged() throws Exception {
        UserManager umgr = getUserManager(root);
        User u1 = getTestUser();
        User u2 = umgr.createUser("u2", null);
        User u3 = umgr.createUser("u3", null);
        root.commit();

        admin.getImpersonation().grantImpersonation(u1.getPrincipal());
        admin.getImpersonation().grantImpersonation(u2.getPrincipal());
        root.commit();
        verify(meter(IMPERSONATORS_GRANTED), times(1)).mark(2);
        verify(meter(IMPERSONATORS_REVOKED), times(1)).mark(0);

        admin.getImpersonation().revokeImpersonation(u1.getPrincipal());
        admin.getImpersonation().grantImpersonation(u3.getPrincipal());
        root.commit();
        verify(meter(IMPERSONATORS_GRANTED), times(1)).mark(1);
        verify(meter(IMPERSONATORS_REVOKED), times(1)).mark(1);

        admin.getImpersonation().revokeImpersonation(u2.getPrincipal());
        admin.getImpersonation().revokeImpersonation(u3.getPrincipal());
        root.commit();
        assertFalse(root.getTree(admin.getPath()).hasProperty(UserConstants.REP_IMPERSONATORS));
        verify(meter(IMPERSONATORS_GRANTED), times(1)).mark(0);
        verify(meter(IMPERSONATORS_REVOKED), times(1)).mark(2);

        verify(meter(PASSWORD_CHANGED), never()).mark();
    }

    @Test
    public void testAdminImpersonatorsOakApi() throws Exception {
        root.getTree(admin.getPath()).setProperty(UserConstants.REP_IMPERSONATORS, Set.of("p1", "p2", "p3"), Type.STRINGS);
        root.commit();

        verify(meter(IMPERSONATORS_GRANTED), times(1)).mark(3);
        verify(meter(IMPERSONATORS_REVOKED), times(1)).mark(0);
    }

    @Test
    public void testNonAdminImpersonatorsChanged() throws Exception {
        User testUser = getTestUser();
        testUser.getImpersonation().grantImpersonation(admin.getPrincipal());
        testUser.getImpersonation().grantImpersonation(getUserManager(root).createUser("u2", null).getPrincipal());
        root.commit();

        assertNoAdminMetrics();
    }

    //----------------------------------------------------------< validator >---

    @Test
    public void testValidatorIgnoresNewAdminNode() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        Tree adminTree = root.getTree(admin.getPath());

        UserValidator validator = new UserValidator(null, adminTree, createValidatorProvider(monitor));
        validator.propertyAdded(adminTree.getProperty(UserConstants.REP_PASSWORD));

        verifyNoInteractions(monitor);
    }

    @Test
    public void testValidatorPasswordAddedToExistingAdmin() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        Tree adminTree = root.getTree(admin.getPath());

        UserValidator validator = new UserValidator(adminTree, adminTree, createValidatorProvider(monitor));
        validator.propertyAdded(adminTree.getProperty(UserConstants.REP_PASSWORD));

        verify(monitor, times(1)).passwordChanged(true, true);
    }

    @Test
    public void testValidatorReportsNonAdminPasswordChanged() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        Tree userTree = root.getTree(getTestUser().getPath());
        PropertyState pw = userTree.getProperty(UserConstants.REP_PASSWORD);

        UserValidator validator = new UserValidator(userTree, userTree, createValidatorProvider(monitor));
        validator.propertyChanged(pw, pw);

        verify(monitor, times(1)).passwordChanged(false, false);
    }

    @Test
    public void testValidatorReportsNonAdminPasswordAdded() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        User u = getUserManager(root).createUser("u2", null);
        root.commit();
        Tree userTree = root.getTree(u.getPath());

        UserValidator validator = new UserValidator(userTree, userTree, createValidatorProvider(monitor));
        validator.propertyAdded(PropertyStates.createProperty(UserConstants.REP_PASSWORD, PasswordUtil.buildPasswordHash("pw")));

        verify(monitor, times(1)).passwordChanged(true, false);
    }

    @Test
    public void testValidatorReportsNonAdminImpersonatorsChanged() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        Tree userTree = root.getTree(getTestUser().getPath());
        PropertyState before = PropertyStates.createProperty(UserConstants.REP_IMPERSONATORS, Set.of("p1", "p2"), Type.STRINGS);
        PropertyState after = PropertyStates.createProperty(UserConstants.REP_IMPERSONATORS, Set.of("p2", "p3", "p4"), Type.STRINGS);

        UserValidator validator = new UserValidator(userTree, userTree, createValidatorProvider(monitor));
        validator.propertyAdded(before);
        validator.propertyChanged(before, after);
        validator.propertyDeleted(after);

        verify(monitor, times(1)).impersonatorsChanged(2, 0, false);
        verify(monitor, times(1)).impersonatorsChanged(2, 1, false);
        verify(monitor, times(1)).impersonatorsChanged(0, 3, false);
    }

    @Test
    public void testValidatorIgnoresFolderConvertedToUser() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        String usersPath = UserUtil.getAuthorizableRootPath(getUserConfiguration().getParameters(), AuthorizableType.USER);
        Tree folder = root.getTree(usersPath).addChild("folder");
        folder.setProperty(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_AUTHORIZABLE_FOLDER, Type.NAME);
        root.commit();

        Tree before = adminSession.getLatestRoot().getTree(folder.getPath());
        Tree after = root.getTree(folder.getPath());
        after.setProperty(JcrConstants.JCR_PRIMARYTYPE, UserConstants.NT_REP_USER, Type.NAME);

        UserValidator validator = new UserValidator(before, after, createValidatorProvider(monitor));
        validator.propertyAdded(PropertyStates.createProperty(UserConstants.REP_PASSWORD, PasswordUtil.buildPasswordHash("pw")));
        validator.propertyAdded(PropertyStates.createProperty(UserConstants.REP_IMPERSONATORS, Set.of("p1"), Type.STRINGS));

        verifyNoInteractions(monitor);
    }

    @Test
    public void testValidatorIgnoresGroup() throws Exception {
        UserMonitor monitor = mock(UserMonitor.class);
        Tree groupTree = root.getTree(getUserManager(root).createGroup("g").getPath());

        UserValidator validator = new UserValidator(groupTree, groupTree, createValidatorProvider(monitor));
        validator.propertyAdded(PropertyStates.createProperty(UserConstants.REP_IMPERSONATORS, Set.of("p1"), Type.STRINGS));

        verifyNoInteractions(monitor);
    }

    @NotNull
    private UserValidatorProvider createValidatorProvider(@NotNull UserMonitor monitor) {
        UserValidatorProvider provider = new UserValidatorProvider(getUserConfiguration().getParameters(), getRootProvider(), getTreeProvider(), monitor);
        Tree rootTree = root.getTree("/");
        provider.getRootValidator(getTreeProvider().asNodeState(rootTree), getTreeProvider().asNodeState(rootTree), CommitInfo.EMPTY);
        return provider;
    }
}

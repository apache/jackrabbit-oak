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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.ValueFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SyncMBeanImplTest extends AbstractJmxTest {

    private static final String SYNC_NAME = "testSyncName";

    private SyncMBeanImpl syncMBean;

    private SyncManager syncMgr;
    private ExternalIdentityProviderManager idpMgr;

    @Before
    public void before() throws Exception {
        super.before();

        syncMgr = new SyncManager() {
            @CheckForNull
            @Override
            public SyncHandler getSyncHandler(@Nonnull String name) {
                if (SYNC_NAME.equals(name)) {
                    return new DefaultSyncHandler(syncConfig);
                } else if (ThrowingSyncHandler.NAME.equals(name)) {
                    return new ThrowingSyncHandler(false);
                } else if (ThrowingSyncHandler.NAME_ALLOWS_IDENTITY_LISTING.equals(name)) {
                    return new ThrowingSyncHandler(true);
                } else {
                    return null;
                }
            }
        };
        idpMgr = new ExternalIdentityProviderManager() {
            @CheckForNull
            @Override
            public ExternalIdentityProvider getProvider(@Nonnull String name) {
                if (name.equals(idp.getName())) {
                    return idp;
                } else {
                    return null;
                }
            }
        };

        syncMBean = createSyncMBeanImpl(SYNC_NAME, idp.getName());
    }

    private Map<String, String> getExpectedUserResult(String expectedOp, boolean includeGroups) throws ExternalIdentityException {
        Map<String, String> expected = new HashMap<>();
        Iterator<ExternalUser> it = idp.listUsers();
        while (it.hasNext()) {
            ExternalUser eu = it.next();
            expected.put(eu.getId(), expectedOp);
            if (includeGroups) {
                for (ExternalIdentityRef ref : eu.getDeclaredGroups()) {
                    expected.put(ref.getId(), expectedOp);
                }
            }
        }
        return expected;
    }

    private SyncMBeanImpl createSyncMBeanImpl(@Nonnull String syncHandlerName, @Nonnull String idpName) {
        return new SyncMBeanImpl(getContentRepository(), getSecurityProvider(), syncMgr, syncHandlerName, idpMgr, idpName);
    }

    private SyncMBeanImpl createThrowingSyncMBean(boolean allowListIdentities) {
        String name = (allowListIdentities) ? ThrowingSyncHandler.NAME_ALLOWS_IDENTITY_LISTING : ThrowingSyncHandler.NAME;
        return new SyncMBeanImpl(getContentRepository(), getSecurityProvider(), syncMgr, name, idpMgr, idp.getName());
    }

    @Test
    public void testGetSyncHandlerName() {
        assertEquals(SYNC_NAME, syncMBean.getSyncHandlerName());
    }

    @Test
    public void testInvalidSyncHandlerName() {
        SyncMBeanImpl syncMBean = createSyncMBeanImpl("invalid", idp.getName());
        assertEquals("invalid", syncMBean.getSyncHandlerName());

        // calling any sync-operation must fail due to the invalid configuration
        try {
            syncMBean.syncAllExternalUsers();
            fail("syncAllExternalUsers with invalid SyncHandlerName must fail");
        } catch (IllegalArgumentException e) {
            //success
        }
    }

    @Test
    public void testGetIDPName() {
        assertEquals(idp.getName(), syncMBean.getIDPName());
    }

    @Test
    public void testInvalidIDPName() {
        SyncMBeanImpl syncMBean = createSyncMBeanImpl(SYNC_NAME, "invalid");
        assertEquals("invalid", syncMBean.getIDPName());

        // calling any sync-operation must fail due to the invalid configuration
        try {
            syncMBean.syncAllExternalUsers();
            fail("syncAllExternalUsers with invalid IDP name must fail");
        } catch (IllegalArgumentException e) {
            //success
        }
    }

    /**
     * test users have never been synced before => result must be NSA
     */
    @Test
    public void testSyncUsersBefore() {
        String[] userIds = new String[] {TestIdentityProvider.ID_TEST_USER, TestIdentityProvider.ID_SECOND_USER};

        String[] result = syncMBean.syncUsers(userIds, false);
        assertResultMessages(result, ImmutableMap.of(TestIdentityProvider.ID_TEST_USER, "nsa", TestIdentityProvider.ID_SECOND_USER, "nsa"));

        result = syncMBean.syncUsers(userIds, true);
        assertResultMessages(result, ImmutableMap.of(TestIdentityProvider.ID_TEST_USER, "nsa", TestIdentityProvider.ID_SECOND_USER, "nsa"));
    }

    @Test
    public void testSyncUsers() throws Exception {
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);

        String[] userIds = new String[]{TestIdentityProvider.ID_TEST_USER, TestIdentityProvider.ID_SECOND_USER};
        String[] result = syncMBean.syncUsers(userIds, false);
        assertResultMessages(result, ImmutableMap.of(TestIdentityProvider.ID_TEST_USER, "upd", TestIdentityProvider.ID_SECOND_USER, "nsa"));

        result = syncMBean.syncUsers(userIds, true);
        assertResultMessages(result, ImmutableMap.of(TestIdentityProvider.ID_TEST_USER, "upd", TestIdentityProvider.ID_SECOND_USER, "nsa"));
    }

    @Test
    public void testSyncUsersAlwaysForcesSync() throws Exception {
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);

        String[] userIds = new String[]{TestIdentityProvider.ID_TEST_USER, TestIdentityProvider.ID_SECOND_USER};
        syncConfig.user().setExpirationTime(Long.MAX_VALUE);

        String[]result = syncMBean.syncUsers(userIds, false);
        assertResultMessages(result, ImmutableMap.of(TestIdentityProvider.ID_TEST_USER, "upd", TestIdentityProvider.ID_SECOND_USER, "nsa"));
    }

    @Test
    public void testSyncGroups() throws Exception {
        sync(idp, "a", true);

        Map<String, String> expected = ImmutableMap.of("a", "upd");
        syncConfig.group().setExpirationTime(Long.MAX_VALUE);

        // force group sync is true by default => exp time is ignored
        String[] result = syncMBean.syncUsers(expected.keySet().toArray(new String[expected.size()]), false);
        assertResultMessages(result, expected);
    }

    @Test
    public void testSyncUsersPurge() throws Exception {
        sync(new TestIdentityProvider.TestUser("thirdUser", idp.getName()), idp);
        sync(new TestIdentityProvider.TestGroup("gr", idp.getName()), idp);

        UserManager userManager = getUserManager();
        Authorizable[] authorizables = new Authorizable[] {
                userManager.getAuthorizable("thirdUser"),
                userManager.getAuthorizable("gr")
        };

        for (Authorizable a : authorizables) {
            String[] ids = new String[]{a.getID()};
            String[] result = syncMBean.syncUsers(ids, false);
            assertResultMessages(result, a.getID(), "mis");
            assertNotNull(userManager.getAuthorizable(a.getID()));

            result = syncMBean.syncUsers(ids, true);
            assertResultMessages(result, a.getID(), "del");
            assertNull(getUserManager().getAuthorizable(a.getID()));
        }
    }

    @Test
    public void testSyncUsersNonExisting() {
        String[] result = syncMBean.syncUsers(new String[] {"nonExisting"}, false);
        assertResultMessages(result, "nonExisting", "nsa");
    }

    @Test
    public void testSyncUsersLocal() {
        String[] result = syncMBean.syncUsers(new String[] {UserConstants.DEFAULT_ANONYMOUS_ID}, false);
        assertResultMessages(result, UserConstants.DEFAULT_ANONYMOUS_ID, "for");
    }

    @Test
    public void testSyncUsersLocalPurge() throws Exception {
        String[] result = syncMBean.syncUsers(new String[] {UserConstants.DEFAULT_ANONYMOUS_ID}, true);
        assertResultMessages(result, UserConstants.DEFAULT_ANONYMOUS_ID, "for");

        assertNotNull(getUserManager().getAuthorizable(UserConstants.DEFAULT_ANONYMOUS_ID));
    }

    @Test
    public void testSyncUsersForeign() throws Exception {
        // sync user from foreign IDP into the repository
        SyncResult res = sync(foreignIDP, TestIdentityProvider.ID_TEST_USER, false);
        assertNotNull(getUserManager().getAuthorizable(TestIdentityProvider.ID_TEST_USER));
        assertEquals(foreignIDP.getUser(TestIdentityProvider.ID_TEST_USER).getExternalId(), res.getIdentity().getExternalIdRef());

        // syncUsers with testIDP must detect the foreign status
        String[] result = syncMBean.syncUsers(new String[]{TestIdentityProvider.ID_TEST_USER}, false);
        assertResultMessages(result, TestIdentityProvider.ID_TEST_USER, "for");
        assertNotNull(getUserManager().getAuthorizable(TestIdentityProvider.ID_TEST_USER));

        // same expected with 'purge' set to true
        result = syncMBean.syncUsers(new String[] {TestIdentityProvider.ID_TEST_USER}, true);
        assertResultMessages(result, TestIdentityProvider.ID_TEST_USER, "for");
        assertNotNull(getUserManager().getAuthorizable(TestIdentityProvider.ID_TEST_USER));
    }

    @Test
    public void testSyncGroupsForeign() throws Exception {
        // sync user from foreign IDP into the repository
        SyncResult res = sync(foreignIDP, "a", true);
        assertNotNull(getUserManager().getAuthorizable("a"));
        assertEquals(foreignIDP.getGroup("a").getExternalId(), res.getIdentity().getExternalIdRef());

        // syncUsers with testIDP must detect the foreign status
        String[] result = syncMBean.syncUsers(new String[]{"a"}, false);
        assertResultMessages(result, "a", "for");
        assertNotNull(getUserManager().getAuthorizable("a"));

        // same expected with 'purge' set to true
        result = syncMBean.syncUsers(new String[] {"a"}, true);
        assertResultMessages(result, "a", "for");
        assertNotNull(getUserManager().getAuthorizable("a"));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4360">OAK-4360</a>
     */
    @Test
    public void testSyncUserException() throws Exception {
        User u = getUserManager().createUser(TestIdentityProvider.ID_EXCEPTION, null);
        u.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, getValueFactory().createValue(new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName()).getString()));
        root.commit();

        String[] result = syncMBean.syncUsers(new String[]{TestIdentityProvider.ID_EXCEPTION}, false);
        assertResultMessages(result, TestIdentityProvider.ID_EXCEPTION, "ERR");
    }

    @Test
    public void testSyncUserThrowingHandler() throws Exception {
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);

        String[] result = createThrowingSyncMBean(false).syncUsers(new String[]{TestIdentityProvider.ID_TEST_USER}, false);
        assertResultMessages(result, TestIdentityProvider.ID_TEST_USER, "ERR");
    }

    @Test
    public void testInitialSyncExternalUsers() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_TEST_USER);
        String[] externalId = new String[] {externalUser.getExternalId().getString()};

        String[] result = syncMBean.syncExternalUsers(externalId);
        assertResultMessages(result, TestIdentityProvider.ID_TEST_USER, "add");

        UserManager userManager = getUserManager();
        User testUser = userManager.getAuthorizable(externalUser.getId(), User.class);
        assertNotNull(testUser);

        for (ExternalIdentityRef groupRef : externalUser.getDeclaredGroups()) {
            assertNotNull(userManager.getAuthorizable(groupRef.getId()));
        }
    }

    @Test
    public void testInitialSyncExternalUsersNoNesting() throws Exception {
        syncConfig.user().setMembershipNestingDepth(-1);

        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_TEST_USER);
        String[] externalId = new String[] {externalUser.getExternalId().getString()};

        String[] result = syncMBean.syncExternalUsers(externalId);
        assertResultMessages(result, TestIdentityProvider.ID_TEST_USER, "add");

        UserManager userManager = getUserManager();
        User testUser = userManager.getAuthorizable(externalUser.getId(), User.class);
        assertNotNull(testUser);

        for (ExternalIdentityRef groupRef : externalUser.getDeclaredGroups()) {
            assertNull(userManager.getAuthorizable(groupRef.getId()));
        }
    }

    @Test
    public void testSyncExternalUsersLastSyncedProperty() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_TEST_USER);
        String[] externalId = new String[]{externalUser.getExternalId().getString()};

        syncMBean.syncExternalUsers(externalId);
        UserManager userManager = getUserManager();
        User testUser = userManager.getAuthorizable(externalUser.getId(), User.class);

        long lastSynced = testUser.getProperty(DefaultSyncContext.REP_LAST_SYNCED)[0].getLong();
        for (ExternalIdentityRef groupRef : externalUser.getDeclaredGroups()) {
            Group gr = userManager.getAuthorizable(groupRef.getId(), Group.class);
            long groupLastSynced = gr.getProperty(DefaultSyncContext.REP_LAST_SYNCED)[0].getLong();

            assertTrue(lastSynced == groupLastSynced);
        }

        while (System.currentTimeMillis() <= lastSynced) {
            // wait for system time to move
        }

        // default value for forceGroup sync is defined to be 'true' => verify result
        syncMBean.syncExternalUsers(externalId);
        userManager = getUserManager();
        testUser = userManager.getAuthorizable(externalUser.getId(), User.class);
        long lastSynced2 = testUser.getProperty(DefaultSyncContext.REP_LAST_SYNCED)[0].getLong();

        assertTrue("lastSynced: " + lastSynced + ", lastSynced2: " + lastSynced2, lastSynced < lastSynced2);
        for (ExternalIdentityRef groupRef : externalUser.getDeclaredGroups()) {
            Group gr = userManager.getAuthorizable(groupRef.getId(), Group.class);
            long groupLastSynced = gr.getProperty(DefaultSyncContext.REP_LAST_SYNCED)[0].getLong();

            assertTrue(lastSynced2 == groupLastSynced);
        }
    }

    @Test
    public void testInitialSyncExternalGroup() throws Exception {
        ExternalGroup externalGroup = idp.getGroup("a");
        String[] externalId = new String[] {externalGroup.getExternalId().getString()};

        String[] result = syncMBean.syncExternalUsers(externalId);
        assertResultMessages(result, "a", "add");

        UserManager userManager = getUserManager();
        Group aGroup = userManager.getAuthorizable(externalGroup.getId(), Group.class);
        assertNotNull(aGroup);

        // membership of groups are not synced (unless imposed by user-sync with membership depth)
        for (ExternalIdentityRef groupRef : externalGroup.getDeclaredGroups()) {
            assertNull(userManager.getAuthorizable(groupRef.getId()));
        }
    }

    @Test
    public void testSyncExternalNonExisting() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef("nonExisting", idp.getName());

        String[] result = syncMBean.syncExternalUsers(new String[]{ref.getString()});
        assertResultMessages(result, "", "nsi");
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4346">OAK-4346</a>
     */
    @Test
    public void testSyncExternalLocal() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(UserConstants.DEFAULT_ANONYMOUS_ID, null);

        String[] result = syncMBean.syncExternalUsers(new String[]{ref.getString()});
        assertResultMessages(result, UserConstants.DEFAULT_ANONYMOUS_ID, "for");
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4346">OAK-4346</a>
     */
    @Test
    public void testSyncExternalForeign() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TestIdentityProvider.ID_TEST_USER, "anotherIDP");

        String[] result = syncMBean.syncExternalUsers(new String[]{ref.getString()});
        assertResultMessages(result, TestIdentityProvider.ID_TEST_USER, "for");

        result = syncMBean.syncExternalUsers(new String[] {ref.getString()});
        assertResultMessages(result, TestIdentityProvider.ID_TEST_USER, "for");
    }

    @Test
    public void testSyncExternalUserException() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName());
        String[] result = syncMBean.syncExternalUsers(new String[] {ref.getString()});
        assertResultMessages(result, TestIdentityProvider.ID_EXCEPTION, "ERR");
    }

    @Test
    public void testSyncExternalUserThrowingHandler() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TestIdentityProvider.ID_TEST_USER, idp.getName());
        String[] result = createThrowingSyncMBean(false).syncExternalUsers(new String[]{ref.getString()});
        assertResultMessages(result, TestIdentityProvider.ID_TEST_USER, "ERR");
    }

    /**
     * test users have never been synced before => result must be empty
     */
    @Test
    public void testSyncAllUsersBefore() throws Exception {
        String[] result = syncMBean.syncAllUsers(false);
        assertEquals(0, result.length);
    }

    @Test
    public void testSyncAllUsers() throws Exception {
        // first sync external users into the repo
        syncMBean.syncAllExternalUsers();

        // verify effect of syncAllUsers
        String[] result = syncMBean.syncAllUsers(false);

        Map<String, String> expected = getExpectedUserResult("upd", true);
        assertResultMessages(result, expected);

        UserManager userManager = getUserManager();
        for (String id : expected.keySet()) {
            ExternalIdentity ei = idp.getUser(id);
            if (ei == null) {
                ei = idp.getGroup(id);
            }
            assertSync(ei, userManager);
        }
    }

    @Test
    public void testSyncAllGroups() throws Exception {
        // first sync external users into the repo
        Map<String, String> expected = new HashMap();
        Iterator<ExternalGroup> grIt = idp.listGroups();
        while (grIt.hasNext()) {
            ExternalGroup eg = grIt.next();
            sync(idp, eg.getId(), true);
            expected.put(eg.getId(), "upd");
        }

        // verify effect of syncAllUsers (which in this case are groups)
        String[] result = syncMBean.syncAllUsers(false);
        assertResultMessages(result, expected);

        UserManager userManager = getUserManager();
        for (String id : expected.keySet()) {
            ExternalIdentity ei = idp.getGroup(id);
            assertSync(ei, userManager);
        }
    }

    @Test
    public void testSyncAllUsersPurgeFalse() throws Exception {
        // first sync external user|group into the repo that does't exist on the IDP (anymore)
        sync(new TestIdentityProvider.TestUser("thirdUser", idp.getName()), idp);
        sync(new TestIdentityProvider.TestGroup("g", idp.getName()), idp);

        // syncAll with purge = false
        String[] result = syncMBean.syncAllUsers(false);
        assertResultMessages(result, ImmutableMap.of("thirdUser", "mis", "g", "mis"));

        UserManager userManager = getUserManager();
        assertNotNull(userManager.getAuthorizable("thirdUser"));
        assertNotNull(userManager.getAuthorizable("g"));
    }

    @Test
    public void testSyncAllUsersPurgeTrue() throws Exception {
        // first sync external user|group into the repo that does't exist on the IDP (anymore)
        sync(new TestIdentityProvider.TestUser("thirdUser", idp.getName()), idp);
        sync(new TestIdentityProvider.TestGroup("g", idp.getName()), idp);

        // syncAll with purge = true
        String[] result = syncMBean.syncAllUsers(true);
        assertResultMessages(result, ImmutableMap.of("thirdUser", "del", "g", "del"));

        UserManager userManager = getUserManager();
        assertNull(userManager.getAuthorizable("thirdUser"));
        assertNull(userManager.getAuthorizable("g"));
    }

    @Test
    public void testSyncAllUsersForeign() throws Exception {
        // first sync external users + groups from 2 different IDPs into the repo
        // but set membership-nesting to 0
        syncConfig.user().setMembershipNestingDepth(0);
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);
        sync(idp, "a", true);
        sync(foreignIDP, TestIdentityProvider.ID_SECOND_USER, false);
        sync(foreignIDP, "aa", true);

        // verify effect of syncAllUsers : foreign user/group must be ignored by the sync.
        String[] result = syncMBean.syncAllUsers(false);
        Map<String, String> expectedResults = ImmutableMap.of(TestIdentityProvider.ID_TEST_USER, "upd", "a", "upd");
        assertResultMessages(result, expectedResults);

        ExternalIdentity[] expectedIds = new ExternalIdentity[] {
                idp.getUser(TestIdentityProvider.ID_TEST_USER),
                foreignIDP.getUser(TestIdentityProvider.ID_SECOND_USER),
                idp.getGroup("a"),
                foreignIDP.getGroup("aa")
        };
        UserManager userManager = getUserManager();
        for (ExternalIdentity externalIdentity : expectedIds) {
            assertSync(externalIdentity, userManager);
        }
    }

    @Test
    public void testSyncAllUsersException() throws Exception {
        User u = getUserManager().createUser(TestIdentityProvider.ID_EXCEPTION, null);
        u.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, getValueFactory().createValue(new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName()).getString()));
        root.commit();

        String[] result = syncMBean.syncAllUsers(false);
        assertResultMessages(result, TestIdentityProvider.ID_EXCEPTION, "ERR");

        result = syncMBean.syncAllUsers(true);
        assertResultMessages(result, TestIdentityProvider.ID_EXCEPTION, "ERR");
    }

    @Test(expected = IllegalStateException.class)
    public void testSyncAllUsersThrowingHandler() throws Exception {
        String[] result = createThrowingSyncMBean(false).syncAllUsers(false);
    }

    @Test
    public void testSyncAllUsersThrowingHandler2() throws Exception {
        syncMBean.syncAllExternalUsers();

        Map<String, String> expected = getExpectedUserResult("ERR", true);
        String[] result = createThrowingSyncMBean(true).syncAllUsers(false);

        assertResultMessages(result, expected);
    }

    @Test
    public void testInitialSyncAllExternalUsers() throws Exception {
        String[] result = syncMBean.syncAllExternalUsers();

        Map<String, String> expected = getExpectedUserResult("add", false);
        assertResultMessages(result, expected);

        UserManager userManager = getUserManager();
        for (String id : expected.keySet()) {
            ExternalIdentity ei = idp.getUser(id);
            if (ei == null) {
                ei = idp.getGroup(id);
            }
            assertSync(ei, userManager);
        }
    }

    @Test
    public void testSyncAllExternalUsersAgain() throws Exception {
        syncMBean.syncAllExternalUsers();

        // sync again
        String[] result = syncMBean.syncAllExternalUsers();

        // verify result
        Map<String, String> expected = getExpectedUserResult("upd", false);
        assertResultMessages(result, expected);

        UserManager userManager = getUserManager();
        for (String id : expected.keySet()) {
            ExternalIdentity ei = idp.getUser(id);
            if (ei == null) {
                ei = idp.getGroup(id);
            }
            assertSync(ei, userManager);
        }
    }

    @Test
    public void testSyncAllExternalUsersThrowingHandler() throws Exception {
        String[] result = createThrowingSyncMBean(false).syncAllExternalUsers();

        Map<String, String> expected = getExpectedUserResult("ERR", false);
        assertResultMessages(result, expected);
    }

    @Test
    public void testListOrphanedUsers() throws Exception {
        syncMBean.syncAllExternalUsers();

        String[] result = syncMBean.listOrphanedUsers();
        assertEquals(0, result.length);

        sync(new TestIdentityProvider.TestUser("thirdUser", idp.getName()), idp);
        sync(new TestIdentityProvider.TestGroup("g", idp.getName()), idp);

        result = syncMBean.listOrphanedUsers();
        assertEquals(2, result.length);
        assertEquals(ImmutableSet.of("thirdUser", "g"), ImmutableSet.copyOf(result));
    }

    @Test
    public void testListOrphanedUsersForeign() throws Exception {
        sync(foreignIDP, "a", true);
        sync(foreignIDP, TestIdentityProvider.ID_TEST_USER, false);

        String[] result = syncMBean.listOrphanedUsers();
        assertEquals(0, result.length);
    }

    @Test
    public void testListOrphanedUsersException () throws Exception {
        User u = getUserManager().createUser(TestIdentityProvider.ID_EXCEPTION, null);
        u.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, getValueFactory().createValue(new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName()).getString()));
        root.commit();

        String[] result = syncMBean.listOrphanedUsers();
        assertEquals(0, result.length);
    }

    @Test
    public void testListOrphanedUsersThrowingHandler() throws Exception {
        sync(new TestIdentityProvider.TestUser("thirdUser", idp.getName()), idp);
        sync(new TestIdentityProvider.TestGroup("g", idp.getName()), idp);

        String[] result = createThrowingSyncMBean(false).listOrphanedUsers();
        assertEquals(0, result.length);

        result = createThrowingSyncMBean(true).listOrphanedUsers();
        assertEquals(2, result.length);
        assertEquals(ImmutableSet.of("thirdUser", "g"), ImmutableSet.copyOf(result));
    }

    @Test
    public void testPurgeOrphanedUsersNoPurge() {
        syncMBean.syncAllExternalUsers();

        String[] result = syncMBean.purgeOrphanedUsers();
        assertEquals(0, result.length);
    }

    @Test
    public void testPurgeOrphanedUsers() throws Exception {
        syncMBean.syncAllExternalUsers();

        sync(new TestIdentityProvider.TestUser("thirdUser", idp.getName()), idp);
        sync(new TestIdentityProvider.TestGroup("g", idp.getName()), idp);

        String[] result = syncMBean.purgeOrphanedUsers();
        assertResultMessages(result, ImmutableMap.of("thirdUser", "del", "g", "del"));

        UserManager userManager = getUserManager();
        assertNull(userManager.getAuthorizable("thirdUser"));
        assertNull(userManager.getAuthorizable("g"));
    }

    @Test
    public void testPurgeOrphanedUsersForeign() throws Exception {
        sync(foreignIDP, "a", true);
        sync(foreignIDP, TestIdentityProvider.ID_TEST_USER, false);

        String[] result = syncMBean.purgeOrphanedUsers();
        assertEquals(0, result.length);
    }

    @Test
    public void testPurgeOrphanedUsersException() throws Exception {
        User u = getUserManager().createUser(TestIdentityProvider.ID_EXCEPTION, null);
        u.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, getValueFactory().createValue(new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName()).getString()));
        root.commit();

        String[] result = syncMBean.purgeOrphanedUsers();
        assertEquals(0, result.length);
    }

    @Test
    public void testPurgeOrphanedUsersThrowingHandler() throws Exception {
        sync(new TestIdentityProvider.TestUser("thirdUser", idp.getName()), idp);
        sync(new TestIdentityProvider.TestGroup("g", idp.getName()), idp);

        String[] result = createThrowingSyncMBean(false).purgeOrphanedUsers();
        assertEquals(0, result.length);

        UserManager userManager = getUserManager();
        assertNotNull(userManager.getAuthorizable("thirdUser"));
        assertNotNull(userManager.getAuthorizable("g"));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4362">OAK-4362</a>
     */
    @Test
    public void testPurgeOrphanedUsersThrowingHandler2() throws Exception {
        sync(new TestIdentityProvider.TestUser("thirdUser", idp.getName()), idp);
        sync(new TestIdentityProvider.TestGroup("g", idp.getName()), idp);

        String[] result = createThrowingSyncMBean(true).purgeOrphanedUsers();
        assertResultMessages(result, ImmutableMap.of("thirdUser", "ERR", "g", "ERR"));

        UserManager userManager = getUserManager();
        assertNotNull(userManager.getAuthorizable("thirdUser"));
        assertNotNull(userManager.getAuthorizable("g"));
    }

    /**
     * SyncHandler implementation that throws Exceptions.
     */
    private final class ThrowingSyncHandler extends DefaultSyncHandler {

        private static final String NAME = "throwing";
        private static final String NAME_ALLOWS_IDENTITY_LISTING = "throwingExceptListIdentities";

        boolean allowsListIdentities = false;

        private ThrowingSyncHandler(boolean allowsListIdentities) {
            super(syncConfig);
            this.allowsListIdentities = allowsListIdentities;
        };

        @Nonnull
        @Override
        public String getName() {
            return allowsListIdentities ? NAME_ALLOWS_IDENTITY_LISTING : NAME;
        }

        @Nonnull
        @Override
        public SyncContext createContext(@Nonnull ExternalIdentityProvider idp, @Nonnull UserManager userManager, @Nonnull ValueFactory valueFactory) throws SyncException {
            return new DefaultSyncContext(syncConfig, idp, userManager, valueFactory) {
                @Nonnull
                @Override
                public SyncResult sync(@Nonnull ExternalIdentity identity) throws SyncException {
                    throw new SyncException("sync " + identity);
                }

                @Nonnull
                @Override
                public SyncResult sync(@Nonnull String id) throws SyncException {
                    throw new SyncException("sync " + id);
                }
            };
        }

        @CheckForNull
        @Override
        public SyncedIdentity findIdentity(@Nonnull UserManager userManager, @Nonnull String id) throws RepositoryException {
            throw new RepositoryException("findIdentity");
        }

        @Override
        public boolean requiresSync(@Nonnull SyncedIdentity identity) {
            return false;
        }

        @Nonnull
        @Override
        public Iterator<SyncedIdentity> listIdentities(@Nonnull UserManager userManager) throws RepositoryException {
            if (!allowsListIdentities) {
                throw new RepositoryException("listIdentities");
            } else {
                return super.listIdentities(userManager);
            }
        }
    }
}
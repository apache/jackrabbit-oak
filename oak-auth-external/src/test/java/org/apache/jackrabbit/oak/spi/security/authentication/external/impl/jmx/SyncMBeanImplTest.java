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
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.ValueFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.Jcr;
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
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SyncMBeanImplTest {

    private static final String SYNC_NAME = "testSyncName";

    private static Repository REPOSITORY;

    private ExternalIdentityProvider idp;
    private ExternalIdentityProvider foreignIDP;
    private DefaultSyncConfig syncConfig;
    private SyncMBeanImpl syncMBean;

    private SyncManager syncMgr;
    private ExternalIdentityProviderManager idpMgr;

    private Session session;
    private UserManager userManager;
    private Set<String> ids;

    @BeforeClass
    public static void beforeClass() {
        REPOSITORY = new Jcr().createRepository();
    }

    @Before
    public void before() throws Exception {
        idp = new TestIdentityProvider();
        foreignIDP = new TestIdentityProvider() {
            @Nonnull
            public String getName() {
                return "anotherIDP";
            }

        };
        syncConfig = new DefaultSyncConfig();
        syncConfig.user().setMembershipNestingDepth(1);

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
        syncMBean = new SyncMBeanImpl(REPOSITORY, syncMgr, SYNC_NAME, idpMgr, idp.getName());

        session = REPOSITORY.login(new SimpleCredentials("admin", "admin".toCharArray()));
        if (!(session instanceof JackrabbitSession)) {
            throw new IllegalStateException();
        } else {
            userManager = ((JackrabbitSession) session).getUserManager();
        }
        ids = Sets.newHashSet(getAllAuthorizableIds(userManager));
    }

    @After
    public void after() throws Exception {
        try {
            session.refresh(false);
            Iterator<String> iter = getAllAuthorizableIds(userManager);
            while (iter.hasNext()) {
                String id = iter.next();
                if (!ids.remove(id)) {
                    Authorizable a = userManager.getAuthorizable(id);
                    if (a != null) {
                        a.remove();
                    }
                }
            }
            session.save();
        } finally {
            session.logout();
        }
    }

    private static Iterator<String> getAllAuthorizableIds(@Nonnull UserManager userManager) throws Exception {
        Iterator<Authorizable> iter = userManager.findAuthorizables("jcr:primaryType", null);
        return Iterators.filter(Iterators.transform(iter, new Function<Authorizable, String>() {
            @Nullable
            @Override
            public String apply(Authorizable input) {
                try {
                    if (input != null) {
                        return input.getID();
                    }
                } catch (RepositoryException e) {
                    // failed to retrieve ID
                }
                return null;
            }
        }), Predicates.notNull());
    }

    private static void assertResultMessages(@Nonnull String[] resultMessages, int expectedSize, @Nonnull String... expectedOperations) {
        assertEquals(expectedSize, resultMessages.length);
        for (int i = 0; i < resultMessages.length; i++) {
            String rm = resultMessages[i];
            String op = rm.substring(rm.indexOf(":") + 2, rm.indexOf("\","));
            assertEquals(expectedOperations[i], op);
        }
    }

    private static void assertSync(@Nonnull ExternalIdentity ei, @Nonnull UserManager userManager) throws Exception {
        Authorizable authorizable;
        if (ei instanceof ExternalUser) {
            authorizable = userManager.getAuthorizable(ei.getId(), User.class);
        } else {
            authorizable = userManager.getAuthorizable(ei.getId(), Group.class);
        }
        assertNotNull(ei.getId(), authorizable);
        assertEquals(ei.getId(), authorizable.getID());
        assertEquals(ei.getExternalId(), ExternalIdentityRef.fromString(authorizable.getProperty(DefaultSyncContext.REP_EXTERNAL_ID)[0].getString()));
    }

    private SyncResult sync(@Nonnull ExternalIdentityProvider idp, @Nonnull String id, boolean isGroup) throws Exception {
        return sync((isGroup) ? idp.getGroup(id) : idp.getUser(id), idp);
    }

    private SyncResult sync(@Nonnull ExternalIdentity externalIdentity, @Nonnull ExternalIdentityProvider idp) throws Exception {
        SyncContext ctx = new DefaultSyncContext(syncConfig, idp, userManager, session.getValueFactory());
        SyncResult res = ctx.sync(externalIdentity);
        session.save();
        return res;
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

    private SyncMBeanImpl createThrowingSyncMBean(boolean allowListIdentities) {
        String name = (allowListIdentities) ? ThrowingSyncHandler.NAME_ALLOWS_IDENTITY_LISTING : ThrowingSyncHandler.NAME;
        return new SyncMBeanImpl(REPOSITORY, syncMgr, name, idpMgr, idp.getName());
    }

    @Test
    public void testGetSyncHandlerName() {
        assertEquals(SYNC_NAME, syncMBean.getSyncHandlerName());
    }

    @Test
    public void testInvalidSyncHandlerName() {
        SyncMBeanImpl syncMBean = new SyncMBeanImpl(REPOSITORY, syncMgr, "invalid", idpMgr, idp.getName());
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
        SyncMBeanImpl syncMBean = new SyncMBeanImpl(REPOSITORY, syncMgr, SYNC_NAME, idpMgr, "invalid");
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
        assertResultMessages(result, userIds.length, "nsa", "nsa");

        result = syncMBean.syncUsers(userIds, true);
        assertResultMessages(result, userIds.length, "nsa", "nsa");
    }

    @Test
    public void testSyncUsers() throws Exception {
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);

        String[] userIds = new String[]{TestIdentityProvider.ID_TEST_USER, TestIdentityProvider.ID_SECOND_USER};
        String[] result = syncMBean.syncUsers(userIds, false);
        assertResultMessages(result, userIds.length, "upd", "nsa");

        result = syncMBean.syncUsers(userIds, true);
        assertResultMessages(result, userIds.length, "upd", "nsa");
    }

    @Test
    public void testSyncUsersAlwaysForcesSync() throws Exception {
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);

        String[] userIds = new String[]{TestIdentityProvider.ID_TEST_USER, TestIdentityProvider.ID_SECOND_USER};
        syncConfig.user().setExpirationTime(Long.MAX_VALUE);

        String[]result = syncMBean.syncUsers(userIds, false);
        assertResultMessages(result, userIds.length, "upd", "nsa");
    }

    @Test
    public void testSyncGroups() throws Exception {
        sync(idp, "a", true);

        String[] ids = new String[]{"a"};
        syncConfig.group().setExpirationTime(Long.MAX_VALUE);

        // force group sync is true by default => exp time is ignored
        String[] result = syncMBean.syncUsers(ids, false);
        assertResultMessages(result, ids.length, "upd");
    }

    @Test
    public void testSyncUsersPurge() throws Exception {
        sync(new TestIdentityProvider.TestUser("thirdUser", idp.getName()), idp);
        sync(new TestIdentityProvider.TestGroup("gr", idp.getName()), idp);

        Authorizable[] authorizables = new Authorizable[] {
                userManager.getAuthorizable("thirdUser"),
                userManager.getAuthorizable("gr")
        };

        for (Authorizable a : authorizables) {
            String[] ids = new String[]{a.getID()};
            String[] result = syncMBean.syncUsers(ids, false);
            assertResultMessages(result, ids.length, "mis");
            assertNotNull(userManager.getAuthorizable(a.getID()));

            result = syncMBean.syncUsers(ids, true);
            assertResultMessages(result, ids.length, "del");
            assertNull(userManager.getAuthorizable(a.getID()));
        }
    }

    @Test
    public void testSyncUsersNonExisting() {
        String[] result = syncMBean.syncUsers(new String[] {"nonExisting"}, false);
        assertResultMessages(result, 1, "nsa");
    }

    @Test
    public void testSyncUsersLocal() {
        String[] result = syncMBean.syncUsers(new String[] {UserConstants.DEFAULT_ANONYMOUS_ID}, false);
        assertResultMessages(result, 1, "for");
    }

    @Test
    public void testSyncUsersLocalPurge() throws Exception {
        String[] result = syncMBean.syncUsers(new String[] {UserConstants.DEFAULT_ANONYMOUS_ID}, true);
        assertResultMessages(result, 1, "for");

        assertNotNull(userManager.getAuthorizable(UserConstants.DEFAULT_ANONYMOUS_ID));
    }

    @Test
    public void testSyncUsersForeign() throws Exception {
        // sync user from foreign IDP into the repository
        SyncResult res = sync(foreignIDP, TestIdentityProvider.ID_TEST_USER, false);
        assertNotNull(userManager.getAuthorizable(TestIdentityProvider.ID_TEST_USER));
        assertEquals(foreignIDP.getUser(TestIdentityProvider.ID_TEST_USER).getExternalId(), res.getIdentity().getExternalIdRef());

        // syncUsers with testIDP must detect the foreign status
        String[] result = syncMBean.syncUsers(new String[]{TestIdentityProvider.ID_TEST_USER}, false);
        assertResultMessages(result, 1, "for");
        assertNotNull(userManager.getAuthorizable(TestIdentityProvider.ID_TEST_USER));

        // same expected with 'purge' set to true
        result = syncMBean.syncUsers(new String[] {TestIdentityProvider.ID_TEST_USER}, true);
        assertResultMessages(result, 1, "for");
        assertNotNull(userManager.getAuthorizable(TestIdentityProvider.ID_TEST_USER));
    }

    @Test
    public void testSyncGroupsForeign() throws Exception {
        // sync user from foreign IDP into the repository
        SyncResult res = sync(foreignIDP, "a", true);
        assertNotNull(userManager.getAuthorizable("a"));
        assertEquals(foreignIDP.getGroup("a").getExternalId(), res.getIdentity().getExternalIdRef());

        // syncUsers with testIDP must detect the foreign status
        String[] result = syncMBean.syncUsers(new String[]{"a"}, false);
        assertResultMessages(result, 1, "for");
        assertNotNull(userManager.getAuthorizable("a"));

        // same expected with 'purge' set to true
        result = syncMBean.syncUsers(new String[] {"a"}, true);
        assertResultMessages(result, 1, "for");
        assertNotNull(userManager.getAuthorizable("a"));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4360">OAK-4360</a>
     */
    @Test
    public void testSyncUserException() throws Exception {
        User u = userManager.createUser(TestIdentityProvider.ID_EXCEPTION, null);
        u.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, session.getValueFactory().createValue(new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName()).getString()));
        session.save();

        String[] result = syncMBean.syncUsers(new String[]{TestIdentityProvider.ID_EXCEPTION}, false);
        assertResultMessages(result, 1, "ERR");
    }

    @Test
    public void testSyncUserThrowingHandler() throws Exception {
        sync(idp, TestIdentityProvider.ID_TEST_USER, false);

        String[] result = createThrowingSyncMBean(false).syncUsers(new String[]{TestIdentityProvider.ID_TEST_USER}, false);
        assertResultMessages(result, 1, "ERR");
    }

    @Test
    public void testInitialSyncExternalUsers() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_TEST_USER);
        String[] externalId = new String[] {externalUser.getExternalId().getString()};

        String[] result = syncMBean.syncExternalUsers(externalId);
        assertResultMessages(result, 1, "add");

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
        assertResultMessages(result, 1, "add");

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
        User testUser = userManager.getAuthorizable(externalUser.getId(), User.class);

        long lastSynced = testUser.getProperty(DefaultSyncContext.REP_LAST_SYNCED)[0].getLong();
        for (ExternalIdentityRef groupRef : externalUser.getDeclaredGroups()) {
            Group gr = userManager.getAuthorizable(groupRef.getId(), Group.class);
            long groupLastSynced = gr.getProperty(DefaultSyncContext.REP_LAST_SYNCED)[0].getLong();

            assertTrue(lastSynced == groupLastSynced);
        }

        // default value for forceGroup sync is defined to be 'true' => verify result
        syncMBean.syncExternalUsers(externalId);
        testUser = userManager.getAuthorizable(externalUser.getId(), User.class);
        long lastSynced2 = testUser.getProperty(DefaultSyncContext.REP_LAST_SYNCED)[0].getLong();

        assertTrue(lastSynced < lastSynced2);
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
        assertResultMessages(result, 1, "add");

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
        assertResultMessages(result, 1, "nsi");
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4346">OAK-4346</a>
     */
    @Test
    public void testSyncExternalLocal() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(UserConstants.DEFAULT_ANONYMOUS_ID, null);

        String[] result = syncMBean.syncExternalUsers(new String[]{ref.getString()});
        assertResultMessages(result, 1, "for");
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4346">OAK-4346</a>
     */
    @Test
    public void testSyncExternalForeign() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TestIdentityProvider.ID_TEST_USER, "anotherIDP");

        String[] result = syncMBean.syncExternalUsers(new String[]{ref.getString()});
        assertResultMessages(result, 1, "for");

        result = syncMBean.syncExternalUsers(new String[] {ref.getString()});
        assertResultMessages(result, 1, "for");
    }

    @Test
    public void testSyncExternalUserException() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName());
        String[] result = syncMBean.syncExternalUsers(new String[] {ref.getString()});
        assertResultMessages(result, 1, "ERR");
    }

    @Test
    public void testSyncExternalUserThrowingHandler() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TestIdentityProvider.ID_TEST_USER, idp.getName());
        String[] result = createThrowingSyncMBean(false).syncExternalUsers(new String[]{ref.getString()});
        assertResultMessages(result, 1, "ERR");
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
        assertResultMessages(result, expected.size(), expected.values().toArray(new String[expected.size()]));
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
        assertResultMessages(result, expected.size(), expected.values().toArray(new String[expected.size()]));
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
        assertResultMessages(result, 2, "mis", "mis");

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
        assertResultMessages(result, 2, "del", "del");

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
        String[] expectedResults = new String[] {"upd", "upd"};
        assertResultMessages(result, expectedResults.length, expectedResults);

        ExternalIdentity[] expectedIds = new ExternalIdentity[] {
                idp.getUser(TestIdentityProvider.ID_TEST_USER),
                foreignIDP.getUser(TestIdentityProvider.ID_SECOND_USER),
                idp.getGroup("a"),
                foreignIDP.getGroup("aa")
        };
        for (ExternalIdentity externalIdentity : expectedIds) {
            assertSync(externalIdentity, userManager);
        }
    }

    @Test
    public void testSyncAllUsersException() throws Exception {
        User u = userManager.createUser(TestIdentityProvider.ID_EXCEPTION, null);
        u.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, session.getValueFactory().createValue(new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName()).getString()));
        session.save();

        String[] result = syncMBean.syncAllUsers(false);
        assertResultMessages(result, 1, "ERR");

        result = syncMBean.syncAllUsers(true);
        assertResultMessages(result, 1, "ERR");
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

        assertResultMessages(result, expected.size(), expected.values().toArray(new String[expected.size()]));
    }

    @Test
    public void testInitialSyncAllExternalUsers() throws Exception {
        String[] result = syncMBean.syncAllExternalUsers();

        Map<String, String> expected = getExpectedUserResult("add", false);
        assertResultMessages(result, expected.size(), expected.values().toArray(new String[expected.size()]));
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
        assertResultMessages(result, expected.size(), expected.values().toArray(new String[expected.size()]));
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
        assertResultMessages(result, expected.size(), expected.values().toArray(new String[expected.size()]));
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
        User u = userManager.createUser(TestIdentityProvider.ID_EXCEPTION, null);
        u.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, session.getValueFactory().createValue(new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName()).getString()));
        session.save();

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
        assertResultMessages(result, 2, "del", "del");

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
        User u = userManager.createUser(TestIdentityProvider.ID_EXCEPTION, null);
        u.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, session.getValueFactory().createValue(new ExternalIdentityRef(TestIdentityProvider.ID_EXCEPTION, idp.getName()).getString()));
        session.save();

        String[] result = syncMBean.purgeOrphanedUsers();
        assertEquals(0, result.length);
    }

    @Test
    public void testPurgeOrphanedUsersThrowingHandler() throws Exception {
        sync(new TestIdentityProvider.TestUser("thirdUser", idp.getName()), idp);
        sync(new TestIdentityProvider.TestGroup("g", idp.getName()), idp);

        String[] result = createThrowingSyncMBean(false).purgeOrphanedUsers();
        assertEquals(0, result.length);
        assertNotNull(userManager.getAuthorizable("thirdUser"));
        assertNotNull(userManager.getAuthorizable("g"));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4362">OAK-4362</a>
     */
    @Ignore("OAK-4362")
    @Test
    public void testPurgeOrphanedUsersThrowingHandler2() throws Exception {
        sync(new TestIdentityProvider.TestUser("thirdUser", idp.getName()), idp);
        sync(new TestIdentityProvider.TestGroup("g", idp.getName()), idp);

        String[] result = createThrowingSyncMBean(true).purgeOrphanedUsers();
        assertResultMessages(result, 2, "ERR", "ERR");
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
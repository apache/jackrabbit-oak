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

import java.util.Iterator;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
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
    }

    @After
    public void after() throws Exception {
        try {
            session.refresh(false);
            Iterator<ExternalIdentity> extIdentities = Iterators.concat(idp.listGroups(), idp.listUsers());
            while (extIdentities.hasNext()) {
                Authorizable a = userManager.getAuthorizable(extIdentities.next().getId());
                if (a != null) {
                    a.remove();
                }
            }
            session.save();
        } finally {
            session.logout();
        }
    }

    private static void assertResultMessages(@Nonnull String[] resultMessages, int expectedSize, @Nonnull String... expectedOperations) {
        assertEquals(expectedSize, resultMessages.length);
        for (int i = 0; i < resultMessages.length; i++) {
            String rm = resultMessages[i];
            String op = rm.substring(rm.indexOf(":") + 2, rm.indexOf("\","));
            assertEquals(expectedOperations[i], op);
        }
    }

    private SyncResult sync(@Nonnull ExternalIdentityProvider idp, @Nonnull String id, boolean isGroup) throws Exception {
        SyncContext ctx = new DefaultSyncContext(syncConfig, idp, userManager, session.getValueFactory());
        SyncResult res = ctx.sync((isGroup) ? idp.getGroup(id) : idp.getUser(id));
        session.save();
        return res;
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
        User u = userManager.createUser("thirdUser", null);
        u.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, session.getValueFactory().createValue(new ExternalIdentityRef(u.getID(), idp.getName()).getString()));
        session.save();

        String[] ids = new String[]{u.getID()};
        String[] result = syncMBean.syncUsers(ids, false);
        assertResultMessages(result, ids.length, "mis");
        assertNotNull(userManager.getAuthorizable(u.getID()));

        result = syncMBean.syncUsers(ids, true);
        assertResultMessages(result, ids.length, "del");
        assertNull(userManager.getAuthorizable(u.getID()));
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
    @Ignore("OAK-4346")
    @Test
    public void testSyncExternalLocal() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(UserConstants.DEFAULT_ANONYMOUS_ID, null);

        String[] result = syncMBean.syncExternalUsers(new String[]{ref.getString()});
        assertResultMessages(result, 1, "for");
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4346">OAK-4346</a>
     */
    @Ignore("OAK-4346")
    @Test
    public void testSyncExternalForeign() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TestIdentityProvider.ID_TEST_USER, "anotherIDP");

        String[] result = syncMBean.syncExternalUsers(new String[]{ref.getString()});
        assertResultMessages(result, 1, "for");

        result = syncMBean.syncExternalUsers(new String[] {ref.getString()});
        assertResultMessages(result, 1, "for");
    }

    @Test
    public void testSyncAllUsers() {
        // TODO
    }

    @Test
    public void testSyncAllExternalUsers() {
        // TODO
    }

    @Test
    public void testListOrphanedUsers() {
        // TODO
    }

    @Test
    public void testPurgeOrphanedUsers() {
        // TODO
    }
}
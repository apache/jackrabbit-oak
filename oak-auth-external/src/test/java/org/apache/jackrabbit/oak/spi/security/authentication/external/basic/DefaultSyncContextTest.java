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
package org.apache.jackrabbit.oak.spi.security.authentication.external.basic;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DefaultSyncContextTest extends AbstractExternalAuthTest {

    private UserManager userManager;
    private ValueFactory valueFactory;
    private DefaultSyncContext syncCtx;

    @Before
    public void before() throws Exception {
        super.before();
        userManager = getUserManager(root);
        valueFactory = getValueFactory();
        syncCtx = new DefaultSyncContext(syncConfig, idp, userManager, valueFactory);
    }

    @After
    public void after() throws Exception {
        try {
            syncCtx.close();
            root.refresh();
        } finally {
            super.after();
        }
    }

    @Override
    protected DefaultSyncConfig createSyncConfig() {
        return new DefaultSyncConfig();
    }

    private Group createTestGroup() throws Exception {
        return userManager.createGroup("group" + UUID.randomUUID());
    }

    /**
     * Test utility method to synchronize the given identity into the repository.
     * This is intended to simplify those tests that require a given user/group
     * to be synchronized before executing the test.
     *
     * @param externalIdentity The external identity to be synchronized.
     * @throws Exception
     */
    private void sync(@Nonnull ExternalIdentity externalIdentity) throws Exception {
        SyncResult result = syncCtx.sync(externalIdentity);
        assertSame(SyncResult.Status.ADD, result.getStatus());
        root.commit();
    }

    private void setExternalID(@Nonnull Authorizable authorizable, @Nullable String idpName) throws RepositoryException {
        authorizable.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, valueFactory.createValue(authorizable.getID() + ';' + idpName));
    }

    @Test
    public void testCreateSyncedIdentityNull() throws Exception {
        assertNull(DefaultSyncContext.createSyncedIdentity(null));
    }

    @Test
    public void testCreateSyncedIdentityLocalGroup() throws Exception {
        Group gr = createTestGroup();
        SyncedIdentity si = DefaultSyncContext.createSyncedIdentity(gr);

        assertNotNull(si);
        assertEquals(gr.getID(), si.getId());
        assertNull(si.getExternalIdRef());
        assertTrue(si.isGroup());
        assertEquals(-1, si.lastSynced());
    }

    @Test
    public void testCreateSyncedIdentityLocalUser() throws Exception {
        User u = getTestUser();
        SyncedIdentity si = DefaultSyncContext.createSyncedIdentity(u);

        assertNotNull(si);
        assertEquals(u.getID(), si.getId());
        assertNull(si.getExternalIdRef());
        assertFalse(si.isGroup());
        assertEquals(-1, si.lastSynced());
    }

    @Test
    public void testCreateSyncedIdentitySyncedGroup() throws Exception {
        ExternalIdentity externalGroup = idp.listGroups().next();
        sync(externalGroup);

        Authorizable a = userManager.getAuthorizable(externalGroup.getId());
        SyncedIdentity si = DefaultSyncContext.createSyncedIdentity(a);

        assertNotNull(si);
        assertEquals(a.getID(), si.getId());
        assertNotNull(si.getExternalIdRef());
        assertTrue(si.isGroup());
        assertEquals(syncCtx.now, si.lastSynced());
    }

    @Test
    public void testCreateSyncedIdentitySyncedUser() throws Exception {
        ExternalIdentity externalUser = idp.listUsers().next();
        sync(externalUser);

        Authorizable a = userManager.getAuthorizable(externalUser.getId());
        SyncedIdentity si = DefaultSyncContext.createSyncedIdentity(a);

        assertNotNull(si);
        assertEquals(a.getID(), si.getId());
        assertNotNull(si.getExternalIdRef());
        assertFalse(si.isGroup());
        assertEquals(syncCtx.now, si.lastSynced());
    }

    @Test
    public void testCreateSyncedIdentityEmptyLastSyncedProperty() throws Exception {
        Group gr = createTestGroup();
        gr.setProperty(DefaultSyncContext.REP_LAST_SYNCED, new Value[0]);

        SyncedIdentity si = DefaultSyncContext.createSyncedIdentity(gr);
        assertNotNull(si);
        assertEquals(-1, si.lastSynced());
    }

    @Test
    public void testGetIdentityRefNull() throws Exception {
        assertNull(DefaultSyncContext.getIdentityRef(null));
    }

    @Test
    public void testGetIdentityRefLocalGroup() throws Exception {
        assertNull(DefaultSyncContext.getIdentityRef(createTestGroup()));
    }

    @Test
    public void testGetIdentityRefLocalUser() throws Exception {
        assertNull(DefaultSyncContext.getIdentityRef(getTestUser()));
    }

    @Test
    public void testGetIdentityRefSyncGroup() throws Exception {
        ExternalIdentity externalGroup = idp.listGroups().next();
        sync(externalGroup);

        ExternalIdentityRef ref = DefaultSyncContext.getIdentityRef(userManager.getAuthorizable(externalGroup.getId()));
        assertNotNull(ref);
        assertEquals(externalGroup.getExternalId(), ref);
    }

    @Test
    public void testGetIdentityRefSyncUser() throws Exception {
        ExternalIdentity externalUser = idp.listUsers().next();
        sync(externalUser);

        ExternalIdentityRef ref = DefaultSyncContext.getIdentityRef(userManager.getAuthorizable(externalUser.getId()));
        assertNotNull(ref);
        assertEquals(externalUser.getExternalId(), ref);
    }

    @Test
    public void testGetIdentityRefEmptyMvProperty() throws Exception {
        Group gr = createTestGroup();
        // NOTE: making rep:externalId a multivalue property without any value
        //       not committing the changes as this prop is expected to become
        //       protected to prevent unintentional or malicious modification.
        gr.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, new Value[0]);

        ExternalIdentityRef ref = DefaultSyncContext.getIdentityRef(gr);
        assertNull(ref);
    }

    @Test
    public void testIsKeepMissing() {
        assertFalse(syncCtx.isKeepMissing());

        assertSame(syncCtx, syncCtx.setKeepMissing(true));
        assertTrue(syncCtx.isKeepMissing());

        assertSame(syncCtx, syncCtx.setKeepMissing(false));
        assertFalse(syncCtx.isKeepMissing());
    }

    @Test
    public void testIsForceUserSync() {
        assertFalse(syncCtx.isForceUserSync());

        assertSame(syncCtx, syncCtx.setForceUserSync(true));
        assertTrue(syncCtx.isForceUserSync());

        assertSame(syncCtx, syncCtx.setForceUserSync(false));
        assertFalse(syncCtx.isForceUserSync());
    }

    @Test
    public void testIsForceGroupSync() {
        assertFalse(syncCtx.isForceGroupSync());

        assertSame(syncCtx, syncCtx.setForceGroupSync(true));
        assertTrue(syncCtx.isForceGroupSync());

        assertSame(syncCtx, syncCtx.setForceGroupSync(false));
        assertFalse(syncCtx.isForceGroupSync());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSyncInvalidExternalIdentity() throws Exception {
        syncCtx.sync(new TestIdentityProvider.TestIdentity());
    }

    @Test
    public void testSyncExternalUser() throws Exception {
        ExternalUser user = idp.listUsers().next();
        assertNotNull(user);

        SyncResult result = syncCtx.sync(user);
        assertEquals(SyncResult.Status.ADD, result.getStatus());

        result = syncCtx.sync(user);
        assertEquals(SyncResult.Status.NOP, result.getStatus());

        syncCtx.setForceUserSync(true);
        result = syncCtx.sync(user);
        assertEquals(SyncResult.Status.UPDATE, result.getStatus());

    }

    @Test
    public void testSyncExternalGroup() throws Exception {
        ExternalGroup gr = idp.listGroups().next();
        assertNotNull(gr);

        SyncResult result = syncCtx.sync(gr);
        assertEquals(SyncResult.Status.ADD, result.getStatus());

        result = syncCtx.sync(gr);
        assertEquals(SyncResult.Status.NOP, result.getStatus());

        syncCtx.setForceGroupSync(true);
        result = syncCtx.sync(gr);
        assertEquals(SyncResult.Status.UPDATE, result.getStatus());
    }

    @Test
    public void testSyncForeignExternalUser() throws Exception {
        ExternalIdentity foreign = new TestIdentityProvider.ForeignExternalUser();

        SyncResult res = syncCtx.sync(foreign);
        assertNotNull(res);
        assertSame(SyncResult.Status.FOREIGN, res.getStatus());

        // expect {@code SyncedIdentity} in accordance with {@code sync(String userId)},
        // where the authorizable is found to be linked to a different IDP.
        SyncedIdentity si = res.getIdentity();
        assertNotNull(si);
        assertEquals(foreign.getId(), si.getId());
        ExternalIdentityRef ref = si.getExternalIdRef();
        assertNotNull(ref);
        assertEquals(foreign.getExternalId(), ref);
        assertFalse(si.isGroup());
        assertEquals(-1, si.lastSynced());

        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testSyncForeignExternalGroup() throws Exception {
        ExternalIdentity foreign = new TestIdentityProvider.ForeignExternalGroup();

        SyncResult res = syncCtx.sync(foreign);
        assertNotNull(res);
        assertSame(SyncResult.Status.FOREIGN, res.getStatus());

        // expect {@code SyncedIdentity} in accordance with {@code sync(String userId)},
        // where the authorizable is found to be linked to a different IDP.
        SyncedIdentity si = res.getIdentity();
        assertNotNull(si);
        assertEquals(foreign.getId(), si.getId());
        ExternalIdentityRef ref = si.getExternalIdRef();
        assertNotNull(ref);
        assertEquals(foreign.getExternalId(), ref);
        assertTrue(si.isGroup());
        assertEquals(-1, si.lastSynced());

        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testSyncExternalForeignLocalUser() throws Exception {
        ExternalUser external = idp.listUsers().next();
        syncCtx.sync(external);

        User u = userManager.getAuthorizable(external.getId(), User.class);
        setExternalID(u, "differentIDP");

        SyncResult result = syncCtx.sync(external);
        assertEquals(SyncResult.Status.FOREIGN, result.getStatus());
        SyncedIdentity si = result.getIdentity();
        assertNotNull(si);
        assertEquals(external.getExternalId(), si.getExternalIdRef());
    }

    @Test
    public void testSyncExternalToForeignLocalGroup() throws Exception {
        ExternalGroup external = idp.listGroups().next();
        syncCtx.sync(external);

        Group gr = userManager.getAuthorizable(external.getId(), Group.class);
        setExternalID(gr, "differentIDP");

        SyncResult result = syncCtx.sync(external);
        assertEquals(SyncResult.Status.FOREIGN, result.getStatus());
        SyncedIdentity si = result.getIdentity();
        assertNotNull(si);
        assertEquals(external.getExternalId(), si.getExternalIdRef());
    }

    @Test
    public void testSyncExternalToExistingLocalUser() throws Exception {
        ExternalUser external = idp.listUsers().next();
        syncCtx.sync(external);

        User u = userManager.getAuthorizable(external.getId(), User.class);
        u.removeProperty(ExternalIdentityConstants.REP_EXTERNAL_ID);

        SyncResult result = syncCtx.sync(external);
        assertEquals(SyncResult.Status.FOREIGN, result.getStatus());
        SyncedIdentity si = result.getIdentity();
        assertNotNull(si);
        assertEquals(external.getExternalId(), si.getExternalIdRef());
    }

    @Test
    public void testSyncExternalToExistingLocalGroup() throws Exception {
        ExternalGroup external = idp.listGroups().next();
        syncCtx.sync(external);

        Group gr = userManager.getAuthorizable(external.getId(), Group.class);
        gr.removeProperty(ExternalIdentityConstants.REP_EXTERNAL_ID);

        SyncResult result = syncCtx.sync(external);
        assertEquals(SyncResult.Status.FOREIGN, result.getStatus());
        SyncedIdentity si = result.getIdentity();
        assertNotNull(si);
        assertEquals(external.getExternalId(), si.getExternalIdRef());
    }

    @Test
    public void testSyncUserById() throws Exception {
        ExternalIdentity externalId = idp.listUsers().next();

        // no initial sync -> sync-by-id doesn't succeed
        SyncResult result = syncCtx.sync(externalId.getId());
        assertEquals(SyncResult.Status.NO_SUCH_AUTHORIZABLE, result.getStatus());

        // force sync
        syncCtx.sync(externalId);

        // try again
        syncCtx.setForceUserSync(true);
        result = syncCtx.sync(externalId.getId());
        assertEquals(SyncResult.Status.UPDATE, result.getStatus());
    }

    @Test
    public void testSyncRemovedUserById() throws Exception {
        // mark a regular repo user as external user from the test IDP
        User u = userManager.createUser("test" + UUID.randomUUID(), null);
        String userId = u.getID();
        setExternalID(u, idp.getName());

        // test sync with 'keepmissing' = true
        syncCtx.setKeepMissing(true);
        SyncResult result = syncCtx.sync(userId);
        assertEquals(SyncResult.Status.MISSING, result.getStatus());
        assertNotNull(userManager.getAuthorizable(userId));

        // test sync with 'keepmissing' = false
        syncCtx.setKeepMissing(false);
        result = syncCtx.sync(userId);
        assertEquals(SyncResult.Status.DELETE, result.getStatus());

        assertNull(userManager.getAuthorizable(userId));
    }

    @Test
    public void testSyncDisabledUserById() throws Exception {
        // configure to disable missing users
        syncConfig.user().setDisableMissing(true);

        // mark a regular repo user as external user from the test IDP
        User u = userManager.createUser("test" + UUID.randomUUID(), null);
        String userId = u.getID();
        setExternalID(u, idp.getName());

        // test sync with 'keepmissing' = true
        syncCtx.setKeepMissing(true);
        SyncResult result = syncCtx.sync(userId);
        assertEquals(SyncResult.Status.MISSING, result.getStatus());
        assertNotNull(userManager.getAuthorizable(userId));

        // test sync with 'keepmissing' = false
        syncCtx.setKeepMissing(false);
        result = syncCtx.sync(userId);
        assertEquals(SyncResult.Status.DISABLE, result.getStatus());

        Authorizable authorizable = userManager.getAuthorizable(userId);
        assertNotNull(authorizable);
        assertTrue(((User)authorizable).isDisabled());

        // add external user back
        addIDPUser(userId);

        result = syncCtx.sync(userId);
        assertEquals(SyncResult.Status.ENABLE, result.getStatus());
        assertNotNull(userManager.getAuthorizable(userId));
        assertFalse(((User)authorizable).isDisabled());
    }

    @Test
    public void testSyncDoesNotEnableUsers() throws Exception {
        // configure to remove missing users, check that sync does not mess with disabled status
        syncConfig.user().setDisableMissing(false);
        // test sync with 'keepmissing' = false
        syncCtx.setKeepMissing(false);

        ExternalUser user = idp.listUsers().next();
        assertNotNull(user);

        SyncResult result = syncCtx.sync(user);
        assertEquals(SyncResult.Status.ADD, result.getStatus());

        Authorizable authorizable = userManager.getAuthorizable(user.getId());
        assertTrue(authorizable instanceof User);
        User u = (User) authorizable;

        // disable user
        u.disable("disabled locally");
        root.commit();

        // sync
        syncCtx.setForceUserSync(true);
        result = syncCtx.sync(user.getId());
        assertEquals(SyncResult.Status.UPDATE, result.getStatus());
        authorizable = userManager.getAuthorizable(user.getId());
        assertNotNull(authorizable);
        assertTrue(authorizable instanceof User);
        assertTrue(((User)authorizable).isDisabled());
    }

    @Test
    public void testSyncGroupById() throws Exception {
        ExternalIdentity externalId = idp.listGroups().next();

        // no initial sync -> sync-by-id doesn't succeed
        SyncResult result = syncCtx.sync(externalId.getId());
        assertEquals(SyncResult.Status.NO_SUCH_AUTHORIZABLE, result.getStatus());

        // force sync
        syncCtx.sync(externalId);

        // try again
        syncCtx.setForceGroupSync(true);
        result = syncCtx.sync(externalId.getId());
        assertEquals(SyncResult.Status.UPDATE, result.getStatus());
    }

    @Test
    public void testSyncRemovedGroupById() throws Exception {
        // mark a regular repo user as external user from the test IDP
        Group gr = createTestGroup();
        String groupId = gr.getID();

        setExternalID(gr, idp.getName());

        // test sync with 'keepmissing' = true
        syncCtx.setKeepMissing(true);
        SyncResult result = syncCtx.sync(groupId);
        assertEquals(SyncResult.Status.MISSING, result.getStatus());
        assertNotNull(userManager.getAuthorizable(groupId));

        // test sync with 'keepmissing' = false
        syncCtx.setKeepMissing(false);
        result = syncCtx.sync(groupId);
        assertEquals(SyncResult.Status.DELETE, result.getStatus());

        assertNull(userManager.getAuthorizable(groupId));
    }

    @Test
    public void testSyncRemovedGroupWithMembers() throws Exception {
        // mark a regular repo user as external user from the test IDP
        Group gr = createTestGroup();
        gr.addMember(getTestUser());

        String groupId = gr.getID();
        setExternalID(gr, idp.getName());

        // test sync with 'keepmissing' = true
        syncCtx.setKeepMissing(true);
        SyncResult result = syncCtx.sync(groupId);
        assertEquals(SyncResult.Status.NOP, result.getStatus());
        assertNotNull(userManager.getAuthorizable(groupId));

        // test sync with 'keepmissing' = false
        syncCtx.setKeepMissing(false);
        result = syncCtx.sync(groupId);
        assertEquals(SyncResult.Status.NOP, result.getStatus());

        assertNotNull(userManager.getAuthorizable(groupId));
    }

    @Test
    public void testSyncByForeignUserId() throws Exception {
        SyncResult result = syncCtx.sync(getTestUser().getID());

        assertEquals(SyncResult.Status.FOREIGN, result.getStatus());
        SyncedIdentity si = result.getIdentity();
        assertNotNull(si);
        assertNull(si.getExternalIdRef());
        assertFalse(si.isGroup());
    }

    @Test
    public void testSyncByForeignGroupId() throws Exception {
        SyncResult result = syncCtx.sync(createTestGroup().getID());

        assertEquals(SyncResult.Status.FOREIGN, result.getStatus());
        SyncedIdentity si = result.getIdentity();
        assertNotNull(si);
        assertNull(si.getExternalIdRef());
        assertTrue(si.isGroup());
    }

    @Test
    public void testSyncByForeignId2() throws Exception {
        User u = userManager.getAuthorizable(getTestUser().getID(), User.class);
        setExternalID(u, "differentIDP");

        SyncResult result = syncCtx.sync(u.getID());
        assertEquals(SyncResult.Status.FOREIGN, result.getStatus());
        SyncedIdentity si = result.getIdentity();
        assertNotNull(si);
        assertEquals(DefaultSyncContext.getIdentityRef(u), si.getExternalIdRef());
    }

    @Test(expected = SyncException.class)
    public void testSyncByIdUsingExceptionId() throws Exception {
        Group gr = userManager.createGroup(TestIdentityProvider.ID_EXCEPTION);
        setExternalID(gr, idp.getName());

        syncCtx.sync(TestIdentityProvider.ID_EXCEPTION);
    }

    @Test
    public void testSyncAutoMembership() throws Exception {
        Group gr = createTestGroup();

        syncConfig.user().setAutoMembership(gr.getID());

        SyncResult result = syncCtx.sync(idp.listUsers().next());
        assertEquals(SyncResult.Status.ADD, result.getStatus());

        Authorizable a = userManager.getAuthorizable(result.getIdentity().getId());
        assertTrue(gr.isDeclaredMember(a));
    }

    @Test
    public void testSyncAutoMembershipListsNonExistingGroup() throws Exception {
        syncConfig.user().setAutoMembership("nonExistingGroup");

        SyncResult result = syncCtx.sync(idp.listUsers().next());
        assertEquals(SyncResult.Status.ADD, result.getStatus());
    }

    @Test
    public void testSyncAutoMembershipListsUser() throws Exception {
        // set auto-membership config to point to a user instead a group
        syncConfig.user().setAutoMembership(getTestUser().getID());
        syncCtx.sync(idp.listUsers().next());
    }

    @Test
    public void testLostMembership() throws Exception {
        // create a group in the repository which is marked as being external
        // and associated with the test-IDP to setup the situation that a
        // repository group is no longer listed in the IDP.
        Group gr = createTestGroup();
        setExternalID(gr, idp.getName());

        // sync an external user from the IDP into the repo and make it member
        // of the test group
        SyncResult result = syncCtx.sync(idp.listUsers().next());
        User user = userManager.getAuthorizable(result.getIdentity().getId(), User.class);
        gr.addMember(user);
        root.commit();

        // enforce synchronization of the user and it's group membership
        syncCtx.setForceUserSync(true);
        syncConfig.user().setMembershipExpirationTime(-1);

        // 1. membership nesting is < 0 => membership not synchronized
        syncConfig.user().setMembershipNestingDepth(-1);
        syncCtx.sync(user.getID()).getStatus();
        assertTrue(gr.isDeclaredMember(user));

        // 2. membership nesting is > 0 => membership gets synchronized
        syncConfig.user().setMembershipNestingDepth(1);
        assertEquals(SyncResult.Status.UPDATE, syncCtx.sync(user.getID()).getStatus());

        assertFalse(gr.isDeclaredMember(user));
    }

    @Test
    public void testLostMembershipDifferentIDP() throws Exception {
        // create a group in the repository which is marked as being external
        // and associated with another IPD.
        Group gr = createTestGroup();
        setExternalID(gr, "differentIDP");

        // sync an external user from the IDP into the repo and make it member
        // of the test group
        SyncResult result = syncCtx.sync(idp.listUsers().next());
        User user = userManager.getAuthorizable(result.getIdentity().getId(), User.class);
        gr.addMember(user);
        root.commit();

        // enforce synchronization of the user and it's group membership
        syncCtx.setForceUserSync(true);
        syncConfig.user().setMembershipExpirationTime(-1);
        syncConfig.user().setMembershipNestingDepth(1);

        assertEquals(SyncResult.Status.UPDATE, syncCtx.sync(user.getID()).getStatus());

        // since the group is not associated with the test-IDP the group-membership
        // must NOT be modified during the sync.
        assertTrue(gr.isDeclaredMember(user));
    }

    @Test
    public void testLostMembershipWithExpirationSet() throws Exception {
        long expTime = 2;
        syncConfig.user().setMembershipNestingDepth(1).setMembershipExpirationTime(expTime).setExpirationTime(expTime);

        Group gr = createTestGroup();
        setExternalID(gr, idp.getName());

        SyncResult result = syncCtx.sync(idp.listUsers().next());
        User user = (User) userManager.getAuthorizable(result.getIdentity().getId());
        gr.addMember(user);
        root.commit();

        waitUntilExpired(user, root, expTime);
        DefaultSyncContext newCtx = new DefaultSyncContext(syncConfig, idp, userManager, valueFactory);

        result = newCtx.sync(user.getID());
        root.commit();
        assertSame(SyncResult.Status.UPDATE, result.getStatus());

        gr = (Group) userManager.getAuthorizable(gr.getID());
        assertFalse(gr.isDeclaredMember(userManager.getAuthorizable(user.getID())));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4397">OAK-4397</a>
     */
    @Test
    public void testMembershipForExistingForeignGroup() throws Exception {
        syncConfig.user().setMembershipNestingDepth(1).setMembershipExpirationTime(-1).setExpirationTime(-1);
        syncConfig.group().setExpirationTime(-1);

        ExternalUser externalUser = idp.getUser(USER_ID);
        ExternalIdentityRef groupRef = externalUser.getDeclaredGroups().iterator().next();

        // create the group as if it had been synced by a foreign IDP
        Group gr = userManager.createGroup(groupRef.getId());
        setExternalID(gr, "foreignIDP");  // but don't set rep:lastSynced :-)
        root.commit();

        SyncResult result = syncCtx.sync(externalUser);
        assertSame(SyncResult.Status.ADD, result.getStatus());

        User user = userManager.getAuthorizable(externalUser.getId(), User.class);
        assertNotNull(user);

        // synchronizing the user from our IDP must _neither_ change the group
        // members of the group belonging to a different IDP nor synchronizing
        // that foreign group with information retrieved from this IDP (e.g.
        // properties and as such must _not_ set the last-synced property.

        // -> verify group last-synced has not been added
        assertFalse(gr.hasProperty(DefaultSyncContext.REP_LAST_SYNCED));

        // -> verify group membership has not changed
        assertFalse(gr.isDeclaredMember(user));
        Iterator<Group> declared = user.declaredMemberOf();
        while (declared.hasNext()) {
            assertFalse(gr.getID().equals(declared.next().getID()));
        }
    }

    @Test
    public void testGetAuthorizableUser() throws Exception {
        ExternalIdentity extUser = idp.listUsers().next();
        User user = syncCtx.getAuthorizable(extUser, User.class);
        assertNull(user);

        sync(extUser);

        user = syncCtx.getAuthorizable(extUser, User.class);
        assertNotNull(user);
    }

    @Test(expected = SyncException.class)
    public void testGetAuthorizableUserWrongType() throws Exception {
        ExternalIdentity extUser = idp.listUsers().next();
        sync(extUser);
        syncCtx.getAuthorizable(extUser, Group.class);
    }

    @Test
    public void testGetAuthorizableGroup() throws Exception {
        ExternalIdentity extGroup = idp.listGroups().next();
        Group gr = syncCtx.getAuthorizable(extGroup, Group.class);
        assertNull(gr);

        sync(extGroup);

        gr = syncCtx.getAuthorizable(extGroup, Group.class);
        assertNotNull(gr);
    }

    @Test(expected = SyncException.class)
    public void testGetAuthorizableGroupWrongType() throws Exception {
        ExternalIdentity extGroup = idp.listGroups().next();
        sync(extGroup);
        syncCtx.getAuthorizable(extGroup, User.class);
    }

    @Test
    public void testSyncMembershipDepthNoSync() throws Exception {
        ExternalUser externalUser = idp.listUsers().next();
        Authorizable a = syncCtx.createUser(externalUser);
        root.commit();

        assertTrue(externalUser.getDeclaredGroups().iterator().hasNext());

        syncCtx.syncMembership(externalUser, a, 0);
        assertFalse(root.hasPendingChanges());

        syncCtx.syncMembership(externalUser, a, -1);
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testSyncMembershipDepth1() throws Exception {
        ExternalUser externalUser = idp.listUsers().next();
        Authorizable a = syncCtx.createUser(externalUser);

        syncCtx.syncMembership(externalUser, a, 1);
        assertTrue(root.hasPendingChanges());

        for (ExternalIdentityRef ref : externalUser.getDeclaredGroups()) {
            Group g = userManager.getAuthorizable(ref.getId(), Group.class);
            assertNotNull(g);
            assertTrue(g.isDeclaredMember(a));
        }
    }

    @Test
    public void testSyncMembershipDepthInfinite() throws Exception {
        ExternalUser externalUser = idp.listUsers().next();
        Authorizable a = syncCtx.createUser(externalUser);

        syncCtx.syncMembership(externalUser, a, Long.MAX_VALUE);
        assertTrue(root.hasPendingChanges());
        root.commit();

        for (ExternalIdentityRef ref : externalUser.getDeclaredGroups()) {
            ExternalIdentity extGr = idp.getIdentity(ref);
            assertNotNull(extGr);

            for (ExternalIdentityRef inheritedGrRef : extGr.getDeclaredGroups()) {
                Group g = userManager.getAuthorizable(inheritedGrRef.getId(), Group.class);
                assertNotNull(g);
                if (Iterables.contains(externalUser.getDeclaredGroups(), inheritedGrRef)) {
                    assertTrue(g.isDeclaredMember(a));
                } else {
                    assertFalse(g.isDeclaredMember(a));
                }
                assertTrue(g.isMember(a));
            }
        }
    }

    @Test
    public void testSyncMembershipGroupIsExternalUser() throws Exception {
        // sync the 'wrong' external group into the repository
        ExternalIdentity externalIdentity = idp.listUsers().next();
        sync(externalIdentity);

        // create external user with an synced-ext-user as declared group
        ExternalUser withWrongDeclaredGroup = new ExternalUserWithDeclaredGroup(externalIdentity.getExternalId());

        try {
            Authorizable a = syncCtx.createUser(withWrongDeclaredGroup);
            root.commit();

            syncCtx.syncMembership(withWrongDeclaredGroup, a, 1);
            assertFalse(root.hasPendingChanges());
        } finally {
            Authorizable a = userManager.getAuthorizable(withWrongDeclaredGroup.getId());
            if (a != null) {
                a.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testSyncMembershipGroupIsSyncedAsUser() throws Exception {
        ExternalUser fromIDP = idp.listUsers().next();
        ExternalIdentityRef groupRef = fromIDP.getDeclaredGroups().iterator().next();

        // sync the the ext-user from the idp (but make it just declare a single group)
        ExternalUser extuser = new ExternalUserWithDeclaredGroup(groupRef, fromIDP);
        Authorizable a = syncCtx.createUser(extuser);

        // create an external-user based on info that the IDP knows as group and sync it
        ExternalUser externalIdentity = new ExternalUserFromGroup(idp.getIdentity(groupRef));
        Authorizable a2 = syncCtx.createUser(externalIdentity);
        assertFalse(a2.isGroup());
        root.commit();

        // now sync-ing the membership should not have any effect as the external
        // group referenced from 'extuser' has already been created in the system
        // as user.
        syncCtx.syncMembership(extuser, a, 1);
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testApplyMembershipNonExistingGroup() throws Exception {
        User u = getTestUser();

        assertNull(userManager.getAuthorizable("anyGroup", Group.class));
        syncCtx.applyMembership(u, ImmutableSet.of("anyGroup"));
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testApplyMembershipNonGroup() throws Exception {
        ExternalUser externalUser = idp.listUsers().next();
        sync(externalUser);
        User u = getTestUser();

        syncCtx.applyMembership(userManager.getAuthorizable(externalUser.getId()), ImmutableSet.of(u.getID()));
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testApplyMembership() throws Exception {
        User u = getTestUser();
        Group gr = createTestGroup();

        syncCtx.applyMembership(u, ImmutableSet.of(gr.getID()));
        assertTrue(gr.isDeclaredMember(u));
        assertTrue(root.hasPendingChanges());
    }

    @Test
    public void testSyncPropertiesEmptyMap() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_SECOND_USER);
        Authorizable a = syncCtx.createUser(externalUser);

        syncCtx.syncProperties(externalUser, a, ImmutableMap.<String, String>of());

        for (String propName : externalUser.getProperties().keySet()) {
            assertFalse(a.hasProperty(propName));
        }
    }

    @Test
    public void testSyncPropertiesEmptyMapExistingProps() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_SECOND_USER);
        Authorizable a = syncCtx.createUser(externalUser);

        Value anyValue = valueFactory.createValue("any");

        Map<String, ?> extProps = externalUser.getProperties();
        for (String propName : extProps.keySet()) {
            a.setProperty(propName, anyValue);
        }

        syncCtx.syncProperties(externalUser, a, ImmutableMap.<String, String>of());
        for (String propName : extProps.keySet()) {
            assertTrue(a.hasProperty(propName));
            assertEquals(anyValue, a.getProperty(propName)[0]);
        }
    }

    @Test
    public void testSyncPropertiesMappingRemovesExisting() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_SECOND_USER);
        sync(externalUser);

        Authorizable a = userManager.getAuthorizable(externalUser.getId());

        // create mapping that doesn't match to names in the external-properties
        // -> previously synced properties must be removed
        Map<String, String> mapping = new HashMap();
        Map<String, ?> extProps = externalUser.getProperties();
        for (String propName : extProps.keySet()) {
            mapping.put(propName, "any");
        }

        syncCtx.syncProperties(externalUser, a, mapping);
        for (String propName : extProps.keySet()) {
            assertFalse(a.hasProperty(propName));
        }
    }

    @Test
    public void testSyncPropertiesMappingConstants() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_SECOND_USER);
        sync(externalUser);

        Authorizable a = userManager.getAuthorizable(externalUser.getId());

        // create mapping that doesn't match to names in the external-properties
        // -> previously synced properties must be removed
        Map<String, String> mapping = new HashMap();
        Map<String, ?> extProps = externalUser.getProperties();
        for (String propName : extProps.keySet()) {
            mapping.put(propName, "\"any\"");
        }

        syncCtx.syncProperties(externalUser, a, mapping);
        Value anyValue = valueFactory.createValue("any");
        for (String propName : extProps.keySet()) {
            assertTrue(a.hasProperty(propName));
            assertEquals(anyValue, a.getProperty(propName)[0]);
        }
    }

    @Test
    public void testSyncPropertiesMappingDQuoteName() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_SECOND_USER);
        sync(externalUser);

        Authorizable a = userManager.getAuthorizable(externalUser.getId());

        // mapping to '"' (i.e. name size = 1) which doesn't qualify as constant
        // -> same behavior expected as with 'testSyncPropertiesMappingRemovesExisting'
        Map<String, String> mapping = new HashMap();
        Map<String, ?> extProps = externalUser.getProperties();
        for (String propName : extProps.keySet()) {
            mapping.put(propName, "\"");
        }

        syncCtx.syncProperties(externalUser, a, mapping);
        for (String propName : extProps.keySet()) {
            assertFalse(a.hasProperty(propName));
        }
    }

    @Test
    public void testSyncPropertiesMappingNameStartsWithDQuote() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_SECOND_USER);
        sync(externalUser);

        Authorizable a = userManager.getAuthorizable(externalUser.getId());

        // mapping to '"any', which doesn't qualify as constant
        // -> same behavior expected as with 'testSyncPropertiesMappingRemovesExisting'
        Map<String, String> mapping = new HashMap();
        Map<String, ?> extProps = externalUser.getProperties();
        for (String propName : extProps.keySet()) {
            mapping.put(propName, "\"any");
        }

        syncCtx.syncProperties(externalUser, a, mapping);
        for (String propName : extProps.keySet()) {
            assertFalse(a.hasProperty(propName));
        }
    }

    @Test
    public void testSyncProperties() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_SECOND_USER);
        Authorizable a = syncCtx.createUser(externalUser);

        // create exact mapping
        Map<String, String> mapping = new HashMap();
        Map<String, ?> extProps = externalUser.getProperties();
        for (String propName : extProps.keySet()) {
            mapping.put(propName, propName);
        }
        syncCtx.syncProperties(externalUser, a, mapping);

        for (String propName : extProps.keySet()) {
            assertTrue(a.hasProperty(propName));

            Object obj = extProps.get(propName);
            Value[] vs = a.getProperty(propName);
            if (vs.length == 1) {
                assertEquals(syncCtx.createValue(obj), a.getProperty(propName)[0]);
            } else {
                Value[] expected = (obj instanceof Collection) ?
                        syncCtx.createValues((Collection) obj) :
                        syncCtx.createValues(Arrays.asList((Object[]) obj));
                assertArrayEquals(expected, a.getProperty(propName));
            }
        }
    }

    @Test
    public void testSyncPropertiesRemapped() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_SECOND_USER);
        Authorizable a = syncCtx.createUser(externalUser);

        // create exact mapping
        Map<String, String> mapping = new HashMap();
        Map<String, ?> extProps = externalUser.getProperties();
        for (String propName : extProps.keySet()) {
            mapping.put("a/"+propName, propName);
        }
        syncCtx.syncProperties(externalUser, a, mapping);

        for (String propName : extProps.keySet()) {
            String relPath = "a/" + propName;

            assertTrue(a.hasProperty(relPath));

            Object obj = extProps.get(propName);
            Value[] vs = a.getProperty(relPath);
            if (vs.length == 1) {
                assertEquals(syncCtx.createValue(obj), a.getProperty(relPath)[0]);
            } else {
                Value[] expected = (obj instanceof Collection) ?
                        syncCtx.createValues((Collection) obj) :
                        syncCtx.createValues(Arrays.asList((Object[]) obj));
                assertArrayEquals(expected, a.getProperty(relPath));
            }
        }
    }

    @Test
    public void testIsExpiredLocalGroup() throws Exception {
        Group gr = createTestGroup();
        assertTrue(syncCtx.isExpired(gr, syncConfig.group().getExpirationTime(), "any"));
    }

    @Test
    public void testIsExpiredEmptyLastSyncedProperty() throws Exception {
        Group gr = createTestGroup();
        gr.setProperty(DefaultSyncContext.REP_LAST_SYNCED, new Value[0]);

        assertTrue(syncCtx.isExpired(gr, syncConfig.group().getExpirationTime(), "any"));
    }

    @Test
    public void testIsExpiredSyncedUser() throws Exception {
        ExternalIdentity externalUser = idp.listUsers().next();
        sync(externalUser);

        Authorizable a = userManager.getAuthorizable(externalUser.getId());
        assertFalse(syncCtx.isExpired(a, syncConfig.user().getExpirationTime(), "any"));
        assertTrue(syncCtx.isExpired(a, -1, "any"));

        // create a ctx with a newer 'now'
        DefaultSyncContext ctx = new DefaultSyncContext(syncConfig, idp, userManager, valueFactory);
        long expTime = ctx.now - syncCtx.now - 1;
        assertTrue(ctx.isExpired(a, expTime, "any"));

        // remove last-sync property
        a.removeProperty(DefaultSyncContext.REP_LAST_SYNCED);
        assertTrue(syncCtx.isExpired(a, syncConfig.user().getExpirationTime(), "any"));
    }

    @Test
    public void testIsExpiredSyncedGroup() throws Exception {
        ExternalIdentity externalGroup = idp.listGroups().next();
        sync(externalGroup);

        Authorizable a = userManager.getAuthorizable(externalGroup.getId());
        assertFalse(syncCtx.isExpired(a, syncConfig.group().getExpirationTime(), "any"));
        assertTrue(syncCtx.isExpired(a, -1, "any"));

        // create a ctx with a newer 'now'
        DefaultSyncContext ctx = new DefaultSyncContext(syncConfig, idp, userManager, valueFactory);
        long expTime = ctx.now - syncCtx.now - 1;
        assertTrue(ctx.isExpired(a, expTime, "any"));

        // remove last-sync property
        a.removeProperty(DefaultSyncContext.REP_LAST_SYNCED);
        assertTrue(syncCtx.isExpired(a, syncConfig.group().getExpirationTime(), "any"));
    }

    @Test
    public void testCreateValueNull() throws Exception {
        assertNull(syncCtx.createValue(null));
    }

    @Test
    public void testCreateValueString() throws Exception {
        Value v = syncCtx.createValue("s");
        assertNotNull(v);
        assertEquals(PropertyType.STRING, v.getType());
        assertEquals("s", v.getString());

        v = syncCtx.createValue(new char[] {'s'});
        assertNotNull(v);
        assertEquals(PropertyType.STRING, v.getType());
        assertEquals("s", v.getString());

        Object o = new TestIdentityProvider.ForeignExternalUser();
        v = syncCtx.createValue(o);
        assertNotNull(v);
        assertEquals(PropertyType.STRING, v.getType());
        assertEquals(o.toString(), v.getString());
    }

    @Test
    public void testCreateValueBoolean() throws Exception {
        Value v = syncCtx.createValue(true);
        assertNotNull(v);
        assertEquals(PropertyType.BOOLEAN, v.getType());
        assertEquals(true, v.getBoolean());
    }

    @Test
    public void testCreateValueLong() throws Exception {
        Value v = syncCtx.createValue(Long.MAX_VALUE);
        assertNotNull(v);
        assertEquals(PropertyType.LONG, v.getType());
        assertEquals(Long.MAX_VALUE, v.getLong());

        v = syncCtx.createValue(Integer.valueOf(23));
        assertNotNull(v);
        assertEquals(PropertyType.LONG, v.getType());
        assertEquals(23, v.getLong());

        v = syncCtx.createValue(Short.MIN_VALUE);
        assertNotNull(v);
        assertEquals(PropertyType.LONG, v.getType());
        assertEquals(Short.MIN_VALUE, v.getLong());

        v = syncCtx.createValue(Byte.MAX_VALUE);
        assertNotNull(v);
        assertEquals(PropertyType.LONG, v.getType());
        assertEquals(Byte.MAX_VALUE, v.getLong());
    }

    @Test
    public void testCreateValueDouble() throws Exception {
        Value v = syncCtx.createValue(Double.valueOf(1.1));
        assertNotNull(v);
        assertEquals(PropertyType.DOUBLE, v.getType());
        assertEquals(1.1, v.getDouble(), 0);

        v = syncCtx.createValue(Float.NaN);
        assertNotNull(v);
        assertEquals(PropertyType.DOUBLE, v.getType());
        assertEquals(Float.NaN, v.getDouble(), 0);
    }

    @Test
    public void testCreateValueDate() throws Exception {
        Date d = new Date();
        Calendar cal = Calendar.getInstance();
        cal.setTime(d);

        Value v = syncCtx.createValue(cal);
        assertNotNull(v);
        assertEquals(PropertyType.DATE, v.getType());

        Value v2 = syncCtx.createValue(d);
        assertNotNull(v2);
        assertEquals(PropertyType.DATE, v2.getType());

        assertEquals(v, v2);
    }

    @Test
    public void testCreateValueDecimal() throws Exception {
        BigDecimal dec = new BigDecimal(123);
        Value v = syncCtx.createValue(dec);
        assertNotNull(v);
        assertEquals(PropertyType.DECIMAL, v.getType());
        assertEquals(dec, v.getDecimal());
    }

    @Test
    public void testCreateValueFromBytesArray() throws Exception {
        byte[] bytes = new byte[]{'a', 'b'};
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        Binary binary = valueFactory.createBinary(is);

        Value v = syncCtx.createValue(bytes);
        assertNotNull(v);
        assertEquals(PropertyType.BINARY, v.getType());
        assertEquals(binary, v.getBinary());
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4231">OAK-4231</a>
     */
    @Test
    public void testCreateValueFromBinary() throws Exception {
        byte[] bytes = new byte[]{'a', 'b'};
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        Binary binary = valueFactory.createBinary(is);

        Value v = syncCtx.createValue(binary);
        assertNotNull(v);
        assertEquals(PropertyType.BINARY, v.getType());
        assertEquals(binary, v.getBinary());
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-4231">OAK-4231</a>
     */
    @Test
    public void testCreateValueFromInputStream() throws Exception {
        byte[] bytes = new byte[]{'a', 'b'};
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        Binary binary = valueFactory.createBinary(is);

        Value v = syncCtx.createValue(is);
        assertNotNull(v);
        assertEquals(PropertyType.BINARY, v.getType());
        assertEquals(binary, v.getBinary());
    }

    @Test
    public void testCreateValuesEmptyCollection() throws Exception {
        Value[] vs = syncCtx.createValues(ImmutableList.of());
        assertNotNull(vs);
        assertEquals(0, vs.length);
    }

    @Test
    public void testCreateValuesSkipsNull() throws Exception {
        List<String> strings = Lists.newArrayList("s", null, null, "t");
        Value[] vs = syncCtx.createValues(strings);
        assertNotNull(vs);
        assertEquals(2, vs.length);
    }

    @Test
    public void testIsSameIDPNull() throws Exception {
        assertFalse(syncCtx.isSameIDP((Authorizable) null));
    }

    @Test
    public void testIsSameIDPLocalGroup() throws Exception {
        assertFalse(syncCtx.isSameIDP(createTestGroup()));
    }

    @Test
    public void testIsSameIDPLocalUser() throws Exception {
        assertFalse(syncCtx.isSameIDP(getTestUser()));
    }

    @Test
    public void testIsSameIDPSyncedGroup() throws Exception {
        ExternalIdentity externalGroup = idp.listGroups().next();
        sync(externalGroup);

        assertTrue(syncCtx.isSameIDP(userManager.getAuthorizable(externalGroup.getId())));
    }

    @Test
    public void testIsSameIDPSyncedUser() throws Exception {
        ExternalIdentity externalUser = idp.listUsers().next();
        sync(externalUser);

        assertTrue(syncCtx.isSameIDP(userManager.getAuthorizable(externalUser.getId())));
    }

    @Test
    public void testIsSameIDPMissingExternalId() throws Exception {
        ExternalIdentity externalUser = idp.listUsers().next();
        sync(externalUser);

        Authorizable a = userManager.getAuthorizable(externalUser.getId());
        a.removeProperty(DefaultSyncContext.REP_EXTERNAL_ID);

        assertFalse(syncCtx.isSameIDP(a));
    }

    @Test
    public void testIsSameIDPForeign() throws Exception {
        Group gr = createTestGroup();
        setExternalID(gr, "some_other_idp");

        assertFalse(syncCtx.isSameIDP(gr));
    }

    @Test
    public void testIsSameIDPExternalIdentityRef() throws Exception {
        assertFalse(syncCtx.isSameIDP(new TestIdentityProvider.ForeignExternalUser().getExternalId()));
        assertFalse(syncCtx.isSameIDP(new TestIdentityProvider.ForeignExternalGroup().getExternalId()));

        assertTrue(syncCtx.isSameIDP(new TestIdentityProvider.TestIdentity().getExternalId()));
        assertTrue(syncCtx.isSameIDP(idp.listGroups().next().getExternalId()));
        assertTrue(syncCtx.isSameIDP(idp.listUsers().next().getExternalId()));
    }

    private final class ExternalUserWithDeclaredGroup extends TestIdentityProvider.TestIdentity implements ExternalUser {

        private final ExternalIdentityRef declaredGroupRef;

        private ExternalUserWithDeclaredGroup(@Nonnull ExternalIdentityRef declaredGroupRef) {
            super("externalId");
            this.declaredGroupRef = declaredGroupRef;
        }

        private ExternalUserWithDeclaredGroup(@Nonnull ExternalIdentityRef declaredGroupRef, @Nonnull ExternalIdentity base) {
            super(base);
            this.declaredGroupRef = declaredGroupRef;
        }

        @Nonnull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredGroups() {
            return ImmutableSet.of(declaredGroupRef);
        }
    }

    private final class ExternalUserFromGroup extends TestIdentityProvider.TestIdentity implements ExternalUser {

        private ExternalUserFromGroup(@Nonnull ExternalIdentity base) {
            super(base);
        }
    }
}
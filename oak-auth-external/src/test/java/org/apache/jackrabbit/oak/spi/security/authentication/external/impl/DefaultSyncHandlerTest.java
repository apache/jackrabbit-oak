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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginTestBase;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncedIdentity;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * DefaultSyncHandlerTest
 */
public class DefaultSyncHandlerTest extends ExternalLoginTestBase {

    private UserManager userManager;
    private DefaultSyncHandler syncHandler;

    @Before
    public void before() throws Exception {
        super.before();

        userManager = getUserManager(root);
        SyncHandler sh = syncManager.getSyncHandler(DefaultSyncConfig.DEFAULT_NAME);

        assertTrue(sh instanceof DefaultSyncHandler);
        syncHandler = (DefaultSyncHandler) sh;
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    @Override
    protected void setSyncConfig(DefaultSyncConfig cfg) {
        if (cfg != null) {
            cfg.user().setExpirationTime(500);
            cfg.group().setExpirationTime(Long.MAX_VALUE);
        }
        super.setSyncConfig(cfg);
    }

    private void sync(@NotNull String id, boolean isGroup) throws Exception {
        SyncContext ctx = syncHandler.createContext(idp, userManager, getValueFactory());
        ExternalIdentity exIdentity = (isGroup) ? idp.getGroup(id) : idp.getUser(id);
        assertNotNull(exIdentity);

        SyncResult res = ctx.sync(exIdentity);
        assertSame(SyncResult.Status.ADD, res.getStatus());
        root.commit();
    }

    @Test
    public void testGetName() {
        assertEquals(syncConfig.getName(), syncHandler.getName());
    }

    @Test
    public void testCreateContext() {
        SyncContext ctx = syncHandler.createContext(idp, userManager, getValueFactory());
        assertTrue(ctx instanceof DefaultSyncContext);
    }

    @Test
    public void testFindMissingIdentity() throws Exception {
        SyncedIdentity id = syncHandler.findIdentity(userManager, "foobar");
        assertNull("unknown authorizable should not exist", id);
    }

    @Test
    public void testFindLocalIdentity() throws Exception {
        SyncedIdentity id = syncHandler.findIdentity(userManager, "admin");
        assertNotNull("known authorizable should exist", id);
        assertNull("local user should not have external ref", id.getExternalIdRef());
    }

    @Test
    public void testFindExternalIdentity() throws Exception {
        login(new SimpleCredentials(USER_ID, new char[0])).close();
        root.refresh();

        SyncedIdentity id = syncHandler.findIdentity(userManager, USER_ID);
        assertNotNull("known authorizable should exist", id);
        ExternalIdentityRef ref = id.getExternalIdRef();
        assertNotNull(ref);
        assertEquals("external user should have correct external ref.idp", idp.getName(), ref.getProviderName());
        assertEquals("external user should have correct external ref.id", USER_ID, id.getExternalIdRef().getId());
    }

    @Test
    public void testFindGroupIdentity() throws Exception {
        SyncedIdentity si = syncHandler.findIdentity(userManager, "c");
        assertNull(si);

        sync("c", true);

        si = syncHandler.findIdentity(userManager, "c");
        assertNotNull(si);
        assertTrue(si.isGroup());
        assertNotNull(si.getExternalIdRef());
    }

    @Test
    public void testFindIdentityWithRemovedExternalId() throws Exception {
        sync(USER_ID, false);

        // NOTE: use system-root to remove the protected rep:externalId property (since Oak 1.5.8)
        Authorizable authorizable = userManager.getAuthorizable(USER_ID);
        Root systemRoot = getSystemRoot();
        systemRoot.getTree(authorizable.getPath()).removeProperty(DefaultSyncContext.REP_EXTERNAL_ID);
        systemRoot.commit();
        root.refresh();

        SyncedIdentity si = syncHandler.findIdentity(userManager, USER_ID);
        assertNotNull(si);
        assertNull(si.getExternalIdRef());
    }

    @Test
    public void testRequiresSyncAfterCreate() throws Exception {
        login(new SimpleCredentials(USER_ID, new char[0])).close();
        root.refresh();

        SyncedIdentity id = syncHandler.findIdentity(userManager, USER_ID);
        assertNotNull("Known authorizable should exist", id);

        assertFalse("Freshly synced id should not require sync", syncHandler.requiresSync(id));
    }

    @Test
    public void testRequiresSyncExpiredSyncProperty() throws Exception {
        login(new SimpleCredentials(USER_ID, new char[0])).close();
        root.refresh();

        final Calendar nowCal = Calendar.getInstance();
        nowCal.setTimeInMillis(nowCal.getTimeInMillis() - 1000);
        Value nowValue = getValueFactory().createValue(nowCal);

        Authorizable a = userManager.getAuthorizable(USER_ID);
        a.setProperty(DefaultSyncContext.REP_LAST_SYNCED, nowValue);
        root.commit();

        SyncedIdentity id = syncHandler.findIdentity(userManager, USER_ID);
        assertNotNull("known authorizable should exist", id);

        assertTrue("synced id should require sync", syncHandler.requiresSync(id));
    }

    @Test
    public void testRequiresSyncMissingSyncProperty() throws Exception {
        sync(USER_ID, false);

        Authorizable a = userManager.getAuthorizable(USER_ID);
        a.removeProperty(DefaultSyncContext.REP_LAST_SYNCED);
        root.commit();

        SyncedIdentity si = syncHandler.findIdentity(userManager, USER_ID);
        assertNotNull(si);
        assertTrue(syncHandler.requiresSync(si));
    }

    @Test
    public void testRequiresSyncMissingExternalIDRef() {
        assertTrue(syncHandler.requiresSync(new DefaultSyncedIdentity(USER_ID, null, false, Long.MAX_VALUE)));
    }

    @Test
    public void testRequiresSyncNotYetSynced() throws Exception {
        assertTrue(syncHandler.requiresSync(new DefaultSyncedIdentity(USER_ID, idp.getUser(USER_ID).getExternalId(), false, Long.MIN_VALUE)));
    }

    @Test
    public void testRequiresSyncGroup() throws Exception {
        sync("c", true);

        SyncedIdentity si = syncHandler.findIdentity(userManager, "c");
        assertNotNull(si);
        assertTrue(si.isGroup());
        assertFalse(syncHandler.requiresSync(si));
    }

    @Test
    public void testListIdentitiesBeforeSync() throws Exception {
        Iterator<SyncedIdentity> identities = syncHandler.listIdentities(userManager);
        if (identities.hasNext()) {
            SyncedIdentity si = identities.next();
            fail("Sync handler returned unexpected identity: " + si);
        }
    }

    @Test
    public void testListIdentitiesAfterSync() throws Exception {
        sync(USER_ID, false);

        // membership-nesting is 1 => expect only 'USER_ID' plus the declared group-membership
        Set<String> expected = CollectionUtils.toSet(USER_ID);
        for (ExternalIdentityRef extRef : idp.getUser(USER_ID).getDeclaredGroups()) {
            expected.add(extRef.getId());
        }

        Iterator<SyncedIdentity> identities = syncHandler.listIdentities(userManager);
        while (identities.hasNext()) {
            SyncedIdentity si = identities.next();
            if (expected.contains(si.getId())) {
                expected.remove(si.getId());
                assertNotNull(si.getExternalIdRef());
            } else {
                fail("Sync handler returned unexpected identity: " + si);
            }
        }
        assertTrue(expected.isEmpty());
    }

    @Test
    public void testListIdentitiesIgnoresLocal() throws Exception {
        sync(USER_ID, false);

        Iterator<SyncedIdentity> identities = syncHandler.listIdentities(userManager);
        while (identities.hasNext()) {
            SyncedIdentity si = identities.next();
            ExternalIdentityRef ref = si.getExternalIdRef();
            assertNotNull(ref);
            assertNotNull(ref.getProviderName());
        }
    }

    @Test
    public void testListIdentitiesIgnoresMissingExternalIdRef() throws Exception {
        Iterator<Authorizable> it = Iterators.singletonIterator(getTestUser());

        UserManager um = mock(UserManager.class);
        when(um.findAuthorizables(DefaultSyncContext.REP_EXTERNAL_ID, null)).thenReturn(it);

        Iterator<SyncedIdentity> identities = syncHandler.listIdentities(um);
        assertFalse(identities.hasNext());
    }

    @Test
    public void testListIdentitiesIgnoresNull() throws Exception {
        Iterator<Authorizable> it = Iterators.singletonIterator(null);

        UserManager um = mock(UserManager.class);
        when(um.findAuthorizables(DefaultSyncContext.REP_EXTERNAL_ID, null)).thenReturn(it);

        Iterator<SyncedIdentity> identities = syncHandler.listIdentities(um);
        assertFalse(identities.hasNext());
    }
    
    @Test
    public void testListIdentitiesWithRepositoryException() throws Exception {
        Authorizable a = when(mock(Authorizable.class).getProperty(REP_EXTERNAL_ID)).thenThrow(new RepositoryException()).getMock();
        Iterator<Authorizable> it = Iterators.singletonIterator(a);

        UserManager um = mock(UserManager.class);
        when(um.findAuthorizables(REP_EXTERNAL_ID, null)).thenReturn(it);

        Iterator<SyncedIdentity> identities = syncHandler.listIdentities(um);
        assertFalse(identities.hasNext());
    }

    @Test
    public void testLastSynced() throws Exception {
        sync(USER_ID, false);

        // force sync on next update
        Authorizable a = userManager.getAuthorizable(USER_ID);
        a.removeProperty(DefaultSyncContext.REP_LAST_SYNCED);
        root.commit();

        // start sync
        SyncContext ctx = syncHandler.createContext(idp, userManager, getValueFactory());
        SyncResult res = ctx.sync(USER_ID);
        assertSame(SyncResult.Status.UPDATE, res.getStatus());

        // concurrent login
        login(new SimpleCredentials(USER_ID, new char[0])).close();

        // the conflict handler needs to fix overlapping update
        root.commit();
    }

    //OAK-9773
    @Test
    public void testSyncMembershipCaseMismatch() throws Exception {
        UserManager userManager = getUserManager(root);
        User user = userManager.createUser("thirduser", "thirduser");
        user.setProperty(REP_EXTERNAL_ID, getValueFactory().createValue("thirduser;test"));
        Group group = userManager.createGroup("thirdgroup");
        group.setProperty(REP_EXTERNAL_ID, getValueFactory().createValue("thirdgroup;test"));
        assertTrue(group.addMember(user));
        root.commit();
        ((TestIdentityProvider) idp).addGroup(new TestIdentityProvider.TestGroup("THIRDGROUP", "test"));
        ((TestIdentityProvider) idp).addUser(
                new TestIdentityProvider.TestUser("THIRDUSER", "test").withGroups("THIRDGROUP"));

        syncHandler.createContext(idp, userManager, getValueFactory()).sync(user.getID());
        assertTrue(group.isMember(user));
    }

    @Test
    public void testActivate() {
        DefaultSyncHandler handler = new DefaultSyncHandler();
        Map<String,Object> props = Map.of(DefaultSyncConfigImpl.PARAM_NAME, "testName");
        context.registerInjectActivateService(handler, props);

        assertEquals("testName", handler.getName());
    }
}

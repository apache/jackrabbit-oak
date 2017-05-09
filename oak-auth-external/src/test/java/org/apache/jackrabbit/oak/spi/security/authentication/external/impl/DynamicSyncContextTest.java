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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DynamicSyncContextTest extends AbstractExternalAuthTest {

    private Root r;
    private UserManager userManager;
    private ValueFactory valueFactory;

    private DynamicSyncContext syncContext;

    @Before
    public void before() throws Exception {
        super.before();
        r = getSystemRoot();
        userManager = getUserManager(r);
        valueFactory = getValueFactory(r);
        syncContext = new DynamicSyncContext(syncConfig, idp, userManager, valueFactory);
    }

    @After
    public void after() throws Exception {
        try {
            syncContext.close();
            r.refresh();
        } finally {
            super.after();
        }
    }

    @Override
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig sc = super.createSyncConfig();
        sc.user().setDynamicMembership(true);
        return sc;
    }

    private void sync(@Nonnull ExternalIdentity externalIdentity, @Nonnull SyncResult.Status expectedStatus) throws Exception {
        SyncResult result = syncContext.sync(externalIdentity);
        assertSame(expectedStatus, result.getStatus());
        r.commit();
    }

    private void assertDynamicMembership(@Nonnull Authorizable a, @Nonnull ExternalIdentity externalIdentity, long depth) throws Exception {
        Value[] vs = a.getProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
        Iterable<String> pNames = Iterables.transform(ImmutableList.copyOf(vs), new Function<Value, String>() {
            @Nullable
            @Override
            public String apply(Value input) {
                try {
                    return input.getString();
                } catch (RepositoryException e) {
                    fail(e.getMessage());
                    return null;
                }
            };
        });

        Set<String> expected = new HashSet<>();
        collectGroupPrincipals(expected, externalIdentity.getDeclaredGroups(), depth);

        assertEquals(expected, ImmutableSet.copyOf(pNames));
    }

    private void collectGroupPrincipals(Set<String> pNames, @Nonnull Iterable<ExternalIdentityRef> declaredGroups, long depth) throws ExternalIdentityException {
        if (depth <= 0) {
            return;
        }
        for (ExternalIdentityRef ref : declaredGroups) {
            ExternalIdentity ei = idp.getIdentity(ref);
            pNames.add(ei.getPrincipalName());
            collectGroupPrincipals(pNames, ei.getDeclaredGroups(), depth - 1);
        }
    }

    private static void assertSyncedMembership(@Nonnull UserManager userManager,
                                               @Nonnull Authorizable a,
                                               @Nonnull ExternalIdentity externalIdentity) throws Exception {
        for (ExternalIdentityRef ref : externalIdentity.getDeclaredGroups()) {
            Group gr = userManager.getAuthorizable(ref.getId(), Group.class);
            assertNotNull(gr);
            assertTrue(gr.isMember(a));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSyncExternalIdentity() throws Exception {
        syncContext.sync(new TestIdentityProvider.TestIdentity());
    }

    @Test
    public void testSyncExternalUser() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        assertNotNull(userManager.getAuthorizable(USER_ID));
    }

    @Test
    public void testSyncExternalUserDepth0() throws Exception {
        syncConfig.user().setMembershipNestingDepth(0);

        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Tree tree = r.getTree(userManager.getAuthorizable(USER_ID).getPath());
        PropertyState extPrincipalNames = tree.getProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
        assertNotNull(extPrincipalNames);
        assertEquals(0, extPrincipalNames.count());
    }

    @Test
    public void testSyncExternalUserDepth1() throws Exception {
        syncConfig.user().setMembershipNestingDepth(1);

        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Tree tree = r.getTree(userManager.getAuthorizable(USER_ID).getPath());
        PropertyState extPrincipalNames = tree.getProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
        assertNotNull(extPrincipalNames);

        Set<String> pNames = Sets.newHashSet(extPrincipalNames.getValue(Type.STRINGS));
        for (ExternalIdentityRef ref : externalUser.getDeclaredGroups()) {
            assertTrue(pNames.remove(idp.getIdentity(ref).getPrincipalName()));
        }
        assertTrue(pNames.isEmpty());
    }

    @Test
    public void testSyncExternalUserDepthInfinite() throws Exception {
        syncConfig.user().setMembershipNestingDepth(Long.MAX_VALUE);

        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Tree tree = r.getTree(userManager.getAuthorizable(USER_ID).getPath());
        PropertyState extPrincipalNames = tree.getProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
        assertNotNull(extPrincipalNames);

        Set<String> pNames = Sets.newHashSet(extPrincipalNames.getValue(Type.STRINGS));
        Set<String> expected = Sets.newHashSet();
        collectGroupPrincipals(expected, externalUser.getDeclaredGroups(), Long.MAX_VALUE);

        assertEquals(expected, pNames);
    }

    @Test
    public void testSyncExternalUserExistingGroups() throws Exception {
        syncConfig.user().setMembershipNestingDepth(1);

        ExternalUser externalUser = idp.getUser(USER_ID);

        DefaultSyncContext ctx = new DefaultSyncContext(syncConfig, idp, userManager, valueFactory);
        ctx.sync(externalUser);
        ctx.close();

        Authorizable a = userManager.getAuthorizable(USER_ID);
        assertSyncedMembership(userManager, a, externalUser);

        syncContext.setForceUserSync(true);
        syncConfig.user().setMembershipExpirationTime(-1);
        syncContext.sync(externalUser);

        Tree t = r.getTree(a.getPath());
        assertFalse(t.hasProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES));

        assertSyncedMembership(userManager, a, externalUser);
    }

    @Test
    public void testSyncExternalGroup() throws Exception {
        ExternalGroup gr = idp.listGroups().next();

        syncContext.sync(gr);
        assertNull(userManager.getAuthorizable(gr.getId()));
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncExternalGroupVerifyStatus() throws Exception {
        ExternalGroup gr = idp.listGroups().next();

        SyncResult result = syncContext.sync(gr);
        assertEquals(SyncResult.Status.NOP, result.getStatus());

        result = syncContext.sync(gr);
        assertEquals(SyncResult.Status.NOP, result.getStatus());

        syncContext.setForceGroupSync(true);
        result = syncContext.sync(gr);
        assertEquals(SyncResult.Status.NOP, result.getStatus());
    }

    @Test
    public void testSyncExternalGroupExisting() throws Exception {
        // create an external external group that already has been synced into the repo
        ExternalGroup externalGroup = idp.listGroups().next();
        SyncContext ctx = new DefaultSyncContext(syncConfig, idp, userManager, valueFactory);
        ctx.sync(externalGroup);
        ctx.close();

        // synchronizing using DynamicSyncContext must update the existing group
        syncContext.setForceGroupSync(true);
        SyncResult result = syncContext.sync(externalGroup);
        assertSame(SyncResult.Status.UPDATE, result.getStatus());
    }

    @Test
    public void testSyncForeignExternalGroup() throws Exception {
        ExternalGroup foreign = new TestIdentityProvider.ForeignExternalGroup();

        SyncResult res = syncContext.sync(foreign);
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

        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncUserByIdUpdate() throws Exception {
        ExternalIdentity externalId = idp.listUsers().next();

        Authorizable a = userManager.createUser(externalId.getId(), null);
        a.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, valueFactory.createValue(externalId.getExternalId().getString()));

        syncContext.setForceUserSync(true);
        SyncResult result = syncContext.sync(externalId.getId());
        assertEquals(SyncResult.Status.UPDATE, result.getStatus());

        Tree t = r.getTree(a.getPath());
        assertTrue(t.hasProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES));
    }

    @Test
    public void testSyncUserIdExistingGroups() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);

        DefaultSyncContext ctx = new DefaultSyncContext(syncConfig, idp, userManager, valueFactory);
        ctx.sync(externalUser);
        ctx.close();

        Authorizable user = userManager.getAuthorizable(externalUser.getId());
        for (ExternalIdentityRef ref : externalUser.getDeclaredGroups()) {
            Group gr = userManager.getAuthorizable(ref.getId(), Group.class);
            assertTrue(gr.isMember(user));
        }

        syncContext.setForceUserSync(true);
        syncContext.sync(externalUser.getId());

        Authorizable a = userManager.getAuthorizable(USER_ID);
        Tree t = r.getTree(a.getPath());
        assertFalse(t.hasProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES));
        assertSyncedMembership(userManager, a, externalUser);
    }

    @Test
    public void testSyncMembershipWithNesting() throws Exception {
        long nesting = 1;
        syncConfig.user().setMembershipNestingDepth(nesting);

        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Authorizable a = userManager.getAuthorizable(externalUser.getId());
        assertDynamicMembership(a, externalUser, nesting);

        // verify that the membership is always reflected in the rep:externalPrincipalNames property
        // 1. membership nesting  = -1
        nesting = -1;
        syncContext.syncMembership(externalUser, a, nesting);
        assertDynamicMembership(a, externalUser, nesting);

        // 2. membership nesting is > 0
        nesting = Long.MAX_VALUE;
        syncContext.syncMembership(externalUser, a, nesting);
        assertDynamicMembership(a, externalUser, nesting);
    }

    @Test
    public void testSyncMembershipWithChangedGroups() throws Exception {
        long nesting = 1;
        syncConfig.user().setMembershipNestingDepth(nesting);

        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Authorizable a = userManager.getAuthorizable(externalUser.getId());
        assertDynamicMembership(a, externalUser, nesting);

        // sync user with modified membership => must be reflected
        // 1. empty set of declared groups
        ExternalUser mod = new TestUserWithGroupRefs(externalUser, ImmutableSet.<ExternalIdentityRef>of());
        syncContext.syncMembership(mod, a, nesting);
        assertDynamicMembership(a, mod, nesting);

        // 2. set with different groups that defined on IDP
        mod = new TestUserWithGroupRefs(externalUser, ImmutableSet.<ExternalIdentityRef>of(
                idp.getGroup("a").getExternalId(),
                idp.getGroup("aa").getExternalId(),
                idp.getGroup("secondGroup").getExternalId()));
        syncContext.syncMembership(mod, a, nesting);
        assertDynamicMembership(a, mod, nesting);
    }

    @Test
    public void testSyncMembershipWithChangedExistingGroups() throws Exception {
        long nesting = 1;
        syncConfig.user().setMembershipNestingDepth(nesting);

        ExternalUser externalUser = idp.getUser(USER_ID);

        DefaultSyncContext ctx = new DefaultSyncContext(syncConfig, idp, userManager, valueFactory);
        ctx.sync(externalUser);
        ctx.close();

        Authorizable a = userManager.getAuthorizable(externalUser.getId());
        assertSyncedMembership(userManager, a, externalUser);

        // sync user with modified membership => must be reflected
        // 1. empty set of declared groups
        ExternalUser mod = new TestUserWithGroupRefs(externalUser, ImmutableSet.<ExternalIdentityRef>of());
        syncContext.syncMembership(mod, a, nesting);
        assertSyncedMembership(userManager, a, mod);

        // 2. set with different groups that defined on IDP
        mod = new TestUserWithGroupRefs(externalUser, ImmutableSet.<ExternalIdentityRef>of(
                        idp.getGroup("a").getExternalId(),
                        idp.getGroup("aa").getExternalId(),
                        idp.getGroup("secondGroup").getExternalId()));
        syncContext.syncMembership(mod, a, nesting);
        assertSyncedMembership(userManager, a, mod);
    }

    @Test
    public void testSyncMembershipForExternalGroup() throws Exception {
        ExternalGroup externalGroup = idp.getGroup("a"); // a group that has declaredGroups
        SyncContext ctx = new DefaultSyncContext(syncConfig, idp, userManager, valueFactory);
        ctx.sync(externalGroup);
        ctx.close();
        r.commit();

        Authorizable gr = userManager.getAuthorizable(externalGroup.getId());
        syncContext.syncMembership(externalGroup, gr, 1);

        assertFalse(gr.hasProperty(ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES));
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testAutoMembership() throws Exception {
        Group gr = userManager.createGroup("group" + UUID.randomUUID());
        r.commit();

        syncConfig.user().setAutoMembership(gr.getID(), "non-existing-group");

        SyncResult result = syncContext.sync(idp.getUser(USER_ID));
        assertSame(SyncResult.Status.ADD, result.getStatus());

        User u = userManager.getAuthorizable(USER_ID, User.class);
        assertFalse(gr.isDeclaredMember(u));
        assertFalse(gr.isMember(u));
    }

    private static final class TestUserWithGroupRefs extends TestIdentityProvider.TestIdentity implements ExternalUser {

        private Iterable<ExternalIdentityRef> declaredGroupRefs;

        private TestUserWithGroupRefs(@Nonnull ExternalUser base, @Nonnull Iterable<ExternalIdentityRef> declaredGroupRefs) {
            super(base);
            this.declaredGroupRefs = declaredGroupRefs;
        }

        public String getPassword() {
            return "";
        }

        @Nonnull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredGroups() {
            return declaredGroupRefs;
        }
    }
}
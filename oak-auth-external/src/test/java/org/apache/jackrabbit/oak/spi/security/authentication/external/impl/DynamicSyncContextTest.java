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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static java.util.stream.Collectors.toSet;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_SECOND_USER;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_TEST_USER;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_LAST_DYNAMIC_SYNC;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.REP_MEMBERS;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.REP_MEMBERS_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DynamicSyncContextTest extends AbstractDynamicTest {

    static final String PREVIOUS_SYNCED_ID = "third";
    static final long PREVIOUS_NESTING_DEPTH = Long.MAX_VALUE;
    static final String GROUP_ID = "aaa";

    /**
     * Synchronized a separate user with DefaultSyncContext to test behavior for previously synchronized user/group
     * with deep membership-nesting => all groups synched
     */
    @NotNull ExternalUser syncPriorToDynamicMembership() throws Exception {
        DefaultSyncConfig priorSyncConfig = createSyncConfig();
        priorSyncConfig.user().setMembershipNestingDepth(PREVIOUS_NESTING_DEPTH);
        
        String idpName = idp.getName();
        TestIdentityProvider tidp = (TestIdentityProvider) idp; 
        tidp.addGroup(new TestIdentityProvider.TestGroup("ttt", idpName));
        tidp.addGroup(new TestIdentityProvider.TestGroup("tt", idpName).withGroups("ttt"));
        tidp.addGroup(new TestIdentityProvider.TestGroup("thirdGroup", idpName).withGroups("tt"));
        tidp.addGroup(new TestIdentityProvider.TestGroup("forthGroup", idpName));
        tidp.addUser(new TestIdentityProvider.TestUser(PREVIOUS_SYNCED_ID, idpName).withGroups("thirdGroup", "forthGroup"));

        UserManager um = getUserManager(r);
        DefaultSyncContext ctx = new DefaultSyncContext(priorSyncConfig, idp, um, getValueFactory(r));
        ExternalUser previouslySyncedUser = idp.getUser(PREVIOUS_SYNCED_ID);
        assertNotNull(previouslySyncedUser);
        SyncResult result = ctx.sync(previouslySyncedUser);
        assertSame(SyncResult.Status.ADD, result.getStatus());
        ctx.close();
        r.commit();
        return previouslySyncedUser;
    }

    protected void assertDynamicMembership(@NotNull ExternalIdentity externalIdentity, long depth) throws Exception {
        Authorizable a = userManager.getAuthorizable(externalIdentity.getId());
        assertNotNull(a);
        assertDynamicMembership(a, externalIdentity, depth);
    }

    private void assertDynamicMembership(@NotNull Authorizable a, @NotNull ExternalIdentity externalIdentity, long depth) throws Exception {
        Value[] vs = a.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
        Set<String> pNames = ImmutableList.copyOf(vs).stream().map(value -> {
            try {
                return value.getString();
            } catch (RepositoryException e) {
                return null;
            }
        }).filter(Objects::nonNull).collect(toSet());

        Set<String> expected = new HashSet<>();
        collectGroupPrincipals(expected, externalIdentity.getDeclaredGroups(), depth);

        assertEquals(expected, pNames);
    }

    private void collectGroupPrincipals(Set<String> pNames, @NotNull Iterable<ExternalIdentityRef> declaredGroups, long depth) throws ExternalIdentityException {
        if (depth <= 0) {
            return;
        }
        for (ExternalIdentityRef ref : declaredGroups) {
            ExternalIdentity ei = idp.getIdentity(ref);
            pNames.add(ei.getPrincipalName());
            collectGroupPrincipals(pNames, ei.getDeclaredGroups(), depth - 1);
        }
    }

    void assertSyncedMembership(@NotNull UserManager userManager,
                                @NotNull Authorizable a,
                                @NotNull ExternalIdentity externalIdentity) throws Exception {
        assertSyncedMembership(userManager, a, externalIdentity, syncConfig.user().getMembershipNestingDepth());
    }

    void assertSyncedMembership(@NotNull UserManager userManager,
                                @NotNull Authorizable a,
                                @NotNull ExternalIdentity externalIdentity,
                                long membershipNestingDepth) throws Exception {
        Iterable<ExternalIdentityRef> declaredGroupRefs = externalIdentity.getDeclaredGroups();
        Set<ExternalIdentityRef> expectedGroupRefs = getExpectedSyncedGroupRefs(membershipNestingDepth, idp, externalIdentity);
        for (ExternalIdentityRef ref : expectedGroupRefs) {
            Group gr = userManager.getAuthorizable(ref.getId(), Group.class);
            assertNotNull(gr);
            assertTrue(gr.isMember(a));
            List<String> ids = getIds(a.memberOf());
            assertTrue("Expected "+ids+ " to contain "+gr.getID(), ids.contains(gr.getID()));
            
            if (Iterables.contains(declaredGroupRefs, ref)) {
                assertTrue(gr.isDeclaredMember(a));
                assertTrue(Iterators.contains(a.declaredMemberOf(), gr));
            }
        }
    }
    
    void assertDeclaredGroups(@NotNull ExternalUser externalUser) throws Exception {
        Set<ExternalIdentityRef> expectedGroupRefs = getExpectedSyncedGroupRefs(syncConfig.user().getMembershipNestingDepth(), idp, externalUser);
        for (ExternalIdentityRef ref : expectedGroupRefs) {
            Authorizable gr = userManager.getAuthorizable(ref.getId());
            if (syncConfig.group().getDynamicGroups()) {
                assertNotNull(gr);
            } else {
                assertNull(gr);
            }
        }
    }


    static boolean hasStoredMembershipInformation(@NotNull Tree groupTree, @NotNull Tree memberTree) {
        String ref = TreeUtil.getString(memberTree, JCR_UUID);
        assertNotNull(ref);

        if (containsMemberRef(groupTree, ref)) {
            return true;
        }
        if (groupTree.hasChild(REP_MEMBERS_LIST)) {
            for (Tree t : groupTree.getChild(REP_MEMBERS_LIST).getChildren()) {
                if (containsMemberRef(t, ref)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean containsMemberRef(@NotNull Tree tree, @NotNull String ref) {
        Iterable<String> memberRefs = TreeUtil.getStrings(tree, REP_MEMBERS);
        return memberRefs != null && Iterables.contains(memberRefs, ref);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSyncExternalIdentity() throws Exception {
        syncContext.sync(new TestIdentityProvider.TestIdentity());
    }

    @Test
    public void testSyncExternalUser() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Authorizable a = userManager.getAuthorizable(USER_ID);
        assertNotNull(a);
        assertDeclaredGroups(externalUser);
    }

    @Test
    public void testSyncExternalUserDepth0() throws Exception {
        syncConfig.user().setMembershipNestingDepth(0);

        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Tree tree = r.getTree(userManager.getAuthorizable(USER_ID).getPath());
        PropertyState extPrincipalNames = tree.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
        assertNotNull(extPrincipalNames);
        assertEquals(0, extPrincipalNames.count());
    }

    @Test
    public void testSyncExternalUserDepth1() throws Exception {
        syncConfig.user().setMembershipNestingDepth(1);

        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Tree tree = r.getTree(userManager.getAuthorizable(USER_ID).getPath());
        PropertyState extPrincipalNames = tree.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
        assertNotNull(extPrincipalNames);

        Set<String> pNames = CollectionUtils.toSet(extPrincipalNames.getValue(Type.STRINGS));
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
        PropertyState extPrincipalNames = tree.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
        assertNotNull(extPrincipalNames);

        Set<String> pNames = CollectionUtils.toSet(extPrincipalNames.getValue(Type.STRINGS));
        Set<String> expected = new HashSet<>();
        collectGroupPrincipals(expected, externalUser.getDeclaredGroups(), Long.MAX_VALUE);

        assertEquals(expected, pNames);
    }

    @Test
    public void testSyncExternalUserGroupConflict() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        
        // create a local group that collides with the external group membership
        // i.e. doesn't have an rep:externalId set
        ExternalIdentity externalGroup = idp.getIdentity(externalUser.getDeclaredGroups().iterator().next());
        assertNotNull(externalGroup);

        assertIgnored(externalUser, externalGroup, externalGroup.getId(), externalGroup.getPrincipalName(), null);    
    }

    @Test
    public void testSyncExternalUserGroupConflictDifferentIDP() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);

        // create a local group that collides with the external group membership
        // i.e. belongs to a different IDP
        ExternalIdentityRef ref = externalUser.getDeclaredGroups().iterator().next();
        ExternalIdentity externalGroup = idp.getIdentity(ref);
        assertNotNull(externalGroup);

        assertIgnored(externalUser, externalGroup, externalGroup.getId(), externalGroup.getPrincipalName(), 
                new ExternalIdentityRef(ref.getId(), ref.getProviderName()+"_mod"));
    }

    @Test
    public void testSyncExternalUserGroupConflictPrincipalNameMismatch() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);

        ExternalIdentityRef ref = externalUser.getDeclaredGroups().iterator().next();
        ExternalIdentity externalGroup = idp.getIdentity(ref);
        assertNotNull(externalGroup);

        // create a local group that has the same ID but a mismatching principal name
        // and verify that the group is ignored;
        assertIgnored(externalUser, externalGroup, externalGroup.getId(), externalGroup.getPrincipalName()+"mismatch", ref);
    }

    @Test
    public void testSyncExternalUserGroupConflictPrincipalNameCaseMismatch() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);

        ExternalIdentityRef ref = externalUser.getDeclaredGroups().iterator().next();
        ExternalIdentity externalGroup = idp.getIdentity(ref);
        assertNotNull(externalGroup);

        // create a local group that has the same ID but a mismatching principal name (only case)
        // and verify that the group is ignored;
        assertIgnored(externalUser, externalGroup, externalGroup.getId(), externalGroup.getPrincipalName().toUpperCase(), ref);
    }

    @Test
    public void testSyncExternalUserGroupConflictIdCaseMismatch() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        
        ExternalIdentityRef ref = externalUser.getDeclaredGroups().iterator().next();
        ExternalIdentity externalGroup = idp.getIdentity(ref);
        assertNotNull(externalGroup);

        // create a local group that has the case-mismatch in ID/principal name
        // and verify that the external group is ignored;
        assertIgnored(externalUser, externalGroup, externalGroup.getId().toUpperCase(), externalGroup.getPrincipalName(), null);
    }
    
    private void assertIgnored(@NotNull ExternalUser externalUser, @NotNull ExternalIdentity externalGroup, 
                               @NotNull String existingId, @NotNull String existingPrincipalName, @Nullable ExternalIdentityRef existingGroupRef) throws Exception {
        
        Group g = userManager.createGroup(existingId, new PrincipalImpl(existingPrincipalName), null);
        if (existingGroupRef != null) {
            g.setProperty(REP_EXTERNAL_ID, getValueFactory().createValue(existingGroupRef.getString()));
        }
        r.commit();

        // sync the user with dynamic membership enabled
        sync(externalUser, SyncResult.Status.ADD);

        // retrieve rep:externalPrincipalNames
        Tree tree = r.getTree(userManager.getAuthorizable(USER_ID).getPath());
        PropertyState extPrincipalNames = tree.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
        assertNotNull(extPrincipalNames);

        // the resulting rep:externalPrincipalNames must NOT contain the name of the colliding principal
        Set<String> pNames = CollectionUtils.toSet(extPrincipalNames.getValue(Type.STRINGS));
        assertFalse(pNames + " must not contain " + externalGroup.getPrincipalName(), pNames.contains(externalGroup.getPrincipalName()));
    }

    @Test
    public void testSyncExternalUserGroupConflictWithUser() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);

        // create a local user that collides with the first external group ref
        ExternalIdentityRef ref = externalUser.getDeclaredGroups().iterator().next();
        ExternalIdentity externalGroup = idp.getIdentity(ref);
        User collision = userManager.createUser(externalGroup.getId(), null, new PrincipalImpl(externalGroup.getPrincipalName()), null);
        r.commit();

        // sync the user with dynamic membership enabled
        sync(externalUser, SyncResult.Status.ADD);

        // retrieve rep:externalPrincipalNames
        Tree tree = r.getTree(userManager.getAuthorizable(USER_ID).getPath());
        PropertyState extPrincipalNames = tree.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
        assertNotNull(extPrincipalNames);

        // the resulting rep:externalPrincipalNames must NOT contain the name of the colliding principal
        Set<String> pNames = CollectionUtils.toSet(extPrincipalNames.getValue(Type.STRINGS));
        assertFalse(pNames + " must not contain " + externalGroup.getPrincipalName(), pNames.contains(externalGroup.getPrincipalName()));
    }

    @Test
    public void testSyncExternalUserExistingGroups() throws Exception {
        // verify group membership of the previously synced user
        Authorizable a = userManager.getAuthorizable(previouslySyncedUser.getId());
        assertSyncedMembership(userManager, a, previouslySyncedUser, Long.MAX_VALUE);
        
        // resync the previously synced user with dynamic-membership enabled.
        syncContext.setForceUserSync(true);
        syncConfig.user().setMembershipExpirationTime(-1);
        syncContext.sync(previouslySyncedUser);

        Tree t = r.getTree(a.getPath());
        assertFalse(t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));

        assertSyncedMembership(userManager, a, previouslySyncedUser);
    }

    @Test
    public void testSyncExternalGroup() throws Exception {
        ExternalGroup gr = idp.getGroup(GROUP_ID);

        syncContext.sync(gr);
        assertNull(userManager.getAuthorizable(gr.getId()));
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncExternalGroupVerifyStatus() throws Exception {
        ExternalGroup gr = idp.getGroup(GROUP_ID);

        SyncResult result = syncContext.sync(gr);
        SyncResult.Status expectedStatus = (syncConfig.group().getDynamicGroups()) ? SyncResult.Status.ADD : SyncResult.Status.NOP;
        assertEquals(expectedStatus, result.getStatus());

        result = syncContext.sync(gr);
        assertEquals(SyncResult.Status.NOP, result.getStatus());

        syncContext.setForceGroupSync(true);
        result = syncContext.sync(gr);
        expectedStatus = (syncConfig.group().getDynamicGroups()) ? SyncResult.Status.UPDATE : SyncResult.Status.NOP;
        assertEquals(expectedStatus, result.getStatus());
    }

    @Test
    public void testSyncExternalGroupExisting() throws Exception {
        // create an external external group that already has been synced into the repo
        ExternalGroup externalGroup = idp.getGroup(previouslySyncedUser.getDeclaredGroups().iterator().next().getId());
        assertNotNull(externalGroup);
        
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
    public void testSyncExternalGroupRepositoryException() throws Exception {
        Exception ex = new RepositoryException();
        UserManager um = mock(UserManager.class);
        when(um.getAuthorizable(any(String.class))).thenThrow(ex);
        
        DynamicSyncContext ctx = new DynamicSyncContext(syncConfig, idp, um, valueFactory);
        try {
            ctx.sync(idp.getGroup(GROUP_ID));
            fail();
        } catch (SyncException e) {
            assertEquals(ex, e.getCause());
        }
        
    }

    @Test
    public void testSyncUserByIdUpdate() throws Exception {
        ExternalIdentity externalId = idp.getUser(ID_SECOND_USER);

        Authorizable a = userManager.createUser(externalId.getId(), null);
        a.setProperty(DefaultSyncContext.REP_EXTERNAL_ID, valueFactory.createValue(externalId.getExternalId().getString()));

        syncContext.setForceUserSync(true);
        SyncResult result = syncContext.sync(externalId.getId());
        assertEquals(SyncResult.Status.UPDATE, result.getStatus());

        Tree t = r.getTree(a.getPath());
        assertTrue(t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
    }

    @Test
    public void testPreviouslySyncedIdentities() throws Exception {
        Authorizable user = userManager.getAuthorizable(PREVIOUS_SYNCED_ID);
        assertNotNull(user);
        assertFalse(user.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        
        assertSyncedMembership(userManager, user, previouslySyncedUser, PREVIOUS_NESTING_DEPTH);
    }

    @Test
    public void testSyncUserIdExistingGroupsMembershipNotExpired() throws Exception {
        // make sure membership is not expired
        long previousExpTime = syncConfig.user().getMembershipExpirationTime();
        DefaultSyncConfig.User uc = syncConfig.user();
        try {
            uc.setMembershipExpirationTime(Long.MAX_VALUE);
            syncContext.setForceUserSync(true);
            syncContext.sync(previouslySyncedUser.getId());

            Authorizable a = userManager.getAuthorizable(PREVIOUS_SYNCED_ID);
            Tree t = r.getTree(a.getPath());
            
            assertFalse(t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
            assertSyncedMembership(userManager, a, previouslySyncedUser);
        } finally {
            uc.setMembershipExpirationTime(previousExpTime);
        }
    }
    
    @Test
    public void testSyncUserIdExistingGroups() throws Exception {
        // mark membership information as expired
        long previousExpTime = syncConfig.user().getMembershipExpirationTime();
        DefaultSyncConfig.User uc = syncConfig.user();
        try {
            uc.setMembershipExpirationTime(-1);
            syncContext.setForceUserSync(true);
            syncContext.sync(previouslySyncedUser.getId());

            Authorizable a = userManager.getAuthorizable(PREVIOUS_SYNCED_ID);
            Tree t = r.getTree(a.getPath());
            
            boolean expectedMigration = (uc.getEnforceDynamicMembership() || syncConfig.group().getDynamicGroups());
            
            if (expectedMigration) {
                assertTrue(t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
                int expSize = getExpectedSyncedGroupRefs(uc.getMembershipNestingDepth(), idp, previouslySyncedUser).size();
                assertEquals(expSize, t.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES).count());
            } else {
                assertFalse(t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
            }
            
            if (uc.getEnforceDynamicMembership() && !syncConfig.group().getDynamicGroups()) {
                for (String id : getExpectedSyncedGroupIds(uc.getMembershipNestingDepth(), idp, previouslySyncedUser)) {
                    assertNull(userManager.getAuthorizable(id));
                }
            } else {
                assertSyncedMembership(userManager, a, previouslySyncedUser);
            }
        } finally {
            uc.setMembershipExpirationTime(previousExpTime);
        }
    }

    @Test
    public void testSyncMembershipWithNesting() throws Exception {
        long nesting = 1;
        syncConfig.user().setMembershipNestingDepth(nesting);

        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Authorizable a = userManager.getAuthorizable(externalUser.getId());
        assertDynamicMembership(externalUser, nesting);

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
        ExternalUser mod = new TestUserWithGroupRefs(externalUser, Set.of());
        syncContext.syncMembership(mod, a, nesting);
        assertDynamicMembership(a, mod, nesting);

        // 2. set with different groups than defined on IDP
        mod = new TestUserWithGroupRefs(externalUser, Set.of(
                idp.getGroup("a").getExternalId(),
                idp.getGroup("aa").getExternalId(),
                idp.getGroup("secondGroup").getExternalId()));
        syncContext.syncMembership(mod, a, nesting);
        assertDynamicMembership(a, mod, nesting);
    }

    @Test
    public void testSyncMembershipWithEmptyExistingGroups() throws Exception {
        long nesting = syncConfig.user().getMembershipNestingDepth();

        Authorizable a = userManager.getAuthorizable(PREVIOUS_SYNCED_ID);

        // sync user with modified membership => must be reflected
        // 1. empty set of declared groups
        ExternalUser mod = new TestUserWithGroupRefs(previouslySyncedUser, Set.of());
        syncContext.syncMembership(mod, a, nesting);
        assertSyncedMembership(userManager, a, mod, nesting);
    }
    
    @Test
    public void testSyncMembershipWithChangedExistingGroups() throws Exception {
        long nesting = syncConfig.user().getMembershipNestingDepth();

        Authorizable a = userManager.getAuthorizable(PREVIOUS_SYNCED_ID);

        // sync user with modified membership => must be reflected
        // 2. set with different groups that defined on IDP
        ExternalUser mod = new TestUserWithGroupRefs(previouslySyncedUser, Set.of(
                        idp.getGroup("a").getExternalId(),
                        idp.getGroup("aa").getExternalId(),
                        idp.getGroup("secondGroup").getExternalId()));
        syncContext.syncMembership(mod, a, nesting);
        // persist changes to have the modified membership being reflected through assertions that use queries
        r.commit();
        assertSyncedMembership(userManager, a, mod);
    }

    @Test
    public void testSyncMembershipForExternalGroup() throws Exception {
        // previously synced 'third-group' has declaredGroups (i.e. nested membership)
        ExternalGroup externalGroup = idp.getGroup(previouslySyncedUser.getDeclaredGroups().iterator().next().getId());
        Authorizable gr = userManager.getAuthorizable(externalGroup.getId());
        syncContext.syncMembership(externalGroup, gr, 1);

        assertFalse(gr.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        assertFalse(r.hasPendingChanges());
    }

    @Test
    public void testSyncMembershipWithForeignGroups() throws Exception {
        TestIdentityProvider.TestUser testuser = (TestIdentityProvider.TestUser) idp.getUser(ID_TEST_USER);
        Set<ExternalIdentityRef> sameIdpGroups = getExpectedSyncedGroupRefs(syncConfig.user().getMembershipNestingDepth(), idp, testuser);

        TestIdentityProvider.ForeignExternalGroup foreignGroup = new TestIdentityProvider.ForeignExternalGroup();
        testuser.withGroups(foreignGroup.getExternalId());
        assertNotEquals(sameIdpGroups, testuser.getDeclaredGroups());

        sync(testuser, SyncResult.Status.ADD);

        Authorizable a = userManager.getAuthorizable(ID_TEST_USER);
        assertTrue(a.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        Value[] extPrincipalNames = a.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);

        assertEquals(Iterables.size(sameIdpGroups), extPrincipalNames.length);
        for (Value v : extPrincipalNames) {
            assertNotEquals(foreignGroup.getPrincipalName(), v.getString());
        }
    }

    @Test
    public void testSyncMembershipWithUserRef() throws Exception {
        TestIdentityProvider.TestUser testuser = (TestIdentityProvider.TestUser) idp.getUser(ID_TEST_USER);
        Set<ExternalIdentityRef> groupRefs = getExpectedSyncedGroupRefs(syncConfig.user().getMembershipNestingDepth(), idp, testuser);

        // verify that the conflicting user has not been synced before
        assertNull(userManager.getAuthorizable(ID_SECOND_USER));
        
        ExternalUser second = idp.getUser(ID_SECOND_USER);
        testuser.withGroups(second.getExternalId());
        assertFalse(Iterables.elementsEqual(groupRefs, testuser.getDeclaredGroups()));

        sync(testuser, SyncResult.Status.ADD);

        Authorizable a = userManager.getAuthorizable(ID_TEST_USER);
        assertTrue(a.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        Value[] extPrincipalNames = a.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);

        assertEquals(Iterables.size(groupRefs), extPrincipalNames.length);
        for (Value v : extPrincipalNames) {
            assertNotEquals(second.getPrincipalName(), v.getString());
        }
    }

    @Test
    public void testSyncMembershipWithUserConflict() throws Exception {
        TestIdentityProvider.TestUser testuser = (TestIdentityProvider.TestUser) idp.getUser(ID_TEST_USER);
        Set<ExternalIdentityRef> groupRefs = getExpectedSyncedGroupRefs(syncConfig.user().getMembershipNestingDepth(), idp, testuser);

        // in contrast to 'testSyncMembershipWithUserRef' the conflicting group-ref refers to a user in the repository
        // and the conflict is spotted as the existing synched identity is not a group.
        testuser.withGroups(previouslySyncedUser.getExternalId());
        assertFalse(Iterables.elementsEqual(groupRefs, testuser.getDeclaredGroups()));

        sync(testuser, SyncResult.Status.ADD);

        Authorizable a = userManager.getAuthorizable(ID_TEST_USER);
        assertTrue(a.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        Value[] extPrincipalNames = a.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);

        assertEquals(Iterables.size(groupRefs), extPrincipalNames.length);
        for (Value v : extPrincipalNames) {
            assertNotEquals(previouslySyncedUser.getPrincipalName(), v.getString());
        }
    }

    @Test
    public void testSyncMembershipDeclaredGroupsFails() throws Exception {
        ExternalIdentityProvider extIdp = spy(idp);

        ExternalUser externalUser = spy(extIdp.getUser(TestIdentityProvider.ID_TEST_USER));

        syncContext.sync(externalUser);
        clearInvocations(extIdp);
        
        Authorizable a = userManager.getAuthorizable(externalUser.getId());
        assertNotNull(a);

        when(externalUser.getDeclaredGroups()).thenThrow(new ExternalIdentityException());
        
        syncContext.syncMembership(externalUser, a, 1);
        verify(extIdp, never()).getIdentity(any(ExternalIdentityRef.class));
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
    
    @Test
    public void testConvertToDynamicMembershipAlreadyDynamic() throws Exception {
        syncConfig.user().setMembershipNestingDepth(1);

        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        User user = userManager.getAuthorizable(externalUser.getId(), User.class);
        assertNotNull(user);
        assertFalse(syncContext.convertToDynamicMembership(user));
    }

    @Test
    public void testConvertToDynamicMembership() throws Exception {
        User user = userManager.getAuthorizable(PREVIOUS_SYNCED_ID, User.class);
        assertNotNull(user);
        assertFalse(user.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        assertFalse(user.hasProperty(REP_LAST_DYNAMIC_SYNC));
        
        assertTrue(syncContext.convertToDynamicMembership(user));
        assertTrue(user.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        assertTrue(user.hasProperty(REP_LAST_DYNAMIC_SYNC));

        assertDeclaredGroups(previouslySyncedUser);
    }

    @Test
    public void testConvertToDynamicMembershipForGroup() throws Exception {
        Authorizable gr = when(mock(Authorizable.class).isGroup()).thenReturn(true).getMock();
        assertFalse(syncContext.convertToDynamicMembership(gr));
    }

    static final class TestUserWithGroupRefs extends TestIdentityProvider.TestIdentity implements ExternalUser {

        private final Iterable<ExternalIdentityRef> declaredGroupRefs;

        TestUserWithGroupRefs(@NotNull ExternalUser base, @NotNull Iterable<ExternalIdentityRef> declaredGroupRefs) {
            super(base);
            this.declaredGroupRefs = declaredGroupRefs;
        }

        public String getPassword() {
            return "";
        }

        @NotNull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredGroups() {
            return declaredGroupRefs;
        }
    }
}

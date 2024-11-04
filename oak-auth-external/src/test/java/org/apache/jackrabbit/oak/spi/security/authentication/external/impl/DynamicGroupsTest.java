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
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.REP_MEMBERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class DynamicGroupsTest extends DynamicSyncContextTest {

    @Parameterized.Parameters(name = "name={2}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { DefaultSyncConfigImpl.PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT, false, "Membership-Nesting-Depth=0" },
                new Object[] { DefaultSyncConfigImpl.PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT+1, false, "Membership-Nesting-Depth=1" },
                // NOTE: shortcut for PrincipalNameResolver is ignored if dynamic-groups are enabled
                new Object[] { DefaultSyncConfigImpl.PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT+1, true, "Membership-Nesting-Depth=1, IDP implements PrincipalNameResolver" },
                new Object[] { DefaultSyncConfigImpl.PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT+2, false, "Membership-Nesting-Depth=2" });
    }
    
    private final long membershipNestingDepth;
    private final boolean isPrincipalNameResolver;

    public DynamicGroupsTest(long membershipNestingDepth, boolean isPrincipalNameResolver, @NotNull String name) {
        this.membershipNestingDepth = membershipNestingDepth;
        this.isPrincipalNameResolver = isPrincipalNameResolver;
    }

    @Override
    @NotNull
    protected ExternalIdentityProvider createIDP() {
        if (isPrincipalNameResolver) {
            return new PrincipalResolutionTest.PrincipalResolvingIDP();
        } else {
            return super.createIDP();
        }
    }

    @Override
    protected @NotNull DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config = super.createSyncConfig();
        config.group().setDynamicGroups(true);
        config.user().setMembershipNestingDepth(membershipNestingDepth);
        return config;
    }

    @Test
    public void testSyncExternalGroup() throws Exception {
        ExternalGroup gr = idp.getGroup(GROUP_ID);
        
        syncContext.sync(gr);
        assertNotNull(userManager.getAuthorizable(gr.getId()));
        assertTrue(r.hasPendingChanges());
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
        
        // dynamic-group option forces migration of previously synced groups
        assertTrue(t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));

        assertSyncedMembership(userManager, a, previouslySyncedUser);
    }

    @Test
    public void testSyncMembershipWithEmptyExistingGroups() throws Exception {
        Authorizable a = userManager.getAuthorizable(PREVIOUS_SYNCED_ID);
        
        // sync user with modified membership => must be reflected
        // 1. empty set of declared groups
        ExternalUser mod = new TestUserWithGroupRefs(previouslySyncedUser, Set.of());
        syncContext.syncMembership(mod, a, membershipNestingDepth);

        assertSyncedMembership(userManager, a, mod, membershipNestingDepth);
        Tree t = r.getTree(a.getPath());
        assertTrue(t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        assertEquals(0, t.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES).count());
    }

    @Test
    public void testSyncMembershipWithChangedExistingGroups() throws Exception {
        Authorizable a = userManager.getAuthorizable(PREVIOUS_SYNCED_ID);

        // sync user with modified membership => must be reflected
        // 2. set with different groups that defined on IDP
        ExternalUser mod = new TestUserWithGroupRefs(previouslySyncedUser, Set.of(
                idp.getGroup("a").getExternalId(),
                idp.getGroup("aa").getExternalId(),
                idp.getGroup("secondGroup").getExternalId()));
        syncContext.syncMembership(mod, a, membershipNestingDepth);

        Tree t = r.getTree(a.getPath());
        assertTrue(t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));

        Set<String> groupIds = getExpectedSyncedGroupIds(membershipNestingDepth, idp, mod);
        assertEquals(groupIds.size(), t.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES).count());
        
        assertMigratedGroups(previouslySyncedUser, t);
        if (membershipNestingDepth == 0) {
            for (String grId : groupIds) {
                assertNull(userManager.getAuthorizable(grId));
            }
        } else {
            assertMigratedGroups(mod, t);
        }
    }

    private void assertMigratedGroups(@NotNull ExternalIdentity externalIdentity, @NotNull Tree userTree) throws Exception {
        for (ExternalIdentityRef ref : externalIdentity.getDeclaredGroups()) {
            Group gr = userManager.getAuthorizable(ref.getId(), Group.class);
            assertNotNull(gr);
            assertFalse(hasStoredMembershipInformation(r.getTree(gr.getPath()), userTree));
        }
    }
    
    @Test
    public void testSyncNewGroup() throws Exception {
        String id = "newGroup";
        TestIdentityProvider.TestGroup gr = new TestIdentityProvider.TestGroup(id, idp.getName());
        TestIdentityProvider testIDP = (TestIdentityProvider) idp;
        testIDP.addGroup(gr);

        sync(gr, SyncResult.Status.ADD);
        assertNotNull(userManager.getAuthorizable(id));
    }
    
    @Test
    public void testReSyncUserMembershipExpired() throws Exception {
        boolean forceSyncGroupPrevious = syncContext.isForceGroupSync();
        boolean forceSyncUserPrevious = syncContext.isForceUserSync();
        long expTimePrevious = syncConfig.user().getMembershipExpirationTime();
        try {
            syncContext.setForceGroupSync(true).setForceUserSync(true);
            syncConfig.user().setMembershipExpirationTime(-1);
            
            // re-sync with dynamic-sync-context and force membership update
            sync(idp.getUser(PREVIOUS_SYNCED_ID), SyncResult.Status.UPDATE);

            // verify that user and groups have been migrated to dynamic membership/group
            User user = userManager.getAuthorizable(PREVIOUS_SYNCED_ID, User.class);
            assertNotNull(user);
            assertTrue(user.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
            Set<String> extPNames = Arrays.stream(user.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES)).filter(Objects::nonNull).map(value -> {
                try {
                    return value.getString();
                } catch (RepositoryException repositoryException) {
                    return "";
                }
            }).collect(Collectors.toSet());
            
            // rep:principalNames are migrated using the current membershipNestingDepth (not the one from previous sync) 
            Set<String> expectedGroupIds = getExpectedSyncedGroupIds(membershipNestingDepth, idp, previouslySyncedUser);
            assertEquals(expectedGroupIds.size(), extPNames.size());

            // group membership must have moved to dynamic membership
            // previously synced membership (with different depth) must be adjusted to new value
            Tree userTree = r.getTree(user.getPath());
            for (String groupId : getExpectedSyncedGroupIds(PREVIOUS_NESTING_DEPTH, idp, previouslySyncedUser)) {
                Group gr = userManager.getAuthorizable(groupId, Group.class);
                // no group must be removed
                assertNotNull(gr);
                
                // with dynamic.group option members must be migrated to dynamic membership even if 'enforce' option is disabled.
                Tree t = r.getTree(gr.getPath());
                assertFalse(hasStoredMembershipInformation(t, userTree));

                boolean stillMember = expectedGroupIds.contains(gr.getID());
                
                // user-group relationship must be covered by the dynamic-membership provider now
                assertEquals(stillMember, gr.isDeclaredMember(user));
                
                // verify that the group principal name is listed in the ext-principal-names property
                assertEquals(stillMember, extPNames.contains(gr.getPrincipal().getName()));
            }
        } finally {
            syncContext.setForceGroupSync(forceSyncGroupPrevious).setForceUserSync(forceSyncUserPrevious);
            syncConfig.user().setMembershipExpirationTime(expTimePrevious);
        }
    }

    @Test
    public void testReSyncGroupCreatedPriorToEnabledDynamic() throws Exception {
        boolean forceSyncPrevious = syncContext.isForceGroupSync();
        String groupId = previouslySyncedUser.getDeclaredGroups().iterator().next().getId();
        try {
            // re-sync with dynamic-sync-context
            syncContext.setForceGroupSync(true);
            sync(idp.getGroup(groupId), SyncResult.Status.UPDATE);

            // NOTE: group membership is NOT touched upon group-sync
            Group gr = userManager.getAuthorizable(groupId, Group.class);
            assertNotNull(gr);

            User user = userManager.getAuthorizable(PREVIOUS_SYNCED_ID, User.class);
            assertNotNull(user);
            // note: previous sync was executed with deep nesting -> current nesting depth doesn't make a difference
            assertTrue(gr.isMember(user));

            // since group-sync does not touch membership information the group members data must still be present 
            // with the group node.
            Tree t = r.getTree(gr.getPath());
            assertTrue(t.hasProperty(REP_MEMBERS));
            assertFalse(t.hasChild(UserConstants.REP_MEMBERS_LIST));
        } finally {
            syncContext.setForceGroupSync(forceSyncPrevious);
        }
    }
    
    @Test
    public void testGroupPrincipalLookup() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        PrincipalManager principalManager = getPrincipalManager(r);
        for (ExternalIdentityRef ref : getExpectedSyncedGroupRefs(membershipNestingDepth, idp, externalUser)) {
            String principalName = idp.getIdentity(ref).getPrincipalName();
            Principal p = principalManager.getPrincipal(principalName);
            assertNotNull(p);
            // verify that this principal has been returned by the user-principal-provider and not the external-group-principal-provider.
            assertTrue(p instanceof ItemBasedPrincipal);
            
            Authorizable group = userManager.getAuthorizable(p);
            assertNotNull(group);
            assertTrue(group.isGroup());
            assertEquals(ref.getId(), group.getID());
        }
    }
    
    @Test
    public void testCrossIDPMembership() throws Exception {
        UserManager um = getUserManager(r);
        PrincipalManager pm = getPrincipalManager(r);
        
        List<ExternalIdentityRef> declaredGroupRefs = ImmutableList.copyOf(previouslySyncedUser.getDeclaredGroups());
        assertTrue(declaredGroupRefs.size() > 1);
        
        String groupId = declaredGroupRefs.get(0).getId();
        String groupId2 = declaredGroupRefs.get(1).getId();
        Group local = um.createGroup("localGroup");
        local.addMembers(groupId, groupId2);
        um.createGroup(EveryonePrincipal.getInstance());
        r.commit();

        Authorizable a = um.getAuthorizable(PREVIOUS_SYNCED_ID);
        assertTrue(getIds(a.memberOf()).contains(local.getID()));
        
        // sync again to establish dynamic membership
        syncContext.setForceUserSync(true);
        syncContext.setForceGroupSync(true);
        syncContext.sync(idp.getUser(PREVIOUS_SYNCED_ID));

        a = um.getAuthorizable(PREVIOUS_SYNCED_ID);
        assertTrue(r.getTree(a.getPath()).hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        
        // verify membership
        List<String> groupIds = getIds(a.memberOf());
        if (membershipNestingDepth == 0) {
            assertFalse(groupIds.contains("localGroup"));
            assertFalse(local.isMember(a));
        } else {
            assertEquals("Found "+groupIds, (membershipNestingDepth > 1) ? 5 : 4, groupIds.size());
            assertTrue(groupIds.contains("localGroup"));
            assertTrue(local.isMember(a));
            
            for (String id : new String[] {groupId, groupId2}) {
                Authorizable extGroup = um.getAuthorizable(id);
                assertTrue(getIds(extGroup.declaredMemberOf()).contains("localGroup"));
                assertTrue(local.isMember(extGroup));
            }
        }
        
        // verify effective principals of external user
        List<String> principalNames = getPrincipalNames(pm.getGroupMembership(a.getPrincipal()));
        assertEquals(membershipNestingDepth != 0, principalNames.contains(local.getPrincipal().getName()));
        for (ExternalIdentityRef ref : declaredGroupRefs) {
            ExternalIdentity eg = idp.getIdentity(ref);
            assertEquals(membershipNestingDepth != 0, principalNames.contains(eg.getPrincipalName()));
        }

        // verify effective principals of dynamic group
        Authorizable dynamicGroup = userManager.getAuthorizable(groupId);
        if (dynamicGroup != null) {
            principalNames = getPrincipalNames(pm.getGroupMembership(dynamicGroup.getPrincipal()));
            assertTrue(principalNames.contains(local.getPrincipal().getName()));
        }
    }
}
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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import java.security.Principal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ExternalGroupPrincipalProviderTest extends AbstractPrincipalTest {

    private void syncWithMembership(@Nonnull ExternalUser externalUser, long depth) throws Exception {
        DefaultSyncConfig sc = new DefaultSyncConfig();
        sc.user().setMembershipNestingDepth(depth);

        Root systemRoot = getSystemRoot();
        PrincipalSyncContext syncContext = new PrincipalSyncContext(sc, idp, getUserManager(systemRoot), getValueFactory(systemRoot));
        syncContext.setForceUserSync(true);
        syncContext.sync(externalUser);
        syncContext.close();
        systemRoot.commit();

        root.refresh();
    }

    private Set<Principal> getDeclaredGroupPrincipals(@Nonnull String userId) throws ExternalIdentityException {
        Set<Principal> principals = ImmutableSet.copyOf(Iterables.transform(idp.getUser(userId).getDeclaredGroups(), new Function<ExternalIdentityRef, Principal>() {
            @Nullable
            @Override
            public Principal apply(ExternalIdentityRef input) {
                try {
                    return new PrincipalImpl(idp.getIdentity(input).getPrincipalName());
                } catch (ExternalIdentityException e) {
                    fail(e.getMessage());
                    return null;
                }
            }

            ;
        }));
        return principals;
    }

    @Test
    public void testGetPrincipalLocalUser() throws Exception {
        assertNull(principalProvider.getPrincipal(getTestUser().getPrincipal().getName()));
    }

    @Test
    public void testGetPrincipalLocalGroup() throws Exception {
        Group gr = createTestGroup();
        assertNull(principalProvider.getPrincipal(gr.getPrincipal().getName()));
    }

    @Test
    public void testGetPrincipalExternalUser() throws Exception {
        UserManager userManager = getUserManager(root);

        // synced by principal-sync-ctx
        User syncedUser = userManager.getAuthorizable(USER_ID, User.class);
        assertNull(principalProvider.getPrincipal(syncedUser.getPrincipal().getName()));

        // synced by default-sync-ctx
        syncedUser = userManager.getAuthorizable(TestIdentityProvider.ID_SECOND_USER, User.class);
        assertNull(principalProvider.getPrincipal(syncedUser.getPrincipal().getName()));
    }


    @Test
    public void testGetPrincipalExternalGroup() throws Exception {
        Group gr = getUserManager(root).getAuthorizable("secondGroup", Group.class);
        assertNotNull(gr);

        assertNull(principalProvider.getPrincipal(gr.getPrincipal().getName()));
    }

    @Test
    public void testGetPrincipalDynamicGroupDepth1() throws Exception {
        for (ExternalIdentityRef ref : idp.getUser(USER_ID).getDeclaredGroups()) {

            String princName = idp.getIdentity(ref).getPrincipalName();
            Principal principal = principalProvider.getPrincipal(princName);

            assertNotNull(principal);
            assertTrue(principal instanceof java.security.acl.Group);
        }
    }

    @Test
    public void testGetPrincipalInheritedGroupsDepth1() throws Exception {
        ImmutableSet<ExternalIdentityRef> declared = ImmutableSet.<ExternalIdentityRef>copyOf(idp.getUser(USER_ID).getDeclaredGroups());

        for (ExternalIdentityRef ref : declared) {
            for (ExternalIdentityRef inheritedGroupRef : idp.getIdentity(ref).getDeclaredGroups()) {
                if (declared.contains(inheritedGroupRef)) {
                    continue;
                }
                String inheritedPrincName = idp.getIdentity(inheritedGroupRef).getPrincipalName();
                assertNull(principalProvider.getPrincipal(inheritedPrincName));
            }
        }
    }

    @Test
    public void testGetPrincipalInheritedGroupsDepthInfinite() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        syncWithMembership(externalUser, Long.MAX_VALUE);

        for (ExternalIdentityRef ref : externalUser.getDeclaredGroups()) {
            ExternalIdentity externalGroup = idp.getIdentity(ref);
            Principal grPrincipal = principalProvider.getPrincipal(externalGroup.getPrincipalName());

            for (ExternalIdentityRef inheritedGroupRef : externalGroup.getDeclaredGroups()) {
                String inheritedPrincName = idp.getIdentity(inheritedGroupRef).getPrincipalName();

                Principal principal = principalProvider.getPrincipal(inheritedPrincName);

                assertNotNull(principal);
                assertTrue(principal instanceof java.security.acl.Group);

                java.security.acl.Group inheritedGrPrincipal = (java.security.acl.Group) principal;
                assertTrue(inheritedGrPrincipal.isMember(new PrincipalImpl(externalUser.getPrincipalName())));
                assertFalse(inheritedGrPrincipal.isMember(grPrincipal));
            }
        }
    }

    @Test
    public void testGetGroupMembershipLocalPrincipal() throws Exception {
        Set<? extends Principal> principals = principalProvider.getGroupMembership(getTestUser().getPrincipal());
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetGroupMembershipLocalGroupPrincipal() throws Exception {
        Group gr = createTestGroup();
        Set<? extends Principal> principals = principalProvider.getGroupMembership(gr.getPrincipal());
        assertTrue(principals.isEmpty());

        // same if the principal is not marked as 'java.security.acl.Group' and not tree-based-principal
        principals = principalProvider.getGroupMembership(new PrincipalImpl(gr.getPrincipal().getName()));
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetGroupMembershipExternalUser() throws Exception {
        Authorizable user = getUserManager(root).getAuthorizable(USER_ID);
        assertNotNull(user);

        Set<Principal> expected = getDeclaredGroupPrincipals(USER_ID);

        Set<? extends Principal> principals = principalProvider.getGroupMembership(user.getPrincipal());
        assertEquals(expected, principals);

        // same if the principal is not a tree-based-principal
        principals = principalProvider.getGroupMembership(new PrincipalImpl(user.getPrincipal().getName()));
        assertEquals(expected, principals);
    }

    @Test
    public void testGetGroupMembershipExternalUser2() throws Exception {
        // synchronized by default sync-context => no 'dynamic' group principals
        Authorizable user = getUserManager(root).getAuthorizable(TestIdentityProvider.ID_SECOND_USER);
        assertNotNull(user);

        Set<? extends Principal> principals = principalProvider.getGroupMembership(user.getPrincipal());
        assertTrue(principals.isEmpty());

        // same if the principal is not a tree-based-principal
        principals = principalProvider.getGroupMembership(new PrincipalImpl(user.getPrincipal().getName()));
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetGroupMembershipExternalUserInfiniteDepth() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        syncWithMembership(externalUser, Long.MAX_VALUE);

        Set<Principal> expectedGrPrincipals = new HashSet();
        collectExpectedPrincipals(expectedGrPrincipals, externalUser.getDeclaredGroups(), Long.MAX_VALUE);

        Authorizable user = getUserManager(root).getAuthorizable(USER_ID);
        Set<? extends Principal> principals = principalProvider.getGroupMembership(user.getPrincipal());
        assertEquals(expectedGrPrincipals, principals);

        // same if the principal is not a tree-based-principal
        principals = principalProvider.getGroupMembership(new PrincipalImpl(user.getPrincipal().getName()));
        assertEquals(expectedGrPrincipals, principals);
    }

    private void collectExpectedPrincipals(Set<Principal> grPrincipals, @Nonnull Iterable<ExternalIdentityRef> declaredGroups, long depth) throws ExternalIdentityException {
        if (depth <= 0) {
            return;
        }
        for (ExternalIdentityRef ref : declaredGroups) {
            ExternalIdentity ei = idp.getIdentity(ref);
            grPrincipals.add(new PrincipalImpl(ei.getPrincipalName()));
            collectExpectedPrincipals(grPrincipals, ei.getDeclaredGroups(), depth - 1);
        }
    }

    @Test
    public void testGetGroupMembershipExternalGroup() throws Exception {
        Authorizable group = getUserManager(root).getAuthorizable("secondGroup");
        assertNotNull(group);

        Set<? extends Principal> principals = principalProvider.getGroupMembership(group.getPrincipal());
        assertTrue(principals.isEmpty());

        // same if the principal is not marked as 'java.security.acl.Group' and not tree-based-principal
        principals = principalProvider.getGroupMembership(new PrincipalImpl(group.getPrincipal().getName()));
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetPrincipalsLocalUser() throws Exception {
        Set<? extends Principal> principals = principalProvider.getPrincipals(getTestUser().getID());
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetPrincipalsLocalGroup() throws Exception {
        Set<? extends Principal> principals = principalProvider.getPrincipals(createTestGroup().getID());
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetPrincipalsExternalUser() throws Exception {
        Set<? extends Principal> principals = principalProvider.getPrincipals(USER_ID);
        assertEquals(getDeclaredGroupPrincipals(USER_ID), principals);
    }

    @Test
    public void testGetPrincipalsExternalUser2() {
        // synchronized by default sync-context => no 'dynamic' group principals
        Set<? extends Principal> principals = principalProvider.getPrincipals(TestIdentityProvider.ID_SECOND_USER);
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetPrincipalsExternalGroup() throws Exception {
        Authorizable authorizable = getUserManager(root).getAuthorizable("secondGroup");
        assertNotNull(authorizable);

        Set<? extends Principal> principals = principalProvider.getPrincipals(authorizable.getID());
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetPrincipalsNonExistingUser() throws Exception {
        assertNull(getUserManager(root).getAuthorizable("nonExistingUser"));
        Set<? extends Principal> principals = principalProvider.getPrincipals("nonExistingUser");
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testFindPrincipalsByHintTypeNotGroup() {
        Iterator<? extends Principal> iter = principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        assertSame(Iterators.emptyIterator(), iter);
    }

    @Test
    public void testFindPrincipalsByHintTypeGroup() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        syncWithMembership(externalUser, Long.MAX_VALUE);

        Set<? extends Principal> expected = ImmutableSet.of(new PrincipalImpl("a"), new PrincipalImpl("aa"), new PrincipalImpl("aaa"));
        Set<? extends Principal> res = ImmutableSet.copyOf(principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_GROUP));

        assertEquals(expected, res);
    }

    @Test
    public void testFindPrincipalsByHintTypeAll() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        syncWithMembership(externalUser, Long.MAX_VALUE);

        Set<? extends Principal> expected = ImmutableSet.of(
                new PrincipalImpl("a"),
                new PrincipalImpl("aa"),
                new PrincipalImpl("aaa"));
        Set<? extends Principal> res = ImmutableSet.copyOf(principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_ALL));

        assertEquals(expected, res);
    }

    @Test
    public void testFindPrincipalsByTypeNotGroup() {
        Iterator<? extends Principal> iter = principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        assertSame(Iterators.emptyIterator(), iter);
    }

    @Test
    public void testFindPrincipalsByTypeGroup() throws Exception {
        Set<? extends Principal> res = ImmutableSet.copyOf(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_GROUP));
        assertEquals(getDeclaredGroupPrincipals(USER_ID), res);
    }

    @Test
    public void testFindPrincipalsByTypeAll() throws Exception {
        Set<? extends Principal> res = ImmutableSet.copyOf(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_ALL));
        assertEquals(getDeclaredGroupPrincipals(USER_ID), res);
    }

    @Test
    public void testFindPrincipalsFiltersDuplicates() throws Exception {
        ExternalUser otherUser = new TestUser("anotherUser", ImmutableSet.of(idp.getGroup("a").getExternalId()));
        syncWithMembership(otherUser, 1);

        Iterator<? extends Principal> res = principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_ALL);
        assertTrue(res.hasNext());
        assertEquals(new PrincipalImpl("a"), res.next());
        assertFalse(res.hasNext());
    }

    private static final class TestUser extends TestIdentityProvider.TestIdentity implements ExternalUser {

        private final Iterable<ExternalIdentityRef> declaredGroups;

        private TestUser(@Nonnull String id, @Nonnull Iterable<ExternalIdentityRef> declaredGroups) {
            super(id);
            this.declaredGroups = declaredGroups;
        }
        @Nonnull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredGroups() {
            return declaredGroups;
        }
    }
}
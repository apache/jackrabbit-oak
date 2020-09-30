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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DynamicSyncContext;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.security.Principal;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ExternalGroupPrincipalProviderTest extends AbstractPrincipalTest {

    void sync(@NotNull ExternalUser externalUser) throws Exception {
        Root systemRoot = getSystemRoot();
        DynamicSyncContext syncContext = new DynamicSyncContext(syncConfig, idp, getUserManager(systemRoot), getValueFactory(systemRoot));
        syncContext.sync(externalUser);
        syncContext.close();
        systemRoot.commit();

        root.refresh();
    }

    @NotNull
    Set<Principal> getExpectedGroupPrincipals(@NotNull String userId) throws Exception {
        if (syncConfig.user().getMembershipNestingDepth() == 1) {
            Set<Principal> principals = ImmutableSet.copyOf(Iterables.transform(idp.getUser(userId).getDeclaredGroups(), (Function<ExternalIdentityRef, Principal>) input -> {
                try {
                    return new PrincipalImpl(idp.getIdentity(input).getPrincipalName());
                } catch (ExternalIdentityException e) {
                    throw new RuntimeException(e);
                }
            }));
            return principals;
        } else {
            Set<Principal> set = new HashSet<>();
            collectExpectedPrincipals(set, idp.getUser(userId).getDeclaredGroups(), syncConfig.user().getMembershipNestingDepth());
            return set;
        }
    }

    @NotNull
    Set<Principal> getExpectedAllSearchResult(@NotNull String userId) throws Exception {
        return getExpectedGroupPrincipals(userId);
    }

    private void collectExpectedPrincipals(Set<Principal> grPrincipals, @NotNull Iterable<ExternalIdentityRef> declaredGroups, long depth) throws Exception {
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
    public void testGetPrincipalDynamicGroup() throws Exception {
        for (ExternalIdentityRef ref : idp.getUser(USER_ID).getDeclaredGroups()) {

            String princName = idp.getIdentity(ref).getPrincipalName();
            Principal principal = principalProvider.getPrincipal(princName);

            assertNotNull(principal);
            assertTrue(principal instanceof GroupPrincipal);
        }
    }

    @Test
    public void testGetPrincipalInheritedGroups() throws Exception {
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
    public void testGetPrincipalUnderscoreSign() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);

        for (ExternalIdentityRef ref : externalUser.getDeclaredGroups()) {
            String pName = idp.getIdentity(ref).getPrincipalName();

            for (String n : new String[]{"_", "_" + pName.substring(1), pName.substring(0, pName.length() - 1) + "_"}) {
                assertNull(principalProvider.getPrincipal(n));
            }
        }
    }

    @Test
    public void testGetPrincipalPercentSign() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);

        for (ExternalIdentityRef ref : externalUser.getDeclaredGroups()) {
            String pName = idp.getIdentity(ref).getPrincipalName();

            for (String n : new String[] {"%", "%" + pName, pName + "%", pName.charAt(0) + "%"}) {
                assertNull(principalProvider.getPrincipal(n));
            }
        }
    }

    @Test
    public void testGetPrincipalGroupsWithQueryWildCard() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_WILDCARD_USER);
        sync(externalUser);

        for (ExternalIdentityRef ref : externalUser.getDeclaredGroups()) {
            String pName = idp.getIdentity(ref).getPrincipalName();
            Principal p = principalProvider.getPrincipal(pName);
            assertNotNull(p);
            assertEquals(pName, p.getName());
        }
    }

    @Test
    public void testGetGroupMembershipLocalPrincipal() throws Exception {
        Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(getTestUser().getPrincipal());
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetGroupMembershipLocalGroupPrincipal() throws Exception {
        Group gr = createTestGroup();
        Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(gr.getPrincipal());
        assertTrue(principals.isEmpty());

        // same if the principal is not marked as 'GroupPrincipal' and not tree-based-principal
        principals = principalProvider.getMembershipPrincipals(new PrincipalImpl(gr.getPrincipal().getName()));
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetGroupMembershipExternalUser() throws Exception {
        Authorizable user = getUserManager(root).getAuthorizable(USER_ID);
        assertNotNull(user);

        Set<Principal> expected = getExpectedGroupPrincipals(USER_ID);

        Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(user.getPrincipal());
        assertEquals(expected, principals);
    }

    @Test
    public void testGetGroupMembershipExternalUser2() throws Exception {
        Authorizable user = getUserManager(root).getAuthorizable(USER_ID);
        assertNotNull(user);

        Set<Principal> expected = getExpectedGroupPrincipals(USER_ID);

        // same as in test before even if the principal is not a tree-based-principal
        Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(new PrincipalImpl(user.getPrincipal().getName()));
        assertEquals(expected, principals);
    }

    @Test
    public void testGetGroupMembershipDefaultSync() throws Exception {
        // synchronized by default sync-context => no 'dynamic' group principals
        Authorizable user = getUserManager(root).getAuthorizable(TestIdentityProvider.ID_SECOND_USER);
        assertNotNull(user);

        Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(user.getPrincipal());
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetGroupMembershipDefaultSync2() throws Exception {
        // synchronized by default sync-context => no 'dynamic' group principals
        Authorizable user = getUserManager(root).getAuthorizable(TestIdentityProvider.ID_SECOND_USER);
        assertNotNull(user);

        // same as in test before even if the principal is not a tree-based-principal
        Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(new PrincipalImpl(user.getPrincipal().getName()));
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetGroupMembershipExternalGroup() throws Exception {
        Authorizable group = getUserManager(root).getAuthorizable("secondGroup");
        assertNotNull(group);

        Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(group.getPrincipal());
        assertTrue(principals.isEmpty());

        // same if the principal is not marked as 'GroupPrincipal' and not tree-based-principal
        principals = principalProvider.getMembershipPrincipals(new PrincipalImpl(group.getPrincipal().getName()));
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
        assertEquals(getExpectedGroupPrincipals(USER_ID), principals);
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
    public void testGetPrincipalsNonExistingUserTree() throws Exception {
        Authorizable a = spy(getUserManager(root).getAuthorizable(USER_ID));
        when(a.getPath()).thenReturn("/path/to/non/existing/item");
        UserManager um = when(mock(UserManager.class).getAuthorizable(USER_ID)).thenReturn(a).getMock();
        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(root, getNamePathMapper())).thenReturn(um).getMock();

        ExternalGroupPrincipalProvider pp = new ExternalGroupPrincipalProvider(root, uc, getNamePathMapper(), ImmutableMap.of(idp.getName(), getAutoMembership()));
        assertTrue(pp.getPrincipals(USER_ID).isEmpty());
    }

    @Test
    public void testGetPrincipalsForGroupTree() throws Exception {
        Authorizable group = getUserManager(root).createGroup("testGroup");
        Authorizable a = spy(getUserManager(root).getAuthorizable(USER_ID));
        when(a.getPath()).thenReturn(group.getPath());
        UserManager um = when(mock(UserManager.class).getAuthorizable(USER_ID)).thenReturn(a).getMock();
        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(root, getNamePathMapper())).thenReturn(um).getMock();

        ExternalGroupPrincipalProvider pp = new ExternalGroupPrincipalProvider(root, uc, getNamePathMapper(), ImmutableMap.of(idp.getName(), getAutoMembership()));
        assertTrue(pp.getPrincipals(USER_ID).isEmpty());
    }

    @Test
    public void testGetPrincipalsMissingIdpName() throws Exception {
        String userPath = getUserManager(root).getAuthorizable(USER_ID).getPath();

        Tree t = root.getTree(userPath);
        t.removeProperty(REP_EXTERNAL_ID);

        String[] automembership = getAutoMembership();
        ExternalGroupPrincipalProvider pp = new ExternalGroupPrincipalProvider(root, getUserConfiguration(), getNamePathMapper(), ImmutableMap.of(idp.getName(), automembership));

        Set<? extends Principal> principals = pp.getPrincipals(USER_ID);
        assertFalse(principals.isEmpty());
        assertFalse(principals.removeAll(ImmutableSet.copyOf(automembership)));
    }

    @Test
    public void testFindPrincipalsByHintTypeNotGroup() {
        Iterator<? extends Principal> iter = principalProvider.findPrincipals("a",
                PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        assertSame(Collections.emptyIterator(), iter);

        Iterator<? extends Principal> iter2 = principalProvider.findPrincipals("a", false,
                PrincipalManager.SEARCH_TYPE_NOT_GROUP, 0, -1);
        assertSame(Collections.emptyIterator(), iter2);
    }

    @Test
    public void testFindPrincipalsByHintTypeGroup() {
        Set<? extends Principal> expected = ImmutableSet.of(new PrincipalImpl("a"));
        Set<? extends Principal> res = ImmutableSet
                .copyOf(principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_GROUP));
        assertEquals(expected, res);

        Set<? extends Principal> res2 = ImmutableSet
                .copyOf(principalProvider.findPrincipals("a", false, PrincipalManager.SEARCH_TYPE_GROUP, 0, -1));
        assertEquals(expected, res2);
    }

    @Test
    public void testFindPrincipalsByHintTypeAll() {
        Set<? extends Principal> expected = ImmutableSet.of(new PrincipalImpl("a"));
        Set<? extends Principal> res = ImmutableSet
                .copyOf(principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_ALL));
        assertEquals(expected, res);

        Set<? extends Principal> res2 = ImmutableSet
                .copyOf(principalProvider.findPrincipals("a", false, PrincipalManager.SEARCH_TYPE_ALL, 0, -1));
        assertEquals(expected, res2);
    }

    @Test
    public void testFindPrincipalsContainingUnderscore() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_WILDCARD_USER);
        sync(externalUser);

        Set<? extends Principal> expected = ImmutableSet.of(new PrincipalImpl("_gr_u_"));
        Set<? extends Principal> res = ImmutableSet
                .copyOf(principalProvider.findPrincipals("_", PrincipalManager.SEARCH_TYPE_ALL));
        assertEquals(expected, res);
        Set<? extends Principal> res2 = ImmutableSet
                .copyOf(principalProvider.findPrincipals("_", false, PrincipalManager.SEARCH_TYPE_ALL, 0, -1));
        assertEquals(expected, res2);
    }

    @Test
    public void testFindPrincipalsContainingPercentSign() throws Exception {
        ExternalUser externalUser = idp.getUser(TestIdentityProvider.ID_WILDCARD_USER);
        sync(externalUser);

        Set<? extends Principal> expected = ImmutableSet.of(
                new PrincipalImpl("g%r%"));
        Set<? extends Principal> res = ImmutableSet.copyOf(principalProvider.findPrincipals("%", PrincipalManager.SEARCH_TYPE_ALL));
        assertEquals(expected, res);
        Set<? extends Principal> res2 = ImmutableSet
                .copyOf(principalProvider.findPrincipals("%", false, PrincipalManager.SEARCH_TYPE_ALL, 0, -1));
        assertEquals(expected, res2);
    }

    @Test
    public void testFindPrincipalsByTypeNotGroup() {
        Iterator<? extends Principal> iter = principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        assertSame(Collections.emptyIterator(), iter);
    }

    @Test
    public void testFindPrincipalsByTypeGroup() throws Exception {
        Set<? extends Principal> res = ImmutableSet.copyOf(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_GROUP));
        assertEquals(getExpectedAllSearchResult(USER_ID), res);

        Set<? extends Principal> res2 = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, 0, -1));
        assertEquals(getExpectedAllSearchResult(USER_ID), res2);
    }

    @Test
    public void testFindPrincipalsByTypeAll() throws Exception {
        Set<? extends Principal> res = ImmutableSet.copyOf(principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_ALL));
        assertEquals(getExpectedAllSearchResult(USER_ID), res);
    }

    @Test
    public void testFindPrincipalsFiltersDuplicates() throws Exception {
        ExternalGroup gr = idp.getGroup("a");
        ExternalUser otherUser = new TestUser("anotherUser", ImmutableSet.of(gr.getExternalId()));
        sync(otherUser);

        Set<Principal> expected = new HashSet<>();
        expected.add(new PrincipalImpl(gr.getPrincipalName()));
        long depth = syncConfig.user().getMembershipNestingDepth();
        if (depth > 1) {
            collectExpectedPrincipals(expected, gr.getDeclaredGroups(), --depth);
        }

        Iterator<? extends Principal> res = principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_ALL);
        assertTrue(res.hasNext());
        assertEquals(expected, ImmutableSet.copyOf(res));
        Iterator<? extends Principal> res2 = principalProvider.findPrincipals("a", false,
                PrincipalManager.SEARCH_TYPE_ALL, 0, -1);
        assertTrue(res2.hasNext());
        assertEquals(expected, ImmutableSet.copyOf(res2));
    }

    @Test
    public void testFindPrincipalsSorted() throws Exception {
        List<Principal> in = Arrays.asList(new PrincipalImpl("p3"), new PrincipalImpl("p1"), new PrincipalImpl("p2"));
        ExternalGroupPrincipalProvider p = new ExternalGroupPrincipalProvider(root,
                getSecurityProvider().getConfiguration(UserConfiguration.class), NamePathMapper.DEFAULT,
                ImmutableMap.of(idp.getName(), getAutoMembership())) {
            @NotNull
            @Override
            public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
                return in.iterator();
            }
        };
        List<Principal> out = ImmutableList.copyOf(p.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_ALL, 0, -1));
        Collections.sort(in, Comparator.comparing(Principal::getName));
        assertEquals(in, out);
    }

    @Test
    public void testFindPrincipalsWithOffset() throws Exception {
        Set<Principal> all = getExpectedAllSearchResult(USER_ID);

        long offset = 2;
        long expectedSize = (all.size() <= offset) ? 0 : all.size()-offset;
        Set<? extends Principal> result = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, offset, -1));
        assertEquals(expectedSize, result.size());
    }

    @Test
    public void testFindPrincipalsWithOffsetEqualsResultSize() throws Exception {
        Set<Principal> all = getExpectedAllSearchResult(USER_ID);

        Set<? extends Principal> result = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, all.size(), -1));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFindPrincipalsWithOffsetExceedsResultSize() throws Exception {
        Set<Principal> all = getExpectedAllSearchResult(USER_ID);

        Set<? extends Principal> result = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, all.size()+1, -1));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFindPrincipalsWithLimit() throws Exception {
        Set<Principal> all = getExpectedAllSearchResult(USER_ID);

        Set<? extends Principal> result = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, 0, 1));
        assertEquals(1, result.size());
    }

    @Test
    public void testFindPrincipalsWithLimitExceedsResultSize() throws Exception {
        Set<Principal> all = getExpectedAllSearchResult(USER_ID);

        Set<? extends Principal> result = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, 0, all.size()+1));
        assertEquals(all, result);
    }

    @Test
    public void testFindPrincipalsWithZeroLimit() throws Exception {
        Set<? extends Principal> result = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, 0, 0));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFindPrincipalsWithOffsetAndLimit() throws Exception {
        Set<Principal> all = getExpectedAllSearchResult(USER_ID);

        long offset = all.size()-1;
        long limit = all.size();
        Set<? extends Principal> result = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, offset, limit));
        assertEquals(1, result.size());
    }

    @Test
    public void testFindPrincipalsWithParseException() throws Exception {
        QueryEngine qe = mock(QueryEngine.class);
        when(qe.executeQuery(anyString(), anyString(), any(Map.class), any(Map.class))).thenThrow(new ParseException("fail", 0));

        Root r = when(mock(Root.class).getQueryEngine()).thenReturn(qe).getMock();
        ExternalGroupPrincipalProvider pp = new ExternalGroupPrincipalProvider(r, getUserConfiguration(), getNamePathMapper(), Collections.emptyMap());

        assertNull(pp.getPrincipal("a"));
        assertFalse(pp.findPrincipals(PrincipalManager.SEARCH_TYPE_GROUP).hasNext());
    }

    private static final class TestUser extends TestIdentityProvider.TestIdentity implements ExternalUser {

        private final Iterable<ExternalIdentityRef> declaredGroups;

        private TestUser(@NotNull String id, @NotNull Iterable<ExternalIdentityRef> declaredGroups) {
            super(id);
            this.declaredGroups = declaredGroups;
        }
        @NotNull
        @Override
        public Iterable<ExternalIdentityRef> getDeclaredGroups() {
            return declaredGroups;
        }
    }
}

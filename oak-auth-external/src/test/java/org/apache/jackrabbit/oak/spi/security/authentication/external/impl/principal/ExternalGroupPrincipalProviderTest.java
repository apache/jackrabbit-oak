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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeAware;
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

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.osgi.framework.ServiceReference;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

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
            return ImmutableSet.copyOf(idp.getUser(userId).getDeclaredGroups()).stream().map(externalIdentityRef -> {
                try {
                    return new PrincipalImpl(idp.getIdentity(externalIdentityRef).getPrincipalName());
                } catch (ExternalIdentityException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toSet());
        } else {
            Set<Principal> set = new HashSet<>();
            collectExpectedPrincipals(set, idp.getUser(userId).getDeclaredGroups(), syncConfig.user().getMembershipNestingDepth());
            return set;
        }
    }

    @NotNull 
    Set<Principal> getExpectedAllSearchResult(@NotNull String userId) throws Exception {
        if (hasDynamicGroups()) {
            return Collections.emptySet();
        } else {
            return getExpectedGroupPrincipals(userId);
        }
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
    
    private boolean hasDynamicGroups() {
        return getIdpNamesWithDynamicGroups().contains(idp.getName());
    }
    
    @NotNull
    private Set<Principal> buildExpectedPrincipals(@NotNull String principalName) {
        if (hasDynamicGroups()) {
            return Collections.emptySet();
        } else {
            return Set.of(new PrincipalImpl(principalName));
        }
    }

    @Test
    public void testProviderConstructor() {
        //Created mainly to fulfill the coverage requirements
        SyncConfigTracker emptySync = mock(SyncConfigTracker.class);
        when(emptySync.getIdpNamesWithDynamicGroups()).thenReturn(Collections.emptySet());
        when(emptySync.getServiceReferences()).thenReturn(new ServiceReference[0]);
        ExternalGroupPrincipalProvider emptyIDPProvider = new ExternalGroupPrincipalProvider(root,
                getUserManager(root),
                getNamePathMapper(),
                emptySync);

        verify(emptySync).getIdpNamesWithDynamicGroups();

        SyncConfigTracker syncWithDynGroups = mock(SyncConfigTracker.class);
        when(syncWithDynGroups.getIdpNamesWithDynamicGroups()).thenReturn(Collections.singleton(idp.getName()));
        when(syncWithDynGroups.getServiceReferences()).thenReturn(new ServiceReference[0]);
        ExternalGroupPrincipalProvider providerWithIDPNames = new ExternalGroupPrincipalProvider(root,
                getUserManager(root),
                getNamePathMapper(),
                syncWithDynGroups);

        verify(syncWithDynGroups).getIdpNamesWithDynamicGroups();
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

            if (hasDynamicGroups()) {
                // dynamic groups that have been synced into the repository don't get served by the 
                // ExternalGroupPrincipalProvider
                assertNull(principal);
            } else {
                assertNotNull(principal);
                assertTrue(principal instanceof GroupPrincipal);
            }
        }
    }

    @Test
    public void testGetPrincipalInheritedGroups() throws Exception {
        ImmutableSet<ExternalIdentityRef> declared = ImmutableSet.copyOf(idp.getUser(USER_ID).getDeclaredGroups());

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
            if (hasDynamicGroups()) {
                // dynamic groups are not served by the external-group-principal-provider
                assertNull(p);
            } else {
                assertNotNull(p);
                assertEquals(pName, p.getName());
            }
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
        
        Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(user.getPrincipal());
        Set<Principal> expected = getExpectedGroupPrincipals(USER_ID);
        assertEquals(expected, principals);
    }

    @Test
    public void testGetGroupMembershipExternalUser2() throws Exception {
        Authorizable user = getUserManager(root).getAuthorizable(USER_ID);
        assertNotNull(user);

        Set<Principal> expected = getExpectedGroupPrincipals(USER_ID);

        // same as in test before even if the principal is not a tree-based-principal
        Principal notTreeBased = new PrincipalImpl(user.getPrincipal().getName());
        Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(notTreeBased);
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
        UserManager um = getUserManager(root);
        Authorizable group = um.getAuthorizable("secondGroup");
        assertNotNull(group);

        for (Principal principal : new Principal[] {group.getPrincipal(), new PrincipalImpl(group.getPrincipal().getName())}) {
            Set<? extends Principal> principals = principalProvider.getMembershipPrincipals(principal);
            if (hasDynamicGroups()) {
                Set<Principal> expected = getExpectedGroupAutomembership(group, um);
                assertEquals(expected, principals);
            } else {
                assertTrue(principals.isEmpty());
            }
        }
    }
    
    private Set<Principal> getExpectedGroupAutomembership(@NotNull Authorizable authorizable, @NotNull UserManager um) {
        return syncConfig.group().getAutoMembership(authorizable).stream().map(id -> {
            try {
                Group gr = um.getAuthorizable(id, Group.class);
                return (gr == null) ? null : gr.getPrincipal();
            } catch (RepositoryException repositoryException) {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toSet());
    }

    @Test
    public void testGetGroupMembershipItemBasedNonExistingPrincipal() {
        Principal p = new ItemBasedPrincipal() {
            @Override
            public @NotNull String getPath() {
                return PathUtils.ROOT_PATH;
            }

            @Override
            public String getName() {
                return "principalName";
            }
        };
        assertTrue(principalProvider.getMembershipPrincipals(p).isEmpty());
    }
    
    @Test
    public void testGetGroupMembershipItemBasedLookupFails() throws Exception {
        UserManager um = spy(getUserManager(root));
        doThrow(new RepositoryException()).when(um).getAuthorizable(any(Principal.class));
        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(root, getNamePathMapper())).thenReturn(um).getMock();

        ExternalGroupPrincipalProvider pp = createPrincipalProvider(root, uc);
        Principal principal = new PrincipalImpl(um.getAuthorizable(USER_ID).getPrincipal().getName());
        assertTrue(pp.getMembershipPrincipals(principal).isEmpty());
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
        Authorizable a = mock(Authorizable.class, withSettings().extraInterfaces(User.class));
        when(a.getID()).thenReturn(USER_ID);
        when(a.getPath()).thenReturn("/path/to/non/existing/item");
        
        UserManager um = when(mock(UserManager.class).getAuthorizable(USER_ID)).thenReturn(a).getMock();
        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(root, getNamePathMapper())).thenReturn(um).getMock();

        ExternalGroupPrincipalProvider pp = createPrincipalProvider(root, uc);
        assertTrue(pp.getPrincipals(USER_ID).isEmpty());
    }

    @Test
    public void testGetPrincipalsForGroupTree() throws Exception {
        Authorizable group = getUserManager(root).createGroup("testGroup");
        Authorizable a = spy(getUserManager(root).getAuthorizable(USER_ID));
        when(a.getPath()).thenReturn(group.getPath());
        if (a instanceof  TreeAware && group instanceof TreeAware) {
            when(((TreeAware) a).getTree()).thenReturn(((TreeAware)group).getTree());
        }
        UserManager um = when(mock(UserManager.class).getAuthorizable(USER_ID)).thenReturn(a).getMock();
        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(root, getNamePathMapper())).thenReturn(um).getMock();

        ExternalGroupPrincipalProvider pp = createPrincipalProvider(root, uc);
        assertTrue(pp.getPrincipals(USER_ID).isEmpty());
    }

    @Test
    public void testGetPrincipalsLookupFails() throws Exception {
        UserManager um = when(mock(UserManager.class).getAuthorizable(anyString())).thenThrow(new RepositoryException()).getMock();
        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(root, getNamePathMapper())).thenReturn(um).getMock();

        ExternalGroupPrincipalProvider pp = createPrincipalProvider(root, uc);
        assertTrue(pp.getPrincipals(USER_ID).isEmpty());
    }

    @Test
    public void testGetPrincipalsMissingIdpName() throws Exception {
        String userPath = getUserManager(root).getAuthorizable(USER_ID).getPath();

        Tree t = root.getTree(userPath);
        t.removeProperty(REP_EXTERNAL_ID);

        ExternalGroupPrincipalProvider pp = createPrincipalProvider(root, getUserConfiguration());

        Set<String> principalNames = pp.getPrincipals(USER_ID).stream().map(Principal::getName).collect(Collectors.toSet());
        assertTrue(principalNames.isEmpty());
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
        Set<? extends Principal> expected = buildExpectedPrincipals("a");
        Set<? extends Principal> res = ImmutableSet
                .copyOf(principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_GROUP));
        assertEquals(expected, res);

        Set<? extends Principal> res2 = ImmutableSet
                .copyOf(principalProvider.findPrincipals("a", false, PrincipalManager.SEARCH_TYPE_GROUP, 0, -1));
        assertEquals(expected, res2);
    }

    @Test
    public void testFindPrincipalsByHintTypeAll() {
        Set<? extends Principal> expected = buildExpectedPrincipals("a");
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

        Set<? extends Principal> expected = buildExpectedPrincipals("_gr_u_");
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

        Set<? extends Principal> expected = buildExpectedPrincipals("g%r%");
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
        ExternalUser otherUser = new TestUser("anotherUser", Set.of(gr.getExternalId()));
        sync(otherUser);

        Set<Principal> expected = new HashSet<>();
        if (!hasDynamicGroups()) {
            expected.add(new PrincipalImpl(gr.getPrincipalName()));
            long depth = syncConfig.user().getMembershipNestingDepth();
            if (depth > 1) {
                collectExpectedPrincipals(expected, gr.getDeclaredGroups(), --depth);
            }
        }

        Iterator<? extends Principal> res = principalProvider.findPrincipals("a", PrincipalManager.SEARCH_TYPE_ALL);
        assertEquals(expected, ImmutableSet.copyOf(res));
        Iterator<? extends Principal> res2 = principalProvider.findPrincipals("a", false,
                PrincipalManager.SEARCH_TYPE_ALL, 0, -1);
        assertEquals(expected, ImmutableSet.copyOf(res2));
    }

    @Test
    public void testFindPrincipalsSorted() {
        List<Principal> in = Arrays.asList(new PrincipalImpl("p3"), new PrincipalImpl("p1"), new PrincipalImpl("p2"));
        ExternalGroupPrincipalProvider p = new ExternalGroupPrincipalProvider(root,
                getUserConfiguration(), NamePathMapper.DEFAULT, idp.getName(), syncConfig, getIdpNamesWithDynamicGroups(), false) {
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
    public void testFindPrincipalsWithLimit() {
        Set<? extends Principal> result = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, 0, 1));
        int expectedSize = (hasDynamicGroups()) ? 0 : 1;
        assertEquals(expectedSize, result.size());
    }

    @Test
    public void testFindPrincipalsWithLimitExceedsResultSize() throws Exception {
        Set<Principal> all = getExpectedAllSearchResult(USER_ID);

        Set<? extends Principal> result = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, 0, all.size()+1));
        assertEquals(all, result);
    }

    @Test
    public void testFindPrincipalsWithZeroLimit() {
        Set<? extends Principal> result = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, 0, 0));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testFindPrincipalsWithOffsetAndLimit() throws Exception {
        Set<Principal> all = getExpectedAllSearchResult(USER_ID);

        long offset = all.size()-1;
        long limit = all.size();
        Set<? extends Principal> result = ImmutableSet.copyOf(principalProvider.findPrincipals(null, false, PrincipalManager.SEARCH_TYPE_GROUP, offset, limit));
        int expectedSize = (hasDynamicGroups()) ? 0 : 1;
        assertEquals(expectedSize, result.size());
    }

    @Test
    public void testFindPrincipalsWithParseException() throws Exception {
        QueryEngine qe = mock(QueryEngine.class);
        when(qe.executeQuery(anyString(), anyString(), any(Map.class), any(Map.class))).thenThrow(new ParseException("fail", 0));

        Root r = when(mock(Root.class).getQueryEngine()).thenReturn(qe).getMock();
        ExternalGroupPrincipalProvider pp = createPrincipalProvider(r, getUserConfiguration());

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

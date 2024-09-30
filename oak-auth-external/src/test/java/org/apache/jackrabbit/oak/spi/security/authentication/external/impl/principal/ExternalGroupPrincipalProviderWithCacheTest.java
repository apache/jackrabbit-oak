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

import static org.apache.jackrabbit.oak.security.user.CacheConfiguration.PARAM_CACHE_EXPIRATION;
import static org.apache.jackrabbit.oak.security.user.CacheConfiguration.PARAM_CACHE_MAX_STALE;
import static org.apache.jackrabbit.oak.security.user.MembershipCacheConstants.REP_CACHE;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.ExternalGroupPrincipalProvider.CACHE_PRINCIPAL_NAMES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.jcr.RepositoryException;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeAware;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.osgi.framework.ServiceReference;

@RunWith(Parameterized.class)
public class ExternalGroupPrincipalProviderWithCacheTest  extends AbstractPrincipalTest {

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Lists.newArrayList(
                new Object[]{"testIdp"},
                new Object[]{""});
    }

    private final String idpName;

    public ExternalGroupPrincipalProviderWithCacheTest(String idpName) {
        this.idpName = idpName;
    }

    @Before
    public void before() throws Exception {
        super.before();

        principalProvider = createPrincipalProvider(getSystemRoot(), getUserConfiguration());
    }

    @Override
    @NotNull ExternalGroupPrincipalProvider createPrincipalProvider(@NotNull Root r, @NotNull UserConfiguration uc) {

        SyncHandlerMappingTracker mappingTracker = new SyncHandlerMappingTracker(context.bundleContext());
        SyncConfigTracker scTracker = spy(new SyncConfigTracker(context.bundleContext(), mappingTracker));

        if (!idpName.isEmpty()) {
            when(scTracker.getIdpNamesWithDynamicGroups()).thenReturn(Collections.singleton(idpName));
        } else {
            when(scTracker.getIdpNamesWithDynamicGroups()).thenReturn(Collections.emptySet());
        }
        when(scTracker.getServiceReferences()).thenReturn(new ServiceReference[] {mock(ServiceReference.class)});
        return new ExternalGroupPrincipalProvider(r, uc, getNamePathMapper(), scTracker);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(
                        PARAM_CACHE_EXPIRATION, 10000,
                        PARAM_CACHE_MAX_STALE, 10000
                )
        );
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

        root.refresh();

        Tree cacheTree = root.getTree(user.getPath()).getChild(REP_CACHE);
        assertNotNull(cacheTree);
        assertTrue(cacheTree.hasProperty(CACHE_PRINCIPAL_NAMES));

        Set<Principal> readFromCache = principalProvider.getMembershipPrincipals(user.getPrincipal());
        assertEquals(expected, readFromCache);
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

        root.refresh();

        Tree cacheTree = root.getTree(user.getPath()).getChild(REP_CACHE);
        assertNotNull(cacheTree);
        assertTrue(cacheTree.hasProperty(CACHE_PRINCIPAL_NAMES));

        Set<Principal> readFromCache = principalProvider.getMembershipPrincipals(user.getPrincipal());
        assertEquals(expected, readFromCache);
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
        if (a instanceof TreeAware && group instanceof TreeAware) {
            when(((TreeAware) a).getTree()).thenReturn(((TreeAware)group).getTree());
        }
        UserManager um = when(mock(UserManager.class).getAuthorizable(USER_ID)).thenReturn(a).getMock();
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
}

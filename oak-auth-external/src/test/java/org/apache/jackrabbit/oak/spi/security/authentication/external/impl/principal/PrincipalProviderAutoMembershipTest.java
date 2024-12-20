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
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Extension of the {@link ExternalGroupPrincipalProviderTest} with 'automembership'
 * configured in the {@link DefaultSyncConfig}.
 */
@RunWith(Parameterized.class)
public class PrincipalProviderAutoMembershipTest extends ExternalGroupPrincipalProviderTest {

    @Parameterized.Parameters(name = "name={2}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { true, false, "Nested automembership = true, Dynamic Groups = false" },
                new Object[] { false, false, "Nested automembership = false, Dynamic Groups = false" },
                new Object[] { false, true, "Nested automembership = false, Dynamic Groups = true" });
    }

    private static final String USER_AUTO_MEMBERSHIP_GROUP_ID = "testGroup-" + UUID.randomUUID();
    private static final String USER_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME = "p-" + USER_AUTO_MEMBERSHIP_GROUP_ID;

    private static final String GROUP_AUTO_MEMBERSHIP_GROUP_ID = "testGroup2-" + UUID.randomUUID();
    private static final String GROUP_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME = "p-" + GROUP_AUTO_MEMBERSHIP_GROUP_ID;

    private static final String CONFIG_AUTO_MEMBERSHIP_GROUP_ID = "testGroup3-" + UUID.randomUUID();
    private static final String CONFIG_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME = "p-" + CONFIG_AUTO_MEMBERSHIP_GROUP_ID;
    
    private static final String NON_EXISTING_GROUP_ID = "nonExistingGroup";
    private static final String NON_EXISTING_GROUP_ID2 = "nonExistingGroup2";

    private final AutoMembershipConfig amc = when(mock(AutoMembershipConfig.class).getAutoMembership(any(Authorizable.class)))
            .thenReturn(Collections.singleton(CONFIG_AUTO_MEMBERSHIP_GROUP_ID)).getMock();

    private final boolean nestedAutomembership;
    private final boolean dynamicGroups;
    
    private Group userAutoMembershipGroup;
    private Group groupAutoMembershipGroup;
    private Group configAutoMembershipGroup;
    private Group baseGroup;
    private Group baseGroup2;

    public PrincipalProviderAutoMembershipTest(boolean nestedAutomembership, boolean dynamicGroups, @NotNull String name) {
        this.nestedAutomembership = nestedAutomembership;
        this.dynamicGroups = dynamicGroups;
    }
    
    @Override
    public void before() throws Exception {
        super.before();

        UserManager uMgr = getUserManager(root);
        userAutoMembershipGroup = uMgr.createGroup(USER_AUTO_MEMBERSHIP_GROUP_ID, new PrincipalImpl(USER_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME), null);
        groupAutoMembershipGroup = uMgr.createGroup(GROUP_AUTO_MEMBERSHIP_GROUP_ID, new PrincipalImpl(GROUP_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME), null);
        configAutoMembershipGroup = uMgr.createGroup(CONFIG_AUTO_MEMBERSHIP_GROUP_ID, new PrincipalImpl(CONFIG_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME), null);
        if (nestedAutomembership) {
            baseGroup = uMgr.createGroup("base");
            baseGroup.addMember(userAutoMembershipGroup);
            baseGroup.addMember(groupAutoMembershipGroup);

            baseGroup2 = uMgr.createGroup("base2");
            baseGroup2.addMember(configAutoMembershipGroup);
        }
        root.commit();

        int expectedTimes = (dynamicGroups) ? 6 : 3;
        verify(amc, times(expectedTimes)).getAutoMembership(any(Authorizable.class));
        clearInvocations(amc);
    }

    @Override
    @NotNull
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig syncConfig = super.createSyncConfig();
        syncConfig.user()
                .setAutoMembership(USER_AUTO_MEMBERSHIP_GROUP_ID, NON_EXISTING_GROUP_ID, USER_ID)
                .setAutoMembershipConfig(getAutoMembershipConfig());
        syncConfig.group()
                .setDynamicGroups(dynamicGroups)
                .setAutoMembership(GROUP_AUTO_MEMBERSHIP_GROUP_ID, NON_EXISTING_GROUP_ID2)
                .setAutoMembershipConfig(getAutoMembershipConfig());
        return syncConfig;
    }

    @Override
    @NotNull AutoMembershipConfig getAutoMembershipConfig() {
        return amc;
    }

    @Override
    @NotNull Set<String> getIdpNamesWithDynamicGroups() {
        if (dynamicGroups) {
            return Collections.singleton(idp.getName());
        } else {
            return super.getIdpNamesWithDynamicGroups();
        }
    }

    @Override
    @NotNull
    Set<Principal> getExpectedGroupPrincipals(@NotNull String userId) throws Exception {
        Set<Principal> builder = new HashSet<>();
                builder.addAll(super.getExpectedGroupPrincipals(userId));
                builder.add(userAutoMembershipGroup.getPrincipal());
                builder.add(groupAutoMembershipGroup.getPrincipal());
        if (nestedAutomembership) {
            builder.add(baseGroup.getPrincipal());
        }
        if (USER_ID.equals(userId)) {
            builder.add(configAutoMembershipGroup.getPrincipal());
            if (nestedAutomembership) {
                builder.add(baseGroup2.getPrincipal());
            }
        }
        return Set.copyOf(builder);
    }

    @Override
    @NotNull
    Set<Principal> getExpectedAllSearchResult(@NotNull String userId) throws Exception {
        if (dynamicGroups) {
            return Collections.emptySet();
        } else {
            // not automembership principals expected when searching for principals => call super method
            return super.getExpectedGroupPrincipals(userId);
        }
    }

    @Test
    public void testGetAutoMembershipPrincipal() throws Exception {
        assertNull(principalProvider.getPrincipal(userAutoMembershipGroup.getPrincipal().getName()));
        assertNull(principalProvider.getPrincipal(groupAutoMembershipGroup.getPrincipal().getName()));
        assertNull(principalProvider.getPrincipal(USER_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME));
        assertNull(principalProvider.getPrincipal(USER_AUTO_MEMBERSHIP_GROUP_ID));
        assertNull(principalProvider.getPrincipal(GROUP_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME));
        assertNull(principalProvider.getPrincipal(GROUP_AUTO_MEMBERSHIP_GROUP_ID));
        assertNull(principalProvider.getPrincipal(CONFIG_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME));
        assertNull(principalProvider.getPrincipal(CONFIG_AUTO_MEMBERSHIP_GROUP_ID));
        assertNull(principalProvider.getPrincipal(NON_EXISTING_GROUP_ID));
        verifyNoInteractions(amc);
    }

    @Test
    public void testGetGroupPrincipals() throws Exception {
        Set<Principal> expected = getExpectedGroupPrincipals(USER_ID);

        Authorizable user = getUserManager(root).getAuthorizable(USER_ID);
        assertNotNull(user);
        
        Set<Principal> result = principalProvider.getMembershipPrincipals(user.getPrincipal());
        assertTrue(result.contains(userAutoMembershipGroup.getPrincipal()));
        assertTrue(result.contains(groupAutoMembershipGroup.getPrincipal()));
        assertTrue(result.contains(configAutoMembershipGroup.getPrincipal()));
        if (nestedAutomembership) {
            assertTrue(result.contains(baseGroup.getPrincipal()));
            assertTrue(result.contains(baseGroup2.getPrincipal()));
        }
        assertFalse(result.contains(user.getPrincipal()));
        assertEquals(expected, result);
    }

    @Test
    public void testGetGroupPrincipalsTwice() throws Exception {
        Authorizable user = getUserManager(root).getAuthorizable(USER_ID);
        assertNotNull(user);
        
        Set<Principal> result = principalProvider.getMembershipPrincipals(user.getPrincipal());
        assertEquals(result, principalProvider.getMembershipPrincipals(user.getPrincipal()));
    }

    @Test
    public void testGetGroupPrincipalsIgnoresNonGroupPrincipals() throws Exception {
        UserConfiguration uc = spy(getUserConfiguration());
        UserManager um = spy(getUserManager(root));

        Principal p = new PrincipalImpl(USER_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME);
        Group gr = when(mock(Group.class).getPrincipal()).thenReturn(p).getMock();
        when(gr.isGroup()).thenReturn(true);
        when(um.getAuthorizable(USER_AUTO_MEMBERSHIP_GROUP_ID)).thenReturn(gr);
        when(uc.getUserManager(root, NamePathMapper.DEFAULT)).thenReturn(um);

        ExternalGroupPrincipalProvider pp = createPrincipalProvider(root, uc);
        Set<Principal> result = pp.getMembershipPrincipals(um.getAuthorizable(USER_ID).getPrincipal());
        assertTrue(result.stream().map(Principal::getName).noneMatch(USER_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME::equals));
    }

    @Test
    public void testGetMembershipPrincipalsUnknownGroupPrincipal() throws Exception {
        GroupPrincipal gp = when(mock(GroupPrincipal.class).getName()).thenReturn(idp.getGroup("a").getPrincipalName()).getMock();

        assertTrue(principalProvider.getMembershipPrincipals(gp).isEmpty());
        verifyNoInteractions(gp);
    }

    @Test
    public void testGetMembershipPrincipalsExternalGroupPrincipal() throws Exception {
        UserManager um = getUserManager(root);
        User extuser = um.getAuthorizable(USER_ID, User.class);
        assertNotNull(extuser);
        
        Principal externalGroupPrincipal = getExternalGroupPrincipal(extuser.getPrincipal());
        assertNotNull(externalGroupPrincipal);
        assertEquals(dynamicGroups, externalGroupPrincipal instanceof ItemBasedPrincipal);

        Set<Principal> dynamicGroupMembership = principalProvider.getMembershipPrincipals(externalGroupPrincipal);
        if (dynamicGroups) {
            Set<Principal> expected = Set.of(groupAutoMembershipGroup.getPrincipal(), configAutoMembershipGroup.getPrincipal());
            assertEquals(expected, dynamicGroupMembership);        
        } else {
            // dynamic-groups not enabled -> group automembership not resolved.
            assertTrue(dynamicGroupMembership.isEmpty());
        }
    }
    
    private @Nullable Principal getExternalGroupPrincipal(@NotNull Principal extUserPrincipal) {
        Iterator<Principal> it = principalProvider.getMembershipPrincipals(extUserPrincipal).iterator();
        assertTrue(it.hasNext());
        while (it.hasNext()) {
            Principal p = it.next();
            if (isExternalGroupPrincipal(p)) {
                return p;
            }
        }
        return null;
    }

    @Test
    public void testGetPrincipals() throws Exception {
        Set<Principal> expected = getExpectedGroupPrincipals(USER_ID);

        Set<? extends Principal> result = principalProvider.getPrincipals(USER_ID);
        assertTrue(result.contains(userAutoMembershipGroup.getPrincipal()));
        assertTrue(result.contains(groupAutoMembershipGroup.getPrincipal()));
        assertTrue(result.contains(configAutoMembershipGroup.getPrincipal()));
        if (nestedAutomembership) {
            assertTrue(result.contains(baseGroup.getPrincipal()));
            assertTrue(result.contains(baseGroup2.getPrincipal()));
        }
        assertFalse(result.contains(getUserManager(root).getAuthorizable(USER_ID).getPrincipal()));
        assertEquals(expected, result);
    }

    @Test
    public void testFindPrincipalsByHint() throws Exception {
        List<String> hints = ImmutableList.of(
                USER_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME,
                GROUP_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME,
                USER_AUTO_MEMBERSHIP_GROUP_ID,
                GROUP_AUTO_MEMBERSHIP_GROUP_ID,
                USER_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME.substring(1, 6),
                GROUP_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME.substring(1, 6));

        for (String hint : hints) {
            Iterator<? extends Principal> res = principalProvider.findPrincipals(hint, PrincipalManager.SEARCH_TYPE_GROUP);

            assertFalse(Iterators.contains(res, userAutoMembershipGroup.getPrincipal()));
            assertFalse(Iterators.contains(res, groupAutoMembershipGroup.getPrincipal()));
            assertFalse(Iterators.contains(res, new PrincipalImpl(NON_EXISTING_GROUP_ID)));
        }
    }

}

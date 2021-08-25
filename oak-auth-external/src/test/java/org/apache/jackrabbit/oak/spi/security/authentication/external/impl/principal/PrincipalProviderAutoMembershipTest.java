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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.security.Principal;
import java.util.Collections;
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
public class PrincipalProviderAutoMembershipTest extends ExternalGroupPrincipalProviderTest {

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

    private Group userAutoMembershipGroup;
    private Group groupAutoMembershipGroup;
    private Group configAutoMembershipGroup;
    
    @Override
    public void before() throws Exception {
        super.before();

        userAutoMembershipGroup = getUserManager(root).createGroup(USER_AUTO_MEMBERSHIP_GROUP_ID, new PrincipalImpl(USER_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME), null);
        groupAutoMembershipGroup = getUserManager(root).createGroup(GROUP_AUTO_MEMBERSHIP_GROUP_ID, new PrincipalImpl(GROUP_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME), null);
        configAutoMembershipGroup = getUserManager(root).createGroup(CONFIG_AUTO_MEMBERSHIP_GROUP_ID, new PrincipalImpl(CONFIG_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME), null);
        root.commit();

        verify(amc, times(2)).getAutoMembership(any(Authorizable.class));
        clearInvocations(amc);
    }

    @Override
    @NotNull
    protected DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig syncConfig = super.createSyncConfig();
        syncConfig.user().setAutoMembership(USER_AUTO_MEMBERSHIP_GROUP_ID, NON_EXISTING_GROUP_ID, USER_ID);
        syncConfig.group().setAutoMembership(GROUP_AUTO_MEMBERSHIP_GROUP_ID, NON_EXISTING_GROUP_ID2);
        syncConfig.user().setAutoMembershipConfig(getAutoMembershipConfig());

        return syncConfig;
    }

    @Override
    AutoMembershipConfig getAutoMembershipConfig() {
        return amc;
    }

    @Override
    @NotNull
    Set<Principal> getExpectedGroupPrincipals(@NotNull String userId) throws Exception {
        ImmutableSet.Builder<Principal> builder = ImmutableSet.<Principal>builder()
                .addAll(super.getExpectedGroupPrincipals(userId))
                .add(userAutoMembershipGroup.getPrincipal())
                .add(groupAutoMembershipGroup.getPrincipal());
        if (USER_ID.equals(userId)) {
            builder.add(configAutoMembershipGroup.getPrincipal());
        }
        return builder.build();
    }

    @Override
    @NotNull
    Set<Principal> getExpectedAllSearchResult(@NotNull String userId) throws Exception {
        // not automembership principals expected when searching for principals => call super method
        return super.getExpectedGroupPrincipals(userId);
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

        ExternalGroupPrincipalProvider pp = createPrincipalProvider(uc, getAutoMembership(), getAutoMembershipConfig());
        Set<Principal> result = pp.getMembershipPrincipals(um.getAuthorizable(USER_ID).getPrincipal());
        assertTrue(result.stream().map(Principal::getName).noneMatch(USER_AUTO_MEMBERSHIP_GROUP_PRINCIPAL_NAME::equals));
    }

    @Test
    public void testGetPrincipals() throws Exception {
        Set<Principal> expected = getExpectedGroupPrincipals(USER_ID);

        Set<? extends Principal> result = principalProvider.getPrincipals(USER_ID);
        assertTrue(result.contains(userAutoMembershipGroup.getPrincipal()));
        assertTrue(result.contains(groupAutoMembershipGroup.getPrincipal()));
        assertTrue(result.contains(configAutoMembershipGroup.getPrincipal()));
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

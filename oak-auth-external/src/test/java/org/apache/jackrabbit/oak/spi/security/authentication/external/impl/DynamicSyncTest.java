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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Test combining dynamic sync, automembership and manual member relationship between external group and local group
 */
public class DynamicSyncTest extends AbstractDynamicTest {
    
    private static final String BASE_ID = "base";
    private static final String BASE2_ID = "base2";
    private static final String BASE3_ID = "base3";
    private static final String BASE4_ID = "base4";
    private static final String AUTO_GROUPS = "autoForGroups";
    private static final String AUTO_USERS = "autoForUsers";

    private Group autoForGroups;
    private Group autoForUsers;
    private Group base;
    private Group base2;
    private Group base3;

    @Override
    public void before() throws Exception {
        super.before();

        autoForGroups = userManager.getAuthorizable(AUTO_GROUPS, Group.class);
        autoForUsers = userManager.getAuthorizable(AUTO_USERS, Group.class);

        base = userManager.createGroup(BASE_ID);
        // add automembership groups
        assertTrue(base.addMembers(AUTO_GROUPS, AUTO_USERS).isEmpty());
        // add external groups as members breaching IDP boundary (Not recommended!)
        assertTrue(base.addMembers("a", "b").isEmpty());
        
        userManager.createGroup(EveryonePrincipal.getInstance());

        base2 = userManager.createGroup(BASE2_ID);
        base2.addMember(autoForUsers);

        base3 = userManager.createGroup(BASE3_ID);
        Group base4 = userManager.createGroup(BASE4_ID);
        base4.addMembers(BASE3_ID);
        
        r.commit();
    }

    @Override
    @NotNull ExternalUser syncPriorToDynamicMembership() {
        // method not needed
        return mock(ExternalUser.class);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(UserConfiguration.NAME,
                ConfigurationParameters.of(
                        ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT)
        );
    }

    @Override
    protected @NotNull DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config = super.createSyncConfig();
        config.group()
                .setDynamicGroups(true)
                .setAutoMembership(AUTO_GROUPS);
        config.user()
                .setEnforceDynamicMembership(true)
                .setMembershipNestingDepth(2)
                .setAutoMembership(AUTO_USERS);
        return config;
    }

    @Test
    public void testSyncedUser() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Authorizable user = userManager.getAuthorizable(USER_ID);
        assertNotNull(user);

        // assert membership
        Set<String> expDeclaredGroupIds = Set.of("a", "b", "c", "aa", "aaa", AUTO_GROUPS, AUTO_USERS, EveryonePrincipal.NAME);
        assertExpectedIds(expDeclaredGroupIds, user.declaredMemberOf());

        Set<String> expGroupIds = Set.of(BASE_ID, BASE2_ID, "a", "b", "c", "aa", "aaa", AUTO_GROUPS, AUTO_USERS, EveryonePrincipal.NAME);
        assertExpectedIds(expGroupIds, user.memberOf());

        // assert groups
        user.declaredMemberOf().forEachRemaining(group -> assertIsMember(group, true, user));
        user.memberOf().forEachRemaining(group -> assertIsMember(group, false, user));

        // assert principals
        List<String> principalNames = getPrincipalNames(getPrincipalManager(r).getGroupMembership(user.getPrincipal()));
        assertEquals(10, principalNames.size());
    }

    @Test
    public void testSyncedUserDifferentBaseGroups() throws Exception {
        // 'b' is member of local group 'base3' (not of 'base')
        // 'a' is member of local group 'base'
        assertTrue(base3.addMembers("b").isEmpty());
        assertTrue(base.removeMembers("b").isEmpty());
        root.commit();

        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Authorizable user = userManager.getAuthorizable(USER_ID);
        assertNotNull(user);

        // assert membership
        Set<String> expDeclaredGroupIds = Set.of("a", "b", "c", "aa", "aaa", AUTO_GROUPS, AUTO_USERS, EveryonePrincipal.NAME);
        assertExpectedIds(expDeclaredGroupIds, user.declaredMemberOf());

        Set<String> expGroupIds = Set.of(BASE_ID, BASE2_ID, BASE3_ID, BASE4_ID, "a", "b", "c", "aa", "aaa", AUTO_GROUPS, AUTO_USERS, EveryonePrincipal.NAME);
        assertExpectedIds(expGroupIds, user.memberOf());

        // assert groups
        user.declaredMemberOf().forEachRemaining(group -> assertIsMember(group, true, user));
        user.memberOf().forEachRemaining(group -> assertIsMember(group, false, user));

        // assert principals
        List<String> principalNames = getPrincipalNames(getPrincipalManager(r).getGroupMembership(user.getPrincipal()));
        assertEquals(12, principalNames.size());
    }

    @Test
    public void testSyncedUserDifferentBaseGroupsWithDuplication() throws Exception {
        // 'b' is member of local group 'base3' and 'base'
        // 'a' is member of local group 'base'
        assertTrue(base3.addMembers("b").isEmpty());
        root.commit();

        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Authorizable user = userManager.getAuthorizable(USER_ID);
        assertNotNull(user);

        // assert membership
        Set<String> expDeclaredGroupIds = Set.of("a", "b", "c", "aa", "aaa", AUTO_GROUPS, AUTO_USERS, EveryonePrincipal.NAME);
        assertExpectedIds(expDeclaredGroupIds, user.declaredMemberOf());

        Set<String> expGroupIds = Set.of(BASE_ID, BASE2_ID, BASE3_ID, BASE4_ID, "a", "b", "c", "aa", "aaa", AUTO_GROUPS, AUTO_USERS, EveryonePrincipal.NAME);
        assertExpectedIds(expGroupIds, user.memberOf());

        // assert groups
        user.declaredMemberOf().forEachRemaining(group -> assertIsMember(group, true, user));
        user.memberOf().forEachRemaining(group -> assertIsMember(group, false, user));

        // assert principals
        List<String> principalNames = getPrincipalNames(getPrincipalManager(r).getGroupMembership(user.getPrincipal()));
        assertEquals(12, principalNames.size());
    }

    @Test
    public void testSyncedGroup() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        // verify group 'a'
        Group aGroup = userManager.getAuthorizable("a", Group.class);
        assertNotNull(aGroup);
        
        assertExpectedIds(Collections.singleton(USER_ID), aGroup.getDeclaredMembers(), aGroup.getMembers());
        
        Set<String> expectedIds = Set.of(AUTO_GROUPS, BASE_ID, EveryonePrincipal.NAME);
        assertExpectedIds(expectedIds, aGroup.declaredMemberOf(), aGroup.memberOf());
    }

    @Test
    public void testAutomembershipGroups() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Authorizable user = userManager.getAuthorizable(USER_ID);
        Group aGroup = userManager.getAuthorizable("a", Group.class);

        // verify group 'autoForGroups'
        Set<String> expMemberIds = Set.of("a", "b", "c", "aa", "aaa", USER_ID);
        assertExpectedIds(expMemberIds, autoForGroups.getDeclaredMembers(), autoForGroups.getMembers());
        assertIsMember(autoForGroups, true, user, aGroup);
        assertIsMember(autoForGroups, false, user, aGroup);
        assertFalse(autoForGroups.isMember(base));
    }

    @Test
    public void testAutomembershipUsers() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Authorizable user = userManager.getAuthorizable(USER_ID);
        Group aGroup = userManager.getAuthorizable("a", Group.class);

        // verify group 'autoForUsers'
        Set<String> expMemberIds = Set.of(USER_ID);
        assertExpectedIds(expMemberIds, autoForUsers.getDeclaredMembers(), autoForUsers.getMembers());
        assertTrue(autoForUsers.isMember(user));

        assertFalse(autoForUsers.isMember(aGroup));
        assertFalse(autoForUsers.isMember(base));
    }

    @Test
    public void testInheritedBaseGroup() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Authorizable user = userManager.getAuthorizable(USER_ID);

        // verify group 'base'
        Set<String> expDeclaredMemberIds = Set.of(AUTO_GROUPS, AUTO_USERS, "a", "b");
        assertExpectedIds(expDeclaredMemberIds, base.getDeclaredMembers());
        assertFalse(base.isDeclaredMember(user));

        Set<String> expMemberIds = Set.of(USER_ID, AUTO_GROUPS, AUTO_USERS, "a", "b", "c", "aa", "aaa");
       assertExpectedIds(expMemberIds, base.getMembers());
        assertTrue(base.isMember(user));
    }

    @Test
    public void testInheritedBase2Group() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        sync(externalUser, SyncResult.Status.ADD);

        Authorizable user = userManager.getAuthorizable(USER_ID);
        
        // verify group 'base2'    
        Set<String> expDeclaredMemberIds = Set.of(AUTO_USERS);
        assertExpectedIds(expDeclaredMemberIds, base2.getDeclaredMembers());

        assertFalse(base2.isDeclaredMember(user));

        Set<String> expMemberIds = Set.of(USER_ID, AUTO_USERS);
        assertExpectedIds(expMemberIds, base2.getMembers());
        assertTrue(base2.isMember(user));
    }

    private static void assertIsMember(@NotNull Group group, boolean declared, @NotNull Authorizable... members) {
        try {
            for (Authorizable member : members) {
                if (declared) {
                    assertTrue(group.isDeclaredMember(member));
                } else {
                    assertTrue(group.isMember(member));
                }
            }
        } catch (RepositoryException e) {
            fail(e.getMessage());
        }
    }
    
    private static void assertExpectedIds(@NotNull Set<String> expectedIds, @NotNull Iterator<? extends Authorizable>... iterators) {
        for (Iterator<? extends Authorizable> it : iterators) {
            List<String> ids = getIds(it);
            assertEquals("Expected "+expectedIds+" found "+ids, expectedIds.size(), ids.size());
            assertTrue("Expected "+expectedIds+" found "+ids, ids.containsAll(expectedIds));
        }
    }
}
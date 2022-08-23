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

import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.RepositoryException;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class DynamicAutomembershipTest extends DynamicSyncContextTest {

    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return Lists.newArrayList(
                new Object[] { false, "DynamicGroups=false" },
                new Object[] { true, "DynamicGroups=true" });
    }
    
    private final boolean hasDynamicGroups;
    
    private Group group1;
    private Group group2;
    private Group group3;
    private Group groupInherited;

    public DynamicAutomembershipTest(boolean hasDynamicGroups, @NotNull String name) {
        this.hasDynamicGroups = hasDynamicGroups;
    }

    @Override
    public void before() throws Exception {
        super.before();
        
        group1 = userManager.getAuthorizable("group1", Group.class);
        group2 = userManager.getAuthorizable("group2", Group.class);
        group3 = userManager.getAuthorizable("group3", Group.class);

        groupInherited = userManager.createGroup("groupInherited");
        groupInherited.addMembers("group1", "group2");
        r.commit();
    }

    @Override
    protected @NotNull DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig config = super.createSyncConfig();
        config.group().setDynamicGroups(hasDynamicGroups);
        config.group().setAutoMembership("group1");
        config.user().setAutoMembership("group2", "group3");
        return config;
    }

    private static boolean containsGroup(@NotNull Iterator<Group> membership, @NotNull Group groupToTest) throws RepositoryException {
        String groupIdToTest = groupToTest.getID();
        Iterable<Group> iterable = () -> membership;
        return StreamSupport.stream(iterable.spliterator(), false).anyMatch(group -> {
            try {
                return groupIdToTest.equals(group.getID());
            } catch (RepositoryException repositoryException) {
                return false;
            }
        });
    }

    @Override
    @Test
    public void testSyncExternalGroup() throws Exception {
        ExternalGroup extGroup = idp.getGroup(GROUP_ID);
        assertNotNull(extGroup);
        
        syncContext.sync(extGroup);
        
        if (hasDynamicGroups) {
            Group gr = userManager.getAuthorizable(extGroup.getId(), Group.class);
            assertNotNull(gr);
            assertTrue(r.hasPendingChanges());

            // verify group1-externalGroup relationship
            assertTrue(containsGroup(gr.declaredMemberOf(), group1));
            assertTrue(containsGroup(gr.memberOf(), group1));
            assertTrue(group1.isDeclaredMember(gr));
            assertTrue(group1.isMember(gr));
            assertFalse(hasStoredMembershipInformation(r.getTree(group1.getPath()), r.getTree(gr.getPath())));

            // user-specific automembership must not be reflected.
            for (Group g : new Group[] {group2, group3}) {
                assertFalse(g.isDeclaredMember(gr));
                assertFalse(g.isMember(gr));
            }
            
            // verify inheritedGroup-externalGroup relationship
            assertFalse(containsGroup(gr.declaredMemberOf(), groupInherited));
            assertTrue(containsGroup(gr.memberOf(), groupInherited));
            assertFalse(groupInherited.isDeclaredMember(gr));
            assertTrue(groupInherited.isMember(gr));
        } else {
            assertNull(userManager.getAuthorizable(extGroup.getId()));
            assertFalse(r.hasPendingChanges());
        }
    }

    @Override
    @Test
    public void testSyncExternalUserExistingGroups() throws Exception {
        // verify group membership of the previously synced user
        Authorizable user = userManager.getAuthorizable(previouslySyncedUser.getId());
        assertSyncedMembership(userManager, user, previouslySyncedUser, Long.MAX_VALUE);

        // resync the previously synced user with dynamic-membership enabled.
        syncContext.setForceUserSync(true);
        syncConfig.user().setMembershipExpirationTime(-1);
        syncContext.sync(previouslySyncedUser);

        Tree t = r.getTree(user.getPath());
        
        assertEquals(hasDynamicGroups, t.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES));
        assertSyncedMembership(userManager, user, previouslySyncedUser);
        
        // verify automembership of the external user
        for (Group gr : new Group[] {group1, group2, group3}) {
            assertTrue(gr.isDeclaredMember(user));
            assertTrue(gr.isMember(user));
            containsGroup(user.declaredMemberOf(), gr);
            containsGroup(user.memberOf(), gr);
            
            // if 'dynamic groups' are enabled the previously synced membership information of the local group 
            // must be migrated to dynamic membership.
            boolean hasStoredMembership = hasStoredMembershipInformation(r.getTree(gr.getPath()), r.getTree(user.getPath()));
            if (hasDynamicGroups) {
                assertFalse(hasStoredMembership);
            } else {
                boolean expected = syncConfig.user().getAutoMembership().contains(gr.getID());
                assertEquals(expected, hasStoredMembership);
            }
        }
        
        // nested membership from auto-membership groups
        assertFalse(groupInherited.isDeclaredMember(user));
        assertTrue(groupInherited.isMember(user));

        Group previousGroup = userManager.getAuthorizable(previouslySyncedUser.getDeclaredGroups().iterator().next().getId(), Group.class);
        assertNotNull(previousGroup);
    }
}
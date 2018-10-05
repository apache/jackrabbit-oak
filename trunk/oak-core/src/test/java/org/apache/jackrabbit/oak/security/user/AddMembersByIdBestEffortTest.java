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
package org.apache.jackrabbit.oak.security.user;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class AddMembersByIdBestEffortTest extends AbstractAddMembersByIdTest {

    private List<Authorizable> toRemove;

    @Override
    public void after() throws Exception {
        try {
            if (toRemove != null) {
                for (Authorizable a : toRemove) {
                    a.remove();
                }
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(UserConfiguration.NAME,
                ConfigurationParameters.of(
                        ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT)
        );
    }

    /**
     * "Oddity" when adding per id + besteffort: everyone group will not be
     * dealt with separately and will end up being listed in a rep:members property.
     */
    @Test
    public void testEveryoneAsMember() throws Exception {
        UserManagerImpl userManager = (UserManagerImpl) getUserManager(root);
        Group everyone = userManager.createGroup(EveryonePrincipal.getInstance());
        try {
            Set<String> failed = testGroup.addMembers(everyone.getID());
            assertTrue(failed.isEmpty());
            root.commit();

            assertFalse(testGroup.isDeclaredMember(everyone));
            assertFalse(testGroup.isMember(everyone));
            for (Iterator<Group> it = everyone.memberOf(); it.hasNext(); ) {
                assertNotEquals(testGroup.getID(), it.next().getID());
            }
            for (Iterator<Group> it = everyone.declaredMemberOf(); it.hasNext(); ) {
                assertNotEquals(testGroup.getID(), it.next().getID());
            }

            // oddity of the current impl that add members without testing for
            boolean found = false;
            MembershipProvider mp = userManager.getMembershipProvider();
            for (Iterator<String> it = mp.getMembership(root.getTree(everyone.getPath()), true); it.hasNext(); ) {
                String p = it.next();
                if (testGroup.getPath().equals(p)) {
                    found = true;
                }
            }
            assertTrue(found);
        } finally {
            everyone.remove();
            root.commit();
        }
    }

    @Test
    public void testNonExistingMember() throws Exception {
        Set<String> failed = addNonExistingMember();
        assertTrue(failed.isEmpty());

        Iterable<String> memberIds = getMemberIds(testGroup);
        Iterables.elementsEqual(ImmutableList.copyOf(NON_EXISTING_IDS), memberIds);

        Iterator<Authorizable> members = testGroup.getDeclaredMembers();
        assertFalse(members.hasNext());

        toRemove = new ArrayList<Authorizable>(NON_EXISTING_IDS.length);
        for (String id : NON_EXISTING_IDS) {
            toRemove.add(getUserManager(root).createGroup(id));
        }
        members = testGroup.getDeclaredMembers();
        assertTrue(members.hasNext());
        for (Authorizable a : toRemove) {
            assertTrue(testGroup.isDeclaredMember(a));
        }
    }

    @Test
    public void testAddByContentID() throws Exception {
        AuthorizableBaseProvider provider = new UserProvider(root, ConfigurationParameters.of(getUserConfiguration().getParameters(), ConfigurationParameters.of(UserConstants.PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE, false)));
        Set<String> failed = testGroup.addMembers(provider.getContentID(getTestUser().getID()));
        assertTrue(failed.isEmpty());

        assertFalse(testGroup.isMember(getTestUser()));
    }

    @Test
    public void testExistingMemberWithoutAccess() throws Exception {
        Set<String> failed = addExistingMemberWithoutAccess();
        assertTrue(failed.isEmpty());

        root.refresh();
        assertTrue(testGroup.isMember(memberGroup));
    }

    /**
     * @since Oak 1.8 : for performance reasons cyclic membership is only check
     * within GroupImpl and those import-behaviours that actually resolve the id to a user/group
     */
    @Override
    @Test
    public void testCyclicMembership() throws Exception {
        memberGroup.addMember(testGroup);
        root.commit();

        Set<String> failed = testGroup.addMembers(memberGroup.getID());
        assertTrue(failed.isEmpty());

        root.commit();

        // cyclic membership must be spotted upon membership resolution
        assertEquals(1, Iterators.size(memberGroup.getMembers()));
        assertEquals(1, Iterators.size(testGroup.getMembers()));
    }

    /**
     * @since Oak 1.8 : for performance reasons cyclic membership is only check
     * within GroupImpl and those import-behaviours that actually resolve the id to a user/group
     */
    @Test
    public void testCyclicWithoutAccess() throws Exception {
        memberGroup.addMember(testGroup);
        root.commit();

        Set<String> failed = addExistingMemberWithoutAccess();
        assertTrue(failed.isEmpty());

        // cyclic membership must be spotted upon membership resolution
        root.refresh();
        UserManager uMgr = getUserManager(root);
        assertEquals(1, Iterators.size(uMgr.getAuthorizable(memberGroup.getID(), Group.class).getMembers()));
        assertEquals(1, Iterators.size(uMgr.getAuthorizable(testGroup.getID(), Group.class).getMembers()));
    }

    @Test
    public void testMemberListExistingMembers() throws Exception {
        MembershipProvider mp = ((UserManagerImpl) getUserManager(root)).getMembershipProvider();
        try {
            mp.setMembershipSizeThreshold(5);
            for (int i = 0; i < 10; i++) {
                testGroup.addMembers("member" + i);
            }

            Set<String> failed = testGroup.addMembers("member2");
            assertFalse(failed.isEmpty());

        } finally {
            mp.setMembershipSizeThreshold(100); // back to default
        }
    }
}
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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.util.Text;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link Group} implementation.
 */
public class GroupTest extends AbstractUserTest {

    private List<String> members = new ArrayList<String>();

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        group.addMember(userMgr.getAuthorizable(superuser.getUserID()));
        group.addMember(user);

        members.add(superuser.getUserID());
        members.add(user.getID());

        superuser.save();
    }

    private static void assertTrueIsMember(Iterator<Authorizable> members, Authorizable auth) throws RepositoryException {
        boolean contained = false;
        while (members.hasNext() && !contained) {
            Object next = members.next();
            assertTrue(next instanceof Authorizable);
            contained = ((Authorizable) next).getID().equals(auth.getID());
        }
        assertTrue("The given set of members must contain '" + auth.getID() + '\'', contained);
    }

    private static void assertFalseIsMember(Iterator<Authorizable> members, Authorizable auth) throws RepositoryException {
        boolean contained = false;
        while (members.hasNext() && !contained) {
            Object next = members.next();
            assertTrue(next instanceof Authorizable);
            contained = ((Authorizable) next).getID().equals(auth.getID());
        }
        assertFalse("The given set of members must not contain '" + auth.getID() + '\'', contained);
    }

    private static void assertTrueMemberOfContainsGroup(Iterator<Group> groups, Group gr) throws RepositoryException {
        boolean contained = false;
        while (groups.hasNext() && !contained) {
            Object next = groups.next();
            assertTrue(next instanceof Group);
            contained = ((Group) next).getID().equals(gr.getID());
        }
        assertTrue("All members of a group must contain that group upon 'memberOf'.", contained);
    }

    private static void assertFalseMemberOfContainsGroup(Iterator<Group> groups, Group gr) throws RepositoryException {
        boolean contained = false;
        while (groups.hasNext() && !contained) {
            Object next = groups.next();
            assertTrue(next instanceof Group);
            contained = ((Group) next).getID().equals(gr.getID());
        }
        assertFalse("All members of a group must contain that group upon 'memberOf'.", contained);
    }

    @Test
    public void testIsGroup() throws NotExecutableException, RepositoryException {
        assertTrue(group.isGroup());
    }

    @Test
    public void testGetID() throws NotExecutableException, RepositoryException {
        assertNotNull(group.getID());
        assertNotNull(userMgr.getAuthorizable(group.getID()).getID());
    }

    @Test
    public void testGetPrincipal() throws RepositoryException, NotExecutableException {
        assertNotNull(group.getPrincipal());
        assertNotNull(userMgr.getAuthorizable(group.getID()).getPrincipal());
    }

    @Test
    public void testGetPath() throws RepositoryException, NotExecutableException {
        assertNotNull(group.getPath());
        assertNotNull(userMgr.getAuthorizable(group.getID()).getPath());
        try {
            assertEquals(getNode(group, superuser).getPath(), group.getPath());
        } catch (UnsupportedRepositoryOperationException e) {
            // ok.
        }
    }

    @Test
    public void testGetDeclaredMembers() throws NotExecutableException, RepositoryException {
        Iterator<Authorizable> it = group.getDeclaredMembers();
        assertNotNull(it);
        while (it.hasNext()) {
            Authorizable a = it.next();
            assertNotNull(a);
            members.remove(a.getID());
        }
        assertTrue(members.isEmpty());
    }

    @Test
    public void testGetMembers() throws NotExecutableException, RepositoryException {
        Iterator<Authorizable> it = group.getMembers();
        assertNotNull(it);
        while (it.hasNext()) {
            assertTrue(it.next() != null);
        }
    }

    @Test
    public void testGetMembersAgainstIsMember() throws NotExecutableException, RepositoryException {
        Iterator<Authorizable> it = group.getMembers();
        while (it.hasNext()) {
            Authorizable auth = it.next();
            assertTrue(group.isMember(auth));
        }
    }

    @Test
    public void testGetMembersAgainstMemberOf() throws NotExecutableException, RepositoryException {
        Iterator<Authorizable> it = group.getMembers();
        while (it.hasNext()) {
            Authorizable auth = it.next();
            assertTrueMemberOfContainsGroup(auth.memberOf(), group);
        }
    }

    @Test
    public void testGetDeclaredMembersAgainstDeclaredMemberOf() throws NotExecutableException, RepositoryException {
        Iterator<Authorizable> it = group.getDeclaredMembers();
        while (it.hasNext()) {
            Authorizable auth = it.next();
            assertTrueMemberOfContainsGroup(auth.declaredMemberOf(), group);
        }
    }

    @Test
    public void testGetMembersContainsDeclaredMembers() throws NotExecutableException, RepositoryException {
        List<String> l = new ArrayList<String>();
        for (Iterator<Authorizable> it = group.getMembers(); it.hasNext(); ) {
            l.add(it.next().getID());
        }
        for (Iterator<Authorizable> it = group.getDeclaredMembers(); it.hasNext(); ) {
            assertTrue("All declared members must also be part of the Iterator " +
                    "returned upon getMembers()", l.contains(it.next().getID()));
        }
    }

    @Test
    public void testGetDeclaredMembersWithMemberRemoved() throws RepositoryException {
        String uid = user.getID();
        user.remove();
        superuser.save();
        user = null;

        Iterator<Authorizable> it = group.getDeclaredMembers();
        while (it.hasNext()) {
            Authorizable a = it.next();
            assertFalse(uid.equals(a.getID()));
        }
    }

    @Test
    public void testAddMember() throws NotExecutableException, RepositoryException {
        User auth = getTestUser(superuser);
        Group newGroup = null;
        try {
            newGroup = userMgr.createGroup(createGroupId());
            superuser.save();

            assertFalse(newGroup.isMember(auth));
            assertFalse(newGroup.removeMember(auth));
            superuser.save();

            assertTrue(newGroup.addMember(auth));
            superuser.save();
            assertTrue(newGroup.isMember(auth));
            assertTrue(newGroup.isMember(userMgr.getAuthorizable(auth.getID())));

        } finally {
            if (newGroup != null) {
                newGroup.removeMember(auth);
                newGroup.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testAddMembers() throws NotExecutableException, RepositoryException {
        User auth = getTestUser(superuser);
        Group newGroup = null;
        int size = 100;
        List<User> users = new ArrayList<User>(size);
        try {
            newGroup = userMgr.createGroup(createGroupId());
            superuser.save();

            for (int k = 0; k < size; k++) {
                users.add(userMgr.createUser("user_" + k, "pass_" + k));
            }
            superuser.save();

            for (User user : users) {
                assertTrue(newGroup.addMember(user));
            }
            superuser.save();

            for (User user : users) {
                assertTrue(newGroup.isMember(user));
            }

            for (User user : users) {
                assertTrue(newGroup.removeMember(user));
            }
            superuser.save();

            for (User user : users) {
                assertFalse(newGroup.isMember(user));
            }
        } finally {
            for (User user : users) {
                user.remove();
                superuser.save();
            }
            if (newGroup != null) {
                newGroup.removeMember(auth);
                newGroup.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testAddRemoveMember() throws NotExecutableException, RepositoryException {
        User auth = getTestUser(superuser);
        Group newGroup1 = null;
        Group newGroup2 = null;
        try {
            newGroup1 = userMgr.createGroup(createGroupId());
            newGroup2 = userMgr.createGroup(createGroupId());
            superuser.save();

            assertFalse(newGroup1.isMember(auth));
            assertFalse(newGroup1.removeMember(auth));
            superuser.save();
            assertFalse(newGroup2.isMember(auth));
            assertFalse(newGroup2.removeMember(auth));
            superuser.save();

            assertTrue(newGroup1.addMember(auth));
            superuser.save();
            assertTrue(newGroup1.isMember(auth));
            assertTrue(newGroup1.isMember(userMgr.getAuthorizable(auth.getID())));

            assertTrue(newGroup2.addMember(auth));
            superuser.save();
            assertTrue(newGroup2.isMember(auth));
            assertTrue(newGroup2.isMember(userMgr.getAuthorizable(auth.getID())));

            assertTrue(newGroup1.removeMember(auth));
            superuser.save();
            assertTrue(newGroup2.removeMember(auth));
            superuser.save();

            assertTrue(newGroup1.addMember(auth));
            superuser.save();
            assertTrue(newGroup1.isMember(auth));
            assertTrue(newGroup1.isMember(userMgr.getAuthorizable(auth.getID())));
            assertTrue(newGroup1.removeMember(auth));
            superuser.save();


        } finally {
            if (newGroup1 != null) {
                newGroup1.removeMember(auth);
                newGroup1.remove();
                superuser.save();
            }
            if (newGroup2 != null) {
                newGroup2.removeMember(auth);
                newGroup2.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testIsDeclaredMember() throws RepositoryException, NotExecutableException {
        User auth = getTestUser(superuser);
        Group newGroup1 = null;
        Group newGroup2 = null;
        try {
            newGroup1 = userMgr.createGroup(createGroupId());
            newGroup2 = userMgr.createGroup(createGroupId());
            superuser.save();

            assertFalse(newGroup1.isDeclaredMember(auth));
            assertFalse(newGroup2.isDeclaredMember(auth));

            assertTrue(newGroup2.addMember(auth));
            superuser.save();
            assertTrue(newGroup2.isDeclaredMember(auth));
            assertTrue(newGroup2.isDeclaredMember(userMgr.getAuthorizable(auth.getID())));

            assertTrue(newGroup1.addMember(newGroup2));
            superuser.save();
            assertTrue(newGroup1.isDeclaredMember(newGroup2));
            assertTrue(newGroup1.isDeclaredMember(userMgr.getAuthorizable(newGroup2.getID())));
            assertTrue(newGroup1.isMember(auth));
            assertTrue(newGroup1.isMember(userMgr.getAuthorizable(auth.getID())));
            assertFalse(newGroup1.isDeclaredMember(auth));
            assertFalse(newGroup1.isDeclaredMember(userMgr.getAuthorizable(auth.getID())));
        } finally {
            if (newGroup1 != null) {
                newGroup1.remove();
                superuser.save();
            }
            if (newGroup2 != null) {
                newGroup2.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testAddMemberTwice() throws NotExecutableException, RepositoryException {
        User auth = getTestUser(superuser);
        Group newGroup = null;
        try {
            newGroup = userMgr.createGroup(createGroupId());
            superuser.save();

            assertTrue(newGroup.addMember(auth));
            superuser.save();
            assertFalse(newGroup.addMember(auth));
            superuser.save();
            assertTrue(newGroup.isMember(auth));

        } finally {
            if (newGroup != null) {
                newGroup.removeMember(auth);
                newGroup.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testAddMemberModifiesMemberOf() throws NotExecutableException, RepositoryException {
        User auth = getTestUser(superuser);
        Group newGroup = null;
        try {
            newGroup = userMgr.createGroup(createGroupId());
            superuser.save();

            assertFalseMemberOfContainsGroup(auth.memberOf(), newGroup);
            assertTrue(newGroup.addMember(auth));
            superuser.save();

            assertTrueMemberOfContainsGroup(auth.declaredMemberOf(), newGroup);
            assertTrueMemberOfContainsGroup(auth.memberOf(), newGroup);
        } finally {
            if (newGroup != null) {
                newGroup.removeMember(auth);
                newGroup.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testAddMemberModifiesGetMembers() throws NotExecutableException, RepositoryException {
        User auth = getTestUser(superuser);
        Group newGroup = null;
        try {
            newGroup = userMgr.createGroup(createGroupId());
            superuser.save();

            assertFalseIsMember(newGroup.getMembers(), auth);
            assertFalseIsMember(newGroup.getDeclaredMembers(), auth);
            assertTrue(newGroup.addMember(auth));
            superuser.save();

            assertTrueIsMember(newGroup.getMembers(), auth);
            assertTrueIsMember(newGroup.getDeclaredMembers(), auth);
        } finally {
            if (newGroup != null) {
                newGroup.removeMember(auth);
                newGroup.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testIndirectMembers() throws NotExecutableException, RepositoryException {
        User user = getTestUser(superuser);
        Group newGroup = null;
        Group newGroup2 = null;
        try {
            newGroup = userMgr.createGroup(createGroupId());
            newGroup2 = userMgr.createGroup(createGroupId());
            superuser.save();

            newGroup.addMember(newGroup2);
            superuser.save();
            assertTrue(newGroup.isMember(newGroup2));

            newGroup2.addMember(user);
            superuser.save();

            // testuser must not be declared member of 'newGroup'
            assertFalseIsMember(newGroup.getDeclaredMembers(), user);
            assertFalseMemberOfContainsGroup(user.declaredMemberOf(), newGroup);

            // testuser must however be member of 'newGroup' (indirect).
            assertTrueIsMember(newGroup.getMembers(), user);
            assertTrueMemberOfContainsGroup(user.memberOf(), newGroup);

            // testuser cannot be removed from 'newGroup'
            assertFalse(newGroup.removeMember(user));
            superuser.save();
        } finally {
            if (newGroup != null) {
                newGroup.removeMember(newGroup2);
                newGroup.remove();
                superuser.save();
            }
            if (newGroup2 != null) {
                newGroup2.removeMember(user);
                newGroup2.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testMembersInPrincipal() throws NotExecutableException, RepositoryException {
        User auth = getTestUser(superuser);
        Group newGroup = null;
        Group newGroup2 = null;
        try {
            newGroup = userMgr.createGroup(createGroupId());
            newGroup2 = userMgr.createGroup(createGroupId());
            superuser.save();

            newGroup.addMember(newGroup2);
            superuser.save();
            newGroup2.addMember(auth);
            superuser.save();

            java.security.acl.Group ngPrincipal = (java.security.acl.Group) newGroup.getPrincipal();
            java.security.acl.Group ng2Principal = (java.security.acl.Group) newGroup2.getPrincipal();

            assertFalse(ng2Principal.isMember(ngPrincipal));

            // newGroup2 must be member of newGroup's principal
            assertTrue(ngPrincipal.isMember(newGroup2.getPrincipal()));

            // testuser must be member of newGroup2's and newGroup's principal (indirect)
            assertTrue(ng2Principal.isMember(auth.getPrincipal()));
            assertTrue(ngPrincipal.isMember(auth.getPrincipal()));

        } finally {
            if (newGroup != null) {
                newGroup.removeMember(newGroup2);
                newGroup.remove();
                superuser.save();
            }
            if (newGroup2 != null) {
                newGroup2.removeMember(auth);
                newGroup2.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testDeeplyNestedGroups() throws NotExecutableException, RepositoryException {
        Set<Group> groups = new HashSet<Group>();
        try {
            User auth = getTestUser(superuser);
            Group topGroup = userMgr.createGroup(createGroupId());

            // Create chain of nested groups with auth member of bottom group
            Group bottomGroup = topGroup;
            for (int k = 0; k < 100; k++) {
                Group g = userMgr.createGroup(createGroupId());
                groups.add(g);
                bottomGroup.addMember(g);
                bottomGroup = g;
            }
            bottomGroup.addMember(auth);

            // Check that every groups has exactly one member
            for (Group g : groups) {
                Iterator<Authorizable> declaredMembers = g.getDeclaredMembers();
                assertTrue(declaredMembers.hasNext());
                declaredMembers.next();
                assertFalse(declaredMembers.hasNext());
            }

            // Check that we get all members from the getMembers call
            HashSet<Group> allGroups = new HashSet<Group>(groups);
            for (Iterator<Authorizable> it = topGroup.getMembers(); it.hasNext(); ) {
                Authorizable a = it.next();
                assertTrue(a.equals(auth) || allGroups.remove(a));
            }
            assertTrue(allGroups.isEmpty());
        } finally {
            for (Group g : groups) {
                g.remove();
            }
        }
    }

    @Test
    public void testInheritedMembers() throws Exception {
        Set<Authorizable> authorizables = new HashSet<Authorizable>();
        try {
            User testUser = userMgr.createUser(createUserId(), "pw");
            authorizables.add(testUser);
            Group group1 = userMgr.createGroup(createGroupId());
            authorizables.add(group1);
            Group group2 = userMgr.createGroup(createGroupId());
            authorizables.add(group2);
            Group group3 = userMgr.createGroup(createGroupId());

            group1.addMember(testUser);
            group2.addMember(testUser);
            group3.addMember(group1);
            group3.addMember(group2);

            Iterator<Authorizable> members = group3.getMembers();
            while (members.hasNext()) {
                Authorizable a = members.next();
                assertTrue(authorizables.contains(a));
                assertTrue(authorizables.remove(a));
            }

            assertTrue(authorizables.isEmpty());
        } finally {
            for (Authorizable a : authorizables) {
                a.remove();
            }
        }
    }

    @Test
    public void testCyclicGroups() throws AuthorizableExistsException, RepositoryException, NotExecutableException {
        Group group1 = null;
        Group group2 = null;
        Group group3 = null;
        try {
            group1 = userMgr.createGroup(createGroupId());
            group2 = userMgr.createGroup(createGroupId());
            group3 = userMgr.createGroup(createGroupId());

            assertTrue(group1.addMember(group2));
            superuser.save();
            assertTrue(group2.addMember(group3));
            superuser.save();

            if (group3.addMember(group1)) {
                superuser.save();
                fail("Cyclic group membership must be detected.");
            }
        } catch (RepositoryException e) {
            assertCyclicCommitFailed(e);
        } finally {
            if (group1 != null) group1.remove();
            if (group2 != null) group2.remove();
            if (group3 != null) group3.remove();
        }
    }

    @Test
    public void testCyclicGroups2() throws AuthorizableExistsException, RepositoryException, NotExecutableException {
        Group group1 = null;
        Group group2 = null;
        Group group3 = null;
        try {
            group1 = userMgr.createGroup(createGroupId());
            group2 = userMgr.createGroup(createGroupId());
            group3 = userMgr.createGroup(createGroupId());

            assertTrue(group1.addMember(group2));
            assertTrue(group2.addMember(group3));
            if (group3.addMember(group1)) {
                // circular membership not detected => try save
                superuser.save();
                fail("Cyclic group membership must be detected.");
            } // else: success, circular membership detected upon addMember
        } catch (RepositoryException e) {
            assertCyclicCommitFailed(e);
        } finally {
            if (group1 != null) group1.remove();
            if (group2 != null) group2.remove();
            if (group3 != null) group3.remove();
        }
    }

    private static void assertCyclicCommitFailed(RepositoryException e) {
        Throwable th = e.getCause();
        if (th != null) {
            assertTrue(th instanceof CommitFailedException);
            assertEquals(31, ((CommitFailedException) th).getCode());
        }
    }

    @Test
    public void testRemoveMemberTwice() throws NotExecutableException, RepositoryException {
        User auth = getTestUser(superuser);
        Group newGroup = null;
        try {
            newGroup = userMgr.createGroup(createGroupId());
            superuser.save();

            assertTrue(newGroup.addMember(auth));
            superuser.save();
            assertTrue(newGroup.removeMember(userMgr.getAuthorizable(auth.getID())));
            superuser.save();
            assertFalse(newGroup.removeMember(auth));
            superuser.save();
        } finally {
            if (newGroup != null) {
                newGroup.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testAddItselfAsMember() throws RepositoryException, NotExecutableException {
        Group newGroup = null;
        try {
            newGroup = userMgr.createGroup(createGroupId());
            superuser.save();

            assertFalse(newGroup.addMember(newGroup));
            superuser.save();
            newGroup.removeMember(newGroup);
            superuser.save();
        } finally {
            if (newGroup != null) {
                newGroup.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testRemoveGroupIfMemberExist() throws RepositoryException, NotExecutableException {
        User auth = getTestUser(superuser);
        String newGroupId = null;

        try {
            Group newGroup = userMgr.createGroup(createGroupId());
            superuser.save();
            newGroupId = newGroup.getID();

            assertTrue(newGroup.addMember(auth));
            newGroup.remove();
            superuser.save();
        } finally {
            Group gr = (Group) userMgr.getAuthorizable(newGroupId);
            if (gr != null) {
                gr.removeMember(auth);
                gr.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testRemoveGroupClearsMembership() throws NotExecutableException, RepositoryException {
        User auth = getTestUser(superuser);
        Group newGroup = null;
        String groupId;
        try {
            newGroup = userMgr.createGroup(createGroupId());
            groupId = newGroup.getID();
            superuser.save();

            assertTrue(newGroup.addMember(auth));
            superuser.save();

            boolean isMember = false;
            Iterator<Group> it = auth.declaredMemberOf();
            while (it.hasNext() && !isMember) {
                isMember = groupId.equals(it.next().getID());
            }
            assertTrue(isMember);

        } finally {
            if (newGroup != null) {
                newGroup.remove();
                superuser.save();
            }
        }

        Iterator<Group> it = auth.declaredMemberOf();
        while (it.hasNext()) {
            assertFalse(groupId.equals(it.next().getID()));
        }

        it = auth.memberOf();
        while (it.hasNext()) {
            assertFalse(groupId.equals(it.next().getID()));
        }
    }

    @Test
    public void testMoveUserToOtherGroup() throws Exception {
        User user1  = userMgr.createUser(createUserId(), "p");
        User user2  = userMgr.createUser(createUserId(), "p");
        Group grp1 = userMgr.createGroup(createGroupId());
        Group grp2 = userMgr.createGroup(createGroupId());

        grp1.addMember(user1);
        grp1.addMember(user2);
        superuser.save();

        checkDeclaredMembers(grp1, user1.getID(), user2.getID());
        checkDeclaredMembers(grp2);
        checkDeclaredMemberOf(user1, grp1.getID());
        checkDeclaredMemberOf(user2, grp1.getID());

        grp1.removeMember(user1);
        superuser.save();

        checkDeclaredMembers(grp1, user2.getID());
        checkDeclaredMembers(grp2);
        checkDeclaredMemberOf(user1);
        checkDeclaredMemberOf(user2, grp1.getID());

        grp2.addMember(user1);
        superuser.save();

        checkDeclaredMembers(grp1, user2.getID());
        checkDeclaredMembers(grp2, user1.getID());
        checkDeclaredMemberOf(user1, grp2.getID());
        checkDeclaredMemberOf(user2, grp1.getID());
    }

    @Test
    public void testRemoveMembership() throws RepositoryException {
        String grId2 = createGroupId();
        Group gr2 = null;

        try {
            gr2 = userMgr.createGroup(grId2);
            gr2.addMember(user);
            superuser.save();

            Iterator<Group> groups = user.declaredMemberOf();
            while (groups.hasNext()) {
                Group group = groups.next();
                group.removeMember(user);
                superuser.save();
            }

            assertFalse(userMgr.getAuthorizable(group.getID(), Group.class).isDeclaredMember(user));
            assertFalse(userMgr.getAuthorizable(grId2, Group.class).isDeclaredMember(user));

            groups = user.declaredMemberOf();
            while (groups.hasNext()) {
                String id = groups.next().getID();
                assertFalse(group.getID().equals(id));
                assertFalse(grId2.equals(id));
            }
        } finally {
            if (gr2 != null) {
                gr2.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testRemoveMembershipWithDifferentSessions() throws Exception {
        String grId2 = createGroupId();
        Group gr2 = null;

        Session s2 = null;
        Session s3 = null;

        try {
            gr2 = userMgr.createGroup(grId2);
            gr2.addMember(user);
            superuser.save();

            s2 = getHelper().getReadWriteSession();
            Authorizable u2 = getUserManager(s2).getAuthorizable(user.getID());

            Iterator<Group> groups = u2.declaredMemberOf();
            while (groups.hasNext()) {
                Group group = groups.next();
                group.removeMember(u2);
            }
            s2.save();

            s3 = getHelper().getReadWriteSession();
            Authorizable u3 = getUserManager(s3).getAuthorizable(user.getID());
            assertFalse(getUserManager(s3).getAuthorizable(group.getID(), Group.class).isDeclaredMember(u3));
            assertFalse(getUserManager(s3).getAuthorizable(grId2, Group.class).isDeclaredMember(u3));

            groups = u3.declaredMemberOf();
            while (groups.hasNext()) {
                String id = groups.next().getID();
                assertFalse(group.getID().equals(id));
                assertFalse(grId2.equals(id));
            }
        } finally {
            if (gr2 != null) {
                gr2.remove();
                superuser.save();
            }
            if (s2 != null) {
                s2.logout();
            }
            if (s3 != null) {
                s3.logout();
            }
        }
    }

    public void testAddMembersById() throws Exception {
        Group newGroup = null;
        try {
            newGroup = userMgr.createGroup(createGroupId());

            Set<String> failed = group.addMembers("nonExistingMember", newGroup.getID());
            assertFalse(failed.isEmpty());
            assertTrue(group.isMember(newGroup));
        } finally {
            if (newGroup != null) {
                newGroup.remove();
                superuser.save();
            }
        }
    }

    public void testAddSelfById() throws Exception {
        Set<String> failed = group.addMembers(group.getID());
        assertFalse(failed.isEmpty());
        assertTrue(failed.contains(group.getID()));
    }

    public void testAddToEveryoneById() throws Exception {
        Group everyone = null;
        try {
            everyone = userMgr.createGroup(EveryonePrincipal.getInstance());

            Set<String> failed = everyone.addMembers(group.getID());
            assertFalse(failed.isEmpty());
            assertTrue(failed.contains(group.getID()));
        } finally {
            if (everyone != null) {
                everyone.remove();
                superuser.save();
            }
        }
    }

    public void testRemoveMembersById() throws Exception {
        Group newGroup = null;
        try {
            newGroup = userMgr.createGroup(createGroupId());

            Set<String> failed = group.removeMembers("nonExistingMember", newGroup.getID(), user.getID());
            assertFalse(failed.isEmpty());
            assertFalse(group.isMember(user));
        } finally {
            if (newGroup != null) {
                newGroup.remove();
                superuser.save();
            }
        }
    }

    public void testRemoveSelfById() throws Exception {
        Set<String> failed = group.removeMembers(group.getID());
        assertFalse(failed.isEmpty());
        assertTrue(failed.contains(group.getID()));
    }

    public void testRemoveFromEveryoneById() throws Exception {
        Group everyone = null;
        try {
            everyone = userMgr.createGroup(EveryonePrincipal.getInstance());

            Set<String> failed = everyone.removeMembers(group.getID());
            assertFalse(failed.isEmpty());
            assertTrue(failed.contains(group.getID()));
        } finally {
            if (everyone != null) {
                everyone.remove();
                superuser.save();
            }
        }
    }

    private void checkDeclaredMembers(Group grp, String ... ids) throws RepositoryException {
        TreeSet<String> members = new TreeSet<String>();
        Iterator<Authorizable> iter = grp.getMembers();
        while (iter.hasNext()) {
            members.add(iter.next().getID());
        }
        Arrays.sort(ids);
        assertEquals(
                "Group members",
                Text.implode(ids, ","),
                Text.implode(members.toArray(new String[members.size()]), ","));
    }

    private void checkDeclaredMemberOf(Authorizable auth, String ... ids) throws RepositoryException {
        TreeSet<String> members = new TreeSet<String>();
        Iterator<Group> iter = auth.declaredMemberOf();
        while (iter.hasNext()) {
            members.add(iter.next().getID());
        }
        Arrays.sort(ids);
        assertEquals(
                "Group memberships",
                Text.implode(ids, ","),
                Text.implode(members.toArray(new String[members.size()]), ","));
    }

}
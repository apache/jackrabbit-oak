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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNull;

/**
 * Tests large group and user graphs.
 * <ul>
 *     <li>{@link #NUM_USERS} users</li>
 *     <li>{@link #NUM_GROUPS} groups</li>
 *     <li>1 group with all users</li>
 *     <li>1 user with all groups</li>
 * </ul>
 *
 * @since OAK 1.0
 */
public class MembershipProviderTest extends AbstractSecurityTest {

    private static final int NUM_USERS = 1000;
    private static final int NUM_GROUPS = 1000;
    private static final int SIZE_TH = 10;

    private UserManagerImpl userMgr;
    private Set<String> testUsers = new HashSet<String>();
    private Set<String> testGroups = new HashSet<String>();

    @Before
    public void before() throws Exception {
        super.before();
        testUsers.clear();
        testGroups.clear();
        userMgr = new UserManagerImpl(root, namePathMapper, getSecurityProvider());
        // set the membership size threshold low for testing
        userMgr.getMembershipProvider().setMembershipSizeThreshold(SIZE_TH);
    }

    @After
    public void after() throws Exception {
        try {
            for (String authId: testGroups) {
                Authorizable auth = userMgr.getAuthorizable(authId);
                if (auth != null) {
                    auth.remove();
                }
            }
            for (String authId: testUsers) {
                Authorizable auth = userMgr.getAuthorizable(authId);
                if (auth != null) {
                    auth.remove();
                }
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testManyMembers() throws Exception {
        Set<String> members = new HashSet<String>();
        Group grp  = createGroup();
        for (int i=0; i<NUM_USERS; i++) {
            User usr = createUser();
            grp.addMember(usr);
            members.add(usr.getID());
        }
        root.commit();
        checkMembers(grp, members);

        // also check storage structure
        Tree tree = root.getTree(grp.getPath());
        assertEquals(
                "rep:members property must have correct number of references",
                SIZE_TH,
                tree.getProperty(UserConstants.REP_MEMBERS).count()
        );

        Tree membersList = tree.getChild(UserConstants.REP_MEMBERS_LIST);
        assertTrue(
                "rep:memberList must exist",
                membersList.exists()
        );

        assertEquals(
                "rep:memberList must have correct primary type.",
                UserConstants.NT_REP_MEMBER_REFERENCES_LIST,
                membersList.getProperty(JcrConstants.JCR_PRIMARYTYPE).getValue(Type.STRING)
        );

        assertEquals(
                "rep:memberList must have correct number of child nodes.",
                (NUM_USERS / SIZE_TH) - 1,
                membersList.getChildrenCount(Long.MAX_VALUE)
        );
    }

    @Test
    public void testManyMemberships() throws Exception {
        Set<String> memberships = new HashSet<String>();
        User usr = createUser();
        for (int i=0; i<NUM_GROUPS; i++) {
            Group grp = createGroup();
            grp.addMember(usr);
            memberships.add(grp.getID());
        }
        root.commit();

        Iterator<Group> iter = usr.declaredMemberOf();
        while (iter.hasNext()) {
            Group group = iter.next();
            Assert.assertTrue(memberships.remove(group.getID()));
        }
        assertEquals(0, memberships.size());
    }

    @Test
    public void testNestedMembers() throws Exception {
        Set<String> members = new HashSet<String>();
        Set<String> declaredMembers = new HashSet<String>();
        Group grp  = createGroup();
        for (int i=0; i<10; i++) {
            Group g1 = createGroup();
            grp.addMember(g1);
            members.add(g1.getID());
            declaredMembers.add(g1.getID());
            for (int j=0; j<10; j++) {
                Group g2 = createGroup();
                g1.addMember(g2);
                members.add(g2.getID());
                for (int k=0; k<10; k++) {
                    User usr = createUser();
                    g2.addMember(usr);
                    members.add(usr.getID());
                }
            }
        }
        root.commit();

        checkMembers(grp, members);

        Iterator<Authorizable> iter = grp.getDeclaredMembers();
        while (iter.hasNext()) {
            Authorizable member = iter.next();
            Assert.assertTrue(declaredMembers.remove(member.getID()));
        }
        assertEquals(0, declaredMembers.size());
    }

    @Test
    public void testNestedMemberships() throws Exception {
        Set<String> memberships = new HashSet<String>();
        User user  = createUser();
        Group grp = createGroup();
        memberships.add(grp.getID());
        for (int i=0; i<10; i++) {
            Group g1 = createGroup();
            grp.addMember(g1);
            memberships.add(g1.getID());
            for (int j=0; j<10; j++) {
                Group g2 = createGroup();
                g1.addMember(g2);
                memberships.add(g2.getID());
                g2.addMember(user);
            }
        }
        root.commit();

        Iterator<Group> iter = user.memberOf();
        while (iter.hasNext()) {
            Group group = iter.next();
            Assert.assertTrue(memberships.remove(group.getID()));
        }
        assertEquals(0, memberships.size());
    }

    @Test
    public void testRemoveMembers() throws Exception {
        Set<String> members = new HashSet<String>();
        String[] users = new String[NUM_USERS];
        Group grp  = createGroup();
        for (int i=0; i<NUM_USERS; i++) {
            User usr = createUser();
            grp.addMember(usr);
            members.add(usr.getID());
            users[i] = usr.getID();
        }
        root.commit();

        // remove the first TH users, this should remove all references from rep:members in the group node and remove
        // the rep:members property
        for (int i=0; i<SIZE_TH; i++) {
            Authorizable auth = userMgr.getAuthorizable(users[i]);
            members.remove(users[i]);
            grp.removeMember(auth);
        }
        root.commit();

        checkMembers(grp, members);

        // also check storage structure
        Tree tree = root.getTree(grp.getPath());
        assertNull(
                "rep:members property not exist",
                tree.getProperty(UserConstants.REP_MEMBERS)
        );

        // now add TH/2 again.
        for (int i=0; i<SIZE_TH/2; i++) {
            Authorizable auth = userMgr.getAuthorizable(users[i]);
            members.add(users[i]);
            grp.addMember(auth);
        }
        root.commit();

        assertEquals(
                "rep:members property must have correct number of references",
                SIZE_TH / 2,
                tree.getProperty(UserConstants.REP_MEMBERS).count()
        );
        checkMembers(grp, members);

        // now remove the users 20-30, this should remove the 2nd overflow node
        for (int i=2*SIZE_TH; i< (3*SIZE_TH); i++) {
            Authorizable auth = userMgr.getAuthorizable(users[i]);
            members.remove(users[i]);
            grp.removeMember(auth);
        }
        root.commit();

        checkMembers(grp, members);

        Tree membersList = tree.getChild(UserConstants.REP_MEMBERS_LIST);
        assertFalse(
                "the first overflow node must not exist",
                membersList.getChild("1").exists()
        );

        // now add 10 users and check if the "1" node exists again
        for (int i=2*SIZE_TH; i< (3*SIZE_TH); i++) {
            Authorizable auth = userMgr.getAuthorizable(users[i]);
            members.add(users[i]);
            grp.addMember(auth);
        }
        root.commit();
        checkMembers(grp, members);

        membersList = tree.getChild(UserConstants.REP_MEMBERS_LIST);
        assertTrue(
                "the first overflow node must not exist",
                membersList.getChild("1").exists()
        );
    }

    private User createUser() throws RepositoryException {
        String userId = "testUser" + testUsers.size();
        User usr = userMgr.createUser(userId, "pw");
        testUsers.add(userId);
        if ((testUsers.size() + testGroups.size()) % 100 == 0) {
            System.out.println("created " + testGroups.size() + " groups, " + testUsers.size() + " users.");
        }
        return usr;
    }

    private Group createGroup() throws RepositoryException {
        String groupName = "testGroup" + testGroups.size();
        Group grp = userMgr.createGroup(groupName);
        testGroups.add(groupName);
        if ((testUsers.size() + testGroups.size()) % 100 == 0) {
            System.out.println("created " + testGroups.size() + " groups, " + testUsers.size() + " users.");
        }
        return grp;
    }

    private void checkMembers(Group grp, Set<String> ms) throws RepositoryException {
        Set<String> members = new HashSet<String>(ms);
        Iterator<Authorizable> iter = grp.getMembers();
        while (iter.hasNext()) {
            Authorizable member = iter.next();
            Assert.assertTrue("Group must have member", members.remove(member.getID()));
        }
        assertEquals("Group must have all members", 0, members.size());
    }
}
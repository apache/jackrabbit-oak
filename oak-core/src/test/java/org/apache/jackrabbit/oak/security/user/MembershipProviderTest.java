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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests large group and user graphs.
 * <ul>
 * <li>{@link #NUM_USERS} users</li>
 * <li>{@link #NUM_GROUPS} groups</li>
 * <li>1 group with all users</li>
 * <li>1 user with all groups</li>
 * </ul>
 *
 * @since OAK 1.0
 */
public class MembershipProviderTest extends AbstractSecurityTest implements UserConstants {

    private static final int NUM_USERS = 1000;
    private static final int NUM_GROUPS = 1000;
    private static final int SIZE_TH = 10;

    private UserManagerImpl userMgr;
    private MembershipProvider mp;
    private Set<String> testUsers = new HashSet<String>();
    private Set<String> testGroups = new HashSet<String>();

    @Before
    public void before() throws Exception {
        super.before();
        userMgr = new UserManagerImpl(root, namePathMapper, getSecurityProvider());
        mp = userMgr.getMembershipProvider();
        // set the threshold low for testing
        mp.setMembershipSizeThreshold(SIZE_TH);
    }

    @After
    public void after() throws Exception {
        try {
            for (String path : Iterables.concat(testUsers, testGroups)) {
                Authorizable auth = userMgr.getAuthorizableByPath(path);
                if (auth != null) {
                    auth.remove();
                }
            }
            root.commit();
        } finally {
            testUsers.clear();
            testGroups.clear();
            super.after();
        }
    }

    @Test
    public void testManyMembers() throws Exception {
        Set<String> members = new HashSet<String>();
        Group grp = createGroup();
        for (int i = 0; i < NUM_USERS; i++) {
            User usr = createUser();
            grp.addMember(usr);
            members.add(usr.getID());
        }
        root.commit();
        assertMembers(grp, members);

        // also check storage structure
        Tree tree = root.getTree(grp.getPath());
        assertEquals(
                "rep:members property must have correct number of references",
                SIZE_TH,
                tree.getProperty(REP_MEMBERS).count()
        );

        Tree membersList = tree.getChild(REP_MEMBERS_LIST);
        assertTrue(
                "rep:memberList must exist",
                membersList.exists()
        );

        assertEquals(
                "rep:memberList must have correct primary type.",
                NT_REP_MEMBER_REFERENCES_LIST,
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
        for (int i = 0; i < NUM_GROUPS; i++) {
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
        Group grp = createGroup();
        for (int i = 0; i < 10; i++) {
            Group g1 = createGroup();
            grp.addMember(g1);
            members.add(g1.getID());
            declaredMembers.add(g1.getID());
            for (int j = 0; j < 10; j++) {
                Group g2 = createGroup();
                g1.addMember(g2);
                members.add(g2.getID());
                for (int k = 0; k < 10; k++) {
                    User usr = createUser();
                    g2.addMember(usr);
                    members.add(usr.getID());
                }
            }
        }
        root.commit();

        assertMembers(grp, members);

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
        User user = createUser();
        Group grp = createGroup();
        memberships.add(grp.getID());
        for (int i = 0; i < 10; i++) {
            Group g1 = createGroup();
            grp.addMember(g1);
            memberships.add(g1.getID());
            for (int j = 0; j < 10; j++) {
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
        Group grp = createGroup();
        for (int i = 0; i < NUM_USERS; i++) {
            User usr = createUser();
            grp.addMember(usr);
            members.add(usr.getID());
            users[i] = usr.getID();
        }
        root.commit();

        // remove the first TH users, this should remove all references from rep:members in the group node and remove
        // the rep:members property
        for (int i = 0; i < SIZE_TH; i++) {
            Authorizable auth = userMgr.getAuthorizable(users[i]);
            members.remove(users[i]);
            grp.removeMember(auth);
        }
        root.commit();

        assertMembers(grp, members);

        // also check storage structure
        Tree tree = root.getTree(grp.getPath());
        assertNull(
                "rep:members property not exist",
                tree.getProperty(REP_MEMBERS)
        );

        // now add TH/2 again.
        for (int i = 0; i < SIZE_TH / 2; i++) {
            Authorizable auth = userMgr.getAuthorizable(users[i]);
            members.add(users[i]);
            grp.addMember(auth);
        }
        root.commit();

        assertEquals(
                "rep:members property must have correct number of references",
                SIZE_TH / 2,
                tree.getProperty(REP_MEMBERS).count()
        );
        assertMembers(grp, members);

        // now remove the users 20-30, this should remove the 2nd overflow node
        for (int i = 2 * SIZE_TH; i < (3 * SIZE_TH); i++) {
            Authorizable auth = userMgr.getAuthorizable(users[i]);
            members.remove(users[i]);
            grp.removeMember(auth);
        }
        root.commit();

        assertMembers(grp, members);

        Tree membersList = tree.getChild(REP_MEMBERS_LIST);
        assertFalse(
                "the first overflow node must not exist",
                membersList.getChild("1").exists()
        );

        // now add 10 users and check if the "1" node exists again
        for (int i = 2 * SIZE_TH; i < (3 * SIZE_TH); i++) {
            Authorizable auth = userMgr.getAuthorizable(users[i]);
            members.add(users[i]);
            grp.addMember(auth);
        }
        root.commit();
        assertMembers(grp, members);

        membersList = tree.getChild(REP_MEMBERS_LIST);
        assertTrue("the first overflow node must not exist", membersList.getChild("1").exists()
        );
    }

    @Test
    public void testAddMembersAgain() throws Exception {
        Set<String> members = new HashSet<String>();
        Group grp = createGroup();
        for (int i = 0; i < NUM_USERS; i++) {
            User usr = createUser();
            grp.addMember(usr);
            members.add(usr.getID());
        }
        root.commit();

        for (String id : members) {
            assertFalse(grp.addMember(userMgr.getAuthorizable(id)));
        }
    }

    @Test
    public void testAddMembersAgainOnMembershipProvider() throws Exception {
        Set<String> memberPaths = new HashSet<String>();
        Group grp = createGroup();
        for (int i = 0; i < NUM_USERS; i++) {
            User usr = createUser();
            grp.addMember(usr);
            memberPaths.add(usr.getPath());
        }
        root.commit();

        Tree groupTree = root.getTree(grp.getPath());
        for (String path : memberPaths) {
            Tree memberTree = root.getTree(path);
            assertFalse(mp.addMember(groupTree, memberTree));
            assertFalse(mp.addMember(groupTree, memberTree));

            String memberId = TreeUtil.getString(memberTree, REP_AUTHORIZABLE_ID);
            Map<String, String> m = new HashMap<String, String>(1);
            m.put(TreeUtil.getString(memberTree, JcrConstants.JCR_UUID), memberId);

            Set<String> failed = mp.addMembers(groupTree, m);
            assertFalse(failed.isEmpty());
            assertTrue(failed.contains(memberId));
        }
    }

    @Test
    public void testIsDeclaredMemberTransient() throws Exception {
        Group g = createGroup();
        List<Authorizable> members = createMembers(g, NUM_USERS/2);

        Tree groupTree = getTree(g);

        // test declared members with transient modifications
        for (Authorizable a : members) {
            assertTrue(mp.isDeclaredMember(groupTree, getTree(a)));
        }
    }

    @Test
    public void testIsDeclaredMember() throws Exception {
        Group g = createGroup();
        List<Authorizable> members = createMembers(g, NUM_USERS/2);
        root.commit();

        Tree groupTree = getTree(g);

        // test declared members after commit
        for (Authorizable a : members) {
            assertTrue(mp.isDeclaredMember(groupTree, getTree(a)));
        }
    }

    @Test
    public void testIsDeclaredMemberFew() throws Exception {
        Group g = createGroup();
        Group m1 = createGroup();
        User m2 = createUser();

        g.addMembers(m1.getID(), m2.getID());

        Tree groupTree = getTree(g);
        assertFalse(groupTree.hasChild(REP_MEMBERS_LIST));

        // test declared members with transient modifications
        assertTrue(mp.isDeclaredMember(groupTree, getTree(m1)));
        assertTrue(mp.isDeclaredMember(groupTree, getTree(m2)));

        // ... and after commit
        root.commit();
        assertTrue(mp.isDeclaredMember(groupTree, getTree(m1)));
        assertTrue(mp.isDeclaredMember(groupTree, getTree(m2)));
    }

    @Test
    public void testIsMemberTransient() throws Exception {
        Group g = createGroup();
        Group g2 = createGroup();
        g.addMember(g2);

        List<Authorizable> members = createMembers(g2, 50);

        Tree groupTree = getTree(g);

        // test members with transient modifications
        for (Authorizable a : members) {
            Tree tree = getTree(a);
            assertFalse(mp.isDeclaredMember(groupTree, tree));
            assertTrue(mp.isMember(groupTree, tree));
        }
    }

    @Test
    public void testIsMember() throws Exception {
        Group g = createGroup();
        Group g2 = createGroup();
        g.addMember(g2);

        List<Authorizable> members = createMembers(g2, NUM_USERS/2);
        root.commit();

        // test members after commit
        Tree groupTree = getTree(g);
        for (Authorizable a : members) {
            Tree tree = getTree(a);
            assertFalse(mp.isDeclaredMember(groupTree, tree));
            assertTrue(mp.isMember(groupTree, tree));
        }
    }

    @Test
    public void testIsMemberFew() throws Exception {
        Group g = createGroup();
        Group g2 = createGroup();
        g.addMember(g2);

        User m1 = createUser();
        Group m2 = createGroup();
        g2.addMember(m1);
        g2.addMember(m2);

        Tree groupTree = getTree(g);

        // test members with transient modifications
        assertFalse(mp.isDeclaredMember(groupTree, getTree(m1)));
        assertFalse(mp.isDeclaredMember(groupTree, getTree(m2)));
        assertTrue(mp.isMember(groupTree, getTree(m1)));
        assertTrue(mp.isMember(groupTree, getTree(m2)));

        // ... and after commit
        root.commit();
        assertFalse(mp.isDeclaredMember(groupTree, getTree(m1)));
        assertFalse(mp.isDeclaredMember(groupTree, getTree(m2)));
        assertTrue(mp.isMember(groupTree, getTree(m1)));
        assertTrue(mp.isMember(groupTree, getTree(m2)));
    }

    @Test
    public void testTransientInMembersList() throws Exception {
        Group g = createGroup();
        createMembers(g, NUM_USERS/2);
        root.commit();

        // add another transient member that will end up in the members-ref-list
        User u = createUser();
        g.addMember(u);

        Tree groupTree = getTree(g);
        Tree memberTree = getTree(u);

        assertTrue(mp.isDeclaredMember(groupTree, memberTree));
        assertTrue(mp.isMember(groupTree, memberTree));

        assertFalse(Iterators.contains(mp.getMembership(memberTree, false), groupTree.getPath()));
        assertFalse(Iterators.contains(mp.getMembership(memberTree, true), groupTree.getPath()));
        root.commit();
        assertTrue(Iterators.contains(mp.getMembership(memberTree, false), groupTree.getPath()));
        assertTrue(Iterators.contains(mp.getMembership(memberTree, true), groupTree.getPath()));
    }

    @Test
    public void testNoMember() throws Exception {
        Group g = createGroup();
        Group notMember = createGroup();
        User notMember2 = createUser();

        assertFalse(g.isDeclaredMember(notMember));
        assertFalse(g.isDeclaredMember(notMember2));

        assertFalse(g.isMember(notMember));
        assertFalse(g.isMember(notMember2));

        root.commit();

        assertFalse(g.isDeclaredMember(notMember));
        assertFalse(g.isDeclaredMember(notMember2));

        assertFalse(g.isMember(notMember));
        assertFalse(g.isMember(notMember2));
    }

    @Test
    public void testAddMembersExceedThreshold() throws Exception {
        Tree groupTree = root.getTree(createGroup().getPath());

        // 1. add array of 21 memberIDs exceeding the threshold
        Map<String, String> memberIds = createIdMap(0, 21);
        mp.addMembers(groupTree, memberIds);

        PropertyState repMembers = groupTree.getProperty(REP_MEMBERS);
        assertNotNull(repMembers);
        assertEquals(SIZE_TH, repMembers.count());

        // the members-list must how have two ref-members nodes one with 10 and
        // one with a single ref-value
        assertMemberList(groupTree, 2, 1);

        // 2. add more members without reaching threshold => still 2 ref-nodes
        memberIds = createIdMap(21, 29);
        mp.addMembers(groupTree, memberIds);
        assertMemberList(groupTree, 2, 9);

        // 3. fill up second ref-members node => a new one must be created
        memberIds = createIdMap(29, 35);
        mp.addMembers(groupTree, memberIds);
        assertMemberList(groupTree, 3, 5);

        // 4. remove members from the initial set => ref nodes as before, rep:members prop on group modified
        memberIds.clear();
        memberIds.put(MembershipProvider.getContentID("member1", false), "member1");
        memberIds.put(MembershipProvider.getContentID("member2", false), "member2");
        mp.removeMembers(groupTree, Maps.newHashMap(memberIds));

        assertMemberList(groupTree, 3, 5);
        assertEquals(8, groupTree.getProperty(REP_MEMBERS).count());

        // 5. add members again => best-tree is the ref-member-node
        memberIds = createIdMap(35, 39);
        mp.addMembers(groupTree, memberIds);

        assertEquals(8, groupTree.getProperty(REP_MEMBERS).count());
        assertMemberList(groupTree, 3, 9);

        // 6. adding more members will fill up rep:members again and create new ref-node
        memberIds = createIdMap(39, 45);
        mp.addMembers(groupTree, memberIds);

        assertEquals(SIZE_TH, groupTree.getProperty(REP_MEMBERS).count());
        assertEquals(4, groupTree.getChild(REP_MEMBERS_LIST).getChildrenCount(10));
    }

    private User createUser() throws RepositoryException {
        String userId = "testUser" + testUsers.size();
        User usr = userMgr.createUser(userId, "pw");
        testUsers.add(usr.getPath());
        return usr;
    }

    private Group createGroup() throws RepositoryException {
        String groupName = "testGroup" + testGroups.size();
        Group grp = userMgr.createGroup(groupName);
        testGroups.add(grp.getPath());
        return grp;
    }

    private List<Authorizable> createMembers(@Nonnull Group g, int cnt) throws Exception {
        List<Authorizable> members = new ArrayList();
        for (int i = 0; i <= cnt; i++) {
            User u = createUser();
            Group gr = createGroup();
            g.addMembers(u.getID(), gr.getID());
            members.add(u);
            members.add(gr);
        }
        return members;
    }

    private static Map<String, String> createIdMap(int start, int end) {
        Map<String, String> memberIds = Maps.newLinkedHashMap();
        for (int i = start; i < end; i++) {
            String memberId = "member" + i;
            //TODO
            memberIds.put(MembershipProvider.getContentID(memberId, false), memberId);
        }
        return memberIds;
    }

    private static void assertMembers(Group grp, Set<String> ms) throws RepositoryException {
        Set<String> members = new HashSet<String>(ms);
        Iterator<Authorizable> iter = grp.getMembers();
        while (iter.hasNext()) {
            Authorizable member = iter.next();
            Assert.assertTrue("Group must have member", members.remove(member.getID()));
        }
        assertEquals("Group must have all members", 0, members.size());
    }

    private static void assertMemberList(@Nonnull Tree groupTree, int cntRefTrees, int cnt) {
        Tree list = groupTree.getChild(REP_MEMBERS_LIST);
        assertTrue(list.exists());
        assertEquals(cntRefTrees, list.getChildrenCount(5));
        for (Tree c : list.getChildren()) {
            PropertyState repMembers = c.getProperty(REP_MEMBERS);
            assertNotNull(repMembers);
            assertTrue(SIZE_TH == repMembers.count() || cnt == repMembers.count());
        }
    }

    private Tree getTree(@Nonnull Authorizable a) throws Exception {
        return root.getTree(a.getPath());
    }
}
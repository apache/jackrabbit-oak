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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * @since OAK 1.0
 */
public class MembershipProviderTest extends MembershipBaseTest {

    @Before
    public void before() throws Exception {
        super.before();
    }

    @Test
    public void testRemoveMember() throws Exception {
        Group gr = createGroup();
        User member = createUser();
        gr.addMember(member);

        assertTrue(mp.removeMember(getTree(gr), getTree(member)));
    }

    @Test
    public void testRemoveMembers() throws Exception {
        Group gr = createGroup();
        User member = createUser();
        Group member2 = createGroup();
        gr.addMembers(member.getID(), member2.getID());

        Map m = Maps.newHashMap();
        m.put(getContentID(member.getID()), member.getID());
        m.put(getContentID(member2.getID()), member2.getID());


        Set<String> failed = mp.removeMembers(getTree(gr), m);
        assertTrue(failed.isEmpty());
    }


    @Test
    public void testRemoveNonMember() throws Exception {
        assertFalse(mp.removeMember(getTree(createGroup()), getTree(getTestUser())));
    }

    @Test
    public void testRemoveNonMembers() throws Exception {
        Map<String, String> nonMemberIds = createIdMap(1, 5);

        Set<String> expected = Sets.newHashSet(nonMemberIds.values());
        Set<String> failed = mp.removeMembers(getTree(createGroup()), nonMemberIds);
        assertEquals(expected, failed);
    }

    @Test
    public void testAddMemberAgain() throws Exception {
        Group grp = createGroup();
        List<String> memberPaths = createMembers(grp, NUM_USERS);
        root.commit();

        Tree groupTree = root.getTree(grp.getPath());
        for (String path : memberPaths) {
            Tree memberTree = root.getTree(path);
            assertFalse(mp.addMember(groupTree, memberTree));
        }
    }

    @Test
    public void testAddMembersAgain() throws Exception {
        Group grp = createGroup();
        List<String> memberPaths = createMembers(grp, NUM_USERS);
        root.commit();

        Map<String, String> m = new HashMap();
        for (String path : memberPaths) {
            Tree memberTree = root.getTree(path);
            String memberId = TreeUtil.getString(memberTree, REP_AUTHORIZABLE_ID);
            m.put(getContentID(memberTree), memberId);
        }

        Set<String> expected = Sets.newHashSet(m.values());
        Set<String> failed = mp.addMembers(getTree(grp), m);
        assertFalse(failed.isEmpty());
        assertEquals(expected, failed);
    }

    @Test
    public void testIsDeclaredMemberTransient() throws Exception {
        Group g = createGroup();
        List<String> memberPaths = createMembers(g, NUM_USERS/2);

        // test declared members with transient modifications
        for (String path : memberPaths) {
            assertTrue(mp.isDeclaredMember(getTree(g), getTree(path)));
        }
    }

    @Test
    public void testIsDeclaredMember() throws Exception {
        Group g = createGroup();
        List<String> memberPaths = createMembers(g, NUM_USERS/2);
        root.commit();

        // test declared members after commit
        for (String path : memberPaths) {
            assertTrue(mp.isDeclaredMember(getTree(g), getTree(path)));
        }
    }

    @Test
    public void testIsDeclaredMemberFew() throws Exception {
        Group g = createGroup();
        Group m1 = createGroup();
        User m2 = createUser();

        g.addMembers(m1.getID(), m2.getID());

        Tree groupTree = getTree(g);

        // test declared members with transient modifications
        assertTrue(mp.isDeclaredMember(groupTree, getTree(m1)));
        assertTrue(mp.isDeclaredMember(groupTree, getTree(m2)));

        // ... and after commit
        root.commit();
        assertTrue(mp.isDeclaredMember(groupTree, getTree(m1)));
        assertTrue(mp.isDeclaredMember(groupTree, getTree(m2)));
    }

    @Test
    public void testIsDeclaredMemberMissingMembersProperty() throws Exception {
        Tree grTree = getTree(createGroup());
        Tree memberTree = getTree(createUser());

        NodeUtil memberList = new NodeUtil(grTree).addChild(REP_MEMBERS_LIST, NT_REP_MEMBER_REFERENCES_LIST);
        memberList.addChild("member1", NT_REP_MEMBER_REFERENCES).setStrings(REP_MEMBERS, getContentID(memberTree));

        assertTrue(mp.isDeclaredMember(grTree, memberTree));
    }

    @Test
    public void testNotIsDeclaredMemberMissingMembersProperty() throws Exception {
        Tree grTree = getTree(createGroup());
        Tree memberTree = getTree(createUser());

        NodeUtil memberList = new NodeUtil(grTree).addChild(REP_MEMBERS_LIST, NT_REP_MEMBER_REFERENCES_LIST);
        memberList.addChild("member1", NT_REP_MEMBER_REFERENCES).setStrings(REP_MEMBERS, getContentID("another"));

        assertFalse(mp.isDeclaredMember(grTree, memberTree));
    }

    @Test
    public void testIsMemberTransient() throws Exception {
        Group g = createGroup();
        Group g2 = createGroup();
        g.addMember(g2);

        List<String> memberPaths = createMembers(g2, NUM_USERS);

        Tree groupTree = getTree(g);

        // test members with transient modifications
        for (String path : memberPaths) {
            Tree tree = getTree(path);
            assertFalse(mp.isDeclaredMember(groupTree, tree));
            assertTrue(mp.isMember(groupTree, tree));
        }
    }

    @Test
    public void testIsMember() throws Exception {
        Group g = createGroup();
        Group g2 = createGroup();
        g.addMember(g2);

        List<String> memberPaths = createMembers(g2, NUM_USERS/2);
        root.commit();

        // test members after commit
        Tree groupTree = getTree(g);
        for (String path : memberPaths) {
            Tree tree = getTree(path);
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
        Tree groupTree = getTree(createGroup());
        Tree notMember = getTree(createGroup());
        Tree notMember2 = getTree(createUser());

        assertFalse(mp.isDeclaredMember(groupTree, notMember));
        assertFalse(mp.isDeclaredMember(groupTree, notMember2));

        assertFalse(mp.isMember(groupTree, notMember));
        assertFalse(mp.isMember(groupTree, notMember2));

        root.commit();

        assertFalse(mp.isDeclaredMember(groupTree, notMember));
        assertFalse(mp.isDeclaredMember(groupTree, notMember2));

        assertFalse(mp.isMember(groupTree, notMember));
        assertFalse(mp.isMember(groupTree, notMember2));
    }

    @Test
    public void testGetMembersWithRemoved() throws Exception {
        Group g = createGroup();
        Group member = createGroup();
        g.addMember(member);
        member.remove();
        root.commit();

        assertFalse(mp.getMembers(getTree(g), false).hasNext());
        assertFalse(mp.getMembers(getTree(g), true).hasNext());
    }

    @Test
    public void testGetMembersInherited() throws Exception {
        Group g = createGroup();
        Group member = createGroup();
        g.addMember(member);
        member.addMember(createUser());
        root.commit();

        Iterator<String> res = mp.getMembers(getTree(g), true);
        assertEquals(2, Iterators.size(res));
    }

    @Test
    public void testGetMembersWithDuplicates() throws Exception {
        Group g = createGroup();
        Group member = createGroup();
        g.addMember(member);

        User user = createUser();
        member.addMember(user);
        g.addMember(user);
        root.commit();

        Iterator<String> res = mp.getMembers(getTree(g), true);
        assertEquals(2, Iterators.size(res));
    }

    @Test
    public void testGetMembers() throws Exception {
        Group g = createGroup();
        Group member = createGroup();
        g.addMember(member);
        member.addMember(createUser());
        root.commit();

        Iterator<String> res = mp.getMembers(getTree(g), false);
        assertEquals(1, Iterators.size(res));
    }

    @Test
    public void testGetMembershipInherited() throws Exception {
        User user = createUser();
        Group g = createGroup();
        Group member = createGroup();
        Group member2 = createGroup();

        g.addMember(member);
        member.addMember(member2);
        member2.addMember(user);

        root.commit();

        Iterator<String> res = mp.getMembership(getTree(user), true);
        assertEquals(3, Iterators.size(res));
    }

    @Test
    public void testGetMembershipWithDuplicates() throws Exception {
        User user = createUser();
        Group g = createGroup();
        Group member = createGroup();
        Group member2 = createGroup();

        g.addMember(member);
        g.addMember(member2);
        member.addMember(member2);
        member2.addMember(user);

        root.commit();

        Iterator<String> res = mp.getMembership(getTree(user), true);
        assertEquals(3, Iterators.size(res));
    }

    @Test
    public void testGetMembership() throws Exception {
        User user = createUser();
        Group g = createGroup();
        Group member = createGroup();
        Group member2 = createGroup();

        g.addMember(member);
        member.addMember(member2);
        member2.addMember(user);

        root.commit();

        Iterator<String> res = mp.getMembership(getTree(user), false);
        assertEquals(1, Iterators.size(res));
    }
}
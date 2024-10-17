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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import javax.jcr.RepositoryException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.clearInvocations;

public abstract class MembershipBaseTest extends AbstractUserTest implements UserConstants {

    static final int SIZE_TH = 10;

    static final int NUM_USERS = 50;
    static final int NUM_GROUPS = 50;

    UserManagerImpl userMgr;
    MembershipProvider mp;

    private final Set<String> testUsers = new HashSet<>();
    private final Set<String> testGroups = new HashSet<>();

    @Before
    public void before() throws Exception {
        super.before();
        userMgr = createUserManagerImpl(root);
        mp = userMgr.getMembershipProvider();
        // set the threshold low for testing
        mp.setMembershipSizeThreshold(SIZE_TH);
    }

    @After
    public void after() throws Exception {
        try {
            clearInvocations(monitor);
            root.refresh();
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

    @NotNull
    User createUser() throws RepositoryException {
        String userId = "testUser" + testUsers.size();
        User usr = userMgr.createUser(userId, "pw");
        testUsers.add(usr.getPath());
        return usr;
    }

    @NotNull
    Group createGroup() throws RepositoryException {
        String groupName = "testGroup" + testGroups.size();
        Group grp = userMgr.createGroup(groupName);
        testGroups.add(grp.getPath());
        return grp;
    }


    @NotNull
    List<String> createMembers(@NotNull Group g, int cnt) throws Exception {
        List<String> memberPaths = new ArrayList<>();
        for (int i = 0; i <= cnt; i++) {
            User u = createUser();
            Group gr = createGroup();
            g.addMembers(u.getID(), gr.getID());
            memberPaths.add(u.getPath());
            memberPaths.add(gr.getPath());
        }
        return memberPaths;
    }

    @NotNull
    Map<String, String> createIdMap(int start, int end) {
        Map<String, String> memberIds = new LinkedHashMap<>();
        for (int i = start; i < end; i++) {
            String memberId = "member" + i;
            memberIds.put(getContentID(memberId), memberId);
        }
        return memberIds;
    }

    @NotNull
    String getContentID(@NotNull String memberId) {
        return userMgr.getMembershipProvider().getContentID(memberId);
    }

    @Nullable
    String getContentID(@NotNull Tree tree) {
        return TreeUtil.getString(tree, JcrConstants.JCR_UUID);
    }

    @NotNull
    Tree getTree(@NotNull Authorizable a) throws Exception {
        return root.getTree(a.getPath());
    }

    @NotNull
    Tree getTree(@NotNull String path) {
        return root.getTree(path);
    }

    static void assertMembers(Group grp, Set<String> ms) throws RepositoryException {
        Set<String> members = new HashSet<>(ms);
        Iterator<Authorizable> iter = grp.getMembers();
        while (iter.hasNext()) {
            Authorizable member = iter.next();
            Assert.assertTrue("Group must have member", members.remove(member.getID()));
        }
        assertEquals("Group must have all members", 0, members.size());
    }

    static void assertMemberList(@NotNull Tree groupTree, int cntRefTrees, int cnt) {
        Tree list = groupTree.getChild(REP_MEMBERS_LIST);
        assertTrue(list.exists());
        assertEquals(cntRefTrees, list.getChildrenCount(5));
        for (Tree c : list.getChildren()) {
            PropertyState repMembers = c.getProperty(REP_MEMBERS);
            assertNotNull(repMembers);
            assertTrue(SIZE_TH == repMembers.count() || cnt == repMembers.count());
        }
    }

}

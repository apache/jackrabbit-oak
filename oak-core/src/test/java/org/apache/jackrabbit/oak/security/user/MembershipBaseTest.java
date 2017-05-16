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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public abstract class MembershipBaseTest extends AbstractSecurityTest implements UserConstants {

    static final int SIZE_TH = 10;

    static final int NUM_USERS = 50;
    static final int NUM_GROUPS = 50;

    UserManagerImpl userMgr;
    MembershipProvider mp;

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

    @Nonnull
    User createUser() throws RepositoryException {
        String userId = "testUser" + testUsers.size();
        User usr = userMgr.createUser(userId, "pw");
        testUsers.add(usr.getPath());
        return usr;
    }

    @Nonnull
    Group createGroup() throws RepositoryException {
        String groupName = "testGroup" + testGroups.size();
        Group grp = userMgr.createGroup(groupName);
        testGroups.add(grp.getPath());
        return grp;
    }


    @Nonnull
    List<String> createMembers(@Nonnull Group g, int cnt) throws Exception {
        List<String> memberPaths = new ArrayList();
        for (int i = 0; i <= cnt; i++) {
            User u = createUser();
            Group gr = createGroup();
            g.addMembers(u.getID(), gr.getID());
            memberPaths.add(u.getPath());
            memberPaths.add(gr.getPath());
        }
        return memberPaths;
    }

    @Nonnull
    Map<String, String> createIdMap(int start, int end) {
        Map<String, String> memberIds = Maps.newLinkedHashMap();
        for (int i = start; i < end; i++) {
            String memberId = "member" + i;
            memberIds.put(getContentID(memberId), memberId);
        }
        return memberIds;
    }

    @Nonnull
    String getContentID(@Nonnull String memberId) {
        return userMgr.getMembershipProvider().getContentID(memberId);
    }

    @CheckForNull
    String getContentID(@Nonnull Tree tree) {
        return TreeUtil.getString(tree, JcrConstants.JCR_UUID);
    }

    @Nonnull
    Tree getTree(@Nonnull Authorizable a) throws Exception {
        return root.getTree(a.getPath());
    }

    @Nonnull
    Tree getTree(@Nonnull String path) throws Exception {
        return root.getTree(path);
    }

    static void assertMembers(Group grp, Set<String> ms) throws RepositoryException {
        Set<String> members = new HashSet<String>(ms);
        Iterator<Authorizable> iter = grp.getMembers();
        while (iter.hasNext()) {
            Authorizable member = iter.next();
            Assert.assertTrue("Group must have member", members.remove(member.getID()));
        }
        assertEquals("Group must have all members", 0, members.size());
    }

    static void assertMemberList(@Nonnull Tree groupTree, int cntRefTrees, int cnt) {
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
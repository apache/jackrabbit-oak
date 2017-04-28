/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.upgrade;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

public class RepositoryGroupMemberUpgradeTest extends AbstractRepositoryUpgradeTest {

    protected static final String TEST_USER_PREFIX = "TestUser-";
    protected static final String TEST_GROUP_PREFIX = "TestGroup-";

    private static final String REPO_CONFIG = "repository-groupmember.xml";

    @Override
    public InputStream getRepositoryConfig() {
        return getClass().getClassLoader().getResourceAsStream(REPO_CONFIG);
    }

    public int getNumUsers() {
        return 5;
    }

    public int getNumGroups() {
        return 2;
    }

    @Override
    protected void createSourceContent(Session session) throws Exception {
        UserManager userMgr = ((JackrabbitSession) session).getUserManager();
        userMgr.autoSave(false);
        User users[] = new User[getNumUsers()];
        for (int i = 0; i < users.length; i++) {
            String userId = TEST_USER_PREFIX + i;
            users[i] = userMgr.createUser(userId, userId);
        }

        for (int i = 0; i < getNumGroups(); i++) {
            Group g = userMgr.createGroup(TEST_GROUP_PREFIX + i);
            for (User user : users) {
                g.addMember(user);
            }
        }
        session.save();
    }

    @Test
    public void verifyGroupNodeTypes() throws Exception {
        JackrabbitSession session = createAdminSession();
        try {
            UserManager userMgr = session.getUserManager();
            for (int i = 0; i < getNumGroups(); i++) {
                Group grp = (Group) userMgr.getAuthorizable(TEST_GROUP_PREFIX + i);
                assertNotNull(grp);
                Node grpNode = session.getNode(grp.getPath());
                NodeType nt = grpNode.getPrimaryNodeType();
                assertEquals("Migrated group needs to be rep:Group", UserConstants.NT_REP_GROUP, nt.getName());
                assertTrue("Migrated group needs to be new node type", nt.isNodeType(UserConstants.NT_REP_MEMBER_REFERENCES));
            }
        } finally {
            session.logout();
        }
    }

    @Test
    public void verifyMembers() throws Exception {
        JackrabbitSession session = createAdminSession();
        try {
            UserManager userMgr = session.getUserManager();
            for (int i = 0; i < getNumGroups(); i++) {
                Group grp = (Group) userMgr.getAuthorizable(TEST_GROUP_PREFIX + i);
                assertNotNull(grp);

                // check if groups have all members
                Set<String> testUsers = new HashSet<String>();
                for (int j = 0; j < getNumUsers(); j++) {
                    testUsers.add(TEST_USER_PREFIX + j);
                }
                Iterator<Authorizable> declaredMembers = grp.getDeclaredMembers();
                while (declaredMembers.hasNext()) {
                    Authorizable auth = declaredMembers.next();
                    assertTrue("group must have member " + auth.getID(), testUsers.remove(auth.getID()));
                }
                assertEquals("group must have all members", 0, testUsers.size());
            }
        } finally {
            session.logout();
        }
    }

    @Test
    public void verifyMemberOf() throws Exception {
        JackrabbitSession session = createAdminSession();
        try {
            UserManager userMgr = session.getUserManager();
            for (int i = 0; i < getNumUsers(); i++) {
                User user = (User) userMgr.getAuthorizable(TEST_USER_PREFIX + i);
                assertNotNull(user);

                Set<String> groups = new HashSet<String>();
                for (int j = 0; j < getNumGroups(); j++) {
                    groups.add(TEST_GROUP_PREFIX + j);
                }

                Iterator<Group> groupIterator = user.declaredMemberOf();
                while (groupIterator.hasNext()) {
                    Group grp = groupIterator.next();
                    assertTrue("user must be member of group " + grp.getID(), groups.remove(grp.getID()));
                }
                assertEquals("user " + user.getID() + " must be member of all groups", 0, groups.size());
            }
        } finally {
            session.logout();
        }
    }

}

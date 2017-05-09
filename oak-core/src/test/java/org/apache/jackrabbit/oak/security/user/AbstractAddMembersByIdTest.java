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

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractAddMembersByIdTest extends AbstractSecurityTest {

    static final String[] NON_EXISTING_IDS = new String[] {"nonExisting1", "nonExisting2"};

    Group testGroup;
    Group memberGroup;

    @Override
    public void before() throws Exception {
        super.before();

        UserManager uMgr = getUserManager(root);
        for (String id : NON_EXISTING_IDS) {
            assertNull(uMgr.getAuthorizable(id));
        }

        testGroup = uMgr.createGroup("testGroup" + UUID.randomUUID().toString());
        memberGroup = uMgr.createGroup("memberGroup" + UUID.randomUUID().toString());
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();

            if (testGroup != null) {
                testGroup.remove();
            }
            if (memberGroup != null) {
                memberGroup.remove();
            }
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Nonnull
    Iterable<String> getMemberIds(@Nonnull Group group) throws RepositoryException {
        // test group tree
        Tree groupTree = root.getTree(group.getPath());
        PropertyState membersProp = groupTree.getProperty(UserConstants.REP_MEMBERS);
        assertNotNull(membersProp);
        return membersProp.getValue(Type.WEAKREFERENCES);
    }

    @Nonnull
    Set<String> addNonExistingMember() throws Exception {
        return testGroup.addMembers(NON_EXISTING_IDS);
    }

    Set<String> addExistingMemberWithoutAccess() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testGroup.getPath());
        if (acl != null) {
            if (acl.addEntry(getTestUser().getPrincipal(), privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_USER_MANAGEMENT), true)) {
                acMgr.setPolicy(testGroup.getPath(), acl);
                root.commit();
            }
        }

        String userId = getTestUser().getID();
        ContentSession testSession = null;
        try {
            testSession = login(new SimpleCredentials(userId, userId.toCharArray()));
            Root testRoot = testSession.getLatestRoot();

            assertFalse(testRoot.getTree(memberGroup.getPath()).exists());

            Group gr = getUserManager(testRoot).getAuthorizable(testGroup.getID(), Group.class);
            Set<String> failed = gr.addMembers(memberGroup.getID());
            testRoot.commit();
            return failed;
        } finally {
            if (testSession != null) {
                testSession.close();
            }
        }
    }

    @Test
    public void testGroupMembers() throws Exception {
        Set<String> failed = testGroup.addMembers(memberGroup.getID());
        assertTrue(failed.isEmpty());

        root.commit();

        assertTrue(testGroup.isDeclaredMember(memberGroup));
    }

    @Test
    public void testUserMembers() throws Exception {
        Set<String> failed = testGroup.addMembers(getTestUser().getID());
        assertTrue(failed.isEmpty());

        root.commit();

        assertTrue(testGroup.isDeclaredMember(getTestUser()));
    }

    @Test
    public void testMixedMembers() throws Exception {
        Set<String> failed = testGroup.addMembers(getTestUser().getID(), memberGroup.getID());
        assertTrue(failed.isEmpty());

        root.commit();

        assertTrue(testGroup.isDeclaredMember(getTestUser()));
        assertTrue(testGroup.isDeclaredMember(memberGroup));
    }

    @Test
    public void testNoMembers() throws Exception {
        Set<String> failed = testGroup.addMembers();
        assertTrue(failed.isEmpty());
    }


    @Test(expected = ConstraintViolationException.class)
    public void testEmptyId() throws Exception {
        testGroup.addMembers("");
    }

    @Test(expected = ConstraintViolationException.class)
    public void testValidAndEmptyId() throws Exception {
        testGroup.addMembers(getTestUser().getID(), "");
    }

    @Test
    public void testValidAndEmptyIdModifiesRoot() throws Exception {
        try {
            testGroup.addMembers(getTestUser().getID(), "");
        } catch (ConstraintViolationException e) {
            // expected
        }
        // no modifications expected as testing for empty id is done before changes are made
        assertFalse(root.hasPendingChanges());
    }

    @Test(expected = NullPointerException.class)
    public void testNullId() throws Exception {
        testGroup.addMembers(null);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testValidAndNullId() throws Exception {
        testGroup.addMembers(getTestUser().getID(), null);
    }

    @Test
    public void testValidAndNullIdModifiesRoot() throws Exception {
        try {
            testGroup.addMembers(getTestUser().getID(), null);
        } catch (ConstraintViolationException e) {
            // expected
        }
        // no modifications expected as testing for null id is done before changes are made
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testSameId() throws Exception {
        Set<String> failed = testGroup.addMembers(testGroup.getID());
        assertEquals(1, failed.size());
        assertTrue(failed.contains(testGroup.getID()));
    }

    @Test
    public void testTwice() throws Exception {
        Set<String> failed = testGroup.addMembers(memberGroup.getID(), memberGroup.getID());
        assertTrue(failed.isEmpty());

        assertTrue(testGroup.isDeclaredMember(memberGroup));

        Iterable<String> memberIds = getMemberIds(testGroup);
        assertEquals(1, Iterables.size(memberIds));
    }

    @Test
    public void testCyclicMembership() throws Exception {
        memberGroup.addMember(testGroup);
        root.commit();

        Set<String> failed = testGroup.addMembers(memberGroup.getID());
        assertFalse(failed.isEmpty());
        assertTrue(failed.contains(memberGroup.getID()));
    }

    @Test
    public void testAlreadyMember() throws Exception {
        testGroup.addMember(getTestUser());
        testGroup.addMember(memberGroup);

        Set<String> failed = testGroup.addMembers(getTestUser().getID(), memberGroup.getID());
        assertEquals(2, failed.size());
    }

    @Test
    public void testEveryoneAsMember() throws Exception {
        UserManagerImpl userManager = (UserManagerImpl) getUserManager(root);
        Group everyone = userManager.createGroup(EveryonePrincipal.getInstance());
        try {
            Set<String> failed = testGroup.addMembers(everyone.getID());
            assertFalse(failed.isEmpty());
            assertTrue(failed.contains(everyone.getID()));
            root.commit();

            assertFalse(testGroup.isDeclaredMember(everyone));
            assertFalse(testGroup.isMember(everyone));
            for (Iterator<Group> it = everyone.memberOf(); it.hasNext(); ) {
                assertNotEquals(testGroup.getID(), it.next().getID());
            }
            for (Iterator<Group> it = everyone.declaredMemberOf(); it.hasNext(); ) {
                assertNotEquals(testGroup.getID(), it.next().getID());
            }

            boolean found = false;
            MembershipProvider mp = userManager.getMembershipProvider();
            for (Iterator<String> it = mp.getMembership(root.getTree(everyone.getPath()), true); it.hasNext(); ) {
                String p = it.next();
                if (testGroup.getPath().equals(p)) {
                    found = true;
                }
            }
            assertFalse(found);
        } finally {
            everyone.remove();
            root.commit();
        }
    }
}
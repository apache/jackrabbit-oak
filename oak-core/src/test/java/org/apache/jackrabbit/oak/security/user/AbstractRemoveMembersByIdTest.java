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

import java.util.Set;
import java.util.UUID;

import javax.jcr.SimpleCredentials;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractRemoveMembersByIdTest extends AbstractSecurityTest {

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
        testGroup.addMember(memberGroup);
        testGroup.addMember(getTestUser());
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

    Set<String> removeNonExistingMember() throws Exception {
        return testGroup.removeMembers(NON_EXISTING_IDS);
    }

    Set<String> removeExistingMemberWithoutAccess() throws Exception {
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
            Set<String> failed = gr.removeMembers(memberGroup.getID());
            testRoot.commit();
            return failed;
        } finally {
            if (testSession != null) {
                testSession.close();
            }
        }
    }

    @Test
    public void testUserMember() throws Exception {
        Set<String> failed = testGroup.removeMembers(getTestUser().getID());
        assertTrue(failed.isEmpty());
    }

    @Test
    public void testGroupMember() throws Exception {
        Set<String> failed = testGroup.removeMembers(memberGroup.getID());
        assertTrue(failed.isEmpty());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEmptyId() throws Exception {
        testGroup.removeMembers("");
    }

    @Test(expected = ConstraintViolationException.class)
    public void testValidAndEmptyId() throws Exception {
        testGroup.removeMembers(getTestUser().getID(), "");
    }

    @Test(expected = NullPointerException.class)
    public void testNullId() throws Exception {
        testGroup.removeMembers(null);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testValidAndNullId() throws Exception {
        testGroup.removeMembers(getTestUser().getID(), null);
    }


    @Test
    public void testSameId() throws Exception {
        Set<String> failed = testGroup.removeMembers(testGroup.getID());
        assertEquals(1, failed.size());
        assertTrue(failed.contains(testGroup.getID()));
    }

    @Test
    public void testTwice() throws Exception {
        Set<String> failed = testGroup.removeMembers(memberGroup.getID(), memberGroup.getID());
        assertTrue(failed.isEmpty());

        assertFalse(testGroup.isDeclaredMember(memberGroup));
    }

    @Test
    public void testNotMember() throws Exception {
        Group gr = null;
        try {
            gr = getUserManager(root).createGroup("testGroup" + UUID.randomUUID().toString());

            Set<String> failed = testGroup.removeMembers(gr.getID());
            assertFalse(failed.isEmpty());
        } finally {
            if (gr != null) {
                gr.remove();
            }
        }
    }
}
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

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import javax.jcr.SimpleCredentials;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.security.AccessControlManager;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public abstract class AbstractRemoveMembersByIdTest extends AbstractUserTest {

    static final String[] NON_EXISTING_IDS = new String[] {"nonExisting1", "nonExisting2"};

    Group testGroup;
    Group memberGroup;
    
    @Override
    public void before() throws Exception {
        super.before();

        UserManager uMgr = createUserManagerImpl(root);
        for (String id : NON_EXISTING_IDS) {
            assertNull(uMgr.getAuthorizable(id));
        }

        testGroup = uMgr.createGroup("testGroup" + UUID.randomUUID().toString());
        memberGroup = uMgr.createGroup("memberGroup" + UUID.randomUUID().toString());
        testGroup.addMember(memberGroup);
        testGroup.addMember(getTestUser());
        root.commit();
        clearInvocations(monitor);
    }

    @Override
    public void after() throws Exception {
        try {
            clearInvocations(monitor);
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
        try (ContentSession testSession = login(new SimpleCredentials(userId, userId.toCharArray()))) {
            Root testRoot = testSession.getLatestRoot();

            assertFalse(testRoot.getTree(memberGroup.getPath()).exists());

            Group gr = getUserManager(testRoot).getAuthorizable(testGroup.getID(), Group.class);
            Set<String> failed = gr.removeMembers(memberGroup.getID());
            testRoot.commit();
            return failed;
        }
    }

    private void verifyMonitor(long totalCnt, long failedCnt) {
        verify(monitor).doneUpdateMembers(anyLong(), eq(totalCnt), eq(failedCnt), eq(true));
        verifyNoMoreInteractions(monitor);
    }

    @Test
    public void testUserMember() throws Exception {
        Set<String> failed = testGroup.removeMembers(getTestUser().getID());
        assertTrue(failed.isEmpty());
        verifyMonitor(1, 0);
    }

    @Test
    public void testGroupMember() throws Exception {
        Set<String> failed = testGroup.removeMembers(memberGroup.getID());
        assertTrue(failed.isEmpty());
        verifyMonitor(1, 0);
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
        verifyMonitor(1, 1);
    }

    @Test
    public void testTwice() throws Exception {
        Set<String> failed = testGroup.removeMembers(memberGroup.getID(), memberGroup.getID());
        assertTrue(failed.isEmpty());

        assertFalse(testGroup.isDeclaredMember(memberGroup));
        verifyMonitor(2, 0);
    }

    @Test
    public void testNotMember() throws Exception {
        Group gr = null;
        try {
            gr = getUserManager(root).createGroup("testGroup" + UUID.randomUUID().toString());

            Set<String> failed = testGroup.removeMembers(gr.getID());
            assertFalse(failed.isEmpty());
            verifyMonitor(1, 1);
        } finally {
            if (gr != null) {
                gr.remove();
            }
        }
    }
}
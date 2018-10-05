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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.jcr.AccessDeniedException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.query.Query;
import javax.jcr.security.Privilege;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing permission evaluation for user management operations.
 *
 * @since OAK 1.0 As of OAK user mgt related operations require a specific
 *        user management permission (unless the system in configured to behave like
 *        jackrabbit 2x).
 */
public class UserManagementTest extends AbstractEvaluationTest {

    private final String userId = "testUser2";
    private final String groupId = "testGroup2";

    private List<String> authorizablesToRemove = Lists.newArrayList(userId, groupId);

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        // setup default permissions
        String authPath = "/rep:security/rep:authorizables";
        AccessControlUtils.addAccessControlEntry(superuser, authPath, EveryonePrincipal.getInstance(), privilegesFromName(Privilege.JCR_READ), true);
        superuser.save();
    }

    @Override
    @Before
    public void tearDown() throws Exception {
        try {
            testSession.refresh(false);
            superuser.refresh(false);

            UserManager userMgr = getUserManager(superuser);
            for (String id : authorizablesToRemove) {
                Authorizable a = userMgr.getAuthorizable(id);
                if (a != null) {
                    a.remove();
                }
            }

            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    private void createUser(String userId) throws Exception {
        getUserManager(superuser).createUser(userId, "pw");
        superuser.save();
        testSession.refresh(false);
    }

    @Test
    public void testCreateUserWithoutPermission() throws Exception {
        UserManager testUserMgr = getUserManager(testSession);

        // testSession has read-only access
        try {
            testUserMgr.createUser(userId, "pw");
            testSession.save();
            fail("Test session doesn't have sufficient permission -> creating user should fail.");
        } catch (AccessDeniedException e) {
            // success
        }

        // testSession has write permission but no user-mgt permission
        // -> should still fail
        modify("/", PrivilegeConstants.REP_WRITE, true);
        try {
            testUserMgr.createUser(userId, "pw");
            testSession.save();
            fail("Test session doesn't have sufficient permission -> creating user should fail.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testCreateUser() throws Exception {
        UserManager testUserMgr = getUserManager(testSession);
        modify("/", PrivilegeConstants.REP_USER_MANAGEMENT, true);

        // creating user should succeed
        testUserMgr.createUser(userId, "pw");
        testSession.save();
    }

    @Test
    public void testCreateUser2() throws Exception {
        UserManager testUserMgr = getUserManager(testSession);
        Privilege[] privs = privilegesFromNames(new String[]{PrivilegeConstants.REP_USER_MANAGEMENT, PrivilegeConstants.REP_WRITE});
        allow("/", privs);

        // creating user should succeed
        testUserMgr.createUser(userId, "pw");
        testSession.save();
    }

    @Test
    public void testCreateGroup() throws Exception {
        UserManager testUserMgr = getUserManager(testSession);
        modify("/", PrivilegeConstants.REP_USER_MANAGEMENT, true);

        // creating group should succeed
        Group gr = testUserMgr.createGroup(groupId);
        testSession.save();
    }

    @Test
    public void testCreateGroup2() throws Exception {
        UserManager testUserMgr = getUserManager(testSession);
        Privilege[] privs = privilegesFromNames(new String[]{PrivilegeConstants.REP_USER_MANAGEMENT, PrivilegeConstants.REP_WRITE});
        allow("/", privs);

        // creating group should succeed
        Group gr = testUserMgr.createGroup(groupId);
        testSession.save();
    }

    @Test
    public void testCreateWithoutReadAccess() throws Exception {
        UserManager testUserMgr = getUserManager(testSession);
        deny("/", privilegesFromName(PrivilegeConstants.JCR_READ));
        allow("/", privilegesFromName(PrivilegeConstants.REP_USER_MANAGEMENT));

        try {
            Group gr = testUserMgr.createGroup(groupId);
            testSession.save();
            fail("Creating group without read-access on the folder node should fail");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testCreateWithIntermediateReadDeny() throws Exception {
        String path = UserConstants.DEFAULT_GROUP_PATH + "/a/b/c";
        Node groupRoot = JcrUtils.getOrCreateByPath(path, UserConstants.NT_REP_AUTHORIZABLE_FOLDER, superuser);
        superuser.save();

        try {
            deny(UserConstants.DEFAULT_GROUP_PATH, privilegesFromName(Privilege.JCR_READ));

            Privilege[] privs = privilegesFromNames(new String[]{Privilege.JCR_READ, PrivilegeConstants.REP_USER_MANAGEMENT, PrivilegeConstants.REP_WRITE});
            allow(path, privs);

            Group gr = getUserManager(testSession).createGroup(groupId, new PrincipalImpl(groupId), "a/b/c");
            testSession.save();
        } finally {
            superuser.refresh(false);
            superuser.getNode(UserConstants.DEFAULT_GROUP_PATH + "/a").remove();
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, UserConstants.DEFAULT_GROUP_PATH);
            if (acl != null) {
                acMgr.removePolicy(UserConstants.DEFAULT_GROUP_PATH, acl);
            }
            superuser.save();
        }
    }

    @Test
    public void testCreateWithIntermediateReadDeny2() throws Exception {
        String path = UserConstants.DEFAULT_GROUP_PATH + "/a";
        JcrUtils.getOrCreateByPath(path, UserConstants.NT_REP_AUTHORIZABLE_FOLDER, superuser);
        superuser.save();

        try {
            deny(UserConstants.DEFAULT_GROUP_PATH, privilegesFromName(Privilege.JCR_READ));

            Privilege[] privs = privilegesFromNames(new String[]{Privilege.JCR_READ, PrivilegeConstants.REP_USER_MANAGEMENT, PrivilegeConstants.REP_WRITE});
            allow(path, privs);

            Group gr = getUserManager(testSession).createGroup(groupId, new PrincipalImpl(groupId), "a/b/c");
            testSession.save();
        } finally {
            superuser.refresh(false);
            superuser.getNode(UserConstants.DEFAULT_GROUP_PATH + "/a").remove();
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, UserConstants.DEFAULT_GROUP_PATH);
            if (acl != null) {
                acMgr.removePolicy(UserConstants.DEFAULT_GROUP_PATH, acl);
            }
            superuser.save();
        }
    }

    @Test
    public void testChangePasswordWithoutPermission() throws Exception {
        createUser(userId);

        UserManager testUserMgr = getUserManager(testSession);
        User user = (User) testUserMgr.getAuthorizable(userId);
        try {
            user.changePassword("pw2");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testChangePasswordWithoutPermission2() throws Exception {
        createUser(userId);

        modify("/", PrivilegeConstants.REP_WRITE, true);

        UserManager testUserMgr = getUserManager(testSession);
        User user = (User) testUserMgr.getAuthorizable(userId);
        try {
            user.changePassword("pw2");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testChangePassword() throws Exception {
        createUser(userId);

        // after granting user-mgt privilege changing the pw must succeed.
        modify("/", PrivilegeConstants.REP_USER_MANAGEMENT, true);

        UserManager testUserMgr = getUserManager(testSession);
        User user = (User) testUserMgr.getAuthorizable(userId);
        user.changePassword("pw2");
        testSession.save();
    }

    @Test
    public void testDisableUserWithoutPermission() throws Exception {
        createUser(userId);

        UserManager testUserMgr = getUserManager(testSession);
        User user = (User) testUserMgr.getAuthorizable(userId);
        try {
            user.disable("disabled!");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testDisableUserWithoutPermission2() throws Exception {
        createUser(userId);

        modify("/", PrivilegeConstants.REP_WRITE, true);

        UserManager testUserMgr = getUserManager(testSession);
        User user = (User) testUserMgr.getAuthorizable(userId);
        try {
            user.disable("disabled!");
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testDisableUser() throws Exception {
        createUser(userId);

        // after granting user-mgt privilege changing the pw must succeed.
        modify("/", PrivilegeConstants.REP_USER_MANAGEMENT, true);

        UserManager testUserMgr = getUserManager(testSession);
        User user = (User) testUserMgr.getAuthorizable(userId);
        user.disable("disabled!");
        testSession.save();
    }

    @Test
    public void testRemoveUserWithoutPermission() throws Exception {
        createUser(userId);

        UserManager testUserMgr = getUserManager(testSession);
        // testSession has read-only access
        try {
            Authorizable a = testUserMgr.getAuthorizable(userId);
            a.remove();
            testSession.save();
            fail("Test session doesn't have sufficient permission to remove a user.");
        } catch (AccessDeniedException e) {
            // success
        }

        // testSession has write permission but no user-mgt permission
        // -> should still fail
        modify("/", PrivilegeConstants.REP_WRITE, true);
        try {
            Authorizable a = testUserMgr.getAuthorizable(userId);
            a.remove();
            testSession.save();
            fail("Test session doesn't have sufficient permission to remove a user.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testRemoveUser() throws Exception {
        createUser(userId);

        // testSession has user-mgt permission -> removal should succeed.
        modify("/", PrivilegeConstants.REP_USER_MANAGEMENT, true);

        UserManager testUserMgr = getUserManager(testSession);
        Authorizable a = testUserMgr.getAuthorizable(userId);
        a.remove();
        testSession.save();
    }

    @Test
    public void testRemoveUser2() throws Exception {
        createUser(userId);

        // testSession has user-mgt permission -> removal should succeed.
        Privilege[] privs = privilegesFromNames(new String[]{
                PrivilegeConstants.REP_USER_MANAGEMENT,
                PrivilegeConstants.REP_WRITE});
        allow("/", privs);

        UserManager testUserMgr = getUserManager(testSession);
        Authorizable a = testUserMgr.getAuthorizable(userId);
        a.remove();
        testSession.save();
    }

    @Test
    public void testChangeUserPropertiesWithoutPermission() throws Exception {
        createUser(userId);

        // testSession has read-only access
        UserManager testUserMgr = getUserManager(testSession);
        try {
            Authorizable a = testUserMgr.getAuthorizable(userId);
            a.setProperty("someProp", testSession.getValueFactory().createValue("value"));
            testSession.save();
            fail("Test session doesn't have sufficient permission to alter user properties.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testChangeUserPropertiesWithoutPermission2() throws Exception {
        createUser(userId);

        // testSession has read and user-mgt permission but lacks permission to
        // alter regular properties
        modify("/", PrivilegeConstants.REP_USER_MANAGEMENT, true);

        UserManager testUserMgr = getUserManager(testSession);
        try {
            Authorizable a = testUserMgr.getAuthorizable(userId);
            a.setProperty("someProp", testSession.getValueFactory().createValue("value"));
            testSession.save();
            fail("Test session doesn't have sufficient permission to alter user properties.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testChangeUserProperties() throws Exception {
        createUser(userId);

        // make sure user can create/modify/remove regular properties
        modify("/", PrivilegeConstants.JCR_MODIFY_PROPERTIES, true);

        UserManager testUserMgr = getUserManager(testSession);
        Authorizable a = testUserMgr.getAuthorizable(userId);
        a.setProperty("someProp", testSession.getValueFactory().createValue("value"));
        testSession.save();

        a.setProperty("someProperty", testSession.getValueFactory().createValue("modified"));
        testSession.save();

        a.removeProperty("someProperty");
        testSession.save();
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/JCR-3412">JCR-3412 :
     *      UserManager.findAuthorizables() does not work, if session does not have
     *      read access to common root of all user and groups. </a>
     */
    @Test
    public void testFindAuthorizables() throws Exception {
        String home = Text.getRelativeParent(UserConstants.DEFAULT_USER_PATH, 1);
        deny(home, privilegesFromName(PrivilegeConstants.JCR_READ));
        allow(getUserManager(superuser).getAuthorizable(testSession.getUserID()).getPath(), privilegesFromName(PrivilegeConstants.JCR_ALL));

        UserManager testUserMgr = getUserManager(testSession);
        Iterator<Authorizable> result = testUserMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_USER);

        Set<String> ids = new HashSet<String>();
        while (result.hasNext()) {
            ids.add(result.next().getID());
        }
        assertFalse(ids.isEmpty());

        NodeIterator nodeIterator = testSession.getWorkspace().getQueryManager().createQuery("/jcr:root//element(*,rep:User)", Query.XPATH).execute().getNodes();
        assertTrue(nodeIterator.hasNext());
        while (nodeIterator.hasNext()) {
            String userId = nodeIterator.nextNode().getProperty(UserConstants.REP_AUTHORIZABLE_ID).getString();
            if (!ids.remove(userId)) {
                fail("UserId " + userId + " missing in result set.");
            }
        }
        assertTrue("Result mismatch", ids.isEmpty());
    }

    @Test
    public void testGlobRestriction() throws Exception {
        String groupHome = Text.getRelativeParent(UserConstants.DEFAULT_GROUP_PATH, 1);
        Privilege[] privs = privilegesFromName(PrivilegeConstants.REP_USER_MANAGEMENT);
        allow(groupHome, privs);
        deny(groupHome, privs, createGlobRestriction("*/" + UserConstants.REP_MEMBERS));

        UserManager testUserMgr = getUserManager(testSession);

        // creating a new group must be allow
        Group gr = testUserMgr.createGroup(groupId);
        testSession.save();

        // modifying group membership must be denied
        try {
            gr.addMember(testUserMgr.getAuthorizable(testSession.getUserID()));
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        } finally {
            testSession.refresh(false);
        }
    }
}
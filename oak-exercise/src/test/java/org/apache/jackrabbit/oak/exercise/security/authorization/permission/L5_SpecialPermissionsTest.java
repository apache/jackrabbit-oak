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
package org.apache.jackrabbit.oak.exercise.security.authorization.permission;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import javax.jcr.AccessDeniedException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.exercise.security.privilege.L3_BuiltInPrivilegesTest;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.junit.Test;

/**
 * <pre>
 * Module: Authorization (Permission Evaluation)
 * =============================================================================
 *
 * Title: Special Permissions
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * So far exercises mostly dealt with regalar read/write privileges. After having
 * completed this exercise you should a better understanding on permission
 * evaluation for non-regular write operations.
 *
 * Exercises:
 *
 * - {@link #testVersioning()}
 *   Performing version operations requires {@link Privilege#JCR_VERSION_MANAGEMENT}
 *   to be granted at the target node. Use this test to verify this.
 *
 * - {@link #testReadVersionInformation()}
 *   Testing read access to version storage at /jcr:system/jcr:versionStore
 *   Use {@link #testVersioning()} again to take a closer look on how versions
 *   and version-histories are being retrieved from the version store and how
 *   the corresponding read-access is being evaluated.
 *
 *   Question: Can you explain what the difference is compared to Jackrabbit 2.x? (hint: oak-doc and jira)
 *   Question: Can you identify cases where access to /jcr:system/jcr:versionstore explicitly needed to be granted?
 *             If yes: explain why and the impact it may have from a security point of view.
 *
 * - {@link #testUserManagement()}
 *   Fix the test case such that there is no privilege escalation from testUser
 *   to the admin user :-)
 *
 * - {@link #testRepositoryLevelPrivileges()}
 *   The following privileges need to be granted globally for the whole repository
 *   as the associated operations also take effect on the whole repository.
 *   In order to do so, you need to use the 'null' paths... verify in this test
 *   that granting those privileges at 'childPath' does not have the desired effect.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Write additional test cases for the following privileges:
 *   - {@link Privilege#JCR_READ_ACCESS_CONTROL}
 *   - {@link Privilege#JCR_MODIFY_ACCESS_CONTROL}
 *   - {@link Privilege#JCR_LOCK_MANAGEMENT}
 *   - {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants#REP_INDEX_DEFINITION_MANAGEMENT}
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L3_BuiltInPrivilegesTest#testMapItems()}
 *
 * </pre>
 *
 *
 * @see org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants
 * @see org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions
 */
public class L5_SpecialPermissionsTest extends AbstractJCRTest {

    private User testUser;
    private User testUser2;
    private Principal testPrincipal;
    private Principal testGroupPrincipal;
    private Session testSession;

    private String childPath;
    private String grandChildPath;
    private String propertyPath;
    private String childPropertyPath;

    private List<String> paths = new ArrayList();

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Property p = testRootNode.setProperty(propertyName1, "val");
        propertyPath = p.getPath();

        Node child = testRootNode.addNode(nodeName1);
        childPath = child.getPath();

        p = child.setProperty(propertyName2, "val");
        childPropertyPath = p.getPath();

        Node grandChild = child.addNode(nodeName2);
        grandChildPath = grandChild.getPath();

        testUser = ExerciseUtility.createTestUser(((JackrabbitSession) superuser).getUserManager());
        testUser2 = ExerciseUtility.createTestUser(((JackrabbitSession) superuser).getUserManager());
        Group testGroup = ExerciseUtility.createTestGroup(((JackrabbitSession) superuser).getUserManager());
        testGroup.addMember(testUser);
        superuser.save();

        testPrincipal = testUser.getPrincipal();
        testGroupPrincipal = testGroup.getPrincipal();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (testSession != null && testSession.isLive()) {
                testSession.logout();
            }
            UserManager uMgr = ((JackrabbitSession) superuser).getUserManager();
            if (testUser != null) {
                testUser.remove();
            }
            if (testUser2 != null) {
                testUser2.remove();
            }
            Authorizable testGroup = uMgr.getAuthorizable(testGroupPrincipal);
            if (testGroup != null) {
                testGroup.remove();
            }

            removePolicies(paths);

            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    private void removePolicies(List<String> paths) throws RepositoryException {
        AccessControlManager acMgr = superuser.getAccessControlManager();
        for (String path : paths) {
            AccessControlPolicy[] policies = acMgr.getPolicies(path);
            for (AccessControlPolicy plc : policies) {
                if (plc instanceof JackrabbitAccessControlList) {
                    acMgr.removePolicy(path, plc);
                }
            }
        }
    }

    private Session createTestSession() throws RepositoryException {
        if (testSession == null) {
            testSession = superuser.getRepository().login(ExerciseUtility.getTestCredentials(testUser.getID()));
        }
        return testSession;
    }

    public void testVersioning() throws Exception {
        // EXERCISE: fix the test case
        superuser.getNode(childPath).addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.getNode(grandChildPath).addMixin(JcrConstants.MIX_VERSIONABLE);

        String privName = Privilege.JCR_VERSION_MANAGEMENT;
        AccessControlUtils.addAccessControlEntry(superuser, grandChildPath, testGroupPrincipal, new String[]{privName}, true);
        superuser.save();

        Session s = createTestSession();

        Node n = s.getNode(childPath);
        Version v = n.checkin();
        n.checkout();

        n = s.getNode(grandChildPath);
        v = n.checkin();
        n.checkout();
    }

    @Test
    public void testReadVersionInformation() throws RepositoryException {
        Node childNode = superuser.getNode(childPath);
        childNode.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();
        childNode.checkin();
        childNode.checkout();

        // EXERCISE: uncomment the following permission setup and test the impact it has!
        // EXERCISE: discuss and explain your findings
//        AccessControlUtils.addAccessControlEntry(superuser, "/jcr:system", testGroupPrincipal, new String[] {Privilege.JCR_READ}, false);
//        paths.add("/jcr:system");
//        superuser.save();
//
        Session s = createTestSession();

        // EXERCISE: uncomment to verify the permission setup is as expected
        //assertFalse(s.nodeExists(VersionConstants.VERSION_STORE_PATH));

        VersionManager versionManager = s.getWorkspace().getVersionManager();
        VersionHistory vh = versionManager.getVersionHistory(childPath);
        Version rootVersion = vh.getRootVersion();
        Version baseVersion = versionManager.getBaseVersion(childPath);
    }

    @Test
    public void testUserManagement() throws RepositoryException {
        // EXERCISE: fix the permission setup and explain why!

        // grant full access to all users for 'testGroup'...
        paths.add(UserConstants.DEFAULT_USER_PATH);
        AccessControlUtils.addAccessControlEntry(superuser,
                UserConstants.DEFAULT_USER_PATH, testGroupPrincipal,
                new String[]{Privilege.JCR_ALL}, true);
        // ... but prevent the test user to write the admin user
        String adminPath = ((JackrabbitSession) superuser).getUserManager().getAuthorizable(superuser.getUserID()).getPath();
        paths.add(adminPath);
        AccessControlUtils.addAccessControlEntry(superuser,
                adminPath, EveryonePrincipal.getInstance(),
                new String[]{PrivilegeConstants.REP_WRITE}, false);


        // execute the test verifying that pw of 'testUser2' can be change
        // but not the pw of the admin user
        JackrabbitSession s = (JackrabbitSession) createTestSession();

        User u2 = s.getUserManager().getAuthorizable(testUser2.getID(), User.class);
        u2.changePassword("gugus");
        s.save();

        try {
            User admin = s.getUserManager().getAuthorizable(superuser.getUserID(), User.class);
            admin.changePassword("gugus");
            s.save();
            fail("privilege escalation!");
        } catch (AccessDeniedException e) {
            // success
        } finally {
            s.refresh(false);
        }
    }

    public void testRepositoryLevelPrivileges() throws RepositoryException {
        // EXERCISE : setup the permissions such that test-session can register a
        // - new namespace|node type|privilege (note: workspace management not yet supported)

        // EXERCISE: refactory the test to verify that granting these privs at 'childPath' doesn;t have the desired effect.

        JackrabbitSession s = (JackrabbitSession) createTestSession();

        NamespaceRegistry nsRegistry = s.getWorkspace().getNamespaceRegistry();
        nsRegistry.registerNamespace("jr", "http://jackrabbit.apache.org");
    }
}
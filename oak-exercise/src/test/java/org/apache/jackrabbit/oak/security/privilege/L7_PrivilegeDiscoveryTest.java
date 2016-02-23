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
package org.apache.jackrabbit.oak.security.privilege;

import java.security.Principal;
import java.util.Set;
import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: Privilege Management | Authorization
 * =============================================================================
 *
 * Title: Privilege Discovery
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * The aim of this exercise is to make you familiar on how to discover privileges
 * granted for a given {@link javax.jcr.Session} or a given set of {@link java.security.Principal}s.
 * After having completed this exercise you should be able to explain the difference
 * compared to permission discovery as well as the benefit/drawback of using
 * this API.
 *
 * Exercises:
 *
 * - {@link #testHasPrivileges()}
 *   TODO
 *
 * - {@link #testHasPrivilegesPropertyPath()}
 *   TODO
 *
 * - {@link #testHasPrivilegeNonExistingPath()}
 *   TODO
 *
 * - {@link #testGetPrivileges()}
 *   Practise {@link AccessControlManager#getPrivileges(String)}, which evaluates
 *   the effective privileges for the editing {@code Session} associated with
 *   the access control manager: fill in the expected privileges at the different
 *   node paths.
 *
 * - {@link #testGetPrivilegesForPrincipals()}
 *   This test illustrates the usage of {@link JackrabbitAccessControlManager#getPrivileges(String, Set)}
 *   for different combinations of principals: fill in the expected privileges
 *   granted at the different paths.
 *   NOTE: the test is executed with the super-privileged adminitrative session.
 *   Compare the results with the next test case.
 *
 * - {@link #testGetPrivilegesForPrincipalsUserSession()}
 *   Same as {@link #testGetPrivilegesForPrincipals()} but this time the method
 *   is called with the user session that as you could see in {@link #testGetPrivileges}
 *   isn't granted to complete set of privileges.
 *   Complete the test case and explain the behavior; in particular in comparison
 *   with the previous test.
 *
 * - {@link #testCanAddNode()}
 *   TODO
 *
 * - {@link #testHasPermissionVsHasPrivilege()}
 *   TODO
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link org.apache.jackrabbit.oak.security.authorization.permission.L2_PermissionDiscoveryTest}
 * - {@link org.apache.jackrabbit.oak.security.authorization.permission.L4_PrivilegesAndPermissionsTest}
 *
 * </pre>
 *
 * @see AccessControlManager#hasPrivileges(String, Privilege[])
 * @see AccessControlManager#getPrivileges(String)
 * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#hasPrivileges(String, Privilege[])
 * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getPrivileges(String)
 */
public class L7_PrivilegeDiscoveryTest extends AbstractJCRTest {

    private Session userSession;

    private Principal uPrincipal;
    private Principal gPrincipal;

    private String testPath;
    private String childPath;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        SimpleCredentials creds = new SimpleCredentials("u", "u".toCharArray());
        UserManager uMgr = ((JackrabbitSession) superuser).getUserManager();
        User u = uMgr.createUser(creds.getUserID(), creds.getUserID());
        Group g = uMgr.createGroup("g");
        g.addMember(u);

        uPrincipal = u.getPrincipal();
        gPrincipal = g.getPrincipal();

        Node n = superuser.getNode(testRoot).addNode(nodeName1);
        testPath = n.getPath();
        Privilege[] privs = AccessControlUtils.privilegesFromNames(superuser,
                Privilege.JCR_VERSION_MANAGEMENT,
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_MODIFY_PROPERTIES);
        AccessControlUtils.addAccessControlEntry(superuser, n.getPath(), gPrincipal,
                privs, true);
        AccessControlUtils.addAccessControlEntry(superuser, n.getPath(), uPrincipal,
                new String[] {Privilege.JCR_VERSION_MANAGEMENT}, false);

        Node child = n.addNode(nodeName2);
        childPath = child.getPath();
        superuser.save();

        userSession = getHelper().getRepository().login(creds);

        // NOTE the following precondition defined by the test-setup!
        assertTrue(userSession.nodeExists(testPath));
        assertTrue(userSession.nodeExists(childPath));
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            userSession.logout();

            superuser.getNode(testPath).remove();

            UserManager uMgr = ((JackrabbitSession) superuser).getUserManager();
            Authorizable a = uMgr.getAuthorizable("u");
            if (a != null) {
                a.remove();
            }
            a = uMgr.getAuthorizable("g");
            if (a != null) {
                a.remove();
            }
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    public void testHasPrivileges() throws Exception {
        AccessControlManager acMgr = userSession.getAccessControlManager();


        // TODO
    }

    public void testHasPrivilegesPropertyPath() throws Exception {
        // TODO
    }

    public void testHasPrivilegeNonExistingPath() throws Exception {
        // TODO
    }

    public void testGetPrivileges() throws Exception {
        AccessControlManager acMgr = userSession.getAccessControlManager();

        Set<Privilege> expected = null; // EXERCISE
        Privilege[] testRootPrivs = acMgr.getPrivileges(testRoot);
        assertEquals(expected, ImmutableSet.copyOf(testRootPrivs));

        expected = null; // EXERCISE
        Privilege[] privs = acMgr.getPrivileges(testPath);
        assertEquals(expected, ImmutableSet.copyOf(privs));

        expected = null; // EXERCISE
        Privilege[] childPrivs = acMgr.getPrivileges(childPath);
        assertEquals(expected, ImmutableSet.copyOf(childPrivs));
    }

    public void testGetPrivilegesForPrincipals() throws Exception {
        JackrabbitAccessControlManager acMgr = (JackrabbitAccessControlManager) superuser.getAccessControlManager();

        // 1. EXERCISE: expected privileges for the 'uPrincipal' only
        Set<Principal> principals = ImmutableSet.of(uPrincipal);
        java.util.Map<String, Set<Privilege>> expected = ImmutableMap.of(
                testRoot, null, // EXERCISE
                testPath, null, // EXERCISE
                childPath, null // EXERCISE
        );
        for (String path : expected.keySet()) {
            Set<Privilege> expectedPrivs = expected.get(path);
            Privilege[] privs = acMgr.getPrivileges(path, principals);
            assertEquals(expectedPrivs, ImmutableSet.copyOf(privs));
        }

        // 2. EXERCISE: expected privileges for the 'gPrincipal' only
        principals = ImmutableSet.of(gPrincipal);
        expected = ImmutableMap.of(
                testRoot, null,
                testPath, null,
                childPath, null
        );
        for (String path : expected.keySet()) {
            Set<Privilege> expectedPrivs = expected.get(path);
            Privilege[] privs = acMgr.getPrivileges(path, principals);
            assertEquals(expectedPrivs, ImmutableSet.copyOf(privs));
        }

        // 3. EXERCISE: expected privileges for the 'uPrincipal' and 'gPrincipal'
        principals = ImmutableSet.of(uPrincipal, gPrincipal);
        expected = ImmutableMap.of(
                testRoot, null,
                testPath, null,
                childPath, null
        );
        for (String path : expected.keySet()) {
            Set<Privilege> expectedPrivs = expected.get(path);
            Privilege[] privs = acMgr.getPrivileges(path, principals);
            assertEquals(expectedPrivs, ImmutableSet.copyOf(privs));
        }

        // 4. EXERCISE: expected privileges for the 'uPrincipal', 'gPrincipal' + everyone
        principals = ImmutableSet.of(uPrincipal, gPrincipal, EveryonePrincipal.getInstance());
        expected = ImmutableMap.of(
                testRoot, null,
                testPath, null,
                childPath, null
        );
        for (String path : expected.keySet()) {
            Set<Privilege> expectedPrivs = expected.get(path);
            Privilege[] privs = acMgr.getPrivileges(path, principals);
            assertEquals(expectedPrivs, ImmutableSet.copyOf(privs));
        }
    }

    public void testGetPrivilegesForPrincipalsUserSession() throws Exception {
        JackrabbitAccessControlManager acMgr = (JackrabbitAccessControlManager) userSession.getAccessControlManager();

        // EXERCISE: complete the test case and explain the behaviour

        Privilege[] privs = acMgr.getPrivileges(testPath, ImmutableSet.of(gPrincipal));
        Set<Privilege> expectedPrivs = null;
        assertEquals(expectedPrivs, ImmutableSet.copyOf(privs));
    }

    public void testCanAddNode() throws Exception {
        // TODO
    }

    public void testHasPermissionVsHasPrivilege() throws Exception {
        // TODO
    }
}
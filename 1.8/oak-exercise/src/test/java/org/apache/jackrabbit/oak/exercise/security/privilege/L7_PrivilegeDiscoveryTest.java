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
package org.apache.jackrabbit.oak.exercise.security.privilege;

import java.security.Principal;
import java.util.Map;
import java.util.Set;
import javax.jcr.Node;
import javax.jcr.Property;
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
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.exercise.security.authorization.permission.L2_PermissionDiscoveryTest;
import org.apache.jackrabbit.oak.exercise.security.authorization.permission.L4_PrivilegesAndPermissionsTest;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
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
 *   Practise {@link AccessControlManager#hasPrivileges(String, Privilege[])},
 *   by filling in granted and denied privileges such that the test passes.
 *   Can you identify the complete set of privileges granted? Could you do this
 *   programmatically?
 *
 * - {@link #testHasPrivilegesPropertyPath()}
 *   Simplified version of the test-case before but this time the target path
 *   points to a JCR property. Complete the test-case such that it passes and
 *   explain the result.
 *
 * - {@link #testHasPrivilegeSpecialPath()}
 *   Yet another variant of the 'hasPrivileges' test case. This time the path
 *   points to a special node. Once you have successfully completed the test-case
 *   in the original version, try the following to variants:
 *   - Use 'superuser' session instead of the user session
 *   - Change the target path to 'testPath + "/otherChild"'
 *   Look at the JCR specification or the code to explain each of the 3 test results.
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
 * - {@link #testHasPermissionVsHasPrivilege()}
 *   Test case illustrating some differences between {@link Session#hasPermission(String, String)}
 *   and {@link AccessControlManager#hasPrivileges(String, Privilege[])}.
 *   Complete the test such that it passes and explain the behavior.
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L2_PermissionDiscoveryTest}
 * - {@link L4_PrivilegesAndPermissionsTest}
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
    private String propPath;

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

        Property p = n.setProperty(propertyName1, "value");
        propPath = p.getPath();

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

        Map<String, String[]> expectedAllow = ImmutableMap.of(
                testRoot, new String[] {null, null, null, null, null, "..."}, // EXERCISE
                testPath, new String[] {null, null, null, null, null, "..."}, // EXERCISE
                childPath, new String[] {null, null, null, null, null, "..."} // EXERCISE
        );
        for (String path : expectedAllow.keySet()) {
            assertTrue(acMgr.hasPrivileges(path, AccessControlUtils.privilegesFromNames(userSession, expectedAllow.get(path))));
        }

        Map<String, String[]> expectedDeny = ImmutableMap.of(
                testRoot, new String[] {null, null, null, null, null, "..."}, // EXERCISE
                testPath, new String[] {null, null, null, null, null, "..."}, // EXERCISE
                childPath, new String[] {null, null, null, null, null, "..."} // EXERCISE
        );
        for (String path : expectedDeny.keySet()) {
            assertFalse(acMgr.hasPrivileges(path, AccessControlUtils.privilegesFromNames(userSession, expectedAllow.get(path))));
        }
    }

    public void testHasPrivilegesPropertyPath() throws Exception {
        AccessControlManager acMgr = userSession.getAccessControlManager();

        // EXERCISE: complete the test
        Privilege[] expectedPrivs = null;
        assertTrue(acMgr.hasPrivileges(propPath, expectedPrivs));
    }

    public void testHasPrivilegeSpecialPath() throws Exception {
        AccessControlManager acMgr = userSession.getAccessControlManager();

        // 1. EXERCISE: complete the test
        Privilege[] expectedPrivs = null;
        String policyPath = PathUtils.concat(testPath, AccessControlConstants.REP_POLICY);
        assertTrue(acMgr.hasPrivileges(policyPath, expectedPrivs));

        // 2. EXERCISE: modify the test-case by replacing user-session by the superuser session
        //              explain the difference


        // 3. EXERCISE: change the target path to testPath + "/otherChild" and run
        //              the test again (user or admin session).
        //              what is the expected outcome? why?

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
        Map<String, Set<Privilege>> expected = ImmutableMap.of(
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

    public void testHasPermissionVsHasPrivilege() throws Exception {
        JackrabbitAccessControlManager acMgr = (JackrabbitAccessControlManager) userSession.getAccessControlManager();

        // EXERCISE: fill in the correct boolean values and compare the difference
        // between hasPermission and hasPrivilege. explain!

        Boolean canAddNode = null;
        assertEquals(canAddNode.booleanValue(), userSession.hasPermission(testPath, Session.ACTION_ADD_NODE));
        Boolean canAddChild = null;
        assertEquals(canAddChild.booleanValue(), userSession.hasPermission(testPath + "/newChild", Session.ACTION_ADD_NODE));

        Boolean hasAddChildPrivilege = null;
        assertEquals(hasAddChildPrivilege.booleanValue(), acMgr.hasPrivileges(testPath, AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_ADD_CHILD_NODES)));

        Boolean canModifyProperty = null;
        assertEquals(canModifyProperty.booleanValue(), userSession.hasPermission(propPath, Session.ACTION_SET_PROPERTY));

        Boolean canAddProperty = null;
        assertEquals(canAddProperty.booleanValue(), userSession.hasPermission(testPath + "/newProp", JackrabbitSession.ACTION_ADD_PROPERTY));

        Boolean hasModifyPropertiesPrivilege = null;
        assertEquals(hasModifyPropertiesPrivilege.booleanValue(), acMgr.hasPrivileges(propPath, AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_MODIFY_PROPERTIES)));
    }
}
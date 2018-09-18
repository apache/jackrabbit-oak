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
import java.util.Map;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

/**
 * <pre>
 * Module: Authorization (Permission Evaluation)
 * =============================================================================
 *
 * Title: PermissionDiscoveryTest
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Become familiar with the permission discovery as provided by {@link javax.jcr.Session}.
 *
 * Exercises:
 *
 * - Overview
 *   Look at {@link javax.jcr.Session} and list the action constants that may
 *   be used to test permissions of the editing session.
 *   Compare this list with the built-in privileges defined in {@link javax.jcr.security.Privilege}
 *   and explain the discrepancy.
 *
 *   Question: Can you find out with the predefined actions if you can lock an existing node?
 *   Question: Can you find out with the predefined actions if you can register a new namespace?
 *
 * - {@link #testReadAccess()}
 *   While there exists {@link javax.jcr.Session#ACTION_READ}, you can equally
 *   use the direct methods to test for existance of a given item.
 *   Use the test-case to learn about the difference and when using
 *   {@link Session#hasPermission(String, String)} could actually make sense.
 *
 * - {@link #testModifyPermissions()}
 *   Test illustrating the usage of {@link Session#ACTION_SET_PROPERTY}. Fill
 *   in the expected values and explain why.
 *
 *   Question: How is {@link Session#ACTION_SET_PROPERTY} mapped to the internal permissions?
 *   Question: Can make a table illustrating the effect of the individual permissions
 *             (granted/denied) on the result depending on whether the item exists or not?
 *
 * - {@link #testRemovePermissions()}
 *   Test illustrating the usage of {@link Session#ACTION_REMOVE}. Fill
 *   in the expected values and explain why.
 *
 *   Question: How is {@link Session#ACTION_REMOVE} mapped to the internal permissions?
 *   Question: Can make a table illustrating the effect of the individual permissions
 *             (granted/denied) on the result depending on whether the item exists or not?
 *   Question: Discuss what is special about the removal of nodes when
 *             comparing the action, the privileges and the internal permissions
 *
 * - {@link #testAddPermissions()}
 *   Test illustrating the usage of {@link Session#ACTION_ADD_NODE} and {@link Session#ACTION_SET_PROPERTY}
 *   if used to create a new non-existing property. Fill in the expected values and explain why.
 *
 *   Question: How is {@link Session#ACTION_ADD_NODE} mapped to the internal permissions?
 *   Question: Can make a table illustrating the effect of the individual permissions
 *             (granted/denied) on the result depending on whether the item exists or not?
 *   Question: Discuss what is special about the creation of new nodes when
 *             comparing the action, the privileges and the internal permissions
 *
 * - {@link #testOakPermissions()}
 *   The default permission implementation in Oak also allows for passing
 *   string representation of the permission constants as 'actions'.
 *   Adjust the test such that it passes.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Session.checkPermission
 *   Apart from {@link javax.jcr.Session#hasPermission(String, String)} there also
 *   exists {@link javax.jcr.Session#checkPermission(String, String)}.
 *
 *   Question: Can you explain why it is generally recommended to use the non-throwing variant?
 *
 * - Explict vs. Builtin Permission Test
 *
 *   Question: Discuss why it is generally preferrable to leave the permission
 *             evaluation to the repository instead of doing this manually in the application?
 *   Question: Can you identify use-cases where this is nevertheless required?
 *             How could they be avoided?
 *
 * </pre>
 *
 * @see javax.jcr.Session#hasPermission(String, String)
 * @see javax.jcr.Session#checkPermission(String, String)
 */
public class L2_PermissionDiscoveryTest extends AbstractJCRTest {

    private Principal testPrincipal;
    private Session testSession;

    private String childPath;
    private String propertyPath;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Property p = testRootNode.setProperty(propertyName1, "val");
        propertyPath = p.getPath();

        Node child = testRootNode.addNode(nodeName1);
        childPath = child.getPath();

        User testUser = ExerciseUtility.createTestUser(((JackrabbitSession) superuser).getUserManager());
        testPrincipal = testUser.getPrincipal();

        Privilege[] privs = AccessControlUtils.privilegesFromNames(superuser, Privilege.JCR_READ, PrivilegeConstants.REP_ADD_PROPERTIES);
        Privilege[] privs2 = AccessControlUtils.privilegesFromNames(superuser, Privilege.JCR_ADD_CHILD_NODES);
        if (!AccessControlUtils.addAccessControlEntry(superuser, testRoot, testPrincipal, privs, true) ||
            !AccessControlUtils.addAccessControlEntry(superuser, childPath, testPrincipal, privs2, true)) {
            throw new NotExecutableException();
        }

        superuser.save();
        testSession = superuser.getRepository().login(ExerciseUtility.getTestCredentials(testUser.getID()));
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (testSession != null && testSession.isLive()) {
                testSession.logout();
            }
            Authorizable testUser = ((JackrabbitSession) superuser).getUserManager().getAuthorizable(testPrincipal);
            if (testUser != null) {
                testUser.remove();
                superuser.save();
            }
        } finally {
            super.tearDown();
        }
    }

    private static Boolean[] existsAndHasPermission(Boolean expectedExists, Boolean expectedHasPermission) {
        return new Boolean[] {expectedExists, expectedHasPermission};
    }

    public void testReadAccess() throws RepositoryException {
        // EXERCISE: fill in the expected values
        Map<String, Boolean[]> nodeTests = ImmutableMap.of(
                "/", existsAndHasPermission(null, null),
                testRoot, existsAndHasPermission(null, null),
                childPath, existsAndHasPermission(null, null),
                childPath + "/new", existsAndHasPermission(null, null)
        );

        for (String nodePath : nodeTests.keySet()) {
            Boolean[] expected = nodeTests.get(nodePath);

            assertEquals(expected[0].booleanValue(), testSession.nodeExists(nodePath));
            assertEquals(expected[1].booleanValue(), testSession.hasPermission(nodePath, Session.ACTION_READ));
        }

        // EXERCISE: fill in the expected values
        Map<String, Boolean[]> propertyTests = ImmutableMap.of(
                "/jcr:primaryType", existsAndHasPermission(null, null),
                propertyPath, existsAndHasPermission(null, null),
                childPath + "/new", existsAndHasPermission(null, null)
        );

        for (String pPath : propertyTests.keySet()) {
            Boolean[] expected = propertyTests.get(pPath);

            assertEquals(expected[0].booleanValue(), testSession.nodeExists(pPath));
            assertEquals(expected[1].booleanValue(), testSession.hasPermission(pPath, Session.ACTION_READ));
        }
    }

    public void testModifyPermissions() throws RepositoryException {
        // EXERCISE: fill in the expected values
        Map<String, Boolean> modifyPropertyTests = ImmutableMap.of(
                "/jcr:primaryType", null,
                testRoot, null,
                propertyPath, null
        );
        for (String pPath : modifyPropertyTests.keySet()) {
            boolean canModifyProperty = modifyPropertyTests.get(pPath);
            assertEquals(canModifyProperty, testSession.hasPermission(pPath, Session.ACTION_SET_PROPERTY));
        }
    }

    public void testRemovePermissions() throws RepositoryException {
        // EXERCISE: fill in the expected values
        Map<String, Boolean> removePropertyTests = ImmutableMap.of(
                "/jcr:primaryType", null,
                propertyPath, null,
                childPath + "/new", null
        );
        for (String pPath : removePropertyTests.keySet()) {
            boolean canRemoveProperty = removePropertyTests.get(pPath);
            assertEquals(canRemoveProperty, testSession.hasPermission(pPath, Session.ACTION_REMOVE));
        }

        // EXERCISE: fill in the expected values
        Map<String, Boolean> removeNodesTests = ImmutableMap.of(
                "/", null,
                testRoot, null,
                childPath, null,
                childPath + "/new", null
        );
        for (String nodePath : removeNodesTests.keySet()) {
            boolean canRemoveNode = removeNodesTests.get(nodePath);
            assertEquals(canRemoveNode, testSession.hasPermission(nodePath, Session.ACTION_REMOVE));
        }


        // EXERCISE : change the permission setup such that the following tests succeed.
        testSession.refresh(false);
        assertTrue(testSession.hasPermission(childPath, Session.ACTION_REMOVE));
        testSession.getNode(childPath).remove();
        testSession.save();

        // EXERCISE : change the permission setup such that the following tests succeed.

        testSession.refresh(false);
        assertTrue(testSession.hasPermission(propertyPath, Session.ACTION_REMOVE));
        testSession.getProperty(propertyPath).remove();
        testSession.save();

    }

    public void testAddPermissions() throws RepositoryException {
        // EXERCISE: fill in the expected values
        Map<String, Boolean> addPropertyTests = ImmutableMap.of(
                "/propertyName1", null,
                testRoot, null,
                propertyPath, null,
                childPath + "/new", null
        );
        for (String pPath : addPropertyTests.keySet()) {
            boolean canAddProperty = addPropertyTests.get(pPath);
            assertEquals(canAddProperty, testSession.hasPermission(pPath, Session.ACTION_SET_PROPERTY));
        }

        // EXERCISE: fill in the expected values
        Map<String, Boolean> addNodesTests = ImmutableMap.of(
                "/childNode", null,
                testRoot, null,
                testRoot + "/new", null,
                childPath, null,
                childPath + "/new", null
        );
        for (String childPath : addNodesTests.keySet()) {
            boolean canAddNode = addNodesTests.get(childPath);
            assertEquals(canAddNode, testSession.hasPermission(childPath, Session.ACTION_ADD_NODE));
        }

        // EXERCISE : change the permission setup such that the following tests succeed.

        testSession.refresh(false);
        testSession.getNode(testRoot).addNode(nodeName2);
        testSession.save();
    }

    public void testOakPermissions() throws RepositoryException {
        String modifyPropertyPermissions = null; // EXERCISE:
        assertFalse(testSession.hasPermission(propertyPath, modifyPropertyPermissions));

        // EXERCISE : modify the permission setup such that the following tests pass

        testSession.refresh(false);
        assertTrue(testSession.hasPermission(propertyPath, modifyPropertyPermissions));
        assertFalse(testSession.hasPermission(propertyPath, Permissions.getString(Permissions.REMOVE_PROPERTY|Permissions.ADD_PROPERTY)));

        String addItemPermissions = null; // EXERCISE
        assertTrue(testSession.hasPermission(childPath, addItemPermissions));

        String permissions = null; // EXERCISE
        assertFalse(testSession.hasPermission(childPath, permissions));

        // EXERCISE : modify the permission setup such that the following tests pass
        assertFalse(testSession.hasPermission(testRoot, permissions));
        assertTrue(testSession.hasPermission(childPath, permissions));

        Node cNode = testSession.getNode(childPath);
        cNode.addMixin(mixVersionable);
        testSession.save();
        cNode.checkin();
        cNode.checkout();
    }

}
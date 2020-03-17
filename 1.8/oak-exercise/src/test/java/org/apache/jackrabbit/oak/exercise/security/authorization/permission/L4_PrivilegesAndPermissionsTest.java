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
import javax.jcr.AccessDeniedException;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.exercise.security.privilege.L5_PrivilegeContentTest;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.api.util.Text;

/**
 * <pre>
 * Module: Authorization (Permission Evaluation)
 * =============================================================================
 *
 * Title: Privileges and Permissions
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * The aim of this is test is to make you familiar with the subtle differences
 * between privileges (that are always granted on an existing node) and effective
 * permissions on the individual items (nodes, properties or even non-existing
 * items).
 * Having completed this exercise you should also be familiar with the oddities
 * of those privileges that allow modify the parent collection, while the effective
 * permission is evaluated the target item.
 *
 * Exercises:
 *
 * - {@link #testAddNodes()}
 *   This test aims to practise the subtle difference between granting 'jcr:addChildNode'
 *   privilege at a given parent node and {@link Session#hasPermission(String, String)}
 *   using {@link Session#ACTION_ADD_NODE}, which effectively tests if a given
 *   new node could be created.
 *   Fill in the expected values for the permission discovery with the given
 *   permission setup. Subsequently, list the expected values for
 *   {@link javax.jcr.security.AccessControlManager#getPrivileges(String)}.
 *   Explain the difference given the mini-explanation in the test introduction.
 *
 * - {@link #testAddProperties()}
 *   This test grants the test user privilege to add properties at 'childPath'.
 *   Fill in the expected result of {@link javax.jcr.Session#hasPermission(String, String)}
 *   both for the regular JCR action {@link Session#ACTION_SET_PROPERTY} and
 *   for the string value of {@link Permissions#ADD_PROPERTY}. Compare and explain
 *   the differences.
 *
 * - {@link #testRemoveNodes()} ()}
 *   This test illustrates what kind of privileges are required in order to
 *   remove a given node. Fix the test-case by setting the correct permission
 *   setup.
 *
 * - {@link #testRemoveProperties()} ()}
 *   In this test the test-user is only granted rep:removeProperties privilege
 *   and lists which properties must be removable. Nevertheless the test-case is
 *   broken... can you identify the root cause without modifying the result map
 *   nor the permission setup?
 *
 * - {@link #testRemoveNonExistingItems()}
 *   Look at the contract of {@link Session#hasPermission(String, String)} and
 *   the implementation present in Oak to understand what happens if you test
 *   permissions for removal of non-existing items.
 *
 * - {@link #testModifyProperties()}
 *   Now the test-user is just allowed to modify properties. Complete the test
 *   by modifying the result-map: add for each path whether accessing and setting
 *   the properties is expected to succeed. Explain for each path why it passes
 *   or fails. In case of doubt debug the test to understand what is going on.
 *
 * - {@link #testSetProperty()}
 *   Properties cannot only be modified by calling {@link Property#setValue}
 *   but also by calling {@link Node#setProperty} each for different types and
 *   single vs multivalued properties. While they are mostly equivalent there
 *   are subtle differences that have an impact on the permission evaluation.
 *   Can you find suitable value(s) for each of these paths such that the
 *   test passes?
 *   Hint: passing a 'null' value is defined to be equivalent to removal.
 *   Discuss your findings.
 *
 *   Question: Can you explain the required values if the test user was allowed
 *   to remove properties instead? -> Modify the test to verify your expectations.
 *
 * - {@link #testChangingPrimaryAndMixinTypes()}
 *   In order to change the primary (or mixin) type(s) of a given existing node
 *   the editing session must have {@link Privilege#JCR_NODE_TYPE_MANAGEMENT}
 *   privilege granted.
 *   For consistency with this requirement, also {@link Node#addNode(String, String)},
 *   which explicitly specifies the primary type requires this privilege as
 *   the effective operation is equivalent to the combination of:
 *   1. {@link Node#addNode(String)} +
 *   2. {@link Node#setPrimaryType(String)}
 *   Use this test case to become familiar with setting or changing the primary
 *   type and modifying the set of mixin types.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Modifying Nodes
 *   Discuss why there is no dedicated privilege (and test case) for "modify nodes".
 *   Explain how {@link Privilege#JCR_NODE_TYPE_MANAGEMENT, {@link Privilege#JCR_ADD_CHILD_NODES},
 *   {@link Privilege#JCR_REMOVE_CHILD_NODES} (and maybe others) are linked to
 *   node modification.
 *
 *
 * Advanced Exercises:
 * -----------------------------------------------------------------------------
 *
 * If you wished to become more familiar with the interals of the Oak permission
 * evaluation, it is indispensible to understand the internal representation of
 * registered privileges (as {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits}
 * and their mapping to {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions}.
 *
 * Precondition for these advanced exercises is the following test:
 *
 * - {@link L5_PrivilegeContentTest }
 *
 * The following exercises aim to provide you with some insight and allow you
 * to understand the internal structure of the permission store.
 *
 * - PrivilegeBits and the corresponding provider class
 *   Take a look at the methods exposed by
 *   {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider}
 *   and the {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits}
 *   class itself. Using these classes Oak generates the repository internal
 *   representation of the privileges that are used for
 *   - calculation of effective privileges
 *   - calculation of final permission from effective privileges granted (or denied)
 *
 * - Mapping Privileges to Permissions
 *   As you could see in the above exercises there is no 1:1 mapping between
 *   privileges and permissions as a few privileges (like jcr:addChildNodes and
 *   jcr:removeChildNodes) are granted for the parent node. This needs to be
 *   handled specially when evaluating if a given node can be created or removed.
 *   Look at {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits#calculatePermissions()}
 *   and compare the code with the results obtained from the test-cases above.
 *
 * - Mapping of JCR Actions to Permissions
 *   The exercises also illustrated the subtle differences between 'actions'
 *   as defined on {@link javax.jcr.Session} and the effective permissions
 *   being evaluated.
 *   Take a closer look at {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions#getPermissions(String, org.apache.jackrabbit.oak.plugins.tree.TreeLocation, boolean)}
 *   to improve your understanding on how the mapping is being implemented
 *   and discuss the consequences for {@link Session#hasPermission(String, String)}.
 *   Compare the code with the test-results.
 *
 *
 * Related Exercises
 * -----------------------------------------------------------------------------
 *
 * - {@link L5_SpecialPermissionsTest }
 * - {@link L7_PermissionContentTest }
 *
 * </pre>
 */
public class L4_PrivilegesAndPermissionsTest extends AbstractJCRTest {

    private User testUser;
    private Principal testPrincipal;
    private Principal testGroupPrincipal;
    private Session testSession;

    private String childPath;
    private String grandChildPath;
    private String propertyPath;
    private String childPropertyPath;

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
            Authorizable testGroup = uMgr.getAuthorizable(testGroupPrincipal);
            if (testGroup != null) {
                testGroup.remove();
            }
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    private Session createTestSession() throws RepositoryException {
        if (testSession == null) {
            testSession = superuser.getRepository().login(ExerciseUtility.getTestCredentials(testUser.getID()));
        }
        return testSession;
    }

    public void testAddNodes() throws Exception {
        // grant the test principal jcr:addChildNode privilege at 'childPath'
        AccessControlUtils.addAccessControlEntry(superuser, childPath, testPrincipal, new String[] {Privilege.JCR_ADD_CHILD_NODES}, true);
        superuser.save();

        Session userSession = createTestSession();

        // EXERCISE: fill in the expected return values for Session.hasPermission as performed below
        // EXERCISE: verify that the test passes and explain the individual results
        Map<String, Boolean> pathHasPermissionMap = ImmutableMap.of(
                testRootNode.getPath(), null,
                childPath, null,
                childPath + "/toCreate", null,
                grandChildPath + "/nextGeneration", null,
                propertyPath, null
        );

        for (String path : pathHasPermissionMap.keySet()) {
            boolean expectedHasPermission = pathHasPermissionMap.get(path);
            assertEquals(expectedHasPermission, userSession.hasPermission(path, Session.ACTION_ADD_NODE));
        }

        // EXERCISE: fill in the expected return values for AccessControlManager#getPrivileges as performed below
        // EXERCISE: verify that the test passes and compare the results with your findings from the permission-discovery
        Map<String, Privilege[]> pathPrivilegesMap = ImmutableMap.of(
                testRootNode.getPath(), null,
                childPath, null,
                childPath + "/toCreate", null,
                grandChildPath + "/nextGeneration", null
        );

        for (String path : pathPrivilegesMap.keySet()) {
            Privilege[] expectedPrivileges = pathPrivilegesMap.get(path);
            assertEquals(ImmutableSet.of(expectedPrivileges), ImmutableSet.of(userSession.getAccessControlManager().getPrivileges(path)));
        }

        // EXERCISE: optionally add nodes at the expected allowed path(s)
        // EXERCISE: using 'userSession' to verify that it actually works and
        // EXERCISE: save the changes to trigger the evaluation
    }

    public void testAddProperties() throws Exception {
        // grant the test principal rep:addProperties privilege at 'childPath'
        // EXERCISE: explain the difference between rep:addProperites and jcr:modifyProperties privilege!
        AccessControlUtils.addAccessControlEntry(superuser, childPath, testPrincipal, new String[] {PrivilegeConstants.REP_ADD_PROPERTIES}, true);
        superuser.save();

        // EXERCISE: fill in the expected return values for Session.hasPermission as performed below
        // EXERCISE: verify that the test passes and explain the individual results
        Map<String, Boolean[]> pathHasPermissionMap = ImmutableMap.of(
                propertyPath, new Boolean[]{null, null},
                childPath + "/newProp", new Boolean[]{null, null},
                childPropertyPath, new Boolean[]{null, null},
                grandChildPath + "/" + JcrConstants.JCR_PRIMARYTYPE, new Boolean[]{null, null}
        );

        Session userSession = createTestSession();
        for (String path : pathHasPermissionMap.keySet()) {
            Boolean[] result = pathHasPermissionMap.get(path);
            boolean setPropertyAction = result[0];
            boolean addPropertiesPermission = result[1];
            assertEquals(setPropertyAction, userSession.hasPermission(path, Session.ACTION_SET_PROPERTY));
            assertEquals(addPropertiesPermission, userSession.hasPermission(path, Permissions.getString(Permissions.ADD_PROPERTY)));
        }
    }

    public void testRemoveNodes() throws Exception {
        // EXERCISE: setup the correct set of privileges such that the test passes
        superuser.save();

        Map<String, Boolean> pathHasPermissionMap = ImmutableMap.of(
                testRootNode.getPath(), false,
                childPath, false,
                grandChildPath, true
        );

        Session userSession = createTestSession();
        for (String path : pathHasPermissionMap.keySet()) {
            boolean expectedHasPermission = pathHasPermissionMap.get(path);
            assertEquals(expectedHasPermission, userSession.hasPermission(path, Session.ACTION_REMOVE));
        }

        AccessControlManager acMgr = userSession.getAccessControlManager();
        assertFalse(acMgr.hasPrivileges(childPath, new Privilege[]{acMgr.privilegeFromName(Privilege.JCR_REMOVE_NODE)}));

        userSession.getNode(grandChildPath).remove();
        userSession.save();
    }

    public void testRemoveProperties() throws Exception {
        // EXERCISE: fix the test case without changing the result-map :-)

        // grant the test principal rep:removeProperties privilege at 'childPath'
        AccessControlUtils.addAccessControlEntry(superuser, childPath, testPrincipal, new String[] {PrivilegeConstants.REP_REMOVE_PROPERTIES}, true);
        superuser.save();

        Map<String, Boolean> pathCanRemoveMap = ImmutableMap.of(
                propertyPath, false,
                childPropertyPath, true,
                grandChildPath + "/" + JcrConstants.JCR_PRIMARYTYPE, false,
                grandChildPath + "/" + propertyName2, false
        );

        Session userSession = createTestSession();
        for (String path : pathCanRemoveMap.keySet()) {
            boolean canRemove = pathCanRemoveMap.get(path);

            try {
                userSession.getProperty(path).remove();
                if (!canRemove) {
                    fail("property at " + path + " should not be removable.");
                }
            } catch (RepositoryException e) {
                if (canRemove) {
                    fail("property at " + path + " should be removable.");
                }
            } finally {
                userSession.refresh(false);
            }
        }
    }

    public void testRemoveNonExistingItems() throws Exception {
        AccessControlUtils.addAccessControlEntry(superuser, childPath, testPrincipal, new String[] {
                Privilege.JCR_REMOVE_NODE,
                Privilege.JCR_REMOVE_CHILD_NODES,
                PrivilegeConstants.REP_REMOVE_PROPERTIES}, true);
        superuser.save();

        // EXERCISE: fill in the expected values for Session.hasPermission(path, Session.ACTION_REMOVE) and explain
        Map<String, Boolean> pathHasPermission = ImmutableMap.of(
                childPath + "_non_existing_sibling", null,
                childPath + "/_non_existing_childitem", null,
                grandChildPath + "/_non_existing_child_item", null
        );

        Session userSession = createTestSession();
        for (String nonExistingItemPath : pathHasPermission.keySet()) {
            boolean hasPermission = pathHasPermission.get(nonExistingItemPath).booleanValue();
            assertEquals(hasPermission, userSession.hasPermission(nonExistingItemPath, Session.ACTION_REMOVE));
        }

        // Additional exercise:
        // EXERCISE: change the set of privileges granted initially and observe the result of Session.hasPermission. Explain the diff
    }

    public void testModifyProperties() throws Exception {
        // grant the test principal rep:alterProperties privilege at 'childPath'
        AccessControlUtils.addAccessControlEntry(superuser, childPath, testPrincipal, new String[] {PrivilegeConstants.REP_ALTER_PROPERTIES}, true);
        superuser.save();

        // EXERCISE: Fill if setting the property at the path(s) is expected to pass or not
        Map<String, Boolean> pathCanModify = ImmutableMap.of(
                propertyPath, null,
                childPropertyPath, null,
                grandChildPath + "/" + JcrConstants.JCR_PRIMARYTYPE, null,
                grandChildPath + "/" + propertyName2, null
        );

        Session userSession = createTestSession();
        for (String path : pathCanModify.keySet()) {
            boolean canModify = pathCanModify.get(path);
            try {
                userSession.getProperty(path).setValue("newVal");
                userSession.save();
                if (!canModify) {
                    fail("setting property at " + path + " should fail.");
                }
            } catch (RepositoryException e) {
                if (canModify) {
                    fail("setting property at " + path + " should not fail.");
                }
            } finally {
                userSession.refresh(false);
            }
        }
    }

    public void testSetProperty() throws RepositoryException {
        // grant the test principal rep:removeProperties privilege at 'childPath'
        AccessControlUtils.addAccessControlEntry(superuser, childPath, testPrincipal, new String[] {PrivilegeConstants.REP_ALTER_PROPERTIES}, true);
        superuser.save();

        // EXERCISE: Fill if new properties values such that the test-cases succeeds
        // EXERCISE: Discuss your findings and explain each value.

        Map<String, Value> pathResultMap = Maps.newHashMap();
        pathResultMap.put(childPropertyPath, (Value) null);
        pathResultMap.put(grandChildPath + "/nonexisting", (Value) null);

        Session userSession = createTestSession();
        for (String path : pathResultMap.keySet()) {
            try {
                Value val = pathResultMap.get(path);

                Node parent = userSession.getNode(Text.getRelativeParent(path, 1));
                parent.setProperty(Text.getName(path), val);

                userSession.save();
            } finally {
                userSession.refresh(false);
            }
        }
    }

    public void testChangingPrimaryAndMixinTypes() throws RepositoryException {
        // 1 - grant privilege to change node type information
        // EXERCISE: fill in the required privilege name such that the test passes
        String privilegeName = null;
        AccessControlUtils.addAccessControlEntry(superuser, childPath, testPrincipal, new String[] {privilegeName}, true);
        superuser.save();

        Session userSession = createTestSession();
        Node n = userSession.getNode(childPath);
        n.setPrimaryType(NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        n.addMixin(JcrConstants.MIX_REFERENCEABLE);
        userSession.save();

        // 2 - additionally grant privilege to add a child node
        // EXERCISE: extend the set of privileges such that the adding a child node succeeds as well
        privilegeName = null;
        AccessControlUtils.addAccessControlEntry(superuser, childPath, testPrincipal, new String[] {privilegeName}, true);
        superuser.save();

        userSession.refresh(false);
        userSession.getNode(childPath).addNode(nodeName4, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        userSession.save();

        // 3 - revoke privilege to change node type information
        // EXERCISE: change the permission setup again such that the rest of the test passes
        superuser.save();

        userSession.refresh(false);
        n = userSession.getNode(childPath);
        n.addNode(nodeName3);
        userSession.save();

        try {
            n.addNode(nodeName1, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            userSession.save();
            fail("Adding node with explicitly the primary type should fail");
        } catch (AccessDeniedException e) {
            // success
        } finally {
            userSession.refresh(false);
        }


        AccessControlUtils.addAccessControlEntry(superuser, childPath, testPrincipal, new String[] {privilegeName}, true);

    }
}
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
package org.apache.jackrabbit.oak.exercise.security.user;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.plugins.lock.LockConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: Remove Authorizables
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand why we strongly recommend not to remove (and recycle) user/group
 * accounts.
 *
 * Exercises:
 *
 * - {@link #testAccessControlEntry()}
 *   Test case illustrating the effect of removing the principal (for simplicity
 *   represented by a test-user) referenced in an access control entry.
 *   Explain the expected behavior and fix the test if necessary
 *
 * - {@link #testCreatedBy()}
 *   Test case illustrating the effect of removing a user which created node
 *   that is of type 'mix:created'.
 *   Explain the expected behavior and fix the test if necessary
 *
 * - {@link #testLastModifiedBy()}
 *   Test case illustrating the effect of removing a user which added the mixin
 *   'mix:lastModified' to the test node.
 *   Explain the expected behavior and fix the test if necessary
 *
 * - {@link #testLock()}
 *   Test case illustrating the effect of removing a user which created a
 *   open-scoped lock.
 *
 * - Based on the experiences from perfoming the above tests, summarize the effect
 *   of removing an existing user and potentially re-using the same ID
 *   at a later point.
 *
 *   Question: What are the implications from a security point of view
 *   Question: What are possible consequences from a legal point of view
 *
 * - Use the user management API to identify alterntive ways such that you don't
 *   need to remove the user.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * In a OSGI-based Oak installation (Sling|Granite|CQ) you can extend this exercise:
 *
 * - Look for additional node types that store references to user or principals.
 *   List node types and the properties
 *
 * - Inspect application code: can you find additional references to user/principals
 *   stored?
 *   Provide a list and discuss the impact from a security|legel point of view
 *
 * - Inspect the various log files for user or principal references
 *   Discuss the legal implications of re-using them for different entities (subjects).
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L10_RemovalAndMembershipTest ()}
 *
 * </pre>
 *
 */
public class L9_RemoveAuthorizableTest extends AbstractJCRTest {

    private UserManager userManager;

    private User testUser;
    private Session testSession;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        userManager = ((JackrabbitSession) superuser).getUserManager();
        testUser = ExerciseUtility.createTestUser(userManager);

        superuser.save();

        // setup full access for test-user on the test-node
        Privilege[] privileges = AccessControlUtils.privilegesFromNames(superuser, Privilege.JCR_ALL);
        if (!AccessControlUtils.addAccessControlEntry(superuser, testRoot, testUser.getPrincipal(), privileges, true)) {
            throw new NotExecutableException();
        }
        superuser.save();

        testSession = getHelper().getRepository().login(ExerciseUtility.getTestCredentials(testUser.getID()));
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (testSession != null) {
                testSession.logout();
            }
            if (testUser != null) {
                testUser.remove();
            }
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    private void removeTestUser() throws RepositoryException {
        testUser.remove();
        superuser.save();
        testUser = null;
    }

    private Node getAuthorizableNode(Authorizable authorizable) throws RepositoryException {
        String path = authorizable.getPath();
        return superuser.getNode(path);
    }

    public void testAccessControlEntry() throws RepositoryException, NotExecutableException {
        // remove test user
        removeTestUser();

        boolean found = false;
        AccessControlList acl = AccessControlUtils.getAccessControlList(superuser, testRoot);
        if (acl != null) {
            for (AccessControlEntry ace : acl.getAccessControlEntries()) {
                if (testUser.getPrincipal().getName().equals(ace.getPrincipal().getName())) {
                    found = true;
                }
            }
        }

        // EXERCISE: do you expect the ACE for test-principal to be still present? explain why and fix the test if necessary.
        assertTrue(found);
    }

    public void testCreatedBy() throws RepositoryException {
        Node testNode = testSession.getNode(testRoot);
        Node folder = testNode.addNode("folder", JcrConstants.NT_FOLDER);
        testSession.save();

        // EXERCISE: explain why the folder node must have a jcr:created property.
        assertTrue(folder.hasProperty(NodeTypeConstants.JCR_CREATEDBY));
        assertEquals(testSession.getUserID(), folder.getProperty(NodeTypeConstants.JCR_CREATEDBY).getString());

        removeTestUser();
        testSession.refresh(false);

        // EXERCISE: do you expect jcr:createdBy property to be still present? explain why and fix the test if necessary.
        assertTrue(folder.hasProperty(NodeTypeConstants.JCR_CREATEDBY));
    }

    public void testLastModifiedBy() throws RepositoryException {
        Node testNode = testSession.getNode(testRoot);
        testNode.addMixin(NodeTypeConstants.MIX_LASTMODIFIED);
        testNode.setProperty(propertyName1, "any value");
        testSession.save();

        assertTrue(testNode.hasProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY));
        assertEquals(testSession.getUserID(), testNode.getProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY).getString());

        removeTestUser();
        testSession.refresh(false);

        // EXERCISE: do you expect the property to be still present? explain why and fix the test if necessary.
        assertTrue(testNode.hasProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY));
    }

    public void testLock() throws RepositoryException {
        Node testNode = testSession.getNode(testRoot);
        testNode.addMixin(JcrConstants.MIX_LOCKABLE);
        testSession.save();

        testNode.lock(true, false);

        try {
            assertTrue(testNode.hasProperty(LockConstants.JCR_LOCKOWNER));
            assertEquals(testSession.getUserID(), testNode.getProperty(LockConstants.JCR_LOCKOWNER).getString());

            removeTestUser();
            testSession.refresh(false);

            // EXERCISE: do you expect the property to be still present? explain why and fix the test if necessary.
            assertTrue(testNode.hasProperty(LockConstants.JCR_LOCKOWNER));

        } finally {
            testNode.unlock();
        }
    }
}
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: Group Membership
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * After having completed this exercise you should be able to manage group
 * membership relations using the Jackrabbit User Management API and understand
 * the basic design on how group membership is store and handled in the
 * Oak repository.
 *
 * Exercises:
 *
 * - Overview
 *   Take a look at the Jackrabbit User Management API and how group membership
 *   is being edited (in {@link org.apache.jackrabbit.api.security.user.Group}
 *   and discovered in both directions on {@link org.apache.jackrabbit.api.security.user.Group}
 *   and {@link org.apache.jackrabbit.api.security.user.Authorizable}.
 *
 * - {@link #testAddRemoveMembers()}
 *   Add (and remove) members to the predefined groups such that the test passes.
 *
 * - {@link #testDeclaredMembership()}
 *   This test illustrates how declared members are retrieved from a group and
 *   how to obtain the declared group membership of a given authorizable (user or
 *   group).
 *
 *   Question: This test is executed with full permission. Can you elaborate
 *   what happens if the editing session has limited read access on the user/group
 *   tree(s)?
 *
 * - {@link #testInheritedMembership()}
 *   This test illustrates how all members of a group are retrieved and
 *   how to obtain full group membership of a given authorizable (user or
 *   group). Complete the test such that it passes.
 *
 * - {@link #testMembersContentStructure()}
 *   In order to complete this exercise look at the built-in node types and
 *   identify those types that deal with group membership.
 *   Once you are familiar with the node type definitions look at
 *   > {@link org.apache.jackrabbit.oak.security.user.MembershipProvider} and
 *   > {@link org.apache.jackrabbit.oak.security.user.MembershipWriter}
 *   and how they deal with massive amount of members on a given group.
 *   Finally fix the test case :-)
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * As you can see from the API calls present as of Jackrabbit API 2.10 you
 * currently need to have an existing authorizable at hand in order to add
 * (or remove) it as member of a given group.
 * Since group membership is stored as {@link javax.jcr.PropertyType#WEAKREFERENCE}
 * the repository is not obligated to enforce the validity of the references
 * and thus might choose to create membership references that cannot (yet) be
 * resolved.
 *
 * Look at the following unit tests present with the oak-jcr module and observe
 * how the configured {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}
 * affects the import of group-membership information that cannot be resolved
 * to an existing authorizable:
 *
 * - {@link org.apache.jackrabbit.oak.jcr.security.user.GroupImportBestEffortTest#testImportNonExistingMemberBestEffort()}
 * - {@link org.apache.jackrabbit.oak.jcr.security.user.GroupImportAbortTest#testImportNonExistingMemberAbort()}
 * - {@link org.apache.jackrabbit.oak.jcr.security.user.GroupImportIgnoreTest#testImportNonExistingMemberIgnore()} ()}
 *
 * Exercises
 *
 * 1. Walk through the XML import and take a close look at
 *    {@link org.apache.jackrabbit.oak.security.user.UserImporter}, where the XML
 *    of the protected items defined with the various authorizable node types
 *    are being handled. Compare the differences wrt to the import behavior.
 *
 * 2. Discuss possible use-cases where creating a group with members that don't
 *    (yet) exist (anymore) might be helpful (or even required).
 *
 *
 * Advanced Exercise:
 * -----------------------------------------------------------------------------
 *
 * Having completed the additional exercises wrt {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}
 * and the XML import of non-existing group members, you may want to complete
 * following exercise.
 *
 * Question: How might the implementation of the API extensions proposed in
 * <a href="https://issues.apache.org/jira/browse/JCR-3880">JCR-3880</a> could
 * look like such that the implementation both matches the API contract and
 * is consistent with the XML import?
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.api.security.user.Authorizable#declaredMemberOf()
 * @see org.apache.jackrabbit.api.security.user.Authorizable#memberOf()
 * @see org.apache.jackrabbit.api.security.user.Group#isMember(org.apache.jackrabbit.api.security.user.Authorizable)
 * @see org.apache.jackrabbit.api.security.user.Group#isDeclaredMember(org.apache.jackrabbit.api.security.user.Authorizable)
 * @see org.apache.jackrabbit.api.security.user.Group#getMembers()
 * @see org.apache.jackrabbit.api.security.user.Group#getDeclaredMembers()
 * @see org.apache.jackrabbit.api.security.user.Group#addMember(org.apache.jackrabbit.api.security.user.Authorizable)
 * @see org.apache.jackrabbit.api.security.user.Group#removeMember(org.apache.jackrabbit.api.security.user.Authorizable)
 */
public class L8_MembershipTest extends AbstractSecurityTest {

    private User user;
    private Group group;
    private Group group2;
    private Group group3;

    private List<Authorizable> toRemove = new ArrayList<Authorizable>();

    @Override
    public void before() throws Exception {
        super.before();

        user = (User) createNewAuthorizable(false);
        group = (Group) createNewAuthorizable(true);
        group2 = (Group) createNewAuthorizable(true);
        group3 = (Group) createNewAuthorizable(true);

        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            for (Authorizable a : toRemove) {
                a.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    private Authorizable createNewAuthorizable(boolean isGroup) throws RepositoryException {
        UserManager uMgr = getUserManager(root);
        Authorizable a = (isGroup) ? ExerciseUtility.createTestGroup(uMgr) : ExerciseUtility.createTestUser(uMgr);
        toRemove.add(a);
        return a;
    }

    @Test
    public void testAddRemoveMembers() throws RepositoryException, CommitFailedException {
        // EXERCISE: add group members such that the following test-cases passes.
        root.commit();

        assertTrue(group.isDeclaredMember(user));
        assertFalse(group2.isDeclaredMember(user));
        assertFalse(group3.isDeclaredMember(user));
        assertTrue(group2.isMember(user));
        assertTrue(group3.isMember(user));

        Boolean isDeclaredMember = null; // EXERCISE
        assertEquals(isDeclaredMember, group.isDeclaredMember(group2));

        isDeclaredMember = null; // EXERCISE
        assertEquals(isDeclaredMember, group3.isDeclaredMember(group));

        Boolean isMember = null; // EXERCISE
        assertEquals(isMember, group.isMember(group2));

        isMember = null; // EXERCISE
        assertEquals(isMember, group3.isMember(group));

        // EXERCISE: now change the group membership such that the following assertions pass
        root.commit();

        assertFalse(group.isMember(user));
        assertTrue(group.isMember(group2));
        assertTrue(group3.isMember(group2));
        assertTrue(group3.isMember(group));
        assertTrue(group3.isMember(user));
    }

    @Test
    public void testDeclaredMembership() throws RepositoryException, CommitFailedException {
        group.addMember(group2);
        group2.addMember(group3);
        group3.addMember(user);
        root.commit();

        Set<Group> expectedGroups = null; // EXERCISE
        Iterator<Group> groups = user.declaredMemberOf();
        while (groups.hasNext()) {
            assertTrue(expectedGroups.remove(groups.next()));
        }
        assertTrue(expectedGroups.isEmpty());

        Set<Authorizable> expectedMembers = null; // EXERCISE
        Iterator<Authorizable> memberIterator = group.getDeclaredMembers();
        while (memberIterator.hasNext()) {
            assertTrue(expectedMembers.remove(memberIterator.next()));
        }
        assertTrue(expectedMembers.isEmpty());
    }

    @Test
    public void testInheritedMembership() throws RepositoryException, CommitFailedException {
        group.addMember(group2);
        group.addMember(group3);
        group3.addMember(user);
        root.commit();

        Set<Group> groups = null; // EXERCISE
        Iterator<Group> groupIterator = user.memberOf();
        while (groupIterator.hasNext()) {
            Group gr = groupIterator.next();
            assertTrue(groups.remove(gr));
        }
        assertTrue(groups.isEmpty());

        Set<Authorizable> expectedMembers = null; // EXERCISE
        Iterator<Authorizable> memberIterator = group.getMembers();
        while (memberIterator.hasNext()) {
            assertTrue(expectedMembers.remove(memberIterator.next()));
        }
        assertTrue(expectedMembers.isEmpty());
    }

    @Test
    public void testMembersContentStructure() throws RepositoryException, CommitFailedException {
        int size = 500; //MembershipWriter.DEFAULT_MEMBERSHIP_THRESHHOLD * 5;

        List<String> memberUuids = new ArrayList<String>();
        for (int i = 0; i < size; i++) {
            Authorizable user = createNewAuthorizable(false);
            String uuid = TreeUtil.getString(root.getTree(user.getPath()), JcrConstants.JCR_UUID);
            //assertNotNull(uuid);
            memberUuids.add(uuid);
            group.addMember(user);
        }
        root.commit();

        Tree groupTree = root.getTree(group.getPath());
        Iterator<String> values = groupTree.getProperty(UserConstants.REP_MEMBERS).getValue(Type.STRINGS).iterator();
        while (values.hasNext()) {
            assertTrue(memberUuids.remove(values.next()));
        }
        assertFalse(memberUuids.isEmpty());

        // EXERCISE: retrieve the rest of the member-information stored with the group
        // EXERCISE: by looking at the tree structure created by the MembershipWriter

        assertTrue(memberUuids.isEmpty());
    }
}
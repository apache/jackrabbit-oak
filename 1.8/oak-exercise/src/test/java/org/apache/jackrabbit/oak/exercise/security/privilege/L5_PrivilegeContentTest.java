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

import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.exercise.security.authorization.permission.L4_PrivilegesAndPermissionsTest;
import org.apache.jackrabbit.oak.exercise.security.authorization.permission.L7_PermissionContentTest;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * <pre>
 * Module: Privilege Management
 * =============================================================================
 *
 * Title: Representation of Privileges in the Repository
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand how privileges are represented in the repository content.
 *
 * Exercises:
 *
 * - {@link #testPrivilegeRoot()}
 *   This test retrieves the root node below which all registered privileges are
 *   being stored. Make yourself familiar with the structure by looking at the
 *   node type definitions in 'builtin-nodetypes.cnd' and complete the test case
 *   such that it passes.
 *
 *   Question: What can you say about the structure of the privileges tree in the repository?
 *   Question: Can you explain why it is located below /jcr:system ?
 *   Question: Go back to {@link L4_CustomPrivilegeTest}
 *   and take a closer look at the tree/node that stores your custom privilege.
 *
 * - {@link #testPrivilegeDefinition()}
 *   Each tree/node presenting a privilege in fact just stores the basic definition
 *   of the privilege. Use this exercise to compare the difference between the
 *   definition (and it's tree) and the resulting privilege.
 *
 *   Question: What properties stored on the privilege definition tree are not exposed by
 *             {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeDefinition}?
 *             -> see advanced exercises.
 *
 *
 * Advanced Exercises
 * -----------------------------------------------------------------------------
 *
 * - {@link #testPrivilegeBits()}
 *   For internal handling of privileges the Oak repository doesn't use the
 *   privilege names (and the expensive resolution of the aggregation) but
 *   rather makes use of internal long representation of the privileges.
 *   Use this exercise to become familiar with
 *   - {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider}
 *   - {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits}
 *   and how they are represented in the privilege content and how they are
 *   used to internally deal with privileges.
 *
 *   Question: How are PrivilegeBits created from privilege name(s)
 *   Question: How are they stored with the privilege definition?
 *   Question: Can you also retrieve the PropertyState on the privilege trees that
 *             effectively stores the long presentation as used in the PrivilegeBits?
 *
 * - {@link #testNext()}
 *   This exercises aims to help you understand how the implementation keeps
 *   track of the internal long representation of privileges and how those
 *   long representations are being calculated for newly registered privileges.
 *   Resolve the EXERCISE marks in the test-case and explain the behavior.
 *
 *   Question: Can you identify where 'rep:next' is being updated?
 *   Question: Try to set the value of rep:next manually and explain what happens.
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L6_JcrAllTest}
 * - {@link L4_PrivilegesAndPermissionsTest}
 * - {@link L7_PermissionContentTest}
 *
 * </pre>
 */
public class L5_PrivilegeContentTest extends AbstractSecurityTest {


    @Test
    public void testPrivilegeRoot() {
        Tree privilegesRoot = root.getTree(PrivilegeConstants.PRIVILEGES_PATH);

        String name = null; // EXERCISE
        assertEquals(name, privilegesRoot.getName());

        String primaryType = null; // EXERCISE
        assertEquals(primaryType, TreeUtil.getPrimaryTypeName(privilegesRoot));

        // EXERCISE: look at the node type definition in the file 'builtin-nodetypes.cnd'
        // Question: can you predict how the tree defined the 'privilegesRoot' tree looks like?
    }

    @Test
    public void testPrivilegeDefinition() throws RepositoryException {
        Tree repWriteTree = PrivilegeUtil.getPrivilegesTree(root).getChild(PrivilegeConstants.REP_WRITE);

        PrivilegeDefinition def = PrivilegeUtil.readDefinition(repWriteTree);

        String expectedName = null; // EXERCISE
        assertEquals(expectedName, def.getName());

        boolean isAbstract = false; // EXERCISE
        assertEquals(isAbstract, def.isAbstract());

        Set<String> expectedAggregates = null; // EXERCISE
        assertEquals(expectedAggregates, def.getDeclaredAggregateNames());

        // EXERCISE: compare the internal privilege definition (and it's tree representation) with the privilege itself.
        Privilege repWritePrivilege = getPrivilegeManager(root).getPrivilege(PrivilegeConstants.REP_WRITE);
    }

    @Test
    public void testPrivilegeBits() {
        Tree jcrReadTree = PrivilegeUtil.getPrivilegesTree(root).getChild(PrivilegeConstants.JCR_READ);
        Tree repWriteTree = PrivilegeUtil.getPrivilegesTree(root).getChild(PrivilegeConstants.REP_WRITE);

        PrivilegeBitsProvider provider = new PrivilegeBitsProvider(root);
        PrivilegeBits privilegeBits = provider.getBits(PrivilegeConstants.REP_WRITE, PrivilegeBits.JCR_READ);

        PrivilegeBits readBits = PrivilegeBits.getInstance(jcrReadTree);
        PrivilegeBits writeBits = PrivilegeBits.getInstance(jcrReadTree);

        // EXERCISE: play with 'PrivilegeBits' methods to compare 'privilegeBits' with 'readBits' and 'writeBits'
        // EXERCISE: retrieve the property that stores the long representation of each privilege above
    }

    @Test
    public void testNext() throws RepositoryException, CommitFailedException {
        PropertyState next = PrivilegeUtil.getPrivilegesTree(root).getProperty(PrivilegeConstants.REP_NEXT);

        PrivilegeManager privilegeManager = getPrivilegeManager(root);
        Privilege newPrivilege = privilegeManager.registerPrivilege("myPrivilege", true, null);
        root.commit();

        // EXERCISE: compare the 'next' property state with rep:bits property of the newly created privilege.

        PropertyState nextAgain = PrivilegeUtil.getPrivilegesTree(root).getProperty(PrivilegeConstants.REP_NEXT);

        // EXERCISE: look at the new value of rep:next and explain it. Q: where did it get modified?

        // EXERCISE: try to modify rep:next manually and explain what happens.
    }
}
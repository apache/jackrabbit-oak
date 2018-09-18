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
package org.apache.jackrabbit.oak.exercise.security.authorization.accesscontrol;

import java.security.Principal;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeType;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

import static org.junit.Assert.assertArrayEquals;

/**
 * <pre>
 * Module: Authorization (Access Control Management)
 * =============================================================================
 *
 * Title: Representation of Access Control Content in the Repository
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand how the default implementation represents access control content
 * in the repository.
 *
 * Exercises:
 *
 * - Overview
 *   Look {@code org/apache/jackrabbit/oak/plugins/nodetype/write/builtin_nodetypes.cnd}
 *   and try to identify the built in node types used to store access control
 *   content.
 *
 *   Question: Can explain the meaning of all types?
 *   Question: Why are most item definitions protected?
 *   Question: Can you identify node types that are not used? Can you explain why?
 *
 * - {@link #testAclContent()}
 *   This test case writes an ACL to the repository.
 *   Fix the test such that it retrieves the policy node and verify the expected
 *   nature of this node.
 *
 * - {@link #testAceContent()}
 *   This test case writes an ACL with entries to the repository.
 *   Fix the test such that it retrieves the policy node and verify the expected
 *   nature of the access control entry nodes.
 *
 * - {@link #testRestrictionContent()}
 *   Same as above but this time you should look at the restrictions and how they
 *   are represented in the content.
 *
 * - {@link #testMixins()}
 *   Fix the test by defining the expected mixin types.
 *
 *   Question: Can you explain why those mixins are present and who added them?
 *   Question: Can make a recommendation for other developers wrt the ac-related mixin types? Should they be added manually?
 *
 * - {@link #testRepoPolicy()}
 *   Same as {@link #testAclContent()} but this time for the 'null' path.
 *   Fix the test case and verify your expections.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Named {@code ReadPolicy}
 *   In the previous exercises you learned about the special named policy
 *   {@link org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlManagerImpl.ReadPolicy}.
 *
 *   Question: Can you find the content representation of this policy?
 *   Question: Can you explain what is happening?
 *
 * </pre>
 */
public class L6_AccessControlContentTest extends AbstractJCRTest {

    private AccessControlManager acMgr;
    private JackrabbitAccessControlList acl;

    private Principal testPrincipal;
    private Privilege[] testPrivileges;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        acMgr = superuser.getAccessControlManager();

        testPrincipal = ExerciseUtility.createTestGroup(((JackrabbitSession) superuser).getUserManager()).getPrincipal();
        superuser.save();

        acl = AccessControlUtils.getAccessControlList(superuser, testRoot);
        if (acl == null) {
            throw new NotExecutableException();
        }

        testPrivileges = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ, Privilege.JCR_WRITE);
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            Authorizable testGroup = ((JackrabbitSession) superuser).getUserManager().getAuthorizable(testPrincipal);
            if (testGroup != null) {
                testGroup.remove();
                superuser.save();
            }
        } finally {
            super.tearDown();
        }
    }

    public void testAclContent() throws RepositoryException {
        acMgr.setPolicy(testRoot, acl);

        // EXERCISE retrieve the policy node and verify the expected name, primary type and child items
        String policyPath = null;
        Node aclNode = superuser.getNode(policyPath);

        String expectedName = null;
        assertEquals(expectedName, aclNode.getName());

        String expectedPrimaryTypeName = null;
        assertEquals(expectedPrimaryTypeName, aclNode.getPrimaryNodeType().getName());

        NodeIterator aclChildren = aclNode.getNodes();
        // EXERCISE verify the correct number + expected nature of the children.
    }

    public void testAceContent() throws RepositoryException {
        acl.addAccessControlEntry(testPrincipal, testPrivileges);
        acl.addEntry(EveryonePrincipal.getInstance(), testPrivileges, false);
        acMgr.setPolicy(testRoot, acl);

        String policyPath = null; // EXERCISE
        Node aclNode = superuser.getNode(policyPath);

        NodeIterator aclChildren = aclNode.getNodes();

        int expectedSize = -1; // EXERCISE
        assertEquals(expectedSize, aclChildren.getSize());

        String expectedPrimaryTypeName = null; // EXERCISE: define the type of the first child node.
        while (aclChildren.hasNext()) {
            Node ace = aclChildren.nextNode();

            assertEquals(expectedPrimaryTypeName, ace.getPrimaryNodeType().getName());
            expectedPrimaryTypeName = null; // EXERCISE: define the type of the next item.
        }

        Node ace = aclNode.getNodes().nextNode();
        // EXERCISE: retrieve all mandatory ac-related properties of this node and verify the expected value.
    }

    public void testRestrictionContent() throws RepositoryException {
        ValueFactory vf = superuser.getValueFactory();
        acl.addEntry(testPrincipal, testPrivileges, false,
                ImmutableMap.of(AccessControlConstants.REP_GLOB, vf.createValue("")),
                ImmutableMap.of(AccessControlConstants.REP_PREFIXES, new Value[] {vf.createValue("jcr"), vf.createValue("mix")}));
        acMgr.setPolicy(testRoot, acl);

        String policyPath = null; // EXERCISE
        Node aclNode = superuser.getNode(policyPath);

        Node ace = aclNode.getNodes().nextNode();
        // EXERCISE: retrieve the restrictions defined for the single ACE node
        // EXERCISE: verify the expected properties and their value(s)
    }

    public void testMixins() throws RepositoryException {
        acMgr.setPolicy(testRoot, acl);

        NodeType[] mixins = superuser.getNode(acl.getPath()).getMixinNodeTypes();
        NodeType[] expectedMixins = null;
        assertArrayEquals(expectedMixins, mixins);
    }

    public void testRepoPolicy() throws RepositoryException {
        AccessControlList repoAcl = AccessControlUtils.getAccessControlList(acMgr, null);

        assertNotNull(repoAcl);
        repoAcl.addAccessControlEntry(testPrincipal, AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT));
        acMgr.setPolicy(null, repoAcl);

        // EXERCISE retrieve the policy node and verify the expected name, primary type and child items
        String policyPath = null;
        Node aclNode = superuser.getNode(policyPath);

        String expectedName = null;
        assertEquals(expectedName, aclNode.getName());

        String expectedPrimaryTypeName = null;
        assertEquals(expectedPrimaryTypeName, aclNode.getPrimaryNodeType().getName());

        NodeIterator aclChildren = aclNode.getNodes();
        // EXERCISE verify the correct number + expected nature of the children.

        // EXERCISE: can you also identify which mixins are being involved and where they got applied?
    }
}

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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import java.security.Principal;
import javax.jcr.AccessDeniedException;
import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AccessControlValidatorTest extends AbstractSecurityTest implements AccessControlConstants {

    private final String testName = "testRoot";
    private final String testPath = '/' + testName;
    private final String aceName = "validAce";

    private Principal testPrincipal;

    @Before
    public void before() throws Exception {
        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"), getNamePathMapper());
        rootNode.addChild(testName, JcrConstants.NT_UNSTRUCTURED);

        root.commit();

        testPrincipal = getTestUser().getPrincipal();
    }

    @After
    public void after() throws Exception {
        try {
            Tree testRoot = root.getTree(testPath);
            if (testRoot.exists()) {
                testRoot.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    private NodeUtil getTestRoot() {
        return new NodeUtil(root.getTree(testPath));
    }

    private NodeUtil createAcl() throws AccessDeniedException {
        NodeUtil testRoot = getTestRoot();
        testRoot.setNames(JcrConstants.JCR_MIXINTYPES, MIX_REP_ACCESS_CONTROLLABLE);

        NodeUtil acl = testRoot.addChild(REP_POLICY, NT_REP_ACL);
        NodeUtil ace = createACE(acl, aceName, NT_REP_GRANT_ACE, testPrincipal.getName(), PrivilegeConstants.JCR_READ);
        ace.addChild(REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        return acl;
    }

    private static NodeUtil createACE(NodeUtil acl, String aceName, String ntName, String principalName, String... privilegeNames) throws AccessDeniedException {
        NodeUtil ace = acl.addChild(aceName, ntName);
        ace.setString(REP_PRINCIPAL_NAME, principalName);
        ace.setNames(REP_PRIVILEGES, privilegeNames);
        return ace;
    }

    @Test
    public void testPolicyWithOutChildOrder() throws AccessDeniedException {
        NodeUtil testRoot = getTestRoot();
        testRoot.setNames(JcrConstants.JCR_MIXINTYPES, MIX_REP_ACCESS_CONTROLLABLE);
        testRoot.addChild(REP_POLICY, NT_REP_ACL);

        try {
            root.commit();
            fail("Policy node with child node ordering");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessControlViolation());
            assertThat(e.getMessage(), containsString("OakAccessControl0004")); // Order of children is not stable
            assertThat(e.getMessage(), containsString("/testRoot/rep:policy"));
        }
    }

    @Test
    public void testOnlyRootIsRepoAccessControllable() {
        NodeUtil testRoot = getTestRoot();
        testRoot.setNames(JcrConstants.JCR_MIXINTYPES, MIX_REP_REPO_ACCESS_CONTROLLABLE);

        try {
            root.commit();
            fail("Only the root node can be made RepoAccessControllable.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessControlViolation());
            assertThat(e.getMessage(), containsString("/testRoot"));
        }
    }

    @Test
    public void testAddInvalidRepoPolicy() throws Exception {
        NodeUtil testRoot = getTestRoot();
        testRoot.setNames(JcrConstants.JCR_MIXINTYPES, MIX_REP_ACCESS_CONTROLLABLE);
        NodeUtil policy = getTestRoot().addChild(REP_REPO_POLICY, NT_REP_ACL);
        try {
            root.commit();
            fail("Attempt to add repo-policy with rep:AccessControllable node.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessControlViolation());
            assertThat(e.getMessage(), containsString("/testRoot"));
        } finally {
            policy.getTree().remove();
        }
    }

    @Test
    public void testAddPolicyWithAcContent() throws Exception {
        NodeUtil acl = createAcl();
        NodeUtil ace = acl.getChild(aceName);

        NodeUtil[] acContent = new NodeUtil[]{acl, ace, ace.getChild(REP_RESTRICTIONS)};
        for (NodeUtil node : acContent) {
            NodeUtil policy = node.addChild(REP_POLICY, NT_REP_ACL);
            try {
                root.commit();
                fail("Adding an ACL below access control content should fail");
            } catch (CommitFailedException e) {
                // success
                assertTrue(e.isConstraintViolation());
                assertThat(e.getMessage(), containsString("/testRoot/rep:policy"));
            } finally {
                policy.getTree().remove();
            }
        }
    }

    @Test
    public void testAddRepoPolicyWithAcContent() throws Exception {
        NodeUtil acl = createAcl();
        NodeUtil ace = acl.getChild(aceName);

        NodeUtil[] acContent = new NodeUtil[]{acl, ace, ace.getChild(REP_RESTRICTIONS)};
        for (NodeUtil node : acContent) {
            NodeUtil policy = node.addChild(REP_REPO_POLICY, NT_REP_ACL);
            try {
                root.commit();
                fail("Adding an ACL below access control content should fail");
            } catch (CommitFailedException e) {
                // success
                assertTrue(e.isConstraintViolation());
                assertThat(e.getMessage(), containsString("/testRoot/rep:policy"));
            } finally {
                policy.getTree().remove();
            }
        }
    }

    @Test
    public void testAddAceWithAcContent() throws Exception {
        NodeUtil acl = createAcl();
        NodeUtil ace = acl.getChild(aceName);

        NodeUtil[] acContent = new NodeUtil[]{ace, ace.getChild(REP_RESTRICTIONS)};
        for (NodeUtil node : acContent) {
            NodeUtil entry = node.addChild("invalidACE", NT_REP_DENY_ACE);
            try {
                root.commit();
                fail("Adding an ACE below an ACE or restriction should fail");
            } catch (CommitFailedException e) {
                // success
                assertTrue(e.isConstraintViolation());
                assertThat(e.getMessage(), containsString("/testRoot/rep:policy/validAce"));
            } finally {
                entry.getTree().remove();
            }
        }
    }

    @Test
    public void testAddRestrictionWithAcContent() throws Exception {
        NodeUtil acl = createAcl();
        NodeUtil ace = acl.getChild(aceName);

        NodeUtil[] acContent = new NodeUtil[]{acl, ace.getChild(REP_RESTRICTIONS)};
        for (NodeUtil node : acContent) {
            NodeUtil entry = node.addChild("invalidRestriction", NT_REP_RESTRICTIONS);
            try {
                root.commit();
                fail("Adding an ACE below an ACE or restriction should fail");
            } catch (CommitFailedException e) {
                // success
                assertTrue(e.isConstraintViolation());
                assertThat(e.getMessage(), containsString("/testRoot/rep:policy"));
            } finally {
                entry.getTree().remove();
            }
        }
    }

    @Test
    public void testAddIsolatedPolicy() throws Exception {
        String[] policyNames = new String[]{"isolatedACL", REP_POLICY, REP_REPO_POLICY};
        NodeUtil node = getTestRoot();

        for (String policyName : policyNames) {
            NodeUtil policy = node.addChild(policyName, NT_REP_ACL);
            try {
                root.commit();
                fail("Writing an isolated ACL without the parent being rep:AccessControllable should fail.");
            } catch (CommitFailedException e) {
                // success
                assertTrue(e.isAccessControlViolation());
                assertThat(e.getMessage(), containsString("/testRoot"));
            } finally {
                // revert pending changes that cannot be saved.
                policy.getTree().remove();
            }
        }

    }

    @Test
    public void testAddIsolatedAce() throws Exception {
        String[] ntNames = new String[]{NT_REP_DENY_ACE, NT_REP_GRANT_ACE};
        NodeUtil node = getTestRoot();

        for (String aceNtName : ntNames) {
            NodeUtil ace = createACE(node, "isolatedACE", aceNtName, testPrincipal.getName(), PrivilegeConstants.JCR_READ);
            try {
                root.commit();
                fail("Writing an isolated ACE should fail.");
            } catch (CommitFailedException e) {
                // success
                assertTrue(e.isAccessControlViolation());
                assertThat(e.getMessage(), containsString("/testRoot/isolatedACE"));
            } finally {
                // revert pending changes that cannot be saved.
                ace.getTree().remove();
            }
        }
    }

    @Test
    public void testAddIsolatedRestriction() throws Exception {
        NodeUtil node = getTestRoot();
        NodeUtil restriction = node.addChild("isolatedRestriction", NT_REP_RESTRICTIONS);
        try {
            root.commit();
            fail("Writing an isolated Restriction should fail.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessControlViolation());
            assertThat(e.getMessage(), containsString("/testRoot"));
        } finally {
            // revert pending changes that cannot be saved.
            restriction.getTree().remove();
        }
    }

    @Test
    public void testInvalidPrivilege() throws Exception {
        NodeUtil acl = createAcl();

        String privName = "invalidPrivilegeName";
        createACE(acl, "invalid", NT_REP_GRANT_ACE, testPrincipal.getName(), privName);
        try {
            root.commit();
            fail("Creating an ACE with invalid privilege should fail.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessControlViolation());
            assertThat(e.getMessage(), containsString("/testRoot/rep:policy"));
        }
    }

    @Test
    public void testAbstractPrivilege() throws Exception {
        PrivilegeManager pMgr = getPrivilegeManager(root);
        pMgr.registerPrivilege("abstractPrivilege", true, new String[0]);

        NodeUtil acl = createAcl();
        createACE(acl, "invalid", NT_REP_GRANT_ACE, testPrincipal.getName(), "abstractPrivilege");
        try {
            root.commit();
            fail("Creating an ACE with an abstract privilege should fail.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessControlViolation());
            assertThat(e.getMessage(), containsString("/testRoot/rep:policy"));
        }
    }

    @Test
    public void testInvalidRestriction() throws Exception {
        NodeUtil restriction = createAcl().getChild(aceName).getChild(REP_RESTRICTIONS);
        restriction.setString("invalid", "value");
        try {
            root.commit();
            fail("Creating an unsupported restriction should fail.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessControlViolation());
            assertThat(e.getMessage(), containsString("/testRoot/rep:policy"));
        }
    }

    @Test
    public void testRestrictionWithInvalidType() throws Exception {
        NodeUtil restriction = createAcl().getChild(aceName).getChild(REP_RESTRICTIONS);
        restriction.setName(REP_GLOB, "rep:glob");
        try {
            root.commit();
            fail("Creating restriction with invalid type should fail.");
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessControlViolation());
            assertThat(e.getMessage(), containsString("/testRoot/rep:policy"));
        }
    }

    @Test
    public void testDuplicateAce() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES));
        acMgr.setPolicy(testPath, acl);

        // add duplicate ac-entry on OAK-API
        NodeUtil policy = new NodeUtil(root.getTree(testPath + "/rep:policy"));
        NodeUtil ace = policy.addChild("duplicateAce", NT_REP_GRANT_ACE);
        ace.setString(REP_PRINCIPAL_NAME, testPrincipal.getName());
        ace.setNames(AccessControlConstants.REP_PRIVILEGES, PrivilegeConstants.JCR_ADD_CHILD_NODES);

        try {
            root.commit();
            fail("Creating duplicate ACE must be detected");
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessControlViolation());
            assertThat(e.getMessage(), containsString("/testRoot/rep:policy/duplicateAce"));
        }
    }

    @Test
    public void testAceDifferentByRestrictionValue() throws Exception {
        ValueFactory vf = getValueFactory(root);

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES), true,
                ImmutableMap.<String, Value>of(),
                ImmutableMap.of(AccessControlConstants.REP_NT_NAMES, new Value[] {vf.createValue(NodeTypeConstants.NT_OAK_UNSTRUCTURED, PropertyType.NAME)}));

        // add ac-entry that only differs by the value of the restriction
        acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES), true,
                ImmutableMap.<String, Value>of(),
                ImmutableMap.of(AccessControlConstants.REP_NT_NAMES, new Value[] {vf.createValue(NodeTypeConstants.NT_UNSTRUCTURED, PropertyType.NAME)}));
        assertEquals(2, acl.getAccessControlEntries().length);

        acMgr.setPolicy(testPath, acl);

        // persisting changes must succeed; the 2 ac-entries must not be treated as equal.
        root.commit();
    }

    @Test
    public void hiddenNodeAdded() throws CommitFailedException {
        AccessControlValidatorProvider provider = new AccessControlValidatorProvider(getSecurityProvider());
        MemoryNodeStore store = new MemoryNodeStore();
        NodeState root = store.getRoot();
        NodeBuilder builder = root.builder();
        NodeBuilder test = builder.child("test");
        NodeBuilder hidden = test.child(":hidden");

        Validator validator = provider.getRootValidator(
                root, builder.getNodeState(), CommitInfo.EMPTY);
        Validator childValidator = validator.childNodeAdded(
                "test", test.getNodeState());
        assertNotNull(childValidator);

        Validator hiddenValidator = childValidator.childNodeAdded(":hidden", hidden.getNodeState());
        assertNull(hiddenValidator);
    }

    @Test
    public void hiddenNodeChanged() throws CommitFailedException {
        AccessControlValidatorProvider provider = new AccessControlValidatorProvider(getSecurityProvider());
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").child(":hidden");
        NodeState root = builder.getNodeState();

        NodeBuilder test = root.builder().child("test");
        NodeBuilder hidden = test.child(":hidden");
        hidden.child("added");

        Validator validator = provider.getRootValidator(
                root, builder.getNodeState(), CommitInfo.EMPTY);
        Validator childValidator = validator.childNodeChanged(
                "test", root.getChildNode("test"), test.getNodeState());
        assertNotNull(childValidator);

        Validator hiddenValidator = childValidator.childNodeChanged(":hidden", root.getChildNode("test").getChildNode(":hidden"), hidden.getNodeState());
        assertNull(hiddenValidator);
    }

    @Test
    public void hiddenNodeDeleted() throws CommitFailedException {
        AccessControlValidatorProvider provider = new AccessControlValidatorProvider(getSecurityProvider());
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").child(":hidden");
        NodeState root = builder.getNodeState();

        builder = root.builder();
        NodeBuilder test = builder.child("test");
        test.child(":hidden").remove();

        Validator validator = provider.getRootValidator(
                root, builder.getNodeState(), CommitInfo.EMPTY);
        Validator childValidator = validator.childNodeChanged("test", root.getChildNode("test"), test.getNodeState());
        assertNotNull(childValidator);

        Validator hiddenValidator = childValidator.childNodeDeleted(
                ":hidden", root.getChildNode("test").getChildNode(":hidden"));
        assertNull(hiddenValidator);
    }
}
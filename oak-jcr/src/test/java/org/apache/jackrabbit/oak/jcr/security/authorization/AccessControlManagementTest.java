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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import javax.jcr.AccessDeniedException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.util.Text;
import org.junit.Test;

/**
 * Permission evaluation tests related to access control management.
 */
public class AccessControlManagementTest extends AbstractEvaluationTest {

    @Test
    public void testReadAccessControlContent() throws Exception {
        // test access to ac content if the corresponding access controlled
        // parent node is not accessible.
        allow(path, privilegesFromName(Privilege.JCR_READ_ACCESS_CONTROL));
        deny(path, privilegesFromName(Privilege.JCR_READ));

        // the policy node however must be visible to the test-user
        assertTrue(testSession.nodeExists(path + "/rep:policy"));
        assertTrue(testSession.propertyExists(path + "/rep:policy/jcr:primaryType"));

        assertFalse(testSession.nodeExists(path));
        assertFalse(testSession.propertyExists(path + "/jcr:primaryType"));
    }

    @Test
    public void testAccessControlPrivileges() throws Exception {
        /* grant 'testUser' rep:write, rep:readAccessControl and
           rep:modifyAccessControl privileges at 'path' */
        Privilege[] privileges = privilegesFromNames(new String[] {
                REP_WRITE,
                Privilege.JCR_READ_ACCESS_CONTROL,
                Privilege.JCR_MODIFY_ACCESS_CONTROL
        });
        JackrabbitAccessControlList acl = allow(path, privileges);

        /*
         testuser must have
         - permission to view AC items
         - permission to modify AC items
        */
        // the policy node however must be visible to the test-user
        assertTrue(testSession.itemExists(acl.getPath() + "/rep:policy"));

        testAcMgr.getPolicies(acl.getPath());
        testAcMgr.removePolicy(acl.getPath(), acl);
    }

    /**
     * Test if a new applicable policy can be applied within a
     * sub-tree where AC-modification is allowed.
     *
     * @see <a href="https://issues.apache.org/jira/browse/JCR-2869">JCR-2869</a>
     */
    @Test
    public void testSetNewPolicy() throws Exception {
        /* grant 'testUser' rep:write, rep:readAccessControl and
           rep:modifyAccessControl privileges at 'path' */
        Privilege[] privileges = privilegesFromNames(new String[] {
                REP_WRITE,
                Privilege.JCR_READ_ACCESS_CONTROL,
                Privilege.JCR_MODIFY_ACCESS_CONTROL
        });
        allow(path, privileges);

        /*
         testuser must be allowed to set a new policy at a child node.
        */
        AccessControlPolicyIterator it = testAcMgr.getApplicablePolicies(childNPath);
        while (it.hasNext()) {
            AccessControlPolicy plc = it.nextAccessControlPolicy();
            testAcMgr.setPolicy(childNPath, plc);
            testSession.save();
            testAcMgr.removePolicy(childNPath, plc);
            testSession.save();
        }
    }

    @Test
    public void testSetModifiedPolicy() throws Exception {
        /* grant 'testUser' rep:write, rep:readAccessControl and
           rep:modifyAccessControl privileges at 'path' */
        Privilege[] privileges = privilegesFromNames(new String[] {
                REP_WRITE,
                Privilege.JCR_READ_ACCESS_CONTROL,
                Privilege.JCR_MODIFY_ACCESS_CONTROL
        });

        JackrabbitAccessControlList acl = allow(path, privileges);

        /*
         testuser must be allowed to set (modified) policy at target node.
        */
        AccessControlPolicy[] policies  = testAcMgr.getPolicies(path);

        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof AccessControlList);

        AccessControlList policy = (AccessControlList) policies[0];
        if (policy.addAccessControlEntry(testUser.getPrincipal(), privilegesFromName(Privilege.JCR_LOCK_MANAGEMENT))) {
            testAcMgr.setPolicy(path, acl);
            testSession.save();
        }
    }

    @Test
    public void testRemovePolicyWithoutPrivilege() throws Exception {
        // re-grant READ in order to have an ACL-node
        Privilege[] privileges = privilegesFromName(Privilege.JCR_READ);
        AccessControlPolicy policy = allow(path, privileges);

        /*
         Testuser must still have READ-only access only and must not be
         allowed to view the acl-node that has been created.
        */
        assertFalse(testAcMgr.hasPrivileges(path, privilegesFromName(Privilege.JCR_MODIFY_ACCESS_CONTROL)));
        try {
            testAcMgr.removePolicy(path, policy);
            fail("Test user must not be allowed to remove the access control policy.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testRemovePolicy() throws Exception {
        // re-grant READ in order to have an ACL-node
        Privilege[] privileges = privilegesFromNames(new String[] {Privilege.JCR_READ,
                Privilege.JCR_READ_ACCESS_CONTROL,
                Privilege.JCR_MODIFY_ACCESS_CONTROL});
        allow(path, privileges);

        /*
         Testuser must be allowed to view and remove the acl-node that has been created.
        */
        assertTrue(testAcMgr.hasPrivileges(path, privilegesFromName(Privilege.JCR_MODIFY_ACCESS_CONTROL)));
        testAcMgr.removePolicy(path, testAcMgr.getPolicies(path)[0]);
        testSession.save();
    }

    @Test
    public void testRetrievePrivilegesOnAcNodes() throws Exception {
        // give 'testUser' jcr:readAccessControl privileges at 'path'
        Privilege[] privileges = privilegesFromName(Privilege.JCR_READ_ACCESS_CONTROL);
        allow(path, privileges);

        /*
         testuser must be allowed to read ac-content at target node.
        */
        assertTrue(testAcMgr.hasPrivileges(path, privileges));

        AccessControlPolicy[] policies  = testAcMgr.getPolicies(path);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof JackrabbitAccessControlList);

        String aclNodePath = null;
        Node n = superuser.getNode(path);
        for (NodeIterator itr = n.getNodes(); itr.hasNext();) {
            Node child = itr.nextNode();
            if (child.isNodeType("rep:Policy")) {
                aclNodePath = child.getPath();
            }
        }

        if (aclNodePath == null) {
            fail("Expected node at " + path + " to have an ACL child node.");
        }

        assertTrue(testAcMgr.hasPrivileges(aclNodePath, privileges));
        assertTrue(testSession.hasPermission(aclNodePath, Session.ACTION_READ));

        for (NodeIterator aceNodes = superuser.getNode(aclNodePath).getNodes(); aceNodes.hasNext();) {
            String aceNodePath = aceNodes.nextNode().getPath();
            assertTrue(testAcMgr.hasPrivileges(aceNodePath, privileges));
            assertTrue(testSession.hasPermission(aceNodePath, Session.ACTION_READ));
        }
    }

    @Test
    public void testReadAccessControlWithoutPrivilege() throws Exception {
        // re-grant READ in order to have an ACL-node
        Privilege[] privileges = privilegesFromName(Privilege.JCR_READ);
        JackrabbitAccessControlList tmpl = allow(path, privileges);
        String policyPath = tmpl.getPath() + "/rep:policy";
        // make sure the 'rep:policy' node has been created.
        assertTrue(superuser.itemExists(policyPath));

        /*
         Testuser must still have READ-only access only and must not be
         allowed to view the acl-node nor any item in the subtree that
         has been created.
        */
        assertFalse(testAcMgr.hasPrivileges(path, privilegesFromName(Privilege.JCR_READ_ACCESS_CONTROL)));
        assertFalse(testSession.itemExists(policyPath));

        assertFalse(testSession.nodeExists(policyPath));
        try {
            testSession.getNode(policyPath);
            fail("Accessing the rep:policy node must throw PathNotFoundException.");
        } catch (PathNotFoundException e) {
            // ok.
        }
        try {
            testAcMgr.getPolicies(tmpl.getPath());
            fail("test user must not have READ_AC privilege.");
        } catch (AccessDeniedException e) {
            // success
        }
        try {
            testAcMgr.getEffectivePolicies(tmpl.getPath());
            fail("test user must not have READ_AC privilege.");
        } catch (AccessDeniedException e) {
            // success
        }
        for (NodeIterator aceNodes = superuser.getNode(policyPath).getNodes(); aceNodes.hasNext();) {
            Node aceNode = aceNodes.nextNode();
            String aceNodePath = aceNode.getPath();
            assertFalse(testSession.nodeExists(aceNodePath));

            for (PropertyIterator it = aceNode.getProperties(); it.hasNext();) {
                assertFalse(testSession.propertyExists(it.nextProperty().getPath()));
            }
        }
    }

    @Test
    public void testReadAccessControl() throws Exception {
        /* give 'testUser' jcr:readAccessControl privileges at subtree below
           path excluding the node at path itself. */
        Privilege[] privileges = privilegesFromName(Privilege.JCR_READ_ACCESS_CONTROL);
        allow(path, privileges);

        /*
         testuser must be allowed to read AC content at the target node...
        */
        assertTrue(testAcMgr.hasPrivileges(path, privileges));
        assertTrue(testSession.nodeExists(path + "/rep:policy"));
        testAcMgr.getPolicies(path);
        /*
         ... and the child node
         */
        assertTrue(testAcMgr.hasPrivileges(childNPath, privileges));
        assertEquals(0, testAcMgr.getPolicies(childNPath).length);
    }

    @Test
    public void testReadAccessControlWithRestriction() throws Exception {
        /* give 'testUser' jcr:readAccessControl privileges at subtree below
           path excluding the node at path itself. */
        Privilege[] privileges = privilegesFromName(Privilege.JCR_READ_ACCESS_CONTROL);
        allow(path, privileges, createGlobRestriction('/' + nodeName2));

        /*
         testuser must not be allowed to read AC content at the target node;
         however, retrieving potential AC content at 'childPath' is granted.
        */
        assertFalse(testAcMgr.hasPrivileges(path, privileges));
        assertFalse(testSession.nodeExists(path + "/rep:policy"));
        try {
            testAcMgr.getPolicies(path);
            fail("AccessDeniedException expected");
        } catch (AccessDeniedException e) {
            // success.
        }
        assertTrue(testAcMgr.hasPrivileges(childNPath, privileges));
        assertEquals(0, testAcMgr.getPolicies(childNPath).length);
    }

    @Test
    public void testAclReferingToRemovedPrincipal() throws Exception {

        JackrabbitAccessControlList acl = allow(path, repWritePrivileges);
        String acPath = acl.getPath();

        // remove the test user
        testUser.remove();
        superuser.save();
        testUser = null;

        // try to retrieve the acl again
        Session s = getHelper().getSuperuserSession();
        try {
            AccessControlManager acMgr = getAccessControlManager(s);
            acMgr.getPolicies(acPath);
        } finally {
            s.logout();
        }
    }

    @Test
    public void testAccessControlModificationWithoutPrivilege() throws Exception {
        // give 'testUser' ADD_CHILD_NODES|MODIFY_PROPERTIES| REMOVE_CHILD_NODES privileges at 'path'
        Privilege[] privileges = privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_REMOVE_CHILD_NODES,
                Privilege.JCR_MODIFY_PROPERTIES
        });
        JackrabbitAccessControlList tmpl = allow(path, privileges);
        String policyPath = tmpl.getPath() + "/rep:policy";
        // make sure the 'rep:policy' node has been created.
        assertTrue(superuser.itemExists(policyPath));

        /*
         testuser must not have
         - permission to modify AC items
        */
        try {
            testAcMgr.setPolicy(tmpl.getPath(), tmpl);
            fail("test user must not have MODIFY_AC privilege.");
        } catch (AccessDeniedException e) {
            // success
        }
        try {
            testAcMgr.removePolicy(tmpl.getPath(), tmpl);
            fail("test user must not have MODIFY_AC privilege.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testAccessControlModification() throws Exception {
        // give 'testUser' READ_AC|MODIFY_AC privileges at 'path'
        Privilege[] privileges = privilegesFromNames(new String[] {
                Privilege.JCR_READ_ACCESS_CONTROL,
                Privilege.JCR_MODIFY_ACCESS_CONTROL
        });
        JackrabbitAccessControlList tmpl = allow(path, privileges);
        /*
         testuser must
         - still have the inherited READ permission.
         - must have permission to view AC items at 'path' (and below)
         - must have permission to modify AC items at 'path'

         testuser must not have
         - permission to view AC items outside of the tree defined by path.
        */

        // make sure the 'rep:policy' node has been created.
        assertTrue(superuser.itemExists(tmpl.getPath() + "/rep:policy"));

        // test: MODIFY_AC granted at 'path'
        assertTrue(testAcMgr.hasPrivileges(path, privilegesFromName(Privilege.JCR_MODIFY_ACCESS_CONTROL)));

        // test if testuser can READ access control on the path and on the
        // entire subtree that gets the policy inherited.
        AccessControlPolicy[] policies = testAcMgr.getPolicies(path);
        testAcMgr.getPolicies(childNPath);

        // test: READ_AC privilege does not apply outside of the tree.
        try {
            testAcMgr.getPolicies(siblingPath);
            fail("READ_AC privilege must not apply outside of the tree it has applied to.");
        } catch (AccessDeniedException e) {
            // success
        }

        // test: MODIFY_AC privilege does not apply outside of the tree.
        assertFalse(testAcMgr.hasPrivileges(siblingPath, privilegesFromName(Privilege.JCR_MODIFY_ACCESS_CONTROL)));

        // test if testuser can modify AC-items
        // 1) add an ac-entry
        AccessControlList acl = (AccessControlList) policies[0];
        acl.addAccessControlEntry(testUser.getPrincipal(), repWritePrivileges);
        testAcMgr.setPolicy(path, acl);
        testSession.save();

        assertTrue(testAcMgr.hasPrivileges(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES)));

        // 2) remove the policy
        testAcMgr.removePolicy(path, policies[0]);
        testSession.save();

        // Finally: testuser removed the policy that granted him permission
        // to modify the AC content. Since testuser removed the policy, it's
        // privileges must be gone again...
        try {
            testAcMgr.getEffectivePolicies(childNPath);
            fail("READ_AC privilege has been revoked -> must throw again.");
        } catch (AccessDeniedException e) {
            // success
        }
        // ... and since the ACE is stored with the policy all right except
        // READ must be gone.
        assertReadOnly(path);
    }

    @Test
    public void testAcContentIsProtected() throws Exception {
        // search for a rep:policy node
        Node policyNode = findPolicyNode(superuser.getRootNode());
        if (policyNode == null) {
            throw new NotExecutableException("no policy node found.");
        }

        assertTrue("The rep:Policy node must be protected", policyNode.getDefinition().isProtected());
        try {
            policyNode.remove();
            fail("rep:Policy node must be protected.");
        } catch (ConstraintViolationException e) {
            // success
        }

        for (NodeIterator it = policyNode.getNodes(); it.hasNext();) {
            Node n = it.nextNode();
            if (n.isNodeType("rep:ACE")) {
                try {
                    n.remove();
                    fail("ACE node must be protected.");
                } catch (ConstraintViolationException e) {
                    // success
                }
                break;
            }
        }

        try {
            policyNode.setProperty("test", "anyvalue");
            fail("rep:policy node must be protected.");
        } catch (ConstraintViolationException e) {
            // success
        }
        try {
            policyNode.addNode("test", "rep:ACE");
            fail("rep:policy node must be protected.");
        } catch (ConstraintViolationException e) {
            // success
        }
    }

    private static Node findPolicyNode(Node start) throws RepositoryException {
        Node policyNode = null;
        if (start.isNodeType("rep:Policy")) {
            policyNode = start;
        }
        for (NodeIterator it = start.getNodes(); it.hasNext() && policyNode == null;) {
            Node n = it.nextNode();
            if (!"jcr:system".equals(n.getName())) {
                policyNode = findPolicyNode(n);
            }
        }
        return policyNode;
    }

    @Test
    public void testReorderPolicyNode() throws Exception {
        Node n = testSession.getNode(path);
        try {
            if (!n.getPrimaryNodeType().hasOrderableChildNodes()) {
                throw new NotExecutableException("Reordering child nodes is not supported..");
            }

            n.orderBefore(Text.getName(childNPath2), Text.getName(childNPath));
            testSession.save();
            fail("test session must not be allowed to reorder nodes.");
        } catch (AccessDeniedException e) {
            // success.
        }

        // grant all privileges
        allow(path, privilegesFromNames(new String[] {Privilege.JCR_ALL}));

        n.orderBefore(Text.getName(childNPath2), Text.getName(childNPath));
        testSession.save();

        n.orderBefore("rep:policy", Text.getName(childNPath2));
        testSession.save();
    }

    @Test
    public void testRemoveMixin() throws Exception {
        Node n = superuser.getNode(path);
        deny(path, readPrivileges);

        assertTrue(n.hasNode("rep:policy"));
        assertTrue(n.isNodeType("rep:AccessControllable"));

        n.removeMixin("rep:AccessControllable");

        superuser.save();
        assertFalse(n.hasNode("rep:policy"));
        assertFalse(n.isNodeType("rep:AccessControllable"));
    }
}
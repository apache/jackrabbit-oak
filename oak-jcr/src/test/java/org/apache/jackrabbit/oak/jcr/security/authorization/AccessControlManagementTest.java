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

import java.util.HashMap;
import java.util.Map;
import javax.jcr.AccessDeniedException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * AccessControlManagementTest... TODO
 *
 * copied from jr2.x AcReadWriteTest
 */
@Ignore("OAK-51")
public class AccessControlManagementTest extends AbstractEvaluationTest {

    private String path;
    private String childNPath;

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        // create some nodes below the test root in order to apply ac-stuff
        Node node = testRootNode.addNode(nodeName1, testNodeType);
        Node cn1 = node.addNode(nodeName2, testNodeType);
        Property cp1 = node.setProperty(propertyName1, "anyValue");
        Node cn2 = node.addNode(nodeName3, testNodeType);

        Property ccp1 = cn1.setProperty(propertyName1, "childNodeProperty");

        Node n2 = testRootNode.addNode(nodeName2, testNodeType);
        superuser.save();

        path = node.getPath();
        childNPath = cn1.getPath();
    }

    @Test
    public void testAccessControlPrivileges() throws Exception {
        /* precondition:
          testuser must have READ-only permission on test-node and below
        */
        checkReadOnly(path);

        /* grant 'testUser' rep:write, rep:readAccessControl and
           rep:modifyAccessControl privileges at 'path' */
        Privilege[] privileges = privilegesFromNames(new String[] {
                REP_WRITE,
                Privilege.JCR_READ_ACCESS_CONTROL,
                Privilege.JCR_MODIFY_ACCESS_CONTROL
        });
        JackrabbitAccessControlList acl = allow(path, privileges);

        Session testSession = getTestSession();
        AccessControlManager testAcMgr = getTestAccessControlManager();
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
     * Test if a new applicable policy can be applied within a individual
     * subtree where AC-modification is allowed.
     *
     * @see <a href="https://issues.apache.org/jira/browse/JCR-2869">JCR-2869</a>
     */
    @Test
    public void testSetNewPolicy() throws Exception {
        /* precondition:
          testuser must have READ-only permission on test-node and below
        */
        checkReadOnly(path);

        /* grant 'testUser' rep:write, rep:readAccessControl and
           rep:modifyAccessControl privileges at 'path' */
        Privilege[] privileges = privilegesFromNames(new String[] {
                REP_WRITE,
                Privilege.JCR_READ_ACCESS_CONTROL,
                Privilege.JCR_MODIFY_ACCESS_CONTROL
        });
        allow(path, privileges);

        AccessControlManager testAcMgr = getTestAccessControlManager();
        /*
         testuser must be allowed to set a new policy at a child node.
        */
        AccessControlPolicyIterator it = testAcMgr.getApplicablePolicies(childNPath);
        while (it.hasNext()) {
            AccessControlPolicy plc = it.nextAccessControlPolicy();
            testAcMgr.setPolicy(childNPath, plc);
            testAcMgr.removePolicy(childNPath, plc);
        }
    }

    @Test
    public void testSetModifiedPolicy() throws Exception {
        /* precondition:
          testuser must have READ-only permission on test-node and below
        */
        checkReadOnly(path);

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
        Session testSession = getTestSession();
        AccessControlManager testAcMgr = getTestAccessControlManager();

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
    public void testRetrievePrivilegesOnAcNodes() throws Exception {
        /* precondition:
          testuser must have READ-only permission on test-node and below
        */
        checkReadOnly(path);

        // give 'testUser' jcr:readAccessControl privileges at 'path'
        Privilege[] privileges = privilegesFromName(Privilege.JCR_READ_ACCESS_CONTROL);
        JackrabbitAccessControlList acl = allow(path, privileges);

        /*
         testuser must be allowed to read ac-content at target node.
        */
        Session testSession = getTestSession();
        AccessControlManager testAcMgr = getTestAccessControlManager();

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
    public void testReadAccessControl() throws Exception {
        /* precondition:
          testuser must have READ-only permission on test-node and below
        */
        checkReadOnly(path);

        /* give 'testUser' jcr:readAccessControl privileges at subtree below
           path excluding the node at path itself. */
        Privilege[] privileges = privilegesFromNames(new String[] {
                Privilege.JCR_READ_ACCESS_CONTROL
        });
        Map<String, Value> restrictions = new HashMap<String, Value>();
        restrictions.put("rep:glob", vf.createValue('/' + nodeName2));
        JackrabbitAccessControlList acl = allow(path, privileges, restrictions);

        /*
         testuser must not be allowed to read AC content at the target node;
         however, retrieving potential AC content at 'childPath' is granted.
        */
        Session testSession = getTestSession();
        AccessControlManager testAcMgr = getTestAccessControlManager();

        assertFalse(testAcMgr.hasPrivileges(path, privileges));
        try {
            testAcMgr.getPolicies(path);
            fail("AccessDeniedException expected");
        } catch (AccessDeniedException e) {
            // success.
        }

        assertTrue(testAcMgr.hasPrivileges(childNPath, privileges));
        assertEquals(0, testAcMgr.getPolicies(childNPath).length);

        /* similarly reading the corresponding AC items at 'path' must be forbidden */
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

        assertFalse(testSession.nodeExists(aclNodePath));

        for (NodeIterator aceNodes = superuser.getNode(aclNodePath).getNodes(); aceNodes.hasNext();) {
            Node aceNode = aceNodes.nextNode();
            String aceNodePath = aceNode.getPath();
            assertFalse(testSession.nodeExists(aceNodePath));

            for (PropertyIterator it = aceNode.getProperties(); it.hasNext();) {
                assertFalse(testSession.propertyExists(it.nextProperty().getPath()));
            }
        }
    }
}
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.jcr.AccessDeniedException;
import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitNode;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.util.Text;
import org.junit.Ignore;
import org.junit.Test;

/**
 * WriteTest... TODO
 */
@Ignore("OAK-51")
public class WriteTest extends AbstractEvaluationTest {

    @Test
    public void testAddChildNodeAndSetProperty() throws Exception {
        // give 'testUser' ADD_CHILD_NODES|MODIFY_PROPERTIES privileges at 'path'
        Privilege[] privileges = privilegesFromNames(new String[] {
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_MODIFY_PROPERTIES
        });
        allow(path, privileges);
        /*
         testuser must now have
         - ADD_NODE permission for child node
         - SET_PROPERTY permission for child props
         - REMOVE permission for child-props
         - READ-only permission for the node at 'path'

         testuser must not have
         - REMOVE permission for child node
        */
        String nonExChildPath = path + "/anyItem";
        assertTrue(testSession.hasPermission(nonExChildPath,
                getActions(Session.ACTION_READ, Session.ACTION_ADD_NODE, Session.ACTION_SET_PROPERTY)));
        assertFalse(testSession.hasPermission(nonExChildPath, Session.ACTION_REMOVE));

        Node testN = testSession.getNode(path);

        // must be allowed to add child node
        testN.addNode(nodeName3);
        testSession.save();

        // must be allowed to remove child-property
        testSession.getProperty(childPPath).remove();
        testSession.save();

        // must be allowed to set child property again
        testN.setProperty(Text.getName(childPPath), "othervalue");
        testSession.save();

        // must not be allowed to remove child nodes
        try {
            testSession.getNode(childNPath).remove();
            testSession.save();
            fail("test-user is not allowed to remove a node below " + path);
        } catch (AccessDeniedException e) {
            // success
        }

        // must have read-only access on 'testN' and it's sibling
        assertTrue(testSession.hasPermission(path, Session.ACTION_READ));
        assertFalse(testSession.hasPermission(path,
                getActions(Session.ACTION_ADD_NODE, Session.ACTION_SET_PROPERTY, Session.ACTION_REMOVE)));
        assertReadOnly(siblingPath);
    }

    @Test
    public void testRemovePermission() throws Exception {
        // add 'remove_child_nodes' privilege at 'path'
        Privilege[] rmChildNodes = privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES);
        allow(path, rmChildNodes);
        /*
         expected result:
         - neither node at path nor at childNPath can be removed since
           REMOVE_NODE privilege is missing.
         */
        assertFalse(testSession.hasPermission(path, javax.jcr.Session.ACTION_REMOVE));
        assertFalse(testSession.hasPermission(childNPath, javax.jcr.Session.ACTION_REMOVE));
    }

    @Test
    public void testRemovePermission2() throws Exception {
        // add 'remove_node' privilege at 'path'
        Privilege[] rmChildNodes = privilegesFromName(Privilege.JCR_REMOVE_NODE);
        allow(path, rmChildNodes);
        /*
         expected result:
         - neither node at path nor at childNPath can be removed permission
           due to missing remove_child_nodes privilege.
         */
        assertFalse(testSession.hasPermission(path, javax.jcr.Session.ACTION_REMOVE));
        assertFalse(testSession.hasPermission(childNPath, javax.jcr.Session.ACTION_REMOVE));
    }

    @Test
    public void testRemovePermission3() throws Exception {
        // add 'remove_node' and 'remove_child_nodes' privilege at 'path'
        Privilege[] privs = privilegesFromNames(new String[] {
                Privilege.JCR_REMOVE_CHILD_NODES, Privilege.JCR_REMOVE_NODE
        });
        allow(path, privs);
        /*
         expected result:
         - missing remove permission at path since REMOVE_CHILD_NODES present
           at path only applies for nodes below. REMOVE_CHILD_NODES must
           be present at the parent instead (which isn't)
         - remove permission is however granted at childNPath.
         - privileges: both at path and at childNPath 'remove_node' and
           'remove_child_nodes' are present.
        */
        assertFalse(testSession.hasPermission(path, javax.jcr.Session.ACTION_REMOVE));
        assertTrue(testSession.hasPermission(childNPath, javax.jcr.Session.ACTION_REMOVE));

        assertTrue(testAcMgr.hasPrivileges(path, privs));
        assertTrue(testAcMgr.hasPrivileges(childNPath, privs));
    }

    @Test
    public void testRemovePermission4() throws Exception {
        Privilege[] rmChildNodes = privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES);
        Privilege[] rmNode = privilegesFromName(Privilege.JCR_REMOVE_NODE);

        // add 'remove_child_nodes' privilege at 'path'...
        allow(path, rmChildNodes);
        // ... and add 'remove_node' privilege at 'childNPath'
        allow(childNPath, rmNode);
        /*
         expected result:
         - remove not allowed for node at path
         - remove-permission present for node at childNPath
         - both remove_node and remove_childNodes privilege present at childNPath
         */
        assertFalse(testSession.hasPermission(path, javax.jcr.Session.ACTION_REMOVE));
        assertTrue(testSession.hasPermission(childNPath, javax.jcr.Session.ACTION_REMOVE));

        assertTrue(testAcMgr.hasPrivileges(childNPath, new Privilege[] {rmChildNodes[0], rmNode[0]}));
    }

    @Test
    public void testRemovePermission5() throws Exception {
        // add 'remove_node' privilege at 'childNPath'
        Privilege[] rmNode = privilegesFromName(Privilege.JCR_REMOVE_NODE);
        allow(childNPath, rmNode);
        /*
         expected result:
         - node at childNPath can't be removed since REMOVE_CHILD_NODES is missing.
         */
        assertFalse(testSession.hasPermission(childNPath, Session.ACTION_REMOVE));
    }

    @Test
    public void testRemovePermission6() throws Exception {
        // add 'remove_child_nodes' and 'remove_node' privilege at 'path'
        Privilege[] privs = privilegesFromNames(new String[]{
                Privilege.JCR_REMOVE_CHILD_NODES, Privilege.JCR_REMOVE_NODE
        });
        allow(path, privs);
        // ... but deny 'remove_node' at childNPath
        Privilege[] rmNode = privilegesFromName(Privilege.JCR_REMOVE_NODE);
        deny(childNPath, rmNode);
        /*
         expected result:
         - neither node at path nor at childNPath could be removed.
         - no remove_node privilege at childNPath
         - read, remove_child_nodes privilege at childNPath
         */
        assertFalse(testSession.hasPermission(path, Session.ACTION_REMOVE));
        assertFalse(testSession.hasPermission(childNPath, Session.ACTION_REMOVE));

        assertTrue(testAcMgr.hasPrivileges(childNPath, privilegesFromNames(new String[] {Privilege.JCR_READ, Privilege.JCR_REMOVE_CHILD_NODES})));
        assertFalse(testAcMgr.hasPrivileges(childNPath, privilegesFromName(Privilege.JCR_REMOVE_NODE)));
    }

    @Test
    public void testRemovePermission7() throws Exception {
        Privilege[] rmChildNodes = privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES);
        Privilege[] rmNode = privilegesFromName(Privilege.JCR_REMOVE_NODE);

        // deny 'remove_child_nodes' at 'path'
        deny(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        // ... but allow 'remove_node' at childNPath
        allow(childNPath, rmNode);
        /*
         expected result:
         - node at childNPath can't be removed.
         */
        assertFalse(testSession.hasPermission(childNPath, Session.ACTION_REMOVE));

        // additionally add remove_child_nodes privilege at 'childNPath'
        allow(childNPath, rmChildNodes);
        /*
         expected result:
         - node at childNPath still can't be removed.
         - but both privileges (remove_node, remove_child_nodes) are present.
         */
        assertFalse(testSession.hasPermission(childNPath, javax.jcr.Session.ACTION_REMOVE));

        assertTrue(testAcMgr.hasPrivileges(childNPath, new Privilege[] {rmChildNodes[0], rmNode[0]}));
    }

    public void testRemovePermission8() throws Exception {
        Privilege[] rmChildNodes = privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES);
        Privilege[] rmNode = privilegesFromName(Privilege.JCR_REMOVE_NODE);

        // add 'remove_child_nodes' at 'path
        allow(path, rmChildNodes);
        // deny 'remove_node' at 'path'
        deny(path, rmNode);
        // and allow 'remove_node' at childNPath
        allow(childNPath, rmNode);
        /*
         expected result:
         - remove permission must be granted at childNPath
         */
        assertTrue(testSession.hasPermission(childNPath, Session.ACTION_REMOVE));
        assertTrue(testAcMgr.hasPrivileges(childNPath, new Privilege[]{rmChildNodes[0], rmNode[0]}));
    }

    @Test
    public void testRemovePermission9() throws Exception {
        Privilege[] rmChildNodes = privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES);
        Privilege[] rmNode = privilegesFromName(Privilege.JCR_REMOVE_NODE);

        // add 'remove_child_nodes' at 'path and allow 'remove_node' at childNPath
        allow(path, rmChildNodes);
        allow(childNPath, rmNode);
        /*
         expected result:
         - rep:policy node can still not be remove for it is access-control
           content that requires jcr:modifyAccessControl privilege instead.
         */
        String policyPath = childNPath + "/rep:policy";
        assertFalse(testSession.hasPermission(policyPath, Session.ACTION_REMOVE));
        assertTrue(testAcMgr.hasPrivileges(policyPath, new Privilege[]{rmChildNodes[0], rmNode[0]}));
    }

    @Test
    public void testGroupPermissions() throws Exception {
        Group testGroup = getTestGroup();

        /* add privileges for the Group the test-user is member of */
        allow(path, testGroup.getPrincipal(), modPropPrivileges);

        /* testuser must get the permissions/privileges inherited from
           the group it is member of.
         */
        String actions = getActions(Session.ACTION_SET_PROPERTY, Session.ACTION_READ);

        assertTrue(testSession.hasPermission(path, actions));
        assertTrue(testAcMgr.hasPrivileges(path, modPropPrivileges));
    }

    @Test
    public void testMixedUserGroupPermissions() throws Exception {
        Group testGroup = getTestGroup();

        /* explicitly withdraw MODIFY_PROPERTIES for the user */
        deny(path, testUser.getPrincipal(), modPropPrivileges);
        /* give MODIFY_PROPERTIES privilege for a Group the test-user is member of */
        allow(path, testGroup.getPrincipal(), modPropPrivileges);
        /*
         since user-permissions overrule the group permissions, testuser must
         not have set_property action / modify_properties privilege.
         */
        assertFalse(testSession.hasPermission(path, Session.ACTION_SET_PROPERTY));
        assertFalse(testAcMgr.hasPrivileges(path, modPropPrivileges));
    }

    /**
     * the ADD_CHILD_NODES privileges assigned on a node to a specific principal
     * grants the corresponding user the permission to add nodes below the
     * target node but not 'at' the target node.
     *
     * @throws Exception If an error occurs.
     */
    @Test
    public void testAddChildNodePrivilege() throws Exception {

        /* create a child node below node at 'path' */
        Node n = superuser.getNode(path);
        n = n.addNode(nodeName2, testNodeType);
        superuser.save();

        /* add 'add_child_nodes' privilege for testSession at path. */
        Privilege[] privileges = privilegesFromName(Privilege.JCR_ADD_CHILD_NODES);
        allow(path, privileges);

        /* test permissions. expected result:
           - testSession cannot add child-nodes at 'path'
           - testSession can add child-nodes below path
         */
        assertFalse(testSession.hasPermission(path, Session.ACTION_ADD_NODE));
        assertTrue(testSession.hasPermission(path+"/anychild", Session.ACTION_ADD_NODE));
        String childPath = n.getPath();
        assertTrue(testSession.hasPermission(childPath, Session.ACTION_ADD_NODE));
    }

    @Test
    public void testSingleDenyAfterAllAllowed() throws Exception {
        /* add 'all' privilege for testSession at path. */
        Privilege[] allPrivileges = privilegesFromName(Privilege.JCR_ALL);
        allow(path, allPrivileges);

        /* deny a single privilege */
        Privilege[] lockPrivileges = privilegesFromName(Privilege.JCR_LOCK_MANAGEMENT);
        deny(path, lockPrivileges);

        /* test permissions. expected result:
           - testSession cannot lock at 'path'
           - testSession doesn't have ALL privilege at path
         */
        AccessControlManager acMgr = testSession.getAccessControlManager();
        assertFalse(acMgr.hasPrivileges(path, allPrivileges));
        assertFalse(acMgr.hasPrivileges(path, lockPrivileges));

        List<Privilege> remainingprivs = new ArrayList<Privilege>(Arrays.asList(allPrivileges[0].getAggregatePrivileges()));
        remainingprivs.remove(lockPrivileges[0]);
        assertTrue(acMgr.hasPrivileges(path, remainingprivs.toArray(new Privilege[remainingprivs.size()])));
    }

    @Test
    public void testReorder() throws Exception {
        Node n = testSession.getNode(path);
        try {
            if (!n.getPrimaryNodeType().hasOrderableChildNodes()) {
                throw new NotExecutableException("Reordering child nodes is not supported..");
            }

            n.orderBefore(Text.getName(childNPath), Text.getName(childNPath2));
            testSession.save();
            fail("test session must not be allowed to reorder nodes.");
        } catch (AccessDeniedException e) {
            // success.
        }

        // give 'add_child_nodes' and 'nt-management' privilege
        // -> not sufficient privileges for a reorder
        allow(path, privilegesFromNames(new String[] {Privilege.JCR_ADD_CHILD_NODES, Privilege.JCR_NODE_TYPE_MANAGEMENT}));
        try {
            n.orderBefore(Text.getName(childNPath), Text.getName(childNPath2));
            testSession.save();
            fail("test session must not be allowed to reorder nodes.");
        } catch (AccessDeniedException e) {
            // success.
        }

        // add 'remove_child_nodes' at 'path
        // -> reorder must now succeed
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        n.orderBefore(Text.getName(childNPath), Text.getName(childNPath2));
        testSession.save();
    }

    @Test
    public void testRename() throws Exception {
        Node child = testSession.getNode(childNPath);
        try {
            ((JackrabbitNode) child).rename("rename");
            testSession.save();
            fail("test session must not be allowed to rename nodes.");
        } catch (AccessDeniedException e) {
            // success.
        }

        // give 'add_child_nodes' and 'nt-management' privilege
        // -> not sufficient privileges for a renaming of the child
        allow(path, privilegesFromNames(new String[] {Privilege.JCR_ADD_CHILD_NODES, Privilege.JCR_NODE_TYPE_MANAGEMENT}));
        try {
            ((JackrabbitNode) child).rename("rename");
            testSession.save();
            fail("test session must not be allowed to rename nodes.");
        } catch (AccessDeniedException e) {
            // success.
        }

        // add 'remove_child_nodes' at 'path
        // -> rename of child must now succeed
        allow(path, privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES));
        ((JackrabbitNode) child).rename("rename");
        testSession.save();
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/JCR-2420">JCR-2420</a>
     */
    @Test
    public void testRemovalJCR242() throws Exception {
        Privilege[] allPriv = privilegesFromNames(new String[] {Privilege.JCR_ALL});

        /* grant ALL privilege for testUser at 'path' */
        allow(path, testUser.getPrincipal(), allPriv);
        /* grant ALL privilege for testUser at 'childNPath' */
        allow(childNPath, testUser.getPrincipal(), allPriv);

        AccessControlManager acMgr = testSession.getAccessControlManager();
        assertTrue(acMgr.hasPrivileges(path, allPriv));
        assertTrue(acMgr.hasPrivileges(childNPath, allPriv));

        assertTrue(testSession.hasPermission(childNPath, Session.ACTION_REMOVE));

        Node child = testSession.getNode(childNPath);
        child.remove();
        testSession.save();
    }

    @Test
    public void testGlobRestriction() throws Exception {
        Node child = superuser.getNode(childNPath).addNode(nodeName3);
        superuser.save();
        String childchildPath = child.getPath();

        String writeActions = getActions(Session.ACTION_ADD_NODE, Session.ACTION_REMOVE, Session.ACTION_SET_PROPERTY);

        // permissions defined @ path
        // restriction: grants write priv to all nodeName3 children
        allow(path, repWritePrivileges, createGlobRestriction("/*"+nodeName3));

        assertFalse(testAcMgr.hasPrivileges(path, repWritePrivileges));
        assertFalse(testSession.hasPermission(path, javax.jcr.Session.ACTION_SET_PROPERTY));

        assertFalse(testAcMgr.hasPrivileges(childNPath, repWritePrivileges));
        assertFalse(testSession.hasPermission(childNPath, javax.jcr.Session.ACTION_SET_PROPERTY));

        assertTrue(testAcMgr.hasPrivileges(childNPath2, repWritePrivileges));
        assertTrue(testSession.hasPermission(childNPath2, Session.ACTION_SET_PROPERTY));
        assertFalse(testSession.hasPermission(childNPath2, writeActions)); // removal req. rmchildnode privilege on parent.

        assertTrue(testAcMgr.hasPrivileges(childchildPath, repWritePrivileges));
    }

    @Test
    public void testGlobRestriction2() throws Exception {
        Node child = superuser.getNode(childNPath).addNode(nodeName3);
        superuser.save();
        String childchildPath = child.getPath();

        Privilege[] addNode = privilegesFromName(Privilege.JCR_ADD_CHILD_NODES);
        Privilege[] rmNode = privilegesFromName(Privilege.JCR_REMOVE_NODE);

        // permissions defined @ path
        // restriction: grants write-priv to nodeName3 grand-children but not direct nodeName3 children.
        allow(path, repWritePrivileges, createGlobRestriction("/*/"+nodeName3));

        assertFalse(testAcMgr.hasPrivileges(path, repWritePrivileges));
        assertFalse(testAcMgr.hasPrivileges(path, rmNode));
        assertFalse(testAcMgr.hasPrivileges(childNPath, addNode));
        assertFalse(testAcMgr.hasPrivileges(childNPath2, repWritePrivileges));
        assertTrue(testAcMgr.hasPrivileges(childchildPath, repWritePrivileges));
    }

    @Test
    public void testGlobRestriction3() throws Exception {
        Node child = superuser.getNode(childNPath).addNode(nodeName3);
        superuser.save();
        String childchildPath = child.getPath();

        Privilege[] addNode = privilegesFromName(Privilege.JCR_ADD_CHILD_NODES);

        // permissions defined @ path
        // restriction: allows write to nodeName3 children
        allow(path, repWritePrivileges, createGlobRestriction("/*/"+nodeName3));
        // and grant add-node only at path (no glob restriction)
        allow(path, addNode);

        assertFalse(testAcMgr.hasPrivileges(path, repWritePrivileges));
        assertTrue(testAcMgr.hasPrivileges(path, addNode));

        assertFalse(testAcMgr.hasPrivileges(childNPath, repWritePrivileges));
        assertTrue(testAcMgr.hasPrivileges(childNPath, addNode));

        assertFalse(testAcMgr.hasPrivileges(childNPath2, repWritePrivileges));
        assertTrue(testAcMgr.hasPrivileges(childchildPath, repWritePrivileges));
    }

    @Test
    public void testGlobRestriction4() throws Exception {
        Node child = superuser.getNode(childNPath).addNode(nodeName3);
        superuser.save();
        String childchildPath = child.getPath();

        Privilege[] addNode = privilegesFromName(Privilege.JCR_ADD_CHILD_NODES);

        allow(path, repWritePrivileges, createGlobRestriction("/*"+nodeName3));
        deny(childNPath2, addNode);

        assertFalse(testAcMgr.hasPrivileges(path, repWritePrivileges));
        assertFalse(testSession.hasPermission(path, javax.jcr.Session.ACTION_REMOVE));
        assertFalse(testAcMgr.hasPrivileges(childNPath, repWritePrivileges));
        assertFalse(testSession.hasPermission(childNPath, javax.jcr.Session.ACTION_REMOVE));
        assertFalse(testAcMgr.hasPrivileges(childNPath2, repWritePrivileges));
        assertTrue(testAcMgr.hasPrivileges(childchildPath, repWritePrivileges));
    }

    @Test
    public void testWriteIfReadingParentIsDenied() throws Exception {
        /* deny READ/WRITE privilege for testUser at 'path' */
        deny(path, testUser.getPrincipal(), readWritePrivileges);
        /* allow READ/WRITE privilege for testUser at 'childNPath' */
        allow(childNPath, testUser.getPrincipal(), readWritePrivileges);


        assertFalse(testSession.nodeExists(path));

        // reading the node and it's definition must succeed.
        assertTrue(testSession.nodeExists(childNPath));
        Node n = testSession.getNode(childNPath);

        n.addNode("someChild");
        n.save();
    }

    @Test
    public void testRemoveNodeWithPolicy() throws Exception {
        /* allow READ/WRITE privilege for testUser at 'path' */
        allow(path, testUser.getPrincipal(), readWritePrivileges);
        /* allow READ/WRITE privilege for testUser at 'childPath' */
        allow(childNPath, testUser.getPrincipal(), readWritePrivileges);

        assertTrue(testSession.nodeExists(childNPath));
        assertTrue(testSession.hasPermission(childNPath, Session.ACTION_REMOVE));

        Node n = testSession.getNode(childNPath);

        // removing the child node must succeed as both remove-node and
        // remove-child-nodes are granted to testsession.
        // the policy node underneath childNPath should silently be removed
        // as the editing session has no knowledge about it's existence.
        n.remove();
        testSession.save();
    }

    @Test
    public void testRemoveNodeWithInvisibleChild() throws Exception {
        Node invisible = superuser.getNode(childNPath).addNode(nodeName3);
        superuser.save();

        /* allow READ/WRITE privilege for testUser at 'path' */
        allow(path, testUser.getPrincipal(), readWritePrivileges);
        /* deny READ privilege at invisible node. (removal is still granted) */
        deny(invisible.getPath(), testUser.getPrincipal(), readPrivileges);

        assertTrue(testSession.nodeExists(childNPath));
        assertTrue(testSession.hasPermission(childNPath, Session.ACTION_REMOVE));

        // removing the child node must succeed as both remove-node and
        // remove-child-nodes are granted to testsession.
        // the policy node underneath childNPath should silently be removed
        // as the editing session has no knowledge about it's existence.
        testSession.getNode(childNPath).remove();
        testSession.save();
    }

    @Test
    public void testRemoveNodeWithInvisibleNonRemovableChild() throws Exception {
        Node invisible = superuser.getNode(childNPath).addNode(nodeName3);
        superuser.save();

        /* allow READ/WRITE privilege for testUser at 'path' */
        allow(path, testUser.getPrincipal(), readWritePrivileges);
        /* deny READ privilege at invisible node. (removal is still granted) */
        deny(invisible.getPath(), testUser.getPrincipal(), readWritePrivileges);

        assertTrue(testSession.nodeExists(childNPath));
        assertTrue(testSession.hasPermission(childNPath, Session.ACTION_REMOVE));

        // removing the child node must fail as a hidden child node cannot
        // be removed.
        try {
            testSession.getNode(childNPath).remove();
            testSession.save();
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/JCR-3131">JCR-3131</a>
     */
    @Test
    public void testEmptySaveNoRootAccess() throws Exception {
        testSession.save();

        try {
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
            acl.addEntry(testUser.getPrincipal(), readPrivileges, false);
            acMgr.setPolicy(acl.getPath(), acl);
            superuser.save();

            // empty save operation
            testSession.save();
        } finally {
            // undo revocation of read privilege
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
            acl.addEntry(testUser.getPrincipal(), readPrivileges, true);
            acMgr.setPolicy(acl.getPath(), acl);
            superuser.save();
        }
    }
}
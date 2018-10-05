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

import java.security.Principal;
import java.util.UUID;
import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Test;

/**
 * Permission evaluation tests focusing on inheritance.
 */
public class InheritanceTest extends AbstractEvaluationTest {

    private Group group2;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        /* create a second group the test user is member of */
        group2 = getUserManager(superuser).createGroup("testGroup" + UUID.randomUUID());
        group2.addMember(testUser);
        superuser.save();

        // recreate test session
        testSession.logout();
        testSession = createTestSession();
        testAcMgr = testSession.getAccessControlManager();
    }

    @Override
    protected void tearDown() throws Exception {
        group2.remove();
        superuser.save();
        super.tearDown();
    }

    @Test
    public void testInheritance() throws Exception {
        // give 'modify_properties' and 'remove_node' privilege on 'path'
        Privilege[] privileges = privilegesFromNames(new String[] {Privilege.JCR_REMOVE_NODE, Privilege.JCR_MODIFY_PROPERTIES});
        allow(path, privileges);
        // give 'add-child-nodes', remove_child_nodes' on 'childNPath'
        privileges = privilegesFromNames(new String[] {Privilege.JCR_ADD_CHILD_NODES, Privilege.JCR_REMOVE_CHILD_NODES});
        allow(childNPath, privileges);

        /*
        since evaluation respects inheritance through the node
        hierarchy, the following privileges must now be given at 'childNPath':
        - jcr:read
        - jcr:modifyProperties
        - jcr:addChildNodes
        - jcr:removeChildNodes
        - jcr:removeNode
        */
        Privilege[] expectedPrivileges =  privilegesFromNames(new String[] {
                Privilege.JCR_READ,
                Privilege.JCR_ADD_CHILD_NODES,
                Privilege.JCR_REMOVE_CHILD_NODES,
                Privilege.JCR_REMOVE_NODE,
                Privilege.JCR_MODIFY_PROPERTIES
        });
        assertTrue(testAcMgr.hasPrivileges(childNPath, expectedPrivileges));

        /*
         ... permissions granted at childNPath:
         - read
         - set-property

         BUT NOT:
         - add-node
         - remove.
         */
        String aActions = javax.jcr.Session.ACTION_SET_PROPERTY + ',' + javax.jcr.Session.ACTION_READ;
        assertTrue(testSession.hasPermission(childNPath, aActions));
        String dActions = javax.jcr.Session.ACTION_REMOVE + ',' + javax.jcr.Session.ACTION_ADD_NODE;
        assertFalse(testSession.hasPermission(childNPath, dActions));

        /*
        ... permissions granted at any child item of child-path:
        - read
        - set-property
        - add-node
        - remove
        */
        String nonExistingItemPath = childNPath + "/anyItem";
        assertTrue(testSession.hasPermission(nonExistingItemPath, aActions + ',' + dActions));

        /* try adding a new child node -> must succeed. */
        Node childN = testSession.getNode(childNPath);
        String testPath = childN.addNode(nodeName2).getPath();

        /* test privileges on the 'new' child node */
        assertTrue(testAcMgr.hasPrivileges(testPath, expectedPrivileges));

        /* repeat test after save. */
        testSession.save();
        assertTrue(testAcMgr.hasPrivileges(testPath, expectedPrivileges));
    }

    @Test
    public void testInheritance2() throws Exception {
        // give jcr:write privilege on 'path' and withdraw them on 'childNPath'
        Privilege[] privileges = privilegesFromNames(new String[] {Privilege.JCR_WRITE});
        allow(path, privileges);
        deny(childNPath, privileges);

        /*
        since evaluation respects inheritance through the node
        hierarchy, the jcr:write privilege must not be granted at childNPath
        */
        assertFalse(testAcMgr.hasPrivileges(childNPath, privileges));

        /*
         ... same for permissions at 'childNPath'
         */
        String actions = getActions(Session.ACTION_SET_PROPERTY, Session.ACTION_REMOVE, Session.ACTION_ADD_NODE);

        String nonExistingItemPath = childNPath + "/anyItem";
        assertFalse(testSession.hasPermission(nonExistingItemPath, actions));

        // yet another level in the hierarchy
        Node grandChild = superuser.getNode(childNPath).addNode(nodeName3);
        superuser.save();
        testSession.refresh(false);
        String gcPath = grandChild.getPath();

        // grant write privilege again
        allow(gcPath, privileges);
        assertTrue(testAcMgr.hasPrivileges(gcPath, privileges));
        assertTrue(testSession.hasPermission(gcPath + "/anyProp", Session.ACTION_SET_PROPERTY));
        // however: removing the grand-child nodes must not be allowed as
        // remove_child_node privilege is missing on the direct ancestor.
        assertFalse(testSession.hasPermission(gcPath, Session.ACTION_REMOVE));
    }

    @Test
    public void testInheritedGroupPermissions() throws Exception {
        /* allow MODIFY_PROPERTIES privilege for testGroup at 'path' */
        allow(path, testGroup.getPrincipal(), modPropPrivileges);
        /* deny MODIFY_PROPERTIES privilege for everyone at 'childNPath' */
        deny(childNPath, EveryonePrincipal.getInstance(), modPropPrivileges);

        // result at 'child path' must be deny
        assertFalse(testAcMgr.hasPrivileges(childNPath, modPropPrivileges));
    }

    @Test
    public void testInheritedGroupPermissions2() throws Exception {
        // NOTE: same as testInheritedGroupPermissions above but using
        // everyone on path, testgroup on childpath -> result must be the same

        /* allow MODIFY_PROPERTIES privilege for everyone at 'path' */
        allow(path, EveryonePrincipal.getInstance(), modPropPrivileges);
        /* deny MODIFY_PROPERTIES privilege for testGroup at 'childNPath' */
        deny(childNPath, testGroup.getPrincipal(), modPropPrivileges);

        // result at 'child path' must be deny
        assertFalse(testAcMgr.hasPrivileges(childNPath, modPropPrivileges));
    }

    @Test
    public void testMultipleGroupPermissionsOnNode() throws Exception {
        /* add privileges for the Group the test-user is member of */
        allow(path, testGroup.getPrincipal(), modPropPrivileges);
        deny(path, group2.getPrincipal(), modPropPrivileges);

        /*
         testuser must get the permissions/privileges inherited from
         the group it is member of.
         the denial of group2 must succeed
        */
        String actions = getActions(Session.ACTION_SET_PROPERTY, Session.ACTION_READ);
        assertFalse(testSession.hasPermission(path, actions));
        assertFalse(testAcMgr.hasPrivileges(path, modPropPrivileges));
    }

    @Test
    public void testMultipleGroupPermissionsOnNode2() throws Exception {
        /* add privileges for the Group the test-user is member of */
        deny(path, testGroup.getPrincipal(), modPropPrivileges);
        allow(path, group2.getPrincipal(), modPropPrivileges);

        /*
         testuser must get the permissions/privileges inherited from
         the group it is member of.
         granting permissions for group2 must be effective
        */
        String actions = getActions(Session.ACTION_SET_PROPERTY, Session.ACTION_READ);
        assertTrue(testSession.hasPermission(path, actions));
        assertTrue(testAcMgr.hasPrivileges(path, modPropPrivileges));
    }

    @Test
    public void testReorderGroupPermissions() throws Exception {
        /* add privileges for the Group the test-user is member of */
        deny(path, testGroup.getPrincipal(), modPropPrivileges);
        allow(path, group2.getPrincipal(), modPropPrivileges);

        /*
         testuser must get the permissions/privileges inherited from
         the group it is member of.
         granting permissions for group2 must be effective
        */
        String actions = getActions(Session.ACTION_SET_PROPERTY, Session.ACTION_READ);
        assertTrue(testSession.hasPermission(path, actions));
        Privilege[] privs = privilegesFromName(Privilege.JCR_MODIFY_PROPERTIES);
        assertTrue(testAcMgr.hasPrivileges(path, privs));

        // reorder the ACEs
        AccessControlEntry srcEntry = null;
        AccessControlEntry destEntry = null;
        JackrabbitAccessControlList acl = (JackrabbitAccessControlList) acMgr.getPolicies(path)[0];
        for (AccessControlEntry entry : acl.getAccessControlEntries()) {
            Principal princ = entry.getPrincipal();
            if (testGroup.getPrincipal().equals(princ)) {
                destEntry = entry;
            } else if (group2.getPrincipal().equals(princ)) {
                srcEntry = entry;
            }
        }
        acl.orderBefore(srcEntry, destEntry);
        acMgr.setPolicy(path, acl);
        superuser.save();
        testSession.refresh(false);

        /* after reordering the permissions must be denied */
        assertFalse(testSession.hasPermission(path, actions));
        assertFalse(testAcMgr.hasPrivileges(path, privs));
    }

    @Test
    public void testInheritanceAndMixedUserGroupPermissions() throws Exception {
        /* give MODIFY_PROPERTIES privilege for testGroup at 'path' */
        allow(path, testGroup.getPrincipal(), modPropPrivileges);

        /* withdraw MODIFY_PROPERTIES for the user at 'path' */
        deny(path, testUser.getPrincipal(), modPropPrivileges);

        /*
         since user-permissions overrule the group permissions, testuser must
         not have set_property action / modify_properties privilege.
         */
        assertFalse(testAcMgr.hasPrivileges(path, modPropPrivileges));

        /*
         give MODIFY_PROPERTIES privilege for everyone at 'childNPath'
         -> user-privileges still overrule group privileges
         */
        allow(childNPath, testGroup.getPrincipal(), modPropPrivileges);
        assertFalse(testAcMgr.hasPrivileges(childNPath, modPropPrivileges));
    }

    @Test
    public void testCancelInheritanceRestriction() throws Exception {
        allow(path, repWritePrivileges, createGlobRestriction(""));

        assertTrue(testAcMgr.hasPrivileges(path, repWritePrivileges));
        assertTrue(testSession.hasPermission(path, Session.ACTION_SET_PROPERTY));

        assertFalse(testAcMgr.hasPrivileges(childNPath, repWritePrivileges));
        assertFalse(testSession.hasPermission(childNPath, Session.ACTION_SET_PROPERTY));

        assertFalse(testAcMgr.hasPrivileges(childNPath2, repWritePrivileges));
        assertFalse(testSession.hasPermission(childNPath2, Session.ACTION_SET_PROPERTY));
    }
}
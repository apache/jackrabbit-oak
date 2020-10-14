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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import java.security.Principal;
import java.util.UUID;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/*
 * Test class created for <a href="https://issues.apache.org/jira/browse/OAK-8855">OAK-8855</a>.
 * Permission evaluation of child nodes broken after :nestedCugs property
 * is removed from parent node.
 */
public class RestoreParentNestedCugTest extends AbstractCugTest {

    private static final String TEST_GROUP2_ID = "testGroup2" + UUID.randomUUID();
    private static final String TEST_GROUP3_ID = "testGroup3" + UUID.randomUUID();
    private static final String TEST_USER1_ID = "testUser1" + UUID.randomUUID();

    @Override
    public void before() throws Exception {
        super.before();
        setupNestedCugsAndAcls();
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();

            Authorizable testGroup2 = getUserManager(root).getAuthorizable(TEST_GROUP2_ID);
            if (testGroup2 != null) {
                testGroup2.remove();
            }
            Authorizable testGroup3 = getUserManager(root).getAuthorizable(TEST_GROUP3_ID);
            if (testGroup3 != null) {
                testGroup3.remove();
            }
            Authorizable testUser1 = getUserManager(root).getAuthorizable(TEST_USER1_ID);
            if (testUser1 != null) {
                testUser1.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    private void setupNestedCugsAndAcls() throws Exception {
        UserManager uMgr = getUserManager(root);

        Principal testGroupPrincipal1 = getTestGroupPrincipal();
        Principal testGroupPrincipal2 = getTestGroupPrincipal(TEST_GROUP2_ID);
        Principal testGroupPrincipal3 = getTestGroupPrincipal(TEST_GROUP3_ID);

        User testUser1 = uMgr.createUser(TEST_USER1_ID, TEST_USER1_ID);
        ((Group) uMgr.getAuthorizable(testGroupPrincipal1)).addMember(testUser1);
        User testUser2 = uMgr.createUser(TEST_USER2_ID, TEST_USER2_ID);
        ((Group) uMgr.getAuthorizable(testGroupPrincipal2)).addMember(testUser2);

        ((Group) uMgr.getAuthorizable(testGroupPrincipal3)).addMember(testUser1);
        ((Group) uMgr.getAuthorizable(testGroupPrincipal3)).addMember(testUser2);

        Tree n = root.getTree(SUPPORTED_PATH);
        createTrees(n, "a", "b1");
        createTrees(n, "a", "b2");

        // - /content/a     : allow user1 & user2, deny everyone
        // - /content/a/b1  : allow user1, deny everyone
        // - /content/a/b2  : allow user2, deny everyone
        createCug("/content/a", testGroupPrincipal3);
        createCug("/content/a/b1", testGroupPrincipal1);
        createCug("/content/a/b2", testGroupPrincipal2);

        // - testUser1  : allow : jcr:read
        // - testUser2  : allow : jcr:read
        AccessControlManager acMgr = getAccessControlManager(root);
        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/content");
        acl.addAccessControlEntry(testUser1.getPrincipal(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acl.addAccessControlEntry(testUser2.getPrincipal(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy("/content", acl);

        root.commit();
    }

    /*
     * Remove :nestedCugs from a/rep:cugPolicy.
     * Note that The existing principals on b1 and b2 are not changed,
     * they are overwritten with the same principals they had earlier.
     * So the effective permissions on b1 and b2 are same as before.
     * This is done to make NestedCugHook consider b1 and b2 as "modified",
     * so that :nestedCugs for a/rep:cugPolicywill can be reconstructed
     * from b1 and b2 when root.commit() is called.
     */
    private void removeParentCug() throws Exception {
        Tree cugPolicyNode = root.getTree("/content/a/rep:cugPolicy");
        cugPolicyNode.removeProperty(HIDDEN_NESTED_CUGS); // remove :nestedCug from rep:cugPolicy
        PropertyState ps1 = PropertyStates.createProperty(CugConstants.REP_PRINCIPAL_NAMES, ImmutableSet.of(TEST_GROUP_ID), Type.STRINGS);
        PropertyState ps2 = PropertyStates.createProperty(CugConstants.REP_PRINCIPAL_NAMES, ImmutableSet.of(TEST_GROUP2_ID), Type.STRINGS);
        root.getTree("/content/a/b1/rep:cugPolicy").setProperty(ps1);
        root.getTree("/content/a/b2/rep:cugPolicy").setProperty(ps2);
        root.commit(); // should restore :nestedCugs in "/content/a/rep:cugPolicy"
    }

    private Principal getTestGroupPrincipal(String testGroupId) throws Exception {
        UserManager uMgr = getUserManager(root);
        Group g = uMgr.getAuthorizable(testGroupId, Group.class);
        if (g == null) {
            g = uMgr.createGroup(testGroupId);
            root.commit();
        }
        return g.getPrincipal();
    }

    @Test
    public void testParentNestedCugRestored() throws Exception {
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content/a");
        assertNestedCugs(root, getRootProvider(), "/content/a", true, "/content/a/b1", "/content/a/b2");

        removeParentCug();

        assertNestedCugs(root, getRootProvider(), "/content/a", true, "/content/a/b1", "/content/a/b2");
    }

    @Test
    public void testUserPermission() throws Exception {
        removeParentCug();

        //check if authorization is working fine
        ContentSession cs = createTestSession2(); //login as testuser2
        Root r = cs.getLatestRoot();
        try {
            Tree a = r.getTree("/content/a");
            assertTrue(a.exists());
            Tree b1 = a.getChild("b1");
            assertFalse(b1.exists());  //testuser2 not authorized to read this
            Tree b2 = a.getChild("b2");
            assertTrue(b2.exists());
        } finally {
            cs.close();
        }
    }
}

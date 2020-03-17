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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.value.StringValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PermissionTest extends AbstractSecurityTest {

    private static final String TEST_ROOT_PATH = "/testRoot";
    private static final String TEST_A_PATH = "/testRoot/a";
    private static final String TEST_B_PATH = "/testRoot/a/b";
    private static final String TEST_C_PATH = "/testRoot/a/b/c";
    private static final String TEST_D_PATH = "/testRoot/a/b/c/d";
    private static final String TEST_E_PATH = "/testRoot/a/b/c/d/e";

    private Principal testPrincipal;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"));
        NodeUtil testRootNode = rootNode.addChild("testRoot", NT_UNSTRUCTURED);
        NodeUtil a = testRootNode.addChild("a", NT_UNSTRUCTURED);
        NodeUtil b = a.addChild("b", NT_UNSTRUCTURED);
        NodeUtil c = b.addChild("c", NT_UNSTRUCTURED);
        NodeUtil d = c.addChild("d", NT_UNSTRUCTURED);
        d.addChild("e", NT_UNSTRUCTURED);
        root.commit();

        testPrincipal = getTestUser().getPrincipal();
    }

    @After
    @Override
    public void after() throws Exception {
        try {
            // revert uncommitted changes
            root.refresh();

            // remove all test content
            root.getTree(TEST_ROOT_PATH).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    private void addEntry(String path, boolean grant, String restriction, String... privilegeNames) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        if (restriction.length() > 0) {
            Map<String, Value> rs = new HashMap<String, Value>();
            rs.put("rep:glob", new StringValue(restriction));
            acl.addEntry(testPrincipal, AccessControlUtils.privilegesFromNames(acMgr, privilegeNames), grant, rs);
        } else {
            acl.addEntry(testPrincipal, AccessControlUtils.privilegesFromNames(acMgr, privilegeNames), grant);
        }
        acMgr.setPolicy(path, acl);
        root.commit();
    }

    private void assertIsGranted(PermissionProvider pp, Root root, boolean allow, String path, long permissions) {
        assertEquals("user should " + (allow ? "" : "not ") + "have " + permissions + " on " + path,
                allow, pp.isGranted(root.getTree(path), null, permissions));
    }

    private PermissionProvider getPermissionProvider(ContentSession session) {
        return getSecurityProvider()
                .getConfiguration(AuthorizationConfiguration.class)
                .getPermissionProvider(root, session.getWorkspaceName(), session.getAuthInfo().getPrincipals());
    }


    @Test
    public void testHasPermission() throws Exception {
        // create permissions
        // allow rep:write      /testroot
        // allow jcr:removeNode /testroot/a/b
        // deny  jcr:removeNode /testroot/a/b/c

        addEntry(TEST_ROOT_PATH, true, "", PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);
        addEntry(TEST_B_PATH, true, "", PrivilegeConstants.JCR_REMOVE_NODE);
        addEntry(TEST_C_PATH, false, "", PrivilegeConstants.JCR_REMOVE_NODE);

        ContentSession testSession = createTestSession();
        try {
            Root testRoot = testSession.getLatestRoot();
            PermissionProvider pp = getPermissionProvider(testSession);

            assertIsGranted(pp, testRoot, true, TEST_A_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_B_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, false, TEST_C_PATH, Permissions.REMOVE_NODE);

            try {
                testRoot.getTree(TEST_C_PATH).remove();
                testRoot.commit();
                fail("removing node on /a/b/c should fail");
            } catch (CommitFailedException e) {
                // all ok
            }
        } finally {
            testSession.close();
        }
    }

    /**
     * Tests if the restrictions are properly inherited.
     * the restriction enable/disable the ACE where it is defined.
     * since the 'allow' on /a/b is after the 'deny' on a/b/c, the allow wins.
     *
     * The test currently fails on evaluation of /a/b/c/d. Probably because the evaluation
     * of /a/b/c yields a deny, which terminates the iteration.
     */
    @Test
    public void testHasPermissionWithRestrictions() throws Exception {
        // create permissions
        // allow rep:write      /testroot
        // deny  jcr:removeNode /testroot/a  glob=*/c
        // allow jcr:removeNode /testroot/a  glob=*/b

        addEntry(TEST_ROOT_PATH, true, "", PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);
        addEntry(TEST_A_PATH, false, "*/c", PrivilegeConstants.JCR_REMOVE_NODE);
        addEntry(TEST_A_PATH, true, "*/b", PrivilegeConstants.JCR_REMOVE_NODE);

        ContentSession testSession = createTestSession();
        try {
            Root testRoot = testSession.getLatestRoot();
            PermissionProvider pp = getPermissionProvider(testSession);

            assertIsGranted(pp, testRoot, true, TEST_A_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_B_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, false, TEST_C_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_D_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_E_PATH, Permissions.REMOVE_NODE);

            // should be able to remove /a/b/c/d
            testRoot.getTree(TEST_D_PATH).remove();
            testRoot.commit();

            // should be able to remove /a/b/c
            try {
                testRoot.getTree(TEST_C_PATH).remove();
                testRoot.commit();
                fail("user should not be able to remove c");
            } catch (CommitFailedException e) {
                // ok
            }

        } finally {
            testSession.close();
        }
    }

    /**
     * Tests if the restrictions are properly inherited.
     * the restriction enable/disable the ACE where it is defined.
     * since the 'deny' on /a/b is after the 'allow' on a/b/c, the deny wins.
     */
    @Test
    public void testHasPermissionWithRestrictions2() throws Exception {
        // create permissions
        // allow rep:write      /testroot
        // allow jcr:removeNode /testroot/a  glob=*/b
        // deny  jcr:removeNode /testroot/a  glob=*/c

        addEntry(TEST_ROOT_PATH, true, "", PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);
        addEntry(TEST_A_PATH, true, "*/b", PrivilegeConstants.JCR_REMOVE_NODE);
        addEntry(TEST_A_PATH, false, "*/c", PrivilegeConstants.JCR_REMOVE_NODE);

        ContentSession testSession = createTestSession();
        try {
            Root testRoot = testSession.getLatestRoot();

            PermissionProvider pp = getPermissionProvider(testSession);

            assertIsGranted(pp, testRoot, true, TEST_A_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_B_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, false, TEST_C_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_D_PATH, Permissions.REMOVE_NODE);

            testRoot.getTree(TEST_D_PATH).remove();
            testRoot.commit();

            try {
                // should not be able to remove /a/b/c
                testRoot.getTree(TEST_C_PATH).remove();
                testRoot.commit();
                fail("should not be able to delete " + TEST_C_PATH);
            } catch (CommitFailedException e) {
                // ok
                testRoot.refresh();
            }

        } finally {
            testSession.close();
        }
    }

    /**
     * Tests the custom restriction provider that checks on the existence of a property.
     * @throws Exception
     */
    @Test
    public void testProtectPropertiesByRestriction() throws Exception {
        // create permissions
        // allow rep:write          /testroot
        // deny  jcr:modifyProperties /testroot/a  glob = */c

        addEntry(TEST_ROOT_PATH, true, "", PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);
        addEntry(TEST_A_PATH, false, "*/c", PrivilegeConstants.JCR_MODIFY_PROPERTIES);

        ContentSession testSession = createTestSession();
        try {
            Root testRoot = testSession.getLatestRoot();

            PermissionProvider pp = getPermissionProvider(testSession);
            assertIsGranted(pp, testRoot, true , TEST_A_PATH, Permissions.MODIFY_PROPERTY);
            assertIsGranted(pp, testRoot, true, TEST_B_PATH, Permissions.MODIFY_PROPERTY);
            assertIsGranted(pp, testRoot, false, TEST_C_PATH, Permissions.MODIFY_PROPERTY);
            assertIsGranted(pp, testRoot, true, TEST_D_PATH, Permissions.MODIFY_PROPERTY);
            assertIsGranted(pp, testRoot, true, TEST_E_PATH, Permissions.MODIFY_PROPERTY);

        } finally {
            testSession.close();
        }
    }


}
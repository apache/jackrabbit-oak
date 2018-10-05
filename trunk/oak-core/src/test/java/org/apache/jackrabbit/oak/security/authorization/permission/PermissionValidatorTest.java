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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.security.Principal;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PermissionValidatorTest extends AbstractSecurityTest {

    private static final String TEST_ROOT_PATH = "/testRoot";
    private static final String TEST_CHILD_PATH = "/testRoot/child";

    private NodeUtil testRootNode;
    private Principal testPrincipal;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"));
        testRootNode = rootNode.addChild("testRoot", NT_UNSTRUCTURED);
        testRootNode.addChild("child", NT_UNSTRUCTURED);
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

    private void grant(String path, String... privilegeNames) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        acl.addEntry(testPrincipal, AccessControlUtils.privilegesFromNames(acMgr, privilegeNames), true);
        acMgr.setPolicy(path, acl);
        root.commit();
    }

    @Test
    public void testChangePrimaryTypeToPolicyNode() throws Exception {
        // create a rep:policy node that is not detected as access control content
        testRootNode.getChild("child").addChild(AccessControlConstants.REP_POLICY, NT_UNSTRUCTURED);
        root.commit();

        // grant the test session the ability to read/write that node but don't
        // allow to modify access control content
        grant(TEST_ROOT_PATH,
                PrivilegeConstants.JCR_READ,
                PrivilegeConstants.JCR_READ_ACCESS_CONTROL,
                PrivilegeConstants.REP_WRITE);

        ContentSession testSession = createTestSession();
        try {
            Root testRoot = testSession.getLatestRoot();

            Tree testChild = testRoot.getTree(TEST_CHILD_PATH);
            testChild.setProperty(PropertyStates.createProperty(JcrConstants.JCR_MIXINTYPES, ImmutableList.of(AccessControlConstants.MIX_REP_ACCESS_CONTROLLABLE), Type.NAMES));

            Tree testPolicy = testChild.getChild(AccessControlConstants.REP_POLICY);
            testPolicy.setOrderableChildren(true);
            testPolicy.setProperty(JCR_PRIMARYTYPE, AccessControlConstants.NT_REP_ACL, Type.NAME);
            testRoot.commit();
            fail("Turning a false policy node into access control content requires the ability to write AC content.");
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
            assertEquals(0, e.getCode());
        } finally {
            testSession.close();
        }
    }
}
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
package org.apache.jackrabbit.oak.security.authorization;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authorization.AbstractAccessControlTest;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * EffectivePolicyTest... TODO
 */
@Ignore("OAK-51")
public class EffectivePolicyTest extends AbstractAccessControlTest {

    private String path;
    private String childNPath;

    private String testUserId = "testUser" + UUID.randomUUID();
    private User testUser;
    protected ContentSession testSession;
    protected Root testRoot;
    protected JackrabbitAccessControlManager testAccessControlManager;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        // create some nodes below the test root in order to apply ac-stuff
        NodeUtil rootNode = new NodeUtil(root.getTree("/"));
        NodeUtil testRootNode = rootNode.getOrAddChild("testRoot", JcrConstants.NT_UNSTRUCTURED);
        NodeUtil testNode = testRootNode.addChild("testNode", JcrConstants.NT_UNSTRUCTURED);
        NodeUtil cn1 = testNode.addChild("child1", JcrConstants.NT_UNSTRUCTURED);
        testNode.setString("property1", "anyValue");
        root.commit();

        path = testNode.getTree().getPath();
        childNPath = cn1.getTree().getPath();

        UserManager uMgr = getUserManager();
        testUser = uMgr.createUser(testUserId, testUserId);
        root.commit();

        testSession = login(new SimpleCredentials(testUserId, testUserId.toCharArray()));
        testRoot = testSession.getLatestRoot();
        testAccessControlManager = getAccessControlManager(testRoot);

        /*
         precondition:
         testuser must have READ-only permission on test-node and below
        */
        Privilege[] privs = testAccessControlManager.getPrivileges(path);
        assertArrayEquals(privilegesFromNames(Privilege.JCR_READ), privs);
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            testSession.close();

            root.getTree(path).remove();
            testUser.remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Nonnull
    private JackrabbitAccessControlList modify(String path, Principal principal,
                                               Privilege[] privileges, boolean isAllow) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        assertNotNull(acl);
        acl.addEntry(principal, privileges, isAllow, Collections.<String, Value>emptyMap());
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();
        return acl;
    }

    @Nonnull
    private JackrabbitAccessControlList allow(String nPath, Privilege[] privileges) throws Exception {
        return modify(nPath, testUser.getPrincipal(), privileges, true);
    }

    @Nonnull
    private JackrabbitAccessControlList deny(String nPath, Privilege[] privileges) throws Exception {
        return modify(nPath, testUser.getPrincipal(), privileges, false);
    }

    @Test
    public void testEffectivePoliciesByPath() throws Exception {
        // give 'testUser' READ_AC privileges at 'path'
        Privilege[] privileges = privilegesFromNames(Privilege.JCR_READ_ACCESS_CONTROL);
        allow(path, privileges);

        assertFalse(testAccessControlManager.hasPrivileges("/", privileges));
        assertTrue(testAccessControlManager.hasPrivileges(path, privileges));

        // since read-ac access is denied on the root that by default is
        // access controlled, getEffectivePolicies must fail due to missing
        // permissions to view all the effective policies.
        try {
            testAccessControlManager.getEffectivePolicies(path);
            fail();
        } catch (AccessDeniedException e) {
            // success
        }

        // ... and same on childNPath.
        try {
            testAccessControlManager.getEffectivePolicies(childNPath);
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testGetEffectivePoliciesByPrincipal() throws Exception {
        // give 'testUser' READ_AC privileges at 'path'
        Privilege[] privileges = privilegesFromNames(Privilege.JCR_READ_ACCESS_CONTROL);

        allow(path, privileges);

        // effective policies for testPrinicpal only on path -> must succeed.
        testAccessControlManager.getEffectivePolicies(Collections.singleton(testUser.getPrincipal()));

        // effective policies for a combination of principals -> must fail since
        // policy for 'everyone' at root node cannot be read by testuser
        Set<Principal> principals = testSession.getAuthInfo().getPrincipals();
        try {
            testAccessControlManager.getEffectivePolicies(principals);
            fail();
        } catch (AccessDeniedException e) {
            // success
        }

        deny(childNPath, privileges);
        testRoot.refresh();

        // the effective policies included the allowed acl at 'path' and
        // the denied acl at 'childNPath' -> must fail
        try {
            testAccessControlManager.getEffectivePolicies(Collections.singleton(testUser.getPrincipal()));
            fail();
        } catch (AccessDeniedException e) {
            // success
        }
    }
}
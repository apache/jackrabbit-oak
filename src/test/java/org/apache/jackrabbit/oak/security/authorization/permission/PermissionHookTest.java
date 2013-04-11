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

import java.util.HashSet;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AbstractAccessControlTest;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * PermissionHookTest... TODO
 */
public class PermissionHookTest extends AbstractAccessControlTest implements AccessControlConstants, PermissionConstants {

    private String testPath = "/testPath";
    private String testPrincipalName = "admin"; // TODO

    private PrivilegeBitsProvider bitsProvider;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"), namePathMapper);
        rootNode.addChild("testPath", JcrConstants.NT_UNSTRUCTURED);

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addAccessControlEntry(new PrincipalImpl(testPrincipalName), privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES));
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        bitsProvider = new PrivilegeBitsProvider(root);
    }

    @After
    public void after() throws Exception {
        root.refresh();
        Tree test = root.getTree(testPath);
        if (test != null) {
            test.remove();
            root.commit();
        }
    }

    private Tree getPrincipalRoot(String principalName) {
        return root.getTree(PERMISSIONS_STORE_PATH).getChild(adminSession.getWorkspaceName()).getChild(principalName);
    }

    private Tree getEntry(String principalName, String accessControlledPath) throws Exception {
        Tree principalRoot = getPrincipalRoot(principalName);
        for (Tree entry : principalRoot.getChildren()) {
            if (accessControlledPath.equals(entry.getProperty(REP_ACCESS_CONTROLLED_PATH).getValue(Type.STRING))) {
                return entry;
            }
        }
        throw new RepositoryException("no such entry");
    }

    @Test
    public void testDuplicateAce() throws Exception {
        // add duplicate policy on OAK-API
        NodeUtil policy = new NodeUtil(root.getTree(testPath + "/rep:policy"));
        NodeUtil ace = policy.addChild("duplicateAce", NT_REP_GRANT_ACE);
        ace.setString(REP_PRINCIPAL_NAME, testPrincipalName);
        ace.setStrings(REP_PRIVILEGES, PrivilegeConstants.JCR_ADD_CHILD_NODES);
        root.commit();

        Tree principalRoot = getPrincipalRoot(testPrincipalName);
        assertEquals(2, principalRoot.getChildrenCount());

        Set<Integer> index = new HashSet<Integer>(2);
        for (Tree entry : principalRoot.getChildren()) {
            assertEquals(bitsProvider.getBits(PrivilegeConstants.JCR_ADD_CHILD_NODES), PrivilegeBits.getInstance(entry.getProperty(REP_PRIVILEGE_BITS)));
            assertEquals(testPath, entry.getProperty(REP_ACCESS_CONTROLLED_PATH).getValue(Type.STRING));
            index.add(entry.getProperty(REP_INDEX).getValue(Type.LONG).intValue());
        }
        assertEquals(ImmutableSet.of(0, 2), index);

        // remove duplicate policy entry again
        root.getTree(testPath + "/rep:policy/duplicateAce").remove();
        root.commit();

        assertEquals(1, getPrincipalRoot(testPrincipalName).getChildrenCount());
    }

    @Test
    public void testModifyRestrictions() throws Exception {
        Tree testAce = root.getTree(testPath + "/rep:policy").getChildren().iterator().next();
        assertEquals(testPrincipalName, testAce.getProperty(REP_PRINCIPAL_NAME).getValue(Type.STRING));

        // add a new restriction node through the OAK API instead of access control manager
        NodeUtil node = new NodeUtil(testAce);
        NodeUtil restrictions = node.addChild(REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        restrictions.setString(REP_GLOB, "*");
        String restritionsPath = restrictions.getTree().getPath();
        root.commit();

        Tree principalRoot = getPrincipalRoot(testPrincipalName);
        assertEquals(1, principalRoot.getChildrenCount());
        assertEquals("*", principalRoot.getChildren().iterator().next().getProperty(REP_GLOB).getValue(Type.STRING));

        // modify the restrictions node
        Tree restrictionsNode = root.getTree(restritionsPath);
        restrictionsNode.setProperty(REP_GLOB, "/*/jcr:content/*");
        root.commit();

        principalRoot = getPrincipalRoot(testPrincipalName);
        assertEquals(1, principalRoot.getChildrenCount());
        assertEquals("/*/jcr:content/*", principalRoot.getChildren().iterator().next().getProperty(REP_GLOB).getValue(Type.STRING));

        // remove the restriction again
        root.getTree(restritionsPath).remove();
        root.commit();

        principalRoot = getPrincipalRoot(testPrincipalName);
        assertEquals(1, principalRoot.getChildrenCount());
        assertNull(principalRoot.getChildren().iterator().next().getProperty(REP_GLOB));

    }

    @Ignore("OAK-526 : PermissionHook#propertyChange") // TODO
    @Test
    public void testReorderAce() throws Exception {
        Tree entry = getEntry(testPrincipalName, testPath);
        assertEquals(0, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());

        Tree aclTree = root.getTree(testPath + "/rep:policy");
        aclTree.getChildren().iterator().next().orderBefore(null);

        root.commit();

        entry = getEntry(testPrincipalName, testPath);
        assertEquals(1, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());
    }

    @Ignore("OAK-526 : PermissionHook#propertyChange") // TODO
    @Test
    public void testReorderAndAddAce() throws Exception {
        Tree entry = getEntry(testPrincipalName, testPath);
        assertEquals(0, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());

        Tree aclTree = root.getTree(testPath + "/rep:policy");
        aclTree.getChildren().iterator().next().orderBefore(null);
        NodeUtil ace = new NodeUtil(aclTree).addChild("denyEveryoneLockMgt", NT_REP_DENY_ACE);
        ace.setString(REP_PRINCIPAL_NAME, EveryonePrincipal.NAME);
        ace.setStrings(REP_PRIVILEGES, PrivilegeConstants.JCR_LOCK_MANAGEMENT);
        root.commit();

        entry = getEntry(testPrincipalName, testPath);
        assertEquals(1, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());
    }
}
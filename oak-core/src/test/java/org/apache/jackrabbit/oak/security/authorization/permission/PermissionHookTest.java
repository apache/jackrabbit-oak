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
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AbstractAccessControlTest;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * PermissionHookTest... TODO
 */
public class PermissionHookTest extends AbstractAccessControlTest implements PermissionConstants {

    private AccessControlManager acMgr;
    @Override
    @Before
    public void before() throws Exception {
        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"), namePathMapper);
        rootNode.addChild("testName", JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        acMgr = getAccessControlManager(root);
    }

    @After
    public void after() throws Exception {
        root.refresh();
        root.getTree("/testName").remove();
        root.commit();
    }

    private Tree getEntry(String principalName, String accessControlledPath) throws Exception {
        Tree permissionTree = root.getTree(PERMISSIONS_STORE_PATH).getChild(adminSession.getWorkspaceName()).getChild(principalName);
        for (Tree entry : permissionTree.getChildren()) {
            if (accessControlledPath.equals(entry.getProperty(REP_ACCESS_CONTROLLED_PATH).getValue(Type.STRING))) {
                return entry;
            }
        }
        throw new RepositoryException("no such entry");
    }

    @Test
    public void testAddDuplicateAce() {
        // TODO
    }

    @Test
    public void testRemoveDuplicateAce() {
        // TODO
    }

    @Test
    public void testAddRestrictionNode() {
        // TODO add restriction node on oak-api
    }

    @Test
    public void testRemoveRestrictionNode() {
        // TODO
    }

    @Test
    public void testModifyRestrictions() {
        // TODO
    }

    @Ignore("PermissionHook#propertyChanged without corresponding child node modifications")
    @Test
    public void testReorderAce() throws Exception {
        Principal testPrincipal = new PrincipalImpl("admin");
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/testName");
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES));
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy("/testName", acl);
        root.commit();

        Tree entry = getEntry("admin", "/testName");
        assertEquals(0, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());

        Tree aclTree = root.getTree("/testName/rep:policy");
        aclTree.getChildren().iterator().next().orderBefore(null);

        root.commit();

        entry = getEntry("admin", "/testName");
        assertEquals(1, entry.getProperty(REP_INDEX).getValue(Type.LONG).longValue());
    }
}
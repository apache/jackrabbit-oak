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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.Collection;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PermissionStoreImplTest extends AbstractSecurityTest implements PermissionConstants {

    private PermissionStoreImpl permissionStore;

    private Principal testPrincipal;

    private String testPath = "/testPath";
    private String childPath = "/testPath/childNode";

    @Before
    public void before() throws Exception {
        super.before();
        testPrincipal = getTestUser().getPrincipal();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"), namePathMapper);
        NodeUtil testNode = rootNode.addChild("testPath", JcrConstants.NT_UNSTRUCTURED);
        testNode.addChild("childNode", JcrConstants.NT_UNSTRUCTURED);

        addAcl(testPath, EveryonePrincipal.getInstance());
        addAcl(childPath, EveryonePrincipal.getInstance());
        root.commit();

        permissionStore = new PermissionStoreImpl(root, root.getContentSession().getWorkspaceName(), getConfig(AuthorizationConfiguration.class).getRestrictionProvider());
    }

    private void addAcl(@NotNull String path, @NotNull Principal principal) throws RepositoryException {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy(path, acl);
    }

    @After
    public void after() throws Exception {
        try {
            AccessControlManager acMgr = getAccessControlManager(root);
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
            acMgr.removePolicy(testPath, acl);
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testLoad() {
        PrincipalPermissionEntries entries = permissionStore.load(EveryonePrincipal.NAME);
        assertNotNull(entries);
        assertTrue(entries.isFullyLoaded());
        assertEquals(2, entries.getSize());
    }

    @Test
    public void testLoadMissingPrincipalRoot() {
        PrincipalPermissionEntries entries = permissionStore.load(testPrincipal.getName());
        assertNotNull(entries);
        assertTrue(entries.isFullyLoaded());
        assertEquals(0, entries.getSize());
    }

    @Test
    public void testLoadWithNesting() throws Exception {
        try {
            Tree everyoneTree = getPermissionRoot(EveryonePrincipal.NAME);
            everyoneTree.removeProperty(REP_NUM_PERMISSIONS);
            for (Tree child : everyoneTree.getChildren()) {
                if (child.hasProperty(REP_ACCESS_CONTROLLED_PATH)) {
                    Tree collision = TreeUtil.addChild(child, "c_"+child.getName(), NT_REP_PERMISSION_STORE);
                    collision.setProperty(REP_ACCESS_CONTROLLED_PATH, "/another/path");
                    Tree entry = TreeUtil.addChild(collision, "1", NT_REP_PERMISSIONS);
                    entry.setProperty(PrivilegeBits.BUILT_IN.get(REP_READ_NODES).asPropertyState(REP_PRIVILEGE_BITS));
                    entry.setProperty(REP_IS_ALLOW, false);
                    break;
                }
            }

            PrincipalPermissionEntries entries = permissionStore.load(EveryonePrincipal.NAME);
            assertNotNull(entries);
            assertTrue(entries.isFullyLoaded());
            assertEquals(3, entries.getSize());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testLoadWithNestingMissingAccessControlledPath() throws Exception {
        try {
            Tree everyoneTree = getPermissionRoot(EveryonePrincipal.NAME);
            everyoneTree.removeProperty(REP_NUM_PERMISSIONS);
            for (Tree child : everyoneTree.getChildren()) {
                if (child.hasProperty(REP_ACCESS_CONTROLLED_PATH)) {
                    Tree collision = TreeUtil.addChild(child, "c_"+child.getName(), NT_REP_PERMISSION_STORE);
                    Tree entry = TreeUtil.addChild(collision, "1", NT_REP_PERMISSIONS);
                    entry.setProperty(PrivilegeBits.BUILT_IN.get(REP_READ_NODES).asPropertyState(REP_PRIVILEGE_BITS));
                    entry.setProperty(REP_IS_ALLOW, false);
                    break;
                }
            }

            PrincipalPermissionEntries entries = permissionStore.load(EveryonePrincipal.NAME);
            assertNotNull(entries);
            assertTrue(entries.isFullyLoaded());
            assertEquals(2, entries.getSize());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testLoadByPath() {
        Collection<PermissionEntry> entries = permissionStore.load(EveryonePrincipal.NAME, testPath);
        assertNotNull(entries);
        assertFalse(entries.isEmpty());
    }

    @Test
    public void testLoadByPathWithoutEntries() {
        assertNull(permissionStore.load(EveryonePrincipal.NAME, testPath + "/notAccessControlled"));
    }

    @Test
    public void testLoadByPathMissingPrincipalRoot() {
        assertNull(permissionStore.load(testPrincipal.getName(), testPath));
    }

    @Test
    public void testLoadByPathWithCollision() throws Exception {
        try {
            Tree everyoneTree = getPermissionRoot(EveryonePrincipal.NAME);
            Tree aa = TreeUtil.addChild(everyoneTree, "/Aa".hashCode() +"", NT_REP_PERMISSION_STORE);
            aa.setProperty(REP_ACCESS_CONTROLLED_PATH, "/Aa");
            Tree entry = TreeUtil.addChild(aa, "1", NT_REP_PERMISSIONS);
            entry.setProperty(PrivilegeBits.BUILT_IN.get(REP_READ_NODES).asPropertyState(REP_PRIVILEGE_BITS));
            entry.setProperty(REP_IS_ALLOW, false);

            Tree collision = TreeUtil.addChild(aa, "c_"+"/BB".hashCode(), NT_REP_PERMISSION_STORE);
            collision.setProperty(REP_ACCESS_CONTROLLED_PATH, "/BB");
            Tree entryBB = TreeUtil.addChild(collision, "1", NT_REP_PERMISSIONS);
            entryBB.setProperty(PrivilegeBits.BUILT_IN.get(REP_READ_NODES).asPropertyState(REP_PRIVILEGE_BITS));
            entryBB.setProperty(REP_IS_ALLOW, true);

            // a single allow entry for /BB (must descend to collision tree)
            Collection<PermissionEntry> entries = permissionStore.load(EveryonePrincipal.NAME, "/BB");
            assertNotNull(entries);
            assertEquals(1, entries.size());
            assertTrue(entries.iterator().next().isAllow);

            // a single deny entry for /Aa
            entries = permissionStore.load(EveryonePrincipal.NAME, "/Aa");
            assertNotNull(entries);
            assertFalse(entries.iterator().next().isAllow);

        } finally {
            root.refresh();
        }
    }

    @Test
    public void testLoadByPathWithCollisionMissingAccessControlledProperty() throws Exception {
        try {
            Tree everyoneTree = getPermissionRoot(EveryonePrincipal.NAME);
            Tree aa = TreeUtil.addChild(everyoneTree, "/Aa".hashCode() +"", NT_REP_PERMISSION_STORE);
            aa.setProperty(REP_ACCESS_CONTROLLED_PATH, "/Aa");
            Tree collision = TreeUtil.addChild(aa, "c_"+"/BB".hashCode(), NT_REP_PERMISSION_STORE);
            Tree entry = TreeUtil.addChild(collision, "1", NT_REP_PERMISSIONS);
            entry.setProperty(PrivilegeBits.BUILT_IN.get(REP_READ_NODES).asPropertyState(REP_PRIVILEGE_BITS));
            entry.setProperty(REP_IS_ALLOW, false);

            Collection<PermissionEntry> entries = permissionStore.load(EveryonePrincipal.NAME, "/BB");
            assertNull(entries);
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testGetNumEntries() {
        assertEquals(NumEntries.valueOf(2, true), permissionStore.getNumEntries(EveryonePrincipal.NAME, Long.MAX_VALUE));
    }

    @Test
    public void testGetNumEntriesMissingPrincipalRoot() {
        assertEquals(NumEntries.valueOf(0, true), permissionStore.getNumEntries(testPrincipal.getName(), Long.MAX_VALUE));
    }

    @Test
    public void testGetNumEntriesMissingProperty() throws Exception {
        try {
            Tree everyoneTree = getPermissionRoot(EveryonePrincipal.NAME);
            everyoneTree.removeProperty(REP_NUM_PERMISSIONS);

            assertEquals(NumEntries.valueOf(2, false), permissionStore.getNumEntries(EveryonePrincipal.NAME, Long.MAX_VALUE));
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testGetNumEntriesMissingPropertyThreshold() throws Exception {
        try {
            Tree everyoneTree = getPermissionRoot(EveryonePrincipal.NAME);
            everyoneTree.removeProperty(REP_NUM_PERMISSIONS);

            long max = 1;
            assertEquals(NumEntries.valueOf(everyoneTree.getChildrenCount(max), false), permissionStore.getNumEntries(EveryonePrincipal.NAME, max));
        } finally {
            root.refresh();
        }
    }

    @Nullable
    private Tree getPermissionRoot(@NotNull String principalName) throws Exception {
        Method m = PermissionStoreImpl.class.getDeclaredMethod("getPrincipalRoot", String.class);
        m.setAccessible(true);

        return (Tree) m.invoke(permissionStore, principalName);
    }
}

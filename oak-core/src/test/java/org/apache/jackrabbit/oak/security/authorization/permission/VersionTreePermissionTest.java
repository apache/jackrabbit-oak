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

import java.lang.reflect.Field;
import java.security.Principal;

import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionTreePermissionTest extends AbstractSecurityTest implements NodeTypeConstants {

    private ReadOnlyVersionManager vMgr;
    private PermissionProvider pp;

    private Tree testTree;

    private Field vpField;
    private Field tpImplTree;

    @Override
    public void before() throws Exception {
        super.before();

        NodeUtil testNode = new NodeUtil(root.getTree("/")).addChild("test", NT_OAK_UNSTRUCTURED);
        testNode.addChild("a", NT_OAK_UNSTRUCTURED).addChild("b", NT_OAK_UNSTRUCTURED).addChild("c", NT_OAK_UNSTRUCTURED);
        TreeUtil.addMixin(testNode.getTree(), MIX_VERSIONABLE, root.getTree(NODE_TYPES_PATH), null);

        AccessControlManager acMgr = getAccessControlManager(root);
        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/test");
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.JCR_READ));
        acMgr.setPolicy("/test", acl);
        root.commit();

        // create a structure in the version storage
        testNode.setBoolean(JCR_ISCHECKEDOUT, false);
        root.commit();
        testNode.setBoolean(JCR_ISCHECKEDOUT, true);
        root.commit();

        testTree = testNode.getTree();
        vMgr = ReadOnlyVersionManager.getInstance(root, NamePathMapper.DEFAULT);
        pp = getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, root.getContentSession().getWorkspaceName(), ImmutableSet.<Principal>of(EveryonePrincipal.getInstance()));

        assertTrue(pp instanceof PermissionProviderImpl);

        vpField = VersionTreePermission.class.getDeclaredField("versionablePermission");
        vpField.setAccessible(true);
        Class cls = Class.forName(CompiledPermissionImpl.class.getName() + "$TreePermissionImpl");
        tpImplTree = cls.getDeclaredField("tree");
        tpImplTree.setAccessible(true);
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            Tree t = root.getTree("/test");
            if (t.exists()) {
                t.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    private static TreePermission getVersionPermission(Root root, PermissionProvider pp, String path) {
        Tree t = root.getTree("/");
        TreePermission tp = pp.getTreePermission(t, TreePermission.EMPTY);
        for (String name : PathUtils.elements(path)) {
            t = t.getChild(name);
            tp = pp.getTreePermission(t, tp);
        }
        return tp;
    }

    private void assertVersionPermission(@Nonnull TreePermission tp, @Nonnull String expectedPath, boolean canRead) throws Exception {
        assertTrue(tp instanceof VersionTreePermission);
        assertEquals(canRead, tp.canRead());
        assertEquals(canRead, tp.canRead(PropertyStates.createProperty("any", "Value")));
        assertEquals(canRead, tp.isGranted(Permissions.READ));
        assertEquals(canRead, tp.isGranted(Permissions.READ, PropertyStates.createProperty("any", "Value")));
        assertEquals(canRead, tp.canReadProperties());
        assertFalse(tp.canReadAll());

        VersionTreePermission vtp = (VersionTreePermission) tp;
        TreePermission delegatee = (TreePermission) vpField.get(vtp);
        Tree delegateeTree = (Tree) tpImplTree.get(delegatee);

        assertEquals(expectedPath, delegateeTree.getPath());
    }

    @Test
    public void testGetTreePermission() throws Exception {
        Tree versionHistory = checkNotNull(vMgr.getVersionHistory(testTree));

        String expectedPath = "/test";

        TreePermission tp = getVersionPermission(root, pp, versionHistory.getPath());
        assertVersionPermission(tp, expectedPath, true);

        Tree vTree = versionHistory.getChild("1.0");
        assertTrue(vTree.exists());
        tp = pp.getTreePermission(vTree, tp);
        assertVersionPermission(tp, expectedPath, true);

        Tree frozen = vTree.getChild(JCR_FROZENNODE);
        assertTrue(frozen.exists());
        tp = pp.getTreePermission(frozen, tp);
        assertVersionPermission(tp, expectedPath, true);

        Tree t = frozen;
        for (String name : new String[] {"a", "b", "c"}) {
            t = t.getChild(name);
            expectedPath = PathUtils.concat(expectedPath, name);
            tp = pp.getTreePermission(t, tp);
            assertVersionPermission(tp, expectedPath, true);
        }
    }

    @Test
    public void testGetChild() throws Exception {
        Tree versionHistory = checkNotNull(vMgr.getVersionHistory(testTree));

        ImmutableTree t = (ImmutableTree) getRootProvider().createReadOnlyRoot(root).getTree("/");
        TreePermission tp = pp.getTreePermission(t, TreePermission.EMPTY);
        for (String name : PathUtils.elements(versionHistory.getPath())) {
            t = t.getChild(name);
            tp = tp.getChildPermission(name, t.getNodeState());
        }

        String expectedPath = "/test";
        assertVersionPermission(tp, "/test", true);

        NodeState ns = t.getChild("1.0").getNodeState();
        tp = tp.getChildPermission("1.0", ns);
        assertVersionPermission(tp, "/test", true);

        ns = ns.getChildNode(JCR_FROZENNODE);
        tp = tp.getChildPermission(JCR_FROZENNODE, ns);
        assertVersionPermission(tp, "/test", true);

        for (String name : new String[] {"a", "b", "c"}) {
            ns = ns.getChildNode(name);
            expectedPath = PathUtils.concat(expectedPath, name);

            tp = tp.getChildPermission(name, ns);
            assertVersionPermission(tp, expectedPath, true);
        }
    }

    @Test
    public void testVersionableRemoved() throws Exception {
        Tree versionHistory = checkNotNull(vMgr.getVersionHistory(testTree));

        testTree.remove();
        root.commit();
        pp.refresh();

        TreePermission tp = getVersionPermission(root, pp, versionHistory.getPath());
        assertVersionPermission(tp, "/", false);

        Tree vTree = versionHistory.getChild("1.0");
        tp = pp.getTreePermission(vTree, tp);
        assertVersionPermission(tp, "/", false);

        Tree frozen = vTree.getChild(JCR_FROZENNODE);
        assertTrue(frozen.exists());
        tp = pp.getTreePermission(frozen, tp);
        assertVersionPermission(tp, "/", false);

        Tree t = frozen;
        String expectedPath = "/";
        for (String name : new String[] {"a", "b", "c"}) {
            t = t.getChild(name);
            expectedPath = PathUtils.concat(expectedPath, name);
            tp = pp.getTreePermission(t, tp);
            assertVersionPermission(tp, expectedPath, false);
        }
    }

    @Test
    public void testVersionableChildRemoved() throws Exception {
        root.getTree("/test/a/b/c").remove();
        root.commit();
        pp.refresh();

        Tree versionHistory = checkNotNull(vMgr.getVersionHistory(testTree));
        String frozenCPath = PathUtils.concat(versionHistory.getPath(), "1.0", JCR_FROZENNODE, "a/b/c");

        TreePermission tp = getVersionPermission(root, pp, frozenCPath);
        assertVersionPermission(tp, "/test/a/b/c", true);

        root.getTree("/test/a").remove();
        root.commit();
        pp.refresh();

        tp = getVersionPermission(root, pp, frozenCPath);
        assertVersionPermission(tp, "/test/a/b/c", true);
    }

    @Test
    public void testVersionableChildRemoved2() throws Exception {
        root.getTree("/test/a/b").remove();
        root.commit();
        pp.refresh();

        Tree versionHistory = checkNotNull(vMgr.getVersionHistory(testTree));

        String frozenAPath = PathUtils.concat(versionHistory.getPath(), "1.0", JCR_FROZENNODE, "a");
        TreePermission tp = getVersionPermission(root, pp, frozenAPath);
        assertVersionPermission(tp, "/test/a", true);

        Tree frozenB = root.getTree(frozenAPath).getChild("b");
        tp = pp.getTreePermission(frozenB, tp);
        assertVersionPermission(tp, "/test/a/b", true);

        Tree frozenC = frozenB.getChild("c");
        tp = pp.getTreePermission(frozenC, tp);
        assertVersionPermission(tp, "/test/a/b/c", true);
    }
}
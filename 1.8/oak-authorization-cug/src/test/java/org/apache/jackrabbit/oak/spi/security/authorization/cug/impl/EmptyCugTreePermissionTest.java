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
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.impl.AbstractTree;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class EmptyCugTreePermissionTest extends AbstractCugTest {

    private CugPermissionProvider pp;
    private EmptyCugTreePermission tp;
    private NodeState rootState;

    @Override
    public void before() throws Exception {
        super.before();

        createCug(SUPPORTED_PATH, EveryonePrincipal.getInstance());
        root.commit();

        pp = createCugPermissionProvider(
                ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2),
                getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        Root readOnlyRoot = getRootProvider().createReadOnlyRoot(root);
        Tree t = readOnlyRoot.getTree("/");
        tp = new EmptyCugTreePermission(t, TreeType.DEFAULT, pp);
        rootState = ((AbstractTree) t).getNodeState();
    }

    @Test
    public void testRootPermission() throws Exception {
        assertCugPermission(tp, false);

        TreePermission rootTp = pp.getTreePermission(root.getTree("/"), TreePermission.EMPTY);
        assertCugPermission(rootTp, false);
    }

    @Test
    public void testJcrSystemPermissions() throws Exception {
        NodeState system = rootState.getChildNode(JcrConstants.JCR_SYSTEM);
        TreePermission systemTp = tp.getChildPermission(JcrConstants.JCR_SYSTEM, system);
        assertCugPermission(systemTp, false);
        assertCugPermission(pp.getTreePermission(root.getTree("/jcr:system"), tp), false);

        NodeState versionStore = system.getChildNode(VersionConstants.JCR_VERSIONSTORAGE);
        TreePermission versionStoreTp = systemTp.getChildPermission(VersionConstants.JCR_VERSIONSTORAGE, versionStore);
        assertCugPermission(versionStoreTp, false);
        assertCugPermission(pp.getTreePermission(root.getTree(VersionConstants.VERSION_STORE_PATH), systemTp), false);

        NodeState nodeTypes = system.getChildNode(NodeTypeConstants.JCR_NODE_TYPES);
        TreePermission nodeTypesTp = systemTp.getChildPermission(NodeTypeConstants.JCR_NODE_TYPES, nodeTypes);
        assertSame(TreePermission.NO_RECOURSE, nodeTypesTp);
    }

    @Test
    public void testGetChildPermission() throws Exception {
        String name = Text.getName(SUPPORTED_PATH2);
        NodeState ns = rootState.getChildNode(name);
        TreePermission child = tp.getChildPermission(name, ns);
        assertSame(TreePermission.NO_RECOURSE, child);

        name = Text.getName(SUPPORTED_PATH);
        ns = rootState.getChildNode(name);
        child = tp.getChildPermission(name, ns);
        assertCugPermission(child, true);
        assertTrue(((CugTreePermission) child).isInCug());
        TreePermission subtree = child.getChildPermission("subtree", ns.getChildNode("subtree"));
        assertCugPermission(subtree, true);
        assertTrue(((CugTreePermission) subtree).isInCug());

        name = Text.getName(UNSUPPORTED_PATH);
        ns = rootState.getChildNode(name);
        TreePermission child2 = tp.getChildPermission(name, ns);
        assertFalse(child2 instanceof EmptyCugTreePermission);
        assertSame(child2, TreePermission.NO_RECOURSE);
    }

    @Test
    public void testCanRead() {
        assertFalse(tp.canRead());
    }

    @Test
    public void testCanReadProperty() {
        assertFalse(tp.canRead(PropertyStates.createProperty("prop", "val")));
    }

    @Test
    public void testCanReadAll() {
        assertFalse(tp.canReadAll());
    }

    @Test
    public void testCanReadProperties() {
        assertFalse(tp.canReadProperties());
    }

    @Test
    public void testIsGranted() {
        assertFalse(tp.isGranted(Permissions.ALL));
        assertFalse(tp.isGranted(Permissions.WRITE));
        assertFalse(tp.isGranted(Permissions.READ));
    }

    @Test
    public void testIsGrantedProperty() {
        PropertyState ps = PropertyStates.createProperty("prop", "val");
        assertFalse(tp.isGranted(Permissions.ALL, ps));
        assertFalse(tp.isGranted(Permissions.WRITE, ps));
        assertFalse(tp.isGranted(Permissions.READ, ps));
    }

}

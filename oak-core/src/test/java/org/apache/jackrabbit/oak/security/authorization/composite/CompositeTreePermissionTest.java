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
package org.apache.jackrabbit.oak.security.authorization.composite;

import java.lang.reflect.Field;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CompositeTreePermissionTest extends AbstractSecurityTest {

    private Root readOnlyRoot;
    private ImmutableTree rootTree;

    private AggregatedPermissionProvider fullScopeProvider;

    @Override
    public void before() throws Exception {
        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"));
        rootNode.addChild("test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        root.commit();

        readOnlyRoot = RootFactory.createReadOnlyRoot(root);
        rootTree = (ImmutableTree) readOnlyRoot.getTree("/");

        fullScopeProvider = new FullScopeProvider(readOnlyRoot);
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            root.getTree("/test").remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    private AggregatedPermissionProvider[] getProviders(AggregatedPermissionProvider... providers) {
        return providers;
    }

    @Test
    public void testEmpty() {
        TreePermission rootTp = CompositeTreePermission.create(rootTree, getProviders());
        assertSame(TreePermission.EMPTY, rootTp);
        assertFalse(rootTp.canRead());
    }

    @Test
    public void testSingle() {
        Class<? extends TreePermission> expected = fullScopeProvider.getTreePermission(rootTree, TreePermission.EMPTY).getClass();

        TreePermission rootTp = CompositeTreePermission.create(rootTree, getProviders(fullScopeProvider));
        assertFalse(rootTp instanceof CompositeTreePermission);
        assertEquals(expected, rootTp.getClass());

        TreePermission testTp = rootTp.getChildPermission("test", rootTree.getChild("test").getNodeState());
        assertEquals(expected, testTp.getClass());
    }

    @Test
    public void testMultiple() {
        TreePermission rootTp = CompositeTreePermission.create(rootTree, getProviders(fullScopeProvider, fullScopeProvider));
        assertTrue(rootTp instanceof CompositeTreePermission);

        TreePermission testTp = rootTp.getChildPermission("test", rootTree.getChild("test").getNodeState());
        assertTrue(testTp instanceof CompositeTreePermission);
    }

    @Test
    public void testMultipleNoRecurse() {
        TreePermission rootTp = CompositeTreePermission.create(rootTree, getProviders(new NoScopeProvider(), new NoScopeProvider()));
        assertTrue(rootTp instanceof CompositeTreePermission);

        assertSame(TreePermission.EMPTY, rootTp.getChildPermission("test", rootTree.getChild("test").getNodeState()));
    }

    @Test
    public void testMultipleToSingle() {
        TreePermission rootTp = CompositeTreePermission.create(rootTree, getProviders(fullScopeProvider, new NoScopeProvider(), new NoScopeProvider()));
        assertTrue(rootTp instanceof CompositeTreePermission);

        NodeState childState = rootTree.getChild("test").getNodeState();
        TreePermission testTp = rootTp.getChildPermission("test", childState);
        TreePermission expected = fullScopeProvider.getTreePermission(rootTree, TreePermission.EMPTY).getChildPermission("test", childState);
        assertEquals(expected.getClass(), testTp.getClass());
    }

    @Test
    public void testCanRead() throws Exception {
        TreePermission rootTp = CompositeTreePermission.create(rootTree, getProviders(fullScopeProvider, fullScopeProvider));

        Field f = CompositeTreePermission.class.getDeclaredField("canRead");
        f.setAccessible(true);

        Object canRead = f.get(rootTp);
        assertNull(canRead);

        rootTp.canRead();

        canRead = f.get(rootTp);
        assertNotNull(canRead);
    }

    @Test
    public void testParentNoRecourse() throws Exception {
        Field f = CompositeTreePermission.class.getDeclaredField("providers");
        f.setAccessible(true);

        TreePermission rootTp = CompositeTreePermission.create(rootTree, getProviders(new NoScopeProvider()));
        assertSame(TreePermission.NO_RECOURSE, rootTp);
    }
}
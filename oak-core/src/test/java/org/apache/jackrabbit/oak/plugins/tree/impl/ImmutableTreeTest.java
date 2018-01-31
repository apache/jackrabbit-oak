/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.tree.impl;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ImmutableTreeTest extends AbstractSecurityTest {

    private static final String HIDDEN_PATH = "/oak:index/acPrincipalName/:index";

    private ImmutableTree immutable;

    @Before
    public void setUp() throws Exception {
        Tree tree = root.getTree("/");
        NodeUtil node = new NodeUtil(tree);
        node.addChild("x", JcrConstants.NT_UNSTRUCTURED).addChild("y", JcrConstants.NT_UNSTRUCTURED).addChild("z", JcrConstants.NT_UNSTRUCTURED);

        Tree orderable = node.addChild("orderable", JcrConstants.NT_UNSTRUCTURED).getTree();
        orderable.setOrderableChildren(true);
        root.commit();

        immutable = new ImmutableTree(((AbstractTree) root.getTree("/")).getNodeState());
    }

    @After
    public void tearDown() {
        root = null;
    }

    @Test
    public void testGetPath() {
        assertEquals("/", immutable.getPath());

        immutable = immutable.getChild("x");
        assertEquals("/x", immutable.getPath());

        immutable = immutable.getChild("y");
        assertEquals("/x/y", immutable.getPath());

        immutable = immutable.getChild("z");
        assertEquals("/x/y/z", immutable.getPath());
    }

    @Test
    public void testGetNodeState() {
        assertNotNull(immutable.getNodeState());

        for (Tree child : immutable.getChildren()) {
            assertTrue(child instanceof ImmutableTree);
            assertNotNull(((ImmutableTree) child).getNodeState());
        }
    }

    @Test
    public void testRootIsRoot() {
        assertTrue(immutable.isRoot());
    }

    @Test(expected = IllegalStateException.class)
    public void testRootGetParent() {
        immutable.getParent();
    }

    @Test
    public void testGetParent() {
        ImmutableTree child = immutable.getChild("x");
        assertNotNull(child.getParent());
        assertEquals("/", child.getParent().getPath());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetParentDisconnected() {
        ImmutableTree child = immutable.getChild("x");
        ImmutableTree disconnected = new ImmutableTree(ImmutableTree.ParentProvider.UNSUPPORTED, child.getName(), child.getNodeState());
        disconnected.getParent();
    }

    @Test
    public void testGetName() {
        assertEquals("x", immutable.getChild("x").getName());
    }

    @Test
    public void testHiddenGetName() {
        assertEquals(Text.getName(HIDDEN_PATH), getHiddenTree(immutable).getName());
    }

    @Test
    public void testNonExistingGetName() {
        assertEquals("nonExisting", immutable.getChild("nonExisting").getName());
    }

    @Test
    public void testRootGetName() {
        assertEquals("", immutable.getName());
    }

    @Test
    public void testExists() {
        ImmutableTree child = immutable.getChild("x");
        assertTrue(child.exists());
    }

    @Test
    public void testHiddenExists() {
        assertTrue(getHiddenTree(immutable).exists());
    }

    @Test
    public void testNonExisting() {
        ImmutableTree child = immutable.getChild("nonExisting");
        assertNotNull(child);
        assertFalse(child.exists());
    }

    @Test
    public void testRootGetStatus() {
        assertSame(Tree.Status.UNCHANGED, immutable.getStatus());
    }

    @Test
    public void testGetStatus() {
        assertSame(Tree.Status.UNCHANGED, immutable.getChild("x").getStatus());
    }

    @Test
    public void testHiddenGetStatus() {
        assertSame(Tree.Status.UNCHANGED, getHiddenTree(immutable).getStatus());
    }

    @Test
    public void testNonExistingGetStatus() {
        assertSame(Tree.Status.UNCHANGED, immutable.getChild("nonExisting").getStatus());
    }

    @Test
    public void testHasChild() {
        assertTrue(immutable.hasChild("x"));
    }

    @Test
    public void testHasHiddenChild() {
        ImmutableTree parent = (ImmutableTree) TreeUtil.getTree(immutable, Text.getRelativeParent(HIDDEN_PATH, 1));
        assertNotNull(parent);
        assertTrue(parent.hasChild(Text.getName(HIDDEN_PATH)));
    }

    @Test
    public void testGetHiddenNode() {
        ImmutableTree hidden = getHiddenTree(immutable);
        assertNotNull(hidden);
    }


    @Test
    public void testHasHiddenProperty() {
        ImmutableTree orderable = immutable.getChild("orderable");
        assertTrue(orderable.hasProperty(TreeConstants.OAK_CHILD_ORDER));
    }

    @Test
    public void testGetHiddenProperty() {
        ImmutableTree orderable = immutable.getChild("orderable");
        assertNotNull(orderable.getProperty(TreeConstants.OAK_CHILD_ORDER));
    }

    @Test
    public void testGetPropertyStatus() {
        ImmutableTree orderable = immutable.getChild("orderable");
        assertSame(Tree.Status.UNCHANGED, orderable.getPropertyStatus(TreeConstants.OAK_CHILD_ORDER));
    }

    @Test
    public void testGetProperties() {
        ImmutableTree orderable = immutable.getChild("orderable");
        List<String> propNames = Lists.newArrayList(TreeConstants.OAK_CHILD_ORDER, JcrConstants.JCR_PRIMARYTYPE);

        for (PropertyState ps : orderable.getProperties()) {
            assertTrue(propNames.remove(ps.getName()));
        }
        assertEquals(2, orderable.getPropertyCount());
    }

    @Test
    public void testGetPropertyCount() {
        ImmutableTree orderable = immutable.getChild("orderable");
        assertEquals(2, orderable.getPropertyCount());
    }

    @Test
    public void orderBefore() throws Exception {
        Tree t = root.getTree("/x/y/z");
        NodeUtil n = new NodeUtil(t);
        n.addChild("node1", JcrConstants.NT_UNSTRUCTURED);
        n.addChild("node2", JcrConstants.NT_UNSTRUCTURED);
        n.addChild("node3", JcrConstants.NT_UNSTRUCTURED);

        t.getChild("node1").orderBefore("node2");
        t.getChild("node3").orderBefore(null);

        root.commit();

        ImmutableTree tree = new ImmutableTree(((AbstractTree) t).getNodeState());
        assertSequence(tree.getChildren(), "node1", "node2", "node3");

        t.getChild("node3").orderBefore("node2");
        root.commit();

        tree = new ImmutableTree(((AbstractTree) t).getNodeState());
        assertSequence(tree.getChildren(), "node1", "node3", "node2");

        t.getChild("node1").orderBefore(null);
        root.commit();

        tree = new ImmutableTree(((AbstractTree) t).getNodeState());
        assertSequence(tree.getChildren(), "node3", "node2", "node1");
    }

    private static ImmutableTree getHiddenTree(@Nonnull ImmutableTree immutable) {
        return (ImmutableTree) TreeUtil.getTree(immutable, HIDDEN_PATH);
    }

    private static void assertSequence(Iterable<Tree> trees, String... names) {
        List<String> actual = Lists.newArrayList(Iterables.transform(trees, new Function<Tree, String>() {
            @Nullable
            @Override
            public String apply(Tree input) {
                return input.getName();
            }
        }));
        assertEquals(Lists.newArrayList(names), actual);
    }

    @Test
    public void testSetType() {
        assertNull(immutable.getType());

        immutable.setType(TreeType.VERSION);
        assertSame(TreeType.VERSION, immutable.getType());

        immutable.setType(TreeType.DEFAULT);
        assertSame(TreeType.DEFAULT, immutable.getType());
    }

    @Test
    public void testGetTypeForImmutableTree() {
        TreeTypeProvider typeProvider = new TreeTypeProvider(getConfig(AuthorizationConfiguration.class).getContext());
        for (String path : new String[] {"/", "/testPath"}) {
            Tree t = getRootProvider().createReadOnlyRoot(root).getTree(path);
            assertEquals(TreeType.DEFAULT, typeProvider.getType(t));
            // also for repeated calls
            assertEquals(TreeType.DEFAULT, typeProvider.getType(t));

            // the type of an immutable tree is set after the first call irrespective of the passed parent type.
            assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.DEFAULT));
            assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.HIDDEN));
        }
    }

    @Test
    public void testGetTypeForImmutableTreeWithParent() {
        TreeTypeProvider typeProvider = new TreeTypeProvider(getConfig(AuthorizationConfiguration.class).getContext());

        Tree t = getRootProvider().createReadOnlyRoot(root).getTree("/:hidden/testPath");
        assertEquals(TreeType.HIDDEN, typeProvider.getType(t, TreeType.HIDDEN));

        // the type of an immutable tree is set after the first call irrespective of the passed parent type.
        assertEquals(TreeType.HIDDEN, typeProvider.getType(t));
        assertEquals(TreeType.HIDDEN, typeProvider.getType(t, TreeType.DEFAULT));
        assertEquals(TreeType.HIDDEN, typeProvider.getType(t, TreeType.ACCESS_CONTROL));
        assertEquals(TreeType.HIDDEN, typeProvider.getType(t, TreeType.VERSION));
    }
}
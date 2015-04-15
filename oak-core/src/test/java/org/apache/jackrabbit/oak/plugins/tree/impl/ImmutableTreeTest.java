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

import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.NodeStoreFixture;
import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.OakAssert.assertSequence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;


public class ImmutableTreeTest extends OakBaseTest {

    private Root root;
    private ImmutableTree immutable;

    public ImmutableTreeTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setUp() throws CommitFailedException {
        ContentSession session = createContentSession();

        // Add test content
        root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        Tree x = tree.addChild("x");
        Tree y = x.addChild("y");
        y.addChild("z");
        Tree orderable = tree.addChild("orderable");
        orderable.setOrderableChildren(true);
        orderable.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        NodeBuilder nb = store.getRoot().builder();
        nb.child(":hidden");
        store.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // Acquire a fresh new root to avoid problems from lingering state
        root = session.getLatestRoot();
        Tree mutableTree = root.getTree("/");

        immutable = new ImmutableTree(((AbstractTree) mutableTree).getNodeState());
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
        assertEquals(":hidden", immutable.getChild(":hidden").getName());
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
        ImmutableTree hidden = immutable.getChild(":hidden");
        assertTrue(hidden.exists());
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
        assertSame(Tree.Status.UNCHANGED, immutable.getChild(":hidden").getStatus());
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
        assertTrue(immutable.hasChild(":hidden"));
    }

    @Test
    public void testGetHiddenNode() {
        ImmutableTree hidden = immutable.getChild(":hidden");
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

        t.addChild("node1");
        t.addChild("node2");
        t.addChild("node3");


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
}

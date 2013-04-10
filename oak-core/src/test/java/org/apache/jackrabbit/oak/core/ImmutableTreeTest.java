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
package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ImmutableTreeTest extends OakBaseTest {

    private Root root;

    @Before
    public void setUp() throws CommitFailedException {
        ContentSession session = createContentSession();

        // Add test content
        root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        Tree x = tree.addChild("x");
        Tree y = x.addChild("y");
        Tree z = y.addChild("z");
        root.commit();

        // Acquire a fresh new root to avoid problems from lingering state
        root = session.getLatestRoot();
    }

    @After
    public void tearDown() {
        root = null;
    }

    @Test
    public void testGetPath() {
        TreeImpl tree = (TreeImpl) root.getTree("/");

        ImmutableTree immutable = new ImmutableTree(tree.getNodeState());
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
        ImmutableTree tree = ImmutableTree.createFromRoot(root, TreeTypeProvider.EMPTY);
        assertNotNull(tree.getNodeState());

        for (Tree child : tree.getChildren()) {
            assertTrue(child instanceof ImmutableTree);
            assertNotNull(((ImmutableTree) child).getNodeState());
        }
    }

    @Test
    public void testRoot() {
        ImmutableTree tree = ImmutableTree.createFromRoot(root, TreeTypeProvider.EMPTY);
        assertTrue(tree.isRoot());
        assertNull(tree.getParent());
        assertEquals("", tree.getName());
        assertEquals(TreeTypeProvider.TYPE_DEFAULT, tree.getType());
    }

    @Test
    public void testGetParent() {
        ImmutableTree tree = ImmutableTree.createFromRoot(root, TreeTypeProvider.EMPTY);
        assertNull(tree.getParent());

        ImmutableTree child = tree.getChild("x");
        assertNotNull(child.getParent());
        assertEquals("/", child.getParent().getPath());

        ImmutableTree disconnected = new ImmutableTree(ImmutableTree.ParentProvider.UNSUPPORTED, child.getName(), child.getNodeState(), TreeTypeProvider.EMPTY);
        try {
            disconnected.getParent();
        } catch (UnsupportedOperationException e) {
            // success
        }
    }
}

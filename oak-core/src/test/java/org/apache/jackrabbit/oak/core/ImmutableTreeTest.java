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

import org.apache.jackrabbit.oak.NodeStoreFixture;
import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.OakAssert.assertSequence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class ImmutableTreeTest extends OakBaseTest {

    private Root root;
    private MutableTree mutableTree;

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
        Tree z = y.addChild("z");
        root.commit();

        // Acquire a fresh new root to avoid problems from lingering state
        root = session.getLatestRoot();
        mutableTree = (MutableTree) root.getTree("/");
    }

    @After
    public void tearDown() {
        root = null;
    }

    @Test
    public void testGetPath() {
        ImmutableTree immutable = new ImmutableTree(mutableTree.getNodeState());
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
        ImmutableTree tree = new ImmutableTree(mutableTree.getNodeState());
        assertNotNull(tree.getNodeState());

        for (Tree child : tree.getChildren()) {
            assertTrue(child instanceof ImmutableTree);
            assertNotNull(((ImmutableTree) child).getNodeState());
        }
    }

    @Test
    public void testRoot() {
        ImmutableTree tree = new ImmutableTree(mutableTree.getNodeState());
        assertTrue(tree.isRoot());
        try {
            tree.getParent();
            fail();
        }
        catch (IllegalStateException expected) { }
        assertEquals("", tree.getName());
    }

    @Test
    public void testGetParent() {
        ImmutableTree tree = new ImmutableTree(mutableTree.getNodeState());
        try {
            tree.getParent();
            fail();
        }
        catch (IllegalStateException expected) { }

        ImmutableTree child = tree.getChild("x");
        assertNotNull(child.getParent());
        assertEquals("/", child.getParent().getPath());

        ImmutableTree disconnected = new ImmutableTree(ImmutableTree.ParentProvider.UNSUPPORTED, child.getName(), child.getNodeState());
        try {
            disconnected.getParent();
        } catch (UnsupportedOperationException e) {
            // success
        }
    }
    
    @Test
    public void orderBefore() throws Exception {
    	 MutableTree t = (MutableTree) root.getTree("/x/y/z");
   
         t.addChild("node1");
         t.addChild("node2");
         t.addChild("node3");
        
         
         t.getChild("node1").orderBefore("node2");
         t.getChild("node3").orderBefore(null);
         
         root.commit();
         
         ImmutableTree tree = new ImmutableTree(t.getNodeState());
         assertSequence(tree.getChildren(), "node1", "node2", "node3");
 
         t.getChild("node3").orderBefore("node2");         
         root.commit();        
         
         tree = new ImmutableTree(t.getNodeState());
         assertSequence(tree.getChildren(), "node1", "node3", "node2");         
         
         t.getChild("node1").orderBefore(null);
         root.commit();
         
         tree = new ImmutableTree(t.getNodeState());
         assertSequence(tree.getChildren(), "node3", "node2", "node1");
    }
}

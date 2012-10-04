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

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RootImplTest extends AbstractCoreTest {

    @Override
    protected NodeState createInitialState(MicroKernel microKernel) {
        String jsop = "^\"a\":1 ^\"b\":2 ^\"c\":3 +\"x\":{\"xx\":{},\"xa\":\"value\"} +\"y\":{} +\"z\":{}";
        microKernel.commit("/", jsop, microKernel.getHeadRevision(), "test data");
        return store.getRoot();
    }

    @Test
    public void getTree() {
        RootImpl root = createRootImpl(null);

        List<String> validPaths = new ArrayList<String>();
        validPaths.add("/");
        validPaths.add("/x");
        validPaths.add("/x/xx");
        validPaths.add("/y");
        validPaths.add("/z");

        for (String treePath : validPaths) {
            Tree tree = root.getTree(treePath);
            assertNotNull(tree);
            assertEquals(treePath, tree.getPath());
        }

        List<String> invalidPaths = new ArrayList<String>();
        invalidPaths.add("/any");
        invalidPaths.add("/x/any");

        for (String treePath : invalidPaths) {
            assertNull(root.getTree(treePath));
        }
    }

    @Test
    public void move() throws CommitFailedException {
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");

        Tree y = tree.getChild("y");

        assertTrue(tree.hasChild("x"));
        root.move("/x", "/y/xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));
        
        root.commit();
        tree = root.getTree("/");

        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChild("y").hasChild("xx"));
    }

    @Test
    public void move2() {
        RootImpl root = createRootImpl(null);
        Tree r = root.getTree("/");
        Tree x = r.getChild("x");
        Tree y = r.getChild("y");

        assertFalse(y.hasChild("x"));
        assertEquals("", x.getParent().getName());
        root.move("/x", "/y/x");
        assertTrue(y.hasChild("x"));
        assertEquals("y", x.getParent().getName());
    }

    @Test
    /**
     * Regression test for OAK-208
     */
    public void removeMoved() throws CommitFailedException {
        RootImpl root = createRootImpl(null);
        Tree r = root.getTree("/");
        r.addChild("a");
        r.addChild("b");

        root.move("/a", "/b/c");
        assertFalse(r.hasChild("a"));
        assertTrue(r.hasChild("b"));

        r.getChild("b").remove();
        assertFalse(r.hasChild("a"));
        assertFalse(r.hasChild("b"));

        root.commit();
        assertFalse(r.hasChild("a"));
        assertFalse(r.hasChild("b"));
    }

    @Test
    public void rename() throws CommitFailedException {
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        root.move("/x", "/xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));
        
        root.commit();
        tree = root.getTree("/");

        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));
    }

    @Test
    public void copy() throws CommitFailedException {
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");

        Tree y = tree.getChild("y");

        assertTrue(tree.hasChild("x"));
        root.copy("/x", "/y/xx");
        assertTrue(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));
        
        root.commit();
        tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChild("y").hasChild("xx"));
    }

    @Test
    public void deepCopy() throws CommitFailedException {
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");

        Tree y = tree.getChild("y");

        root.getTree("/x").addChild("x1");
        root.copy("/x", "/y/xx");
        assertTrue(y.hasChild("xx"));
        assertTrue(y.getChild("xx").hasChild("x1"));

        root.commit();
        tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChild("y").hasChild("xx"));
        assertTrue(tree.getChild("y").getChild("xx").hasChild("x1"));

        Tree x = tree.getChild("x");
        Tree xx = tree.getChild("y").getChild("xx");
        checkEqual(x, xx);
    }

    @Test
    public void rebase() throws CommitFailedException {
        RootImpl root1 = createRootImpl(null);
        RootImpl root2 = createRootImpl(null);

        checkEqual(root1.getTree("/"), root2.getTree("/"));

        root2.getTree("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.rebase();
        checkEqual(root1.getTree("/"), (root2.getTree("/")));

        Tree one = root2.getTree("/one");
        one.getChild("two").remove();
        one.addChild("four");
        root2.commit();

        root1.rebase();
        checkEqual(root1.getTree("/"), (root2.getTree("/")));
    }

    private static void checkEqual(Tree tree1, Tree tree2) {
        assertEquals(tree1.getChildrenCount(), tree2.getChildrenCount());
        assertEquals(tree1.getPropertyCount(), tree2.getPropertyCount());

        for (PropertyState property1 : tree1.getProperties()) {
            assertEquals(property1, tree2.getProperty(property1.getName()));
        }

        for (Tree child1 : tree1.getChildren()) {
            checkEqual(child1, tree2.getChild(child1.getName()));
        }
    }
}

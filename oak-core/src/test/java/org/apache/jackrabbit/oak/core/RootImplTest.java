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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RootImplTest {

    private ContentSession session;

    @Before
    public void setUp() throws CommitFailedException {
        session = new Oak().createContentSession();

        // Add test content
        Root root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        tree.setProperty("a", 1);
        tree.setProperty("b", 2);
        tree.setProperty("c", 3);
        Tree x = tree.addChild("x");
        x.addChild("xx");
        x.setProperty("xa", "value");
        tree.addChild("y");
        tree.addChild("z");
        root.commit();
    }

    @After
    public void tearDown() {
        session = null;
    }

    @Test
    public void getTree() {
        Root root = session.getLatestRoot();

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
        Root root = session.getLatestRoot();
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

    /**
     * Regression test for OAK-208
     */
    @Test
    public void removeMoved() throws CommitFailedException {
        Root root = session.getLatestRoot();
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
        Root root = session.getLatestRoot();
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
        Root root = session.getLatestRoot();
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
        Root root = session.getLatestRoot();
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
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

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

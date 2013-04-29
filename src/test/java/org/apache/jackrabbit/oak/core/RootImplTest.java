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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.api.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RootImplTest extends OakBaseTest {

    private ContentSession session;

    @Before
    public void setUp() throws CommitFailedException {
        session = createContentSession();

        // Add test content
        Root root = session.getLatestRoot();
        Tree tree = root.getTreeOrNull("/");
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
            Tree tree = root.getTreeOrNull(treePath);
            assertNotNull(tree);
            assertEquals(treePath, tree.getPath());
        }

        List<String> invalidPaths = new ArrayList<String>();
        invalidPaths.add("/any");
        invalidPaths.add("/x/any");

        for (String treePath : invalidPaths) {
            assertNull(root.getTreeOrNull(treePath));
        }
    }

    @Test
    public void move() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree tree = root.getTreeOrNull("/");

        Tree y = tree.getChildOrNull("y");
        Tree x = tree.getChildOrNull("x");
        assertNotNull(x);

        root.move("/x", "/y/xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));
        assertEquals("/y/xx", x.getPath());

        root.commit();

        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChildOrNull("y").hasChild("xx"));
    }

    @Test
    public void moveRemoveAdd() {
        Root root = session.getLatestRoot();

        Tree x = root.getTreeOrNull("/x");
        Tree z = root.getTreeOrNull("/z");
        z.setProperty("p", "1");

        root.move("/z", "/x/z");
        root.getTreeOrNull("/x/z").remove();

        assertFalse(z.exists());

        x.addChild("z");
        assertEquals(Status.EXISTING, z.getStatus());

        x.getChildOrNull("z").setProperty("p", "2");
        PropertyState p = z.getProperty("p");
        assertNotNull(p);
        assertEquals("2", p.getValue(Type.STRING));
    }

    @Test
    public void moveNew() {
        Root root = session.getLatestRoot();
        Tree tree = root.getTreeOrNull("/");

        Tree t = tree.addChild("new");

        root.move("/new", "/y/new");
        assertEquals("/y/new", t.getPath());

        assertNull(tree.getChildOrNull("new"));
    }

    @Test
    public void moveExistingParent() throws CommitFailedException {
        Root root = session.getLatestRoot();
        root.getTreeOrNull("/").addChild("parent").addChild("new");
        root.commit();

        Tree parent = root.getTreeOrNull("/parent");
        Tree n = root.getTreeOrNull("/parent/new");

        root.move("/parent", "/moved");

        assertEquals(Status.EXISTING, parent.getStatus());
        assertEquals(Status.EXISTING, n.getStatus());

        assertEquals("/moved", parent.getPath());
        assertEquals("/moved/new", n.getPath());
    }

    /**
     * Regression test for OAK-208
     */
    @Test
    public void removeMoved() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree r = root.getTreeOrNull("/");
        r.addChild("a");
        r.addChild("b");

        root.move("/a", "/b/c");
        assertFalse(r.hasChild("a"));
        assertTrue(r.hasChild("b"));

        r.getChildOrNull("b").remove();
        assertFalse(r.hasChild("a"));
        assertFalse(r.hasChild("b"));

        root.commit();
        assertFalse(r.hasChild("a"));
        assertFalse(r.hasChild("b"));
    }

    @Test
    public void rename() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree tree = root.getTreeOrNull("/");
        Tree x = tree.getChildOrNull("x");
        assertNotNull(x);

        root.move("/x", "/xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));
        assertEquals("/xx", x.getPath());
        
        root.commit();

        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));
    }

    @Test
    public void copy() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree tree = root.getTreeOrNull("/");

        Tree y = tree.getChildOrNull("y");
        Tree x = tree.getChildOrNull("x");
        assertNotNull(x);

        assertTrue(tree.hasChild("x"));
        root.copy("/x", "/y/xx");
        assertTrue(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));
        
        root.commit();

        assertTrue(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChildOrNull("y").hasChild("xx"));
    }

    @Test
    public void deepCopy() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree tree = root.getTreeOrNull("/");

        Tree y = tree.getChildOrNull("y");

        root.getTreeOrNull("/x").addChild("x1");
        root.copy("/x", "/y/xx");
        assertTrue(y.hasChild("xx"));
        assertTrue(y.getChildOrNull("xx").hasChild("x1"));

        root.commit();

        assertTrue(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChildOrNull("y").hasChild("xx"));
        assertTrue(tree.getChildOrNull("y").getChildOrNull("xx").hasChild("x1"));

        Tree x = tree.getChildOrNull("x");
        Tree xx = tree.getChildOrNull("y").getChildOrNull("xx");
        checkEqual(x, xx);
    }

    @Test
    public void rebase() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTreeOrNull("/"), root2.getTreeOrNull("/"));

        root2.getTreeOrNull("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.rebase();
        checkEqual(root1.getTreeOrNull("/"), (root2.getTreeOrNull("/")));

        Tree one = root2.getTreeOrNull("/one");
        one.getChildOrNull("two").remove();
        one.addChild("four");
        root2.commit();

        root1.rebase();
        checkEqual(root1.getTreeOrNull("/"), (root2.getTreeOrNull("/")));
    }

    @Test
    public void rebaseWithAddNode() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTreeOrNull("/"), root2.getTreeOrNull("/"));

        root2.getTreeOrNull("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.getTreeOrNull("/").addChild("child");
        root1.rebase();

        root2.getTreeOrNull("/").addChild("child");
        checkEqual(root1.getTreeOrNull("/"), (root2.getTreeOrNull("/")));
    }

    @Test
    public void rebaseWithRemoveNode() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTreeOrNull("/"), root2.getTreeOrNull("/"));

        root2.getTreeOrNull("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.getTreeOrNull("/").getChildOrNull("x").remove();
        root1.rebase();

        root2.getTreeOrNull("/").getChildOrNull("x").remove();
        checkEqual(root1.getTreeOrNull("/"), (root2.getTreeOrNull("/")));
    }

    @Test
    public void rebaseWithAddProperty() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTreeOrNull("/"), root2.getTreeOrNull("/"));

        root2.getTreeOrNull("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.getTreeOrNull("/").setProperty("new", 42);
        root1.rebase();

        root2.getTreeOrNull("/").setProperty("new", 42);
        checkEqual(root1.getTreeOrNull("/"), (root2.getTreeOrNull("/")));
    }

    @Test
    public void rebaseWithRemoveProperty() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTreeOrNull("/"), root2.getTreeOrNull("/"));

        root2.getTreeOrNull("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.getTreeOrNull("/").removeProperty("a");
        root1.rebase();

        root2.getTreeOrNull("/").removeProperty("a");
        checkEqual(root1.getTreeOrNull("/"), (root2.getTreeOrNull("/")));
    }

    @Test
    public void rebaseWithSetProperty() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTreeOrNull("/"), root2.getTreeOrNull("/"));

        root2.getTreeOrNull("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.getTreeOrNull("/").setProperty("a", 42);
        root1.rebase();

        root2.getTreeOrNull("/").setProperty("a", 42);
        checkEqual(root1.getTreeOrNull("/"), (root2.getTreeOrNull("/")));
    }

    @Test
    public void rebaseWithMove() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTreeOrNull("/"), root2.getTreeOrNull("/"));

        root2.getTreeOrNull("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.move("/x", "/y/x-moved");
        root1.rebase();

        root2.move("/x", "/y/x-moved");
        checkEqual(root1.getTreeOrNull("/"), (root2.getTreeOrNull("/")));
    }

    @Test
    public void rebaseWithCopy() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTreeOrNull("/"), root2.getTreeOrNull("/"));

        root2.getTreeOrNull("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.copy("/x", "/y/x-copied");
        root1.rebase();

        root2.copy("/x", "/y/x-copied");
        checkEqual(root1.getTreeOrNull("/"), (root2.getTreeOrNull("/")));
    }

    private static void checkEqual(Tree tree1, Tree tree2) {
        assertEquals(tree1.getChildrenCount(), tree2.getChildrenCount());
        assertEquals(tree1.getPropertyCount(), tree2.getPropertyCount());

        for (PropertyState property1 : tree1.getProperties()) {
            assertEquals(property1, tree2.getProperty(property1.getName()));
        }

        for (Tree child1 : tree1.getChildren()) {
            checkEqual(child1, tree2.getChildOrNull(child1.getName()));
        }
    }
}

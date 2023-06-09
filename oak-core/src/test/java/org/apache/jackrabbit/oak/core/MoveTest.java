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
package org.apache.jackrabbit.oak.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.jackrabbit.oak.api.Tree.Status.NEW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

/**
 * Test methods (mostly) copied from oak-it RootTest and parameterized to run
 * with {@link MutableRoot.ClassicMove} and {@link MutableRoot.NeoMove}.
 */
@RunWith(Parameterized.class)
public class MoveTest {

    private final boolean classicMove;

    private ContentSession session;

    @Parameterized.Parameters(name="Classic Move: ({0})")
    public static List<Boolean> classicMove() {
        return Arrays.asList(true, false);
    }

    public MoveTest(boolean classicMove) {
        this.classicMove = classicMove;
    }

    @Before
    public void setUp() throws Exception {
        Oak oak = new Oak().with(new OpenSecurityProvider());
        Whiteboard whiteboard = oak.getWhiteboard();
        session = oak.createContentSession();
        if (classicMove) {
            Tracker<FeatureToggle> toggleTracker = whiteboard.track(FeatureToggle.class);
            for (FeatureToggle ft : toggleTracker.getServices()) {
                // enable classic move implementation
                if ("FT_CLASSIC_MOVE_OAK-10147".equals(ft.getName())) {
                    ft.setEnabled(true);
                }
            }
        }
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
            assertTrue(tree.exists());
            assertEquals(treePath, tree.getPath());
        }

        List<String> invalidPaths = new ArrayList<String>();
        invalidPaths.add("/any");
        invalidPaths.add("/x/any");

        for (String treePath : invalidPaths) {
            assertFalse(root.getTree(treePath).exists());
        }
    }

    @Test
    public void move() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree tree = root.getTree("/");

        Tree y = tree.getChild("y");
        Tree x = tree.getChild("x");
        assertTrue(x.exists());

        assertFalse(root.hasPendingChanges());
        root.move("/x", "/y/xx");
        assertTrue(root.hasPendingChanges());
        assertFalse(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));
        assertEquals("/y/xx", x.getPath());

        root.commit();
        assertFalse(root.hasPendingChanges());

        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChild("y").hasChild("xx"));
    }

    @Test
    public void moveRemoveAdd() {
        Root root = session.getLatestRoot();

        Tree x = root.getTree("/x");
        Tree z = root.getTree("/z");
        z.setProperty("p", "1");

        root.move("/z", "/x/z");
        root.getTree("/x/z").remove();

        assertFalse(z.exists());

        x.addChild("z");
        assertEquals(Tree.Status.NEW, z.getStatus());

        x.getChild("z").setProperty("p", "2");
        PropertyState p = z.getProperty("p");
        assertNotNull(p);
        assertEquals("2", p.getValue(Type.STRING));
    }

    @Test
    public void moveNew() {
        Root root = session.getLatestRoot();
        Tree tree = root.getTree("/");

        Tree t = tree.addChild("new");

        root.move("/new", "/y/new");
        assertEquals("/y/new", t.getPath());

        assertFalse(tree.getChild("new").exists());
    }

    @Test
    public void moveExistingParent() throws CommitFailedException {
        Root root = session.getLatestRoot();
        root.getTree("/").addChild("parent").addChild("new");
        root.commit();

        Tree parent = root.getTree("/parent");
        Tree n = root.getTree("/parent/new");

        root.move("/parent", "/moved");

        assertEquals(Tree.Status.NEW, parent.getStatus());
        assertEquals(Tree.Status.NEW, n.getStatus());

        assertEquals("/moved", parent.getPath());
        assertEquals("/moved/new", n.getPath());
    }

    @Test
    public void moveToSelf() throws CommitFailedException {
        Root root = session.getLatestRoot();
        root.getTree("/").addChild("s");
        root.commit();

        assertTrue(root.move("/s", "/s"));
    }

    @Test
    public void moveToDescendant() throws CommitFailedException {
        Root root = session.getLatestRoot();
        root.getTree("/").addChild("s");
        root.commit();

        assertFalse(root.move("/s", "/s/t"));
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
    public void moveMoved() throws CommitFailedException {
        assumeFalse("Known issue with classic move implementation", classicMove);
        Root root = session.getLatestRoot();
        Tree xx = root.getTree("/x/xx");

        assertTrue(root.move("/x/xx", "/y/xx"));
        assertTrue(root.move("/y", "/a"));

        assertTrue(xx.exists());
        assertEquals("/a/xx", xx.getPath());

        root.commit();
        assertTrue(xx.exists());
        assertEquals("/a/xx", xx.getPath());
    }

    @Test
    public void moveMovedMoved() throws CommitFailedException {
        assumeFalse("Known issue with classic move implementation", classicMove);
        Root root = session.getLatestRoot();
        Tree xx = root.getTree("/x/xx");

        assertTrue(root.move("/x/xx", "/y/xx"));
        assertTrue(root.move("/y", "/a"));
        assertTrue(root.move("/a", "/z/a"));

        assertTrue(xx.exists());
        assertEquals("/z/a/xx", xx.getPath());

        root.commit();
        assertTrue(xx.exists());
        assertEquals("/z/a/xx", xx.getPath());
    }

    @Test
    public void rename() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        Tree x = tree.getChild("x");
        assertTrue(x.exists());

        root.move("/x", "/xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));
        assertEquals("/xx", x.getPath());

        root.commit();

        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));
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

    @Test
    public void rebasePreservesStatus() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        Tree x = root1.getTree("/x");
        Tree added = x.addChild("added");
        assertEquals(NEW, added.getStatus());

        root2.getTree("/x").addChild("bar");
        root2.commit();

        root1.rebase();

        assertTrue(x.hasChild("added"));
        assertEquals(NEW, x.getChild("added").getStatus());
        assertTrue(x.hasChild("bar"));
    }

    @Test
    public void purgePreservesStatus() {
        Tree x = session.getLatestRoot().getTree("/x");
        Tree added = x.addChild("added");

        for (int k = 0; k < 10000; k++) {
            assertEquals("k=" + k, NEW, x.getChild("added").getStatus());
            x.addChild("k" + k);
        }
    }

    @Test
    public void rebaseWithAddNode() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTree("/"), root2.getTree("/"));

        root2.getTree("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.getTree("/").addChild("child");
        root1.rebase();

        root2.getTree("/").addChild("child");
        checkEqual(root1.getTree("/"), (root2.getTree("/")));
    }

    @Test
    public void rebaseWithRemoveNode() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTree("/"), root2.getTree("/"));

        root2.getTree("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.getTree("/").getChild("x").remove();
        root1.rebase();

        root2.getTree("/").getChild("x").remove();
        checkEqual(root1.getTree("/"), (root2.getTree("/")));
    }

    @Test
    public void rebaseWithAddProperty() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTree("/"), root2.getTree("/"));

        root2.getTree("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.getTree("/").setProperty("new", 42);
        root1.rebase();

        root2.getTree("/").setProperty("new", 42);
        checkEqual(root1.getTree("/"), (root2.getTree("/")));
    }

    @Test
    public void rebaseWithRemoveProperty() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTree("/"), root2.getTree("/"));

        root2.getTree("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.getTree("/").removeProperty("a");
        root1.rebase();

        root2.getTree("/").removeProperty("a");
        checkEqual(root1.getTree("/"), (root2.getTree("/")));
    }

    @Test
    public void rebaseWithSetProperty() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTree("/"), root2.getTree("/"));

        root2.getTree("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.getTree("/").setProperty("a", 42);
        root1.rebase();

        root2.getTree("/").setProperty("a", 42);
        checkEqual(root1.getTree("/"), (root2.getTree("/")));
    }

    @Test
    public void rebaseWithMove() throws CommitFailedException {
        Root root1 = session.getLatestRoot();
        Root root2 = session.getLatestRoot();

        checkEqual(root1.getTree("/"), root2.getTree("/"));

        root2.getTree("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", "V1");
        root2.commit();

        root1.move("/x", "/y/x-moved");
        root1.rebase();

        root2.move("/x", "/y/x-moved");
        checkEqual(root1.getTree("/"), (root2.getTree("/")));
    }

    @Test
    public void oak962() throws CommitFailedException {
        Root root = session.getLatestRoot();
        Tree r = root.getTree("/").addChild("root");
        r.addChild("N3");
        r.addChild("N6");
        r.getChild("N6").addChild("N7");
        root.commit();
        root.move("/root/N6/N7", "/root/N3/N12");
        r.getChild("N3").getChild("N12").remove();
        r.getChild("N6").remove();
        root.commit();
    }

    @Test
    public void nonExistentSource() {
        Root root = session.getLatestRoot();
        assertFalse(root.move("/d", "/e"));
    }

    @Test
    public void nonExistentDestinationParent() {
        Root root = session.getLatestRoot();
        assertFalse(root.move("/x", "/q/y"));
    }

    @Test
    public void destinationExists() {
        Root root = session.getLatestRoot();
        assertFalse(root.move("/x", "/y"));
    }

    @Test
    public void moveOne() throws Exception {
        Root root = session.getLatestRoot();
        Tree xx = root.getTree("/x/xx");
        assertTrue(root.move("/x/xx", "/y/xx"));
        assertEquals("/y/xx", xx.getPath());
        root.commit();
        assertEquals("/y/xx", xx.getPath());
    }

    @Test
    public void moveSubTree() throws Exception {
        Root root = session.getLatestRoot();
        Tree xx = root.getTree("/x/xx");
        assertTrue(root.move("/x", "/y/x"));
        assertEquals("/y/x/xx", xx.getPath());
        root.commit();
        assertEquals("/y/x/xx", xx.getPath());
    }

    @Test
    public void moveBack() throws Exception {
        Root root = session.getLatestRoot();
        Tree xx = root.getTree("/x/xx");
        assertTrue(root.move("/x/xx", "/y/q"));
        assertTrue(root.move("/y/q", "/x/xx"));
        assertEquals("/x/xx", xx.getPath());
        root.commit();
        assertEquals("/x/xx", xx.getPath());
    }

    @Test
    public void moveBackSubTree() throws Exception {
        Root root = session.getLatestRoot();
        Tree xx = root.getTree("/x/xx");
        assertTrue(root.move("/x", "/q"));
        assertTrue(root.move("/q", "/x"));
        assertEquals("/x/xx", xx.getPath());
        root.commit();
        assertEquals("/x/xx", xx.getPath());
    }

    @Test
    public void moveLoop() throws Exception {
        Root root = session.getLatestRoot();
        Tree xx = root.getTree("/x/xx");
        for (int i = 0; i < 5; i++) {
            assertTrue(root.move("/x/xx", "/y/q"));
            assertTrue(root.move("/y/q", "/x/xx"));
        }
        assertEquals("/x/xx", xx.getPath());
        root.commit();
        assertEquals("/x/xx", xx.getPath());
    }

    @Test
    public void moveLoopWithCommit() throws Exception {
        Root root = session.getLatestRoot();
        Tree xx = root.getTree("/x/xx");
        for (int i = 0; i < 5; i++) {
            assertTrue(root.move("/x/xx", "/y/q"));
            root.commit();
            assertTrue(root.move("/y/q", "/x/xx"));
        }
        assertEquals("/x/xx", xx.getPath());
        root.commit();
        assertEquals("/x/xx", xx.getPath());
    }

    private static void checkEqual(Tree tree1, Tree tree2) {
        assertEquals(tree1.getChildrenCount(Long.MAX_VALUE), tree2.getChildrenCount(Long.MAX_VALUE));
        assertEquals(tree1.getPropertyCount(), tree2.getPropertyCount());

        for (PropertyState property1 : tree1.getProperties()) {
            assertEquals(property1, tree2.getProperty(property1.getName()));
        }

        for (Tree child1 : tree1.getChildren()) {
            checkEqual(child1, tree2.getChild(child1.getName()));
        }
    }
}

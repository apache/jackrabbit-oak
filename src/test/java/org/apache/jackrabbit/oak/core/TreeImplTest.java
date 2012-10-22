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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * TreeImplTest...
 */
public class TreeImplTest {

    private Root root;

    @Before
    public void setUp() throws CommitFailedException {
        ContentSession session = new Oak().createContentSession();

        // Add test content
        root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        tree.setProperty("a", 1);
        tree.setProperty("b", 2);
        tree.setProperty("c", 3);
        tree.addChild("x");
        tree.addChild("y");
        tree.addChild("z");
        root.commit();

        // Acquire a fresh new root to avoid problems from lingering state
        root = session.getLatestRoot();
    }

    @After
    public void tearDown() {
        root = null;
    }

    @Test
    public void getChild() {
        Tree tree = root.getTree("/");

        Tree child = tree.getChild("any");
        assertNull(child);

        child = tree.getChild("x");
        assertNotNull(child);
    }

    @Test
    public void getProperty() {
        Tree tree = root.getTree("/");

        PropertyState propertyState = tree.getProperty("any");
        assertNull(propertyState);

        propertyState = tree.getProperty("a");
        assertNotNull(propertyState);
        assertFalse(propertyState.isArray());
        assertEquals(LONG, propertyState.getType());
        assertEquals(1, (long) propertyState.getValue(LONG));
    }

    @Test
    public void getChildren() {
        Tree tree = root.getTree("/");

        Iterable<Tree> children = tree.getChildren();

        Set<String> expectedPaths = new HashSet<String>();
        Collections.addAll(expectedPaths, "/x", "/y", "/z");

        for (Tree child : children) {
            assertTrue(expectedPaths.remove(child.getPath()));
        }
        assertTrue(expectedPaths.isEmpty());

        assertEquals(3, tree.getChildrenCount());
    }

    @Test
    public void getProperties() {
        Tree tree = root.getTree("/");

        Set<PropertyState> expectedProperties = Sets.newHashSet(
                LongPropertyState.createLongProperty("a", 1L),
                LongPropertyState.createLongProperty("b", 2L),
                LongPropertyState.createLongProperty("c", 3L));

        Iterable<? extends PropertyState> properties = tree.getProperties();
        for (PropertyState property : properties) {
            assertTrue(expectedProperties.remove(property));
        }

        assertTrue(expectedProperties.isEmpty());
        assertEquals(3, tree.getPropertyCount());
    }

    @Test
    public void addChild() throws CommitFailedException {
        Tree tree = root.getTree("/");

        assertFalse(tree.hasChild("new"));
        Tree added = tree.addChild("new");
        assertNotNull(added);
        assertEquals("new", added.getName());
        assertTrue(tree.hasChild("new"));

        root.commit();
        tree = root.getTree("/");

        assertTrue(tree.hasChild("new"));

        tree.getChild("new").addChild("more");
        assertTrue(tree.getChild("new").hasChild("more"));
    }

    @Test
    public void addExistingChild() throws CommitFailedException {
        Tree tree = root.getTree("/");

        assertFalse(tree.hasChild("new"));
        tree.addChild("new");

        root.commit();
        tree = root.getTree("/");

        assertTrue(tree.hasChild("new"));
        Tree added = tree.addChild("new");
        assertNotNull(added);
        assertEquals("new", added.getName());
    }

    @Test
    public void removeChild() throws CommitFailedException {
        Tree tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        tree.getChild("x").remove();
        assertFalse(tree.hasChild("x"));

        root.commit();
        tree = root.getTree("/");

        assertFalse(tree.hasChild("x"));
    }

    @Test
    public void setProperty() throws CommitFailedException {
        Tree tree = root.getTree("/");

        assertFalse(tree.hasProperty("new"));
        tree.setProperty("new", "value");
        PropertyState property = tree.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals("value", property.getValue(STRING));

        root.commit();
        tree = root.getTree("/");

        property = tree.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals("value", property.getValue(STRING));
    }

    @Test
    public void removeProperty() throws CommitFailedException {
        Tree tree = root.getTree("/");

        assertTrue(tree.hasProperty("a"));
        tree.removeProperty("a");
        assertFalse(tree.hasProperty("a"));

        root.commit();
        tree = root.getTree("/");

        assertFalse(tree.hasProperty("a"));
    }

    @Test
    public void getChildrenCount() {
        Tree tree = root.getTree("/");

        assertEquals(3, tree.getChildrenCount());

        tree.getChild("x").remove();
        assertEquals(2, tree.getChildrenCount());

        tree.addChild("a");
        assertEquals(3, tree.getChildrenCount());

        tree.addChild("x");
        assertEquals(4, tree.getChildrenCount());
    }

    @Test
    public void getPropertyCount() {
        Tree tree = root.getTree("/");

        assertEquals(3, tree.getPropertyCount());

        tree.setProperty("a", "foo");
        assertEquals(3, tree.getPropertyCount());

        tree.removeProperty("a");
        assertEquals(2, tree.getPropertyCount());

        tree.setProperty("x", "foo");
        assertEquals(3, tree.getPropertyCount());

        tree.setProperty("a", "foo");
        assertEquals(4, tree.getPropertyCount());
    }

    @Test
    public void addAndRemoveProperty() throws CommitFailedException {
        Tree tree = root.getTree("/");

        tree.setProperty("P0", "V1");
        root.commit();
        tree = root.getTree("/");
        assertTrue(tree.hasProperty("P0"));

        tree.removeProperty("P0");
        root.commit();
        tree = root.getTree("/");
        assertFalse(tree.hasProperty("P0"));
    }

    @Test
    public void nodeStatus() throws CommitFailedException {
        Tree tree = root.getTree("/");

        tree.addChild("new");
        assertEquals(Tree.Status.NEW, tree.getChild("new").getStatus());
        root.commit();

        tree = root.getTree("/");
        assertEquals(Tree.Status.EXISTING, tree.getChild("new").getStatus());
        Tree added = tree.getChild("new");
        added.addChild("another");
        assertEquals(Tree.Status.MODIFIED, tree.getChild("new").getStatus());
        root.commit();

        tree = root.getTree("/");
        assertEquals(Tree.Status.EXISTING, tree.getChild("new").getStatus());
        tree.getChild("new").getChild("another").remove();
        assertEquals(Tree.Status.MODIFIED, tree.getChild("new").getStatus());
        root.commit();

        tree = root.getTree("/");
        assertEquals(Tree.Status.EXISTING, tree.getChild("new").getStatus());
        assertNull(tree.getChild("new").getChild("another"));

        Tree x = root.getTree("/x");
        Tree y = x.addChild("y");
        x.remove();
        assertEquals(Status.REMOVED, x.getStatus());
        assertEquals(Status.REMOVED, y.getStatus());
    }

    @Test
    public void propertyStatus() throws CommitFailedException {
        Tree tree = root.getTree("/");

        tree.setProperty("new", "value1");
        assertEquals(Tree.Status.NEW, tree.getPropertyStatus("new"));
        root.commit();

        tree = root.getTree("/");
        assertEquals(Tree.Status.EXISTING, tree.getPropertyStatus("new"));
        tree.setProperty("new", "value2");
        assertEquals(Tree.Status.MODIFIED, tree.getPropertyStatus("new"));
        root.commit();

        tree = root.getTree("/");
        assertEquals(Tree.Status.EXISTING, tree.getPropertyStatus("new"));
        tree.removeProperty("new");
        assertEquals(Tree.Status.REMOVED, tree.getPropertyStatus("new"));
        root.commit();

        tree = root.getTree("/");
        assertNull(tree.getPropertyStatus("new"));

        Tree x = root.getTree("/x");
        x.setProperty("y", "value1");
        x.remove();
        assertEquals(Status.REMOVED, x.getPropertyStatus("y"));
    }

    @Test
    public void noTransitiveModifiedStatus() throws CommitFailedException {
        Tree tree = root.getTree("/");
        tree.addChild("one").addChild("two");
        root.commit();

        tree = root.getTree("/");
        tree.getChild("one").getChild("two").addChild("three");
        assertEquals(Tree.Status.EXISTING, tree.getChild("one").getStatus());
        assertEquals(Tree.Status.MODIFIED, tree.getChild("one").getChild("two").getStatus());
    }

    @Test
    public void largeChildList() throws CommitFailedException {
        Tree tree = root.getTree("/");

        Set<String> added = new HashSet<String>();

        tree.addChild("large");
        tree = tree.getChild("large");
        for (int c = 0; c < 10000; c++) {
            String name = "n" + c;
            added.add(name);
            tree.addChild(name);
        }

        root.commit();
        tree = root.getTree("/");
        tree = tree.getChild("large");

        for (Tree child : tree.getChildren()) {
            assertTrue(added.remove(child.getName()));
        }

        assertTrue(added.isEmpty());
    }
}
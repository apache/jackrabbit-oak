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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RootImplTest extends AbstractOakTest {

    @Override
    protected NodeState createInitialState(MicroKernel microKernel) {
        String jsop = "^\"a\":1 ^\"b\":2 ^\"c\":3 +\"x\":{} +\"y\":{} +\"z\":{}";
        microKernel.commit("/", jsop, microKernel.getHeadRevision(), "test data");
        return store.getRoot();
    }

    @Test
    public void getChild() {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        Tree child = tree.getChild("any");
        assertNull(child);

        child = tree.getChild("x");
        assertNotNull(child);
    }

    @Test
    public void getProperty() {
        RootImpl root = new RootImpl(store, "test");
        Tree tree = root.getTree("/");

        PropertyState propertyState = tree.getProperty("any");
        assertNull(propertyState);

        propertyState = tree.getProperty("a");
        assertNotNull(propertyState);
        assertFalse(propertyState.isArray());
        assertEquals(PropertyType.LONG, propertyState.getValue().getType());
        assertEquals(1, propertyState.getValue().getLong());
    }

    @Test
    public void getChildren() {
        RootImpl root = new RootImpl(store, "test");
        Tree tree = root.getTree("/");

        Iterable<Tree> children = tree.getChildren();

        Set<String> expectedPaths = new HashSet<String>();
        Collections.addAll(expectedPaths, "x", "y", "z");

        for (Tree child : children) {
            assertTrue(expectedPaths.remove(child.getPath()));
        }
        assertTrue(expectedPaths.isEmpty());

        assertEquals(3, tree.getChildrenCount());
    }

    @Test
    public void getProperties() {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        Map<String, CoreValue> expectedProperties = new HashMap<String, CoreValue>();
        expectedProperties.put("a", valueFactory.createValue(1));
        expectedProperties.put("b", valueFactory.createValue(2));
        expectedProperties.put("c", valueFactory.createValue(3));

        Iterable<? extends PropertyState> properties = tree.getProperties();
        for (PropertyState property : properties) {
            CoreValue value = expectedProperties.remove(property.getName());
            assertNotNull(value);
            assertFalse(property.isArray());
            assertEquals(value, property.getValue());
        }

        assertTrue(expectedProperties.isEmpty());

        assertEquals(3, tree.getPropertyCount());
    }

    @Test
    public void addChild() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        assertFalse(tree.hasChild("new"));
        Tree added = tree.addChild("new");
        assertNotNull(added);
        assertEquals("new", added.getName());
        assertTrue(tree.hasChild("new"));

        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");

        assertTrue(tree.hasChild("new"));

        tree.getChild("new").addChild("more");
        assertTrue(tree.getChild("new").hasChild("more"));
    }

    @Test
    public void addExistingChild() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        assertFalse(tree.hasChild("new"));
        tree.addChild("new");

        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");

        assertTrue(tree.hasChild("new"));
        Tree added = tree.addChild("new");
        assertNotNull(added);
        assertEquals("new", added.getName());
    }

    @Test
    public void removeChild() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        tree.removeChild("x");
        assertFalse(tree.hasChild("x"));

        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");
        
        assertFalse(tree.hasChild("x"));
    }

    @Test
    public void setProperty() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        assertFalse(tree.hasProperty("new"));
        CoreValue value = valueFactory.createValue("value");
        tree.setProperty("new", value);
        PropertyState property = tree.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals(value, property.getValue());

        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");
        
        property = tree.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals(value, property.getValue());
    }

    @Test
    public void removeProperty() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        assertTrue(tree.hasProperty("a"));
        tree.removeProperty("a");
        assertFalse(tree.hasProperty("a"));

        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");

        assertFalse(tree.hasProperty("a"));
    }

    @Test
    public void move() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        Tree y = tree.getChild("y");

        assertTrue(tree.hasChild("x"));
        root.move("x", "y/xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));
        
        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");

        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChild("y").hasChild("xx"));
    }

    @Test
    public void move2() {
        RootImpl root = new RootImpl(store, null);
        Tree r = root.getTree("");
        Tree x = r.getChild("x");
        Tree y = r.getChild("y");

        assertFalse(y.hasChild("x"));
        assertEquals("", x.getParent().getName());
        root.move("x", "y/x");
        assertTrue(y.hasChild("x"));
        assertEquals("y", x.getParent().getName());
    }

    @Test
    public void rename() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        root.move("x", "xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));
        
        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");

        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));
    }

    @Test
    public void copy() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        Tree y = tree.getChild("y");

        assertTrue(tree.hasChild("x"));
        root.copy("x", "y/xx");
        assertTrue(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));
        
        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        assertTrue(tree.hasChild("y"));
        assertTrue(tree.getChild("y").hasChild("xx"));
    }

    @Test
    public void deepCopy() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        Tree y = tree.getChild("y");

        root.getTree("x").addChild("x1");
        root.copy("x", "y/xx");
        assertTrue(y.hasChild("xx"));
        assertTrue(y.getChild("xx").hasChild("x1"));

        root.commit(DefaultConflictHandler.OURS);
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
    public void getChildrenCount() {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        assertEquals(3, tree.getChildrenCount());

        tree.removeChild("x");
        assertEquals(2, tree.getChildrenCount());

        tree.addChild("a");
        assertEquals(3, tree.getChildrenCount());

        tree.addChild("x");
        assertEquals(4, tree.getChildrenCount());
    }

    @Test
    public void getPropertyCount() {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        assertEquals(3, tree.getPropertyCount());

        CoreValue value = valueFactory.createValue("foo");
        tree.setProperty("a", value);
        assertEquals(3, tree.getPropertyCount());

        tree.removeProperty("a");
        assertEquals(2, tree.getPropertyCount());

        tree.setProperty("x", value);
        assertEquals(3, tree.getPropertyCount());

        tree.setProperty("a", value);
        assertEquals(4, tree.getPropertyCount());
    }

    @Test
    public void addAndRemoveProperty() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        tree.setProperty("P0", valueFactory.createValue("V1"));
        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");
        assertTrue(tree.hasProperty("P0"));

        tree.removeProperty("P0");
        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");
        assertFalse(tree.hasProperty("P0"));
    }

    @Test
    public void nodeStatus() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        tree.addChild("new");
        assertEquals(Status.NEW, tree.getChildStatus("new"));
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        assertEquals(Status.EXISTING, tree.getChildStatus("new"));
        Tree added = tree.getChild("new");
        added.addChild("another");
        assertEquals(Status.MODIFIED, tree.getChildStatus("new"));
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        assertEquals(Status.EXISTING, tree.getChildStatus("new"));
        tree.getChild("new").removeChild("another");
        assertEquals(Status.MODIFIED, tree.getChildStatus("new"));
        assertEquals(Status.REMOVED, tree.getChild("new").getChildStatus("another"));
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        assertEquals(Status.EXISTING, tree.getChildStatus("new"));
        assertNull(tree.getChild("new").getChild("another"));
        assertNull(tree.getChild("new").getChildStatus("another"));
    }

    @Test
    public void propertyStatus() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");
        CoreValue value1 = valueFactory.createValue("V1");
        CoreValue value2 = valueFactory.createValue("V2");

        tree.setProperty("new", value1);
        assertEquals(Status.NEW, tree.getPropertyStatus("new"));
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        assertEquals(Status.EXISTING, tree.getPropertyStatus("new"));
        tree.setProperty("new", value2);
        assertEquals(Status.MODIFIED, tree.getPropertyStatus("new"));
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        assertEquals(Status.EXISTING, tree.getPropertyStatus("new"));
        tree.removeProperty("new");
        assertEquals(Status.REMOVED, tree.getPropertyStatus("new"));
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        assertNull(tree.getPropertyStatus("new"));
    }

    @Test
    public void noTransitiveModifiedStatus() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");
        tree.addChild("one").addChild("two");
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        tree.getChild("one").getChild("two").addChild("three");
        assertEquals(Status.EXISTING, tree.getChildStatus("one"));
        assertEquals(Status.MODIFIED, tree.getChild("one").getChildStatus("two"));
    }

    @Test
    public void rebase() throws CommitFailedException {
        RootImpl root1 = new RootImpl(store, null);
        RootImpl root2 = new RootImpl(store, null);

        checkEqual(root1.getTree("/"), root2.getTree("/"));

        CoreValue value = valueFactory.createValue("V1");
        root2.getTree("/").addChild("one").addChild("two").addChild("three")
                .setProperty("p1", value);
        root2.commit(DefaultConflictHandler.OURS);

        root1.rebase(DefaultConflictHandler.OURS);
        checkEqual(root1.getTree("/"), (root2.getTree("/")));

        Tree one = root2.getTree("/one");
        one.removeChild("two");
        one.addChild("four");
        root2.commit(DefaultConflictHandler.OURS);

        root1.rebase(DefaultConflictHandler.OURS);
        checkEqual(root1.getTree("/"), (root2.getTree("/")));
    }

    @Test
    public void largeChildList() throws CommitFailedException {
        RootImpl root = new RootImpl(store, null);
        Tree tree = root.getTree("/");

        Set<String> added = new HashSet<String>();

        tree.addChild("large");
        tree = tree.getChild("large");
        for (int c = 0; c < 10000; c++) {
            String name = "n" + c;
            added.add(name);
            tree.addChild(name);
        }

        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");
        tree = tree.getChild("large");

        for (Tree child : tree.getChildren()) {
            assertTrue(added.remove(child.getName()));
        }

        assertTrue(added.isEmpty());
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

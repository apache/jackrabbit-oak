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

/**
 * TreeImplTest...
 */
public class TreeImplTest extends AbstractCoreTest {

    @Override
    protected NodeState createInitialState(MicroKernel microKernel) {
        String jsop = "^\"a\":1 ^\"b\":2 ^\"c\":3 +\"x\":{} +\"y\":{} +\"z\":{}";
        microKernel.commit("/", jsop, microKernel.getHeadRevision(), "test data");
        return store.getRoot();
    }


    @Test
    public void getChild() {
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");

        Tree child = tree.getChild("any");
        assertNull(child);

        child = tree.getChild("x");
        assertNotNull(child);
    }

    @Test
    public void getProperty() {
        RootImpl root = createRootImpl("test");
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
        RootImpl root = createRootImpl("test");
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
        RootImpl root = createRootImpl(null);
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
        RootImpl root = createRootImpl(null);
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
        RootImpl root = createRootImpl(null);
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
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        tree.getChild("x").remove();
        assertFalse(tree.hasChild("x"));

        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");

        assertFalse(tree.hasChild("x"));
    }

    @Test
    public void setProperty() throws CommitFailedException {
        RootImpl root = createRootImpl(null);
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
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");

        assertTrue(tree.hasProperty("a"));
        tree.removeProperty("a");
        assertFalse(tree.hasProperty("a"));

        root.commit(DefaultConflictHandler.OURS);
        tree = root.getTree("/");

        assertFalse(tree.hasProperty("a"));
    }

    @Test
    public void getChildrenCount() {
        RootImpl root = createRootImpl(null);
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
        RootImpl root = createRootImpl(null);
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
        RootImpl root = createRootImpl(null);
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
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");

        tree.addChild("new");
        assertEquals(Tree.Status.NEW, tree.getChild("new").getStatus());
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        assertEquals(Tree.Status.EXISTING, tree.getChild("new").getStatus());
        Tree added = tree.getChild("new");
        added.addChild("another");
        assertEquals(Tree.Status.MODIFIED, tree.getChild("new").getStatus());
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        assertEquals(Tree.Status.EXISTING, tree.getChild("new").getStatus());
        tree.getChild("new").getChild("another").remove();
        assertEquals(Tree.Status.MODIFIED, tree.getChild("new").getStatus());
        root.commit(DefaultConflictHandler.OURS);

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
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");
        CoreValue value1 = valueFactory.createValue("V1");
        CoreValue value2 = valueFactory.createValue("V2");

        tree.setProperty("new", value1);
        assertEquals(Tree.Status.NEW, tree.getPropertyStatus("new"));
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        assertEquals(Tree.Status.EXISTING, tree.getPropertyStatus("new"));
        tree.setProperty("new", value2);
        assertEquals(Tree.Status.MODIFIED, tree.getPropertyStatus("new"));
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        assertEquals(Tree.Status.EXISTING, tree.getPropertyStatus("new"));
        tree.removeProperty("new");
        assertEquals(Tree.Status.REMOVED, tree.getPropertyStatus("new"));
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        assertNull(tree.getPropertyStatus("new"));

        Tree x = root.getTree("/x");
        x.setProperty("y", value1);
        x.remove();
        assertEquals(Status.REMOVED, x.getPropertyStatus("y"));
    }

    @Test
    public void noTransitiveModifiedStatus() throws CommitFailedException {
        RootImpl root = createRootImpl(null);
        Tree tree = root.getTree("/");
        tree.addChild("one").addChild("two");
        root.commit(DefaultConflictHandler.OURS);

        tree = root.getTree("/");
        tree.getChild("one").getChild("two").addChild("three");
        assertEquals(Tree.Status.EXISTING, tree.getChild("one").getStatus());
        assertEquals(Tree.Status.MODIFIED, tree.getChild("one").getChild("two").getStatus());
    }

    @Test
    public void largeChildList() throws CommitFailedException {
        RootImpl root = createRootImpl(null);
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
}
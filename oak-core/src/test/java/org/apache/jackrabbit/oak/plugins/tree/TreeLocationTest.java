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
package org.apache.jackrabbit.oak.plugins.tree;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class TreeLocationTest {

    private Tree rootTree;
    private Tree nonExisting;
    private Root root;

    @Before
    public void setUp() throws Exception {

        rootTree = mockTree("/", null);
        when(rootTree.hasProperty("p")).thenReturn(true);
        when(rootTree.getProperty("p")).thenReturn(PropertyStates.createProperty("p", 1));

        nonExisting = Mockito.mock(Tree.class);
        when(nonExisting.exists()).thenReturn(false);
        when(nonExisting.getName()).thenReturn("nonExisting");

        Tree x = mockTree("/x", rootTree);
        Tree subTree = mockTree("/z", rootTree);
        Tree child = mockTree("/z/child", subTree);
        when(child.hasProperty("p")).thenReturn(true);
        when(child.getProperty("p")).thenReturn(PropertyStates.createProperty("p", "value"));

        when(subTree.getChild("child")).thenReturn(child);
        when(rootTree.getChild("z")).thenReturn(subTree);
        when(rootTree.getChild("x")).thenReturn(x);

        root = Mockito.mock(Root.class);
        when(root.getTree("/")).thenReturn(rootTree);
    }

    private Tree mockTree(String path, Tree parent) {
        Tree t = Mockito.mock(Tree.class);
        when(t.getPath()).thenReturn(path);
        when(t.getName()).thenReturn(PathUtils.getName(path));
        if (PathUtils.denotesRoot(path)) {
            when(t.getParent()).thenThrow(IllegalStateException.class);
            when(t.isRoot()).thenReturn(true);
        } else {
            when(t.getParent()).thenReturn(parent);
            when(t.isRoot()).thenReturn(false);
        }
        when(t.exists()).thenReturn(true);
        when(t.hasProperty("nonExisting")).thenReturn(false);
        when(t.hasChild("nonExisting")).thenReturn(false);
        when(t.getChild("nonExisting")).thenReturn(nonExisting);
        return t;
    }

    @Test
    public void testNullLocation() {
        TreeLocation nullLocation = TreeLocation.create(root).getParent();
        assertNull(nullLocation.getTree());
        assertTrue(nullLocation.getName().isEmpty());

        TreeLocation child = nullLocation.getChild("any");
        assertNotNull(child);
        assertNull(child.getTree());
        assertNull(child.getProperty());

        TreeLocation child2 = nullLocation.getChild("x");
        assertNotNull(child2);
        assertNull(child.getTree());
        assertNull(child.getProperty());
    }

    @Test
    public void testParentOfRoot() {
        TreeLocation nullLocation = TreeLocation.create(root).getParent();
        assertSame(nullLocation, nullLocation.getParent());
        assertTrue(nullLocation.getName().isEmpty());
    }

    @Test
    public void testNodeLocation() {
        TreeLocation x = TreeLocation.create(root, "/x");
        assertNotNull(x.getTree());

        assertEquals(rootTree, x.getParent().getTree());
    }

    @Test
    public void testPropertyLocation() {
        TreeLocation propLocation = TreeLocation.create(root, "/p");
        assertNotNull(propLocation.getProperty());

        TreeLocation abc = propLocation.getChild("b").getChild("c");
        assertEquals("/p/b/c", abc.getPath());
        assertNull(abc.getProperty());

        TreeLocation ab = abc.getParent();
        assertEquals("/p/b", ab.getPath());
        assertNull(ab.getProperty());

        assertEquals(propLocation.getProperty(), ab.getParent().getProperty());
    }

    @Test
    public void getDeepLocation() {
        TreeLocation child = TreeLocation.create(root, "/z/child");
        assertNotNull(child.getTree());
        assertNull(child.getProperty());

        TreeLocation p = TreeLocation.create(root, "/z/child/p");
        assertNotNull(p.getProperty());
        assertNull(p.getTree());
        assertEquals("/z/child/p", p.getPath());

        TreeLocation n = TreeLocation.create(root, "/z/child/p/3/4");
        assertNull(n.getTree());
        assertNull(n.getProperty());
        assertEquals("/z/child/p/3/4", n.getPath());

        TreeLocation t = n.getParent().getParent();
        assertNull(t.getTree());
        assertNotNull(t.getProperty());

        TreeLocation t2 = t.getParent();
        assertNotNull(t2.getTree());
        assertEquals("/z/child", t2.getPath());
    }
}
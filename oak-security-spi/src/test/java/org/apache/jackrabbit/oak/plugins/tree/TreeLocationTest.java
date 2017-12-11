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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TreeLocationTest extends AbstractTreeTest {

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
        assertEquals("p", propLocation.getName());
        assertTrue(propLocation.exists());

        TreeLocation abc = propLocation.getChild("b").getChild("c");
        assertEquals("/p/b/c", abc.getPath());
        assertNull(abc.getProperty());

        TreeLocation ab = abc.getParent();
        assertEquals("/p/b", ab.getPath());
        assertEquals("b", ab.getName());
        assertNull(ab.getProperty());

        assertEquals(propLocation.getProperty(), ab.getParent().getProperty());
    }

    @Test
    public void testRemovePropertyLocation() {
        TreeLocation propLocation = TreeLocation.create(root, "/p");
        assertTrue(propLocation.remove());
    }

    @Test
    public void getDeepLocation() {
        TreeLocation child = TreeLocation.create(root, "/z/child");
        assertNotNull(child.getTree());
        assertEquals("child", child.getName());
        assertNull(child.getProperty());

        TreeLocation p = TreeLocation.create(root, "/z/child/p");
        assertNotNull(p.getProperty());
        assertNull(p.getTree());
        assertEquals("/z/child/p", p.getPath());

        TreeLocation n = TreeLocation.create(root, "/z/child/p/3/4");
        assertNull(n.getTree());
        assertNull(n.getProperty());
        assertEquals("/z/child/p/3/4", n.getPath());
        assertFalse(n.exists());
        assertFalse(n.remove());

        TreeLocation t = n.getParent().getParent();
        assertNull(t.getTree());
        assertNotNull(t.getProperty());

        TreeLocation t2 = t.getParent();
        assertNotNull(t2.getTree());
        assertEquals("/z/child", t2.getPath());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveExisting() {
        TreeLocation child = TreeLocation.create(root, "/z/child");
        child.remove();
    }

    @Test
    public void testRemoveNonExisting() {
        TreeLocation nonExisting = TreeLocation.create(root, "/nonExisting");
        // remove must not throw as it doesn't exist
        assertFalse(nonExisting.remove());
    }

    @Test
    public void testNonExisting() {
        TreeLocation nonExisting = TreeLocation.create(root, "/nonExisting");
        assertNull(nonExisting.getTree());

        assertEquals("nonExisting", nonExisting.getName());
        assertEquals("/nonExisting", nonExisting.getPath());
    }
}
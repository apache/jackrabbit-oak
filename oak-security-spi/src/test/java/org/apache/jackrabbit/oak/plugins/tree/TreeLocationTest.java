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

import org.apache.jackrabbit.util.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TreeLocationTest extends AbstractTreeTest {

    private TreeLocation nodeLocation;

    @Before
    public void before() throws Exception {
        super.before();
        nodeLocation = TreeLocation.create(child);
    }

    @Test
    public void testExists() {
        assertTrue(nodeLocation.exists());
    }

    @Test
    public void testGetName() {
        assertEquals(Text.getName(CHILD_PATH), nodeLocation.getName());
    }

    @Test
    public void testGetPath() {
        assertEquals(CHILD_PATH, nodeLocation.getPath());
    }

    @Test
    public void testGetTree() {
        assertEquals(child, nodeLocation.getTree());
    }

    @Test
    public void testGetProperty() {
        assertNull(nodeLocation.getProperty());
    }

    @Test
    public void testGetParent() {
        assertEquals(z, nodeLocation.getParent().getTree());
        assertEquals(rootTree, nodeLocation.getParent().getParent().getTree());
    }

    @Test
    public void testGetChild() {
        TreeLocation propertyChild = nodeLocation.getChild(Text.getName(PROPERTY_PATH));
        assertNotNull(propertyChild);
        assertTrue(propertyChild.exists());

        assertEquals(child, nodeLocation.getParent().getChild(nodeLocation.getName()).getTree());
    }

    @Test
    public void testGetNonExistingChild() {
        TreeLocation location = nodeLocation.getChild(Text.getName(NON_EXISTING_PATH));
        assertFalse(location.exists());
        assertNull(location.getTree());
    }

    @Test
    public void testRemove() {
        when(child.remove()).thenReturn(true);

        assertTrue(nodeLocation.remove());
        verify(child).remove();
    }

    @Test
    public void testRemove2() {
        when(child.remove()).thenReturn(false);

        assertFalse(nodeLocation.remove());
        verify(child).remove();
    }

    @Test
    public void testGetNonExisting() {
        TreeLocation location = TreeLocation.create(root, NON_EXISTING_PATH);
        assertFalse(location.exists());
        assertNull(location.getTree());
        assertEquals(Text.getName(NON_EXISTING_PATH), nonExisting.getName());
        assertEquals(NON_EXISTING_PATH, nonExisting.getPath());
    }

    @Test
    public void testRemoveNonExisting() {
        TreeLocation location = TreeLocation.create(root, NON_EXISTING_PATH);
        assertFalse(location.remove());
        verify(nonExisting, never()).remove();
    }

    @Test
    public void testToString() {
        TreeLocation location = TreeLocation.create(rootTree);
        assertEquals(TreeLocation.create(rootTree).toString(), location.toString());
    }
}
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
package org.apache.jackrabbit.oak.plugins.tree;

import org.apache.jackrabbit.util.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PropertyLocationTest extends AbstractTreeTest {

    private TreeLocation propertyLocation;

    @Before
    public void before() throws Exception {
        super.before();

        propertyLocation = TreeLocation.create(root, PROPERTY_PATH);
    }

    @Test
    public void testExists() {
        assertTrue(propertyLocation.exists());
    }

    @Test
    public void testGetName() {
        assertEquals(Text.getName(PROPERTY_PATH), propertyLocation.getName());
    }

    @Test
    public void testGetTree() {
        assertNull(propertyLocation.getTree());
    }

    @Test
    public void testGetProperty() {
        assertEquals(root.getTree(CHILD_PATH).getProperty(propertyLocation.getName()), propertyLocation.getProperty());
    }

    @Test
    public void testGetChild() {
        TreeLocation child = propertyLocation.getChild("child");
        assertFalse(child.exists());
        assertEquals(propertyLocation.getProperty(), child.getParent().getProperty());
    }

    @Test
    public void testGetParent() {
        TreeLocation parent = propertyLocation.getParent();
        assertTrue(parent.exists());
        assertEquals(CHILD_PATH, parent.getPath());
        assertNotNull(parent.getTree());
    }

    @Test
    public void testRemovePropertyLocation() {
        String propName = propertyLocation.getName();
        assertTrue(propertyLocation.remove());
        verify(child, times(1)).removeProperty(propName);
    }
}
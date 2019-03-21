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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class NullLocationTest extends AbstractTreeTest {

    private static final String NULL_LOCATION_PATH = PROPERTY_PATH + "/null";

    private TreeLocation nullLocation;

    @Before
    public void before() throws Exception {
        super.before();

        nullLocation = TreeLocation.create(root, NULL_LOCATION_PATH);
    }

    @Test
    public void testExists() {
        assertFalse(nullLocation.exists());
    }

    @Test
    public void testGetTree() {
        assertNull(nullLocation.getTree());
    }

    @Test
    public void testGetProperty() {
        assertNull(nullLocation.getProperty());
    }

    @Test
    public void testGetName() {
        assertEquals("null", nullLocation.getName());
    }

    @Test
    public void testGetPath() {
        assertEquals(NULL_LOCATION_PATH, nullLocation.getPath());
    }

    @Test
    public void testRemove() {
        assertFalse(nullLocation.remove());
    }

    @Test
    public void testGetChild() {
        TreeLocation child = nullLocation.getChild("child");
        assertNotNull(child);
        assertFalse(child.exists());
        assertNull(child.getTree());
        assertNull(child.getProperty());
        assertEquals("child", child.getName());
    }


    @Test
    public void testGetDeepChild() {
        TreeLocation child = nullLocation.getChild("b").getChild("c");
        assertEquals(NULL_LOCATION_PATH + "/b/c", child.getPath());

        TreeLocation b = child.getParent();
        assertEquals(NULL_LOCATION_PATH + "/b", b.getPath());
        assertEquals("b", b.getName());
    }

    @Test
    public void testToString() {
        assertEquals(TreeLocation.create(root, PROPERTY_PATH).getChild("null").toString(), nullLocation.toString());
    }

    @Test
    public void testRootParent() {
        TreeLocation nullLocation = TreeLocation.create(root).getParent();
        assertSame(nullLocation, nullLocation.getParent());
        assertTrue(nullLocation.getName().isEmpty());
        assertTrue(nullLocation.getPath().isEmpty());
    }
}
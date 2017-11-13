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

import java.util.UUID;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TreeUtilTest extends AbstractTreeTest {

    @Test
    public void testGetPrimaryTypeName() {
        assertEquals(JcrConstants.NT_UNSTRUCTURED, TreeUtil.getPrimaryTypeName(child));
        assertNull(TreeUtil.getPrimaryTypeName(nonExisting));
    }

    @Test
    public void testGetStrings() {
        assertNull(TreeUtil.getStrings(nonExisting, "pp"));
        Iterable<String> values = TreeUtil.getStrings(child, "pp");
        assertNotNull(values);
        assertTrue(values.iterator().hasNext());
    }

    @Test
    public void testGetString() {
        assertNull(TreeUtil.getString(nonExisting, "p"));
        assertNull(TreeUtil.getString(child, "pp"));
        assertEquals("value", TreeUtil.getString(child, "p"));
    }

    @Test
    public void testGetStringWithDefault() {
        assertEquals("def", TreeUtil.getString(nonExisting, "p", "def"));
        assertEquals("def", TreeUtil.getString(child, "pp", "def"));
        assertEquals("value", TreeUtil.getString(child, "p", "def"));
    }

    @Test
    public void testGetBoolean() {
        assertFalse(TreeUtil.getBoolean(nonExisting, "p"));
        assertFalse(TreeUtil.getBoolean(child, "pp"));
        assertFalse(TreeUtil.getBoolean(child, "p"));
    }

    @Test
    public void testGetName() {
        assertNull(TreeUtil.getName(nonExisting, "p"));
        assertNull(TreeUtil.getName(child, "pp"));
        assertNull(TreeUtil.getName(child, "p"));
        assertNotNull(TreeUtil.getName(child, JcrConstants.JCR_PRIMARYTYPE));
        assertNull(TreeUtil.getName(child, JcrConstants.JCR_MIXINTYPES));
    }

    @Test
    public void testGetNames() {
        assertFalse(TreeUtil.getNames(nonExisting, "p").iterator().hasNext());
        assertFalse(TreeUtil.getNames(child, "pp").iterator().hasNext());
        assertFalse(TreeUtil.getNames(child, "p").iterator().hasNext());
        assertFalse(TreeUtil.getNames(child, JcrConstants.JCR_PRIMARYTYPE).iterator().hasNext());
        assertTrue(TreeUtil.getNames(child, JcrConstants.JCR_MIXINTYPES).iterator().hasNext());
    }

    @Test
    public void testGetLong() {
        assertEquals(2, TreeUtil.getLong(nonExisting, "p", 2));
        assertEquals(2, TreeUtil.getLong(child, "pp", 2));
        assertEquals(2, TreeUtil.getLong(child, JcrConstants.JCR_MIXINTYPES, 2));
    }

    @Test(expected = NumberFormatException.class)
    public void testGetLongInvalidConversion() {
        TreeUtil.getLong(child, "p", 2);
    }

    @Test
    public void testAutoCreateProperty() {
        Tree propDef = child; // TODO
        assertNull(TreeUtil.autoCreateProperty("anyName", propDef, "userId"));

        UUID.fromString(TreeUtil.autoCreateProperty(JcrConstants.JCR_UUID, propDef, null).getValue(Type.STRING));

        assertEquals("userId", TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_CREATEDBY, propDef, "userId").getValue(Type.STRING));
        assertTrue(TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_CREATEDBY, propDef, null).getValue(Type.STRING).isEmpty());

        assertEquals("userId", TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY, propDef, "userId").getValue(Type.STRING));
        assertTrue(TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY, propDef, null).getValue(Type.STRING).isEmpty());

    }

    @Test
    public void testIsReadOnlyTree() {
        assertFalse(TreeUtil.isReadOnlyTree(child));
    }
}
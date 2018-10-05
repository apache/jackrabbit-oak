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

import javax.jcr.AccessDeniedException;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class TreeUtilTest extends AbstractTreeTest {

    private Tree typeRoot;
    private Tree propDef;
    private Tree ntDef;

    @Override
    public void before() throws Exception {
        super.before();

        when(rootTree.addChild(nonExisting.getName())).thenReturn(nonExisting);
        when(rootTree.addChild(z.getName())).thenReturn(z);

        Tree newChild = mockTree("/newChild", rootTree, true, JcrConstants.NT_FOLDER);
        when(newChild.exists()).thenReturn(false);
        when(rootTree.addChild("newChild")).thenReturn(newChild);
        when(rootTree.getChild("newChild")).thenReturn(newChild);

        typeRoot = rootTree; // TODO

        ntDef = mockTree("/typeDef", typeRoot, true);
        when(typeRoot.getChild(NodeTypeConstants.NT_OAK_UNSTRUCTURED)).thenReturn(ntDef);
        when(typeRoot.getChild(NodeTypeConstants.MIX_LOCKABLE)).thenReturn(ntDef);
        when(typeRoot.getChild(NodeTypeConstants.MIX_VERSIONABLE)).thenReturn(ntDef);
        when(typeRoot.getChild("rep:NonExistingType")).thenReturn(nonExisting);

        propDef = child; // TODO
    }

    @Test
    public void testGetPrimaryTypeName() {
        assertEquals(NodeTypeConstants.NT_OAK_UNSTRUCTURED, TreeUtil.getPrimaryTypeName(child));
        assertNull(TreeUtil.getPrimaryTypeName(rootTree.getChild("x")));
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
        assertEquals(STRING_VALUE, TreeUtil.getString(child, "p"));
    }

    @Test
    public void testGetStringWithDefault() {
        assertEquals("def", TreeUtil.getString(nonExisting, "p", "def"));
        assertEquals("def", TreeUtil.getString(child, "pp", "def"));
        assertEquals(STRING_VALUE, TreeUtil.getString(child, "p", "def"));
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
        assertEquals(LONG_VALUE, TreeUtil.getLong(rootTree, "p", 2));
    }

    @Test(expected = NumberFormatException.class)
    public void testGetLongInvalidConversion() {
        TreeUtil.getLong(child, "p", 2);
    }


    @Test
    public void testGetTree() {
        String relPath = PathUtils.relativize(PathUtils.ROOT_PATH, child.getPath());

        Tree t = TreeUtil.getTree(rootTree, relPath);
        assertEquals(CHILD_PATH, t.getPath());
    }

    @Test
    public void testGetTreeWithCurrentElements() {
        String relPath = "./././"+PathUtils.relativize(PathUtils.ROOT_PATH, child.getPath());

        Tree t = TreeUtil.getTree(rootTree, relPath);
        assertEquals(CHILD_PATH, t.getPath());
    }

    @Test
    public void testGetTreeWithParentElements() {
        String relPath = PathUtils.relativize(PathUtils.ROOT_PATH, child.getPath()) + "/..";

        Tree t = TreeUtil.getTree(rootTree, relPath);
        assertEquals(child.getParent().getPath(), t.getPath());
    }

    @Test
    public void testGetTreeWithAbsolutePath() {
        assertEquals(CHILD_PATH, TreeUtil.getTree(rootTree, child.getPath()).getPath());
    }

    @Test
    public void testGetTreeWithNonExisting() {
        assertEquals(nonExisting.getPath(), TreeUtil.getTree(rootTree, "nonExisting").getPath());
    }

    @Test
    public void testGetTreeWithParentOfRootNonExisting() {
        assertNull(TreeUtil.getTree(rootTree, "x/../../../x"));
    }

    @Test(expected = AccessDeniedException.class)
    public void testAddChildNonExisting() throws Exception {
        TreeUtil.addChild(rootTree, nonExisting.getName(), NodeTypeConstants.NT_OAK_UNSTRUCTURED);
    }

    @Test
    public void testAddChild() throws Exception {
        assertEquals(z.getPath(), TreeUtil.addChild(rootTree, z.getName(), NodeTypeConstants.NT_OAK_UNSTRUCTURED).getPath());
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetOrAddChildNonExisting() throws Exception {
        TreeUtil.getOrAddChild(rootTree, nonExisting.getName(), NodeTypeConstants.NT_OAK_UNSTRUCTURED);
    }

    @Test
    public void testGetOrAddChild() throws Exception {
        assertEquals(z.getPath(), TreeUtil.getOrAddChild(rootTree, z.getName(), NodeTypeConstants.NT_OAK_UNSTRUCTURED).getPath());
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetOrAddChild2() throws Exception {
        TreeUtil.getOrAddChild(rootTree, "newChild", JcrConstants.NT_FOLDER);
    }

    @Test
    public void testAutoCreateProperty() {
        assertNull(TreeUtil.autoCreateProperty("anyName", propDef, "userId"));

        UUID.fromString(TreeUtil.autoCreateProperty(JcrConstants.JCR_UUID, propDef, null).getValue(Type.STRING));

        assertEquals("userId", TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_CREATEDBY, propDef, "userId").getValue(Type.STRING));
        assertTrue(TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_CREATEDBY, propDef, null).getValue(Type.STRING).isEmpty());

        assertEquals("userId", TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY, propDef, "userId").getValue(Type.STRING));
        assertTrue(TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY, propDef, null).getValue(Type.STRING).isEmpty());

        assertEquals(Type.DATE, TreeUtil.autoCreateProperty(JcrConstants.JCR_CREATED, propDef, null).getType());
        assertEquals(Type.DATE, TreeUtil.autoCreateProperty(JcrConstants.JCR_LASTMODIFIED, propDef, null).getType());
    }

    @Test
    public void testGetEffectiveTypeNoPrimary() {
        assertTrue(TreeUtil.getEffectiveType(rootTree.getChild("x"), typeRoot).isEmpty());
    }

    @Test
    public void testGetEffectiveTypeNoMixins() {
        assertEquals(ImmutableList.of(ntDef), TreeUtil.getEffectiveType(z, typeRoot));
    }

    @Test
    public void testGetEffectiveType() {
        assertEquals(ImmutableList.of(ntDef, ntDef, ntDef), TreeUtil.getEffectiveType(child, typeRoot));
    }

    @Test
    public void testGetEffectiveTypeNonExistingDef() {
        assertEquals(ImmutableList.of(), TreeUtil.getEffectiveType(mockTree("/anotherTree", rootTree, false, "rep:NonExistingType"), typeRoot));
    }

    @Test
    public void testFindDefaultPrimaryTypeUnknownDefinition() {
        assertNull(TreeUtil.findDefaultPrimaryType(typeRoot, false));
        assertNull(TreeUtil.findDefaultPrimaryType(typeRoot, true));
    }

    @Test
    public void testIsNodeType() {
        assertTrue(TreeUtil.isNodeType(child, NodeTypeConstants.NT_OAK_UNSTRUCTURED, typeRoot));
    }

    @Test
    public void testIsNodeTypeMixin() {
        assertTrue(TreeUtil.isNodeType(child, JcrConstants.MIX_LOCKABLE, typeRoot));
        assertFalse(TreeUtil.isNodeType(z, JcrConstants.MIX_LOCKABLE, typeRoot));
    }

    @Test
    public void testNotIsReadOnlyTree() {
        assertFalse(TreeUtil.isReadOnlyTree(child));
    }

    @Test
    public void testIsReadOnlyTree() {
        Tree readOnly = mockTree("/readOnly", rootTree, true, ReadOnly.class);
        assertTrue(TreeUtil.isReadOnlyTree(readOnly));
    }
}
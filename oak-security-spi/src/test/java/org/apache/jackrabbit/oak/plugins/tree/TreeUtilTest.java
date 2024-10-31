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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.LazyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.junit.Test;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import java.util.List;

import static org.apache.jackrabbit.JcrConstants.JCR_AUTOCREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_DEFAULTPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_DEFAULTVALUES;
import static org.apache.jackrabbit.JcrConstants.JCR_HASORDERABLECHILDNODES;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_MULTIPLE;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SAMENAMESIBLINGS;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.MIX_LOCKABLE;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_HIERARCHYNODE;
import static org.apache.jackrabbit.JcrConstants.NT_RESOURCE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_IS_ABSTRACT;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.MIX_CREATED;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.MIX_LASTMODIFIED;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_REP_ROOT;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_MIXIN_SUBTYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_MIXIN_TYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_NAMED_PROPERTY_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_PRIMARY_SUBTYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_PRIMARY_TYPE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_RESIDUAL_CHILD_NODE_DEFINITIONS;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_SUPERTYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.REP_UUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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

        typeRoot = mockTree("/definitions", rootTree, true);

        ntDef = mockTree("/definitions/definition", typeRoot, true);
        when(typeRoot.getChild(NT_OAK_UNSTRUCTURED)).thenReturn(ntDef);
        when(typeRoot.getChild(NodeTypeConstants.MIX_LOCKABLE)).thenReturn(ntDef);
        when(typeRoot.getChild(NodeTypeConstants.MIX_VERSIONABLE)).thenReturn(ntDef);
        when(typeRoot.getChild("rep:NonExistingType")).thenReturn(nonExisting);

        propDef = child; // TODO
    }

    @Test
    public void testGetPrimaryTypeName() {
        assertEquals(NT_OAK_UNSTRUCTURED, TreeUtil.getPrimaryTypeName(child));
        assertNull(TreeUtil.getPrimaryTypeName(rootTree.getChild("x")));
    }

    @Test
    public void testGetPrimaryTypeNameUnusedLazy() {
        assertEquals(NT_OAK_UNSTRUCTURED, TreeUtil.getPrimaryTypeName(child, mock(LazyValue.class)));
    }

    @Test
    public void testGetPrimaryTypeNameNewTreeLazy() {
        Tree newTree = when(rootTree.getChild("x").getStatus()).thenReturn(Tree.Status.NEW).getMock();
        assertNull(TreeUtil.getPrimaryTypeName(newTree, new LazyValue<Tree>() {
            @Override
            protected Tree createValue() {
                throw new RuntimeException("should not get here");
            }
        }));
    }

    @Test
    public void testGetPrimaryTypeNameFromLazy() {
        assertEquals(NT_OAK_UNSTRUCTURED, TreeUtil.getPrimaryTypeName(rootTree.getChild("x"), new LazyValue<Tree>() {
            @Override
            protected Tree createValue() {
                return when(mock(Tree.class).getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME)).getMock();
            }
        }));
    }

    @Test
    public void testGetMixinTypes() {
        assertTrue(Iterables.elementsEqual(TreeUtil.getNames(child, JcrConstants.JCR_MIXINTYPES), TreeUtil.getMixinTypeNames(child)));
        assertTrue(Iterables.elementsEqual(TreeUtil.getNames(rootTree, JcrConstants.JCR_MIXINTYPES), TreeUtil.getMixinTypeNames(rootTree)));
    }

    @Test
    public void testGetMixinTypeNamesUnusedLazy() {
        assertTrue(Iterables.elementsEqual(
                TreeUtil.getNames(child, JcrConstants.JCR_MIXINTYPES),
                TreeUtil.getMixinTypeNames(child, mock(LazyValue.class))));
    }

    @Test
    public void testGetMixinTypeNamesNewTreeLazy() {
        Tree newTree = when(rootTree.getChild("x").getStatus()).thenReturn(Tree.Status.NEW).getMock();
        assertTrue(Iterables.isEmpty(TreeUtil.getMixinTypeNames(newTree, new LazyValue<Tree>() {
            @Override
            protected Tree createValue() {
                throw new RuntimeException("should not get here");
            }
        })));
    }

    @Test
    public void testGetMixinTypeNamesFromLazy() {
        assertTrue(Iterables.elementsEqual(TreeUtil.getNames(child, JcrConstants.JCR_MIXINTYPES), TreeUtil.getMixinTypeNames(rootTree.getChild("x"), new LazyValue<Tree>() {
            @Override
            protected Tree createValue() {
                return child;
            }
        })));
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

    @Test(expected = ConstraintViolationException.class)
    public void testAddChildNullTypeNameNoDefault() throws Exception {
        Tree parent = mockTree("/some/tree", true);
        TreeUtil.addChild(parent, "grandChild", null, typeRoot, "userid");
    }

    @Test(expected = NoSuchNodeTypeException.class)
    public void testAddChildWithNonExistingTypeName() throws Exception {
        Tree parent = mockTree("/some/tree", true);
        TreeUtil.addChild(parent, "name", "rep:NonExistingType", typeRoot, "userid");
    }

    @Test(expected = ConstraintViolationException.class)
    public void testAddChildWithMixinName() throws Exception {
        Tree parent = mockTree("/some/tree", true);
        when(ntDef.getProperty(JCR_ISMIXIN)).thenReturn(PropertyStates.createProperty(JCR_ISMIXIN, true, Type.BOOLEAN));
        TreeUtil.addChild(parent, "name", MIX_LOCKABLE, typeRoot, "userid");
    }

    @Test(expected = ConstraintViolationException.class)
    public void testAddChildWithAbstractName() throws Exception {
        Tree parent = mockTree("/some/tree", true);
        when(ntDef.getProperty(JCR_IS_ABSTRACT)).thenReturn(PropertyStates.createProperty(JCR_IS_ABSTRACT, true, Type.BOOLEAN));
        TreeUtil.addChild(parent, "name", MIX_LOCKABLE, typeRoot, "userid");
    }

    @Test
    public void testAddChildWithOrderableChildren() throws Exception {
        Tree t = mock(Tree.class);
        Tree parent = when(mockTree("/some/tree", true).addChild("name")).thenReturn(t).getMock();

        when(ntDef.getProperty(JCR_HASORDERABLECHILDNODES)).thenReturn(PropertyStates.createProperty(JCR_HASORDERABLECHILDNODES, true, Type.BOOLEAN));
        Tree defWithoutChildren = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of()).getMock();
        when(ntDef.getChild(REP_NAMED_PROPERTY_DEFINITIONS)).thenReturn(defWithoutChildren);
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(defWithoutChildren);

        TreeUtil.addChild(parent, "name", NT_OAK_UNSTRUCTURED, typeRoot, "userid");

        verify(t, times(1)).setOrderableChildren(true);
    }
    
    @Test
    public void testAddChildWithDefaultChildType() throws Exception {
        Tree t = mock(Tree.class);
        Tree parent = when(mockTree("/some/tree", true).addChild("name")).thenReturn(t).getMock();
        when(parent.getProperty(JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME));

        Tree defWithChild = mock(Tree.class);
        when(defWithChild.getProperty(JCR_DEFAULTPRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JCR_DEFAULTPRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME));
        when(defWithChild.getChild("name")).thenReturn(defWithChild);
        when(defWithChild.getChildren()).thenReturn(ImmutableList.of(defWithChild));
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(defWithChild);
        
        Tree defWithoutChildren = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of()).getMock();
        when(ntDef.getChild(REP_NAMED_PROPERTY_DEFINITIONS)).thenReturn(defWithoutChildren);
        when(ntDef.getProperty(JCR_IS_ABSTRACT)).thenReturn(PropertyStates.createProperty(JCR_IS_ABSTRACT, true, Type.BOOLEAN));
        
        TreeUtil.addChild(parent, "name", null, typeRoot, "userid");
        verify(parent).addChild("name");
        verify(t).setProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME);
        verifyNoMoreInteractions(t);
        
        verify(ntDef, never()).getChild(REP_RESIDUAL_CHILD_NODE_DEFINITIONS);
    }

    @Test(expected = AccessDeniedException.class)
    public void testAddChildNonExisting() throws Exception {
        TreeUtil.addChild(rootTree, nonExisting.getName(), NT_OAK_UNSTRUCTURED);
    }

    @Test
    public void testAddChild() throws Exception {
        assertEquals(z.getPath(), TreeUtil.addChild(rootTree, z.getName(), NT_OAK_UNSTRUCTURED).getPath());
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetOrAddChildNonExisting() throws Exception {
        TreeUtil.getOrAddChild(rootTree, nonExisting.getName(), NT_OAK_UNSTRUCTURED);
    }

    @Test
    public void testGetOrAddChildExists() throws Exception {
        assertEquals(z.getPath(), TreeUtil.getOrAddChild(rootTree, z.getName(), NT_OAK_UNSTRUCTURED).getPath());
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetOrAddChildNonExistingAfterAdd() throws Exception {
        Tree t = mockTree("/newChild", false);
        when(rootTree.addChild("newChild")).thenReturn(t);
        TreeUtil.getOrAddChild(rootTree, "newChild", JcrConstants.NT_FOLDER);
    }

    @Test
    public void testGetOrAddChild() throws Exception {
        Tree t = mockTree("/newChild", true);
        when(rootTree.addChild("newChild")).thenReturn(t);
        assertEquals(t, TreeUtil.getOrAddChild(rootTree, "newChild", JcrConstants.NT_FOLDER));
    }

    @Test(expected = NoSuchNodeTypeException.class)
    public void testAddMixinNonExisting() throws Exception {
        TreeUtil.addMixin(child, "rep:NonExistingType", typeRoot, "userId");
    }

    @Test(expected = ConstraintViolationException.class)
    public void testAddMixinAbstract() throws Exception {
        when(ntDef.getProperty(JCR_IS_ABSTRACT)).thenReturn(PropertyStates.createProperty(JCR_IS_ABSTRACT, true, Type.BOOLEAN));
        TreeUtil.addMixin(child, NT_OAK_UNSTRUCTURED, typeRoot, "userId");
    }

    @Test(expected = ConstraintViolationException.class)
    public void testAddMixinNotMixin() throws Exception {
        when(ntDef.getProperty(JCR_ISMIXIN)).thenReturn(PropertyStates.createProperty(JCR_ISMIXIN, false, Type.BOOLEAN));
        TreeUtil.addMixin(child, NT_OAK_UNSTRUCTURED, typeRoot, "userId");
    }

    @Test
    public void testAddMixinAlreadyContained() throws Exception {
        when(ntDef.getProperty(JCR_ISMIXIN)).thenReturn(PropertyStates.createProperty(JCR_ISMIXIN, true, Type.BOOLEAN));

        TreeUtil.addMixin(child, MIX_LOCKABLE, typeRoot, "userId");

        verify(child, times(1)).getProperty(JcrConstants.JCR_MIXINTYPES);
        verify(child, never()).setProperty(JcrConstants.JCR_MIXINTYPES, List.of(MIX_LOCKABLE, MIX_VERSIONABLE, MIX_CREATED), Type.NAMES);
    }

    @Test
    public void testAddMixinAlreadyContainedNoPrimary() throws Exception {
        when(ntDef.getProperty(JCR_ISMIXIN)).thenReturn(PropertyStates.createProperty(JCR_ISMIXIN, true, Type.BOOLEAN));
        when(child.getProperty(JCR_PRIMARYTYPE)).thenReturn(null);

        TreeUtil.addMixin(child, MIX_LOCKABLE, typeRoot, "userId");

        verify(child, times(1)).getProperty(JcrConstants.JCR_MIXINTYPES);
        verify(child, never()).setProperty(JcrConstants.JCR_MIXINTYPES, List.of(MIX_LOCKABLE, MIX_VERSIONABLE, MIX_CREATED), Type.NAMES);
    }

    @Test
    public void testAddMixinSuPrimary() throws Exception {
        when(ntDef.getProperty(JCR_ISMIXIN)).thenReturn(PropertyStates.createProperty(JCR_ISMIXIN, true, Type.BOOLEAN));
        when(ntDef.getProperty(REP_PRIMARY_SUBTYPES)).thenReturn(PropertyStates.createProperty(REP_PRIMARY_SUBTYPES, ImmutableList.of(NT_OAK_UNSTRUCTURED), Type.NAMES));
        when(typeRoot.getChild("containsSubPrimary")).thenReturn(ntDef);

        TreeUtil.addMixin(child, "containsSubPrimary", typeRoot, "userId");

        verify(child, times(1)).getProperty(JcrConstants.JCR_PRIMARYTYPE);
        verify(child, never()).setProperty(anyString(), any(), any(Type.class));
    }

    @Test
    public void testAddMixinSubMixin() throws Exception {
        when(ntDef.getProperty(JCR_ISMIXIN)).thenReturn(PropertyStates.createProperty(JCR_ISMIXIN, true, Type.BOOLEAN));
        when(ntDef.getProperty(REP_MIXIN_SUBTYPES)).thenReturn(PropertyStates.createProperty(REP_MIXIN_SUBTYPES, ImmutableList.of(MIX_VERSIONABLE), Type.NAMES));
        when(typeRoot.getChild("containsSubMixin")).thenReturn(ntDef);

        TreeUtil.addMixin(child, "containsSubMixin", typeRoot, "userId");

        verify(child, times(1)).getProperty(JcrConstants.JCR_MIXINTYPES);
        verify(child, never()).setProperty(anyString(), any(), any(Type.class));
    }

    @Test
    public void testAddMixin() throws Exception {
        Tree defWithoutChildren = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of()).getMock();

        when(ntDef.getProperty(JCR_ISMIXIN)).thenReturn(PropertyStates.createProperty(JCR_ISMIXIN, true, Type.BOOLEAN));
        when(ntDef.getChild(REP_NAMED_PROPERTY_DEFINITIONS)).thenReturn(defWithoutChildren);
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(defWithoutChildren);

        when(typeRoot.getChild(MIX_CREATED)).thenReturn(ntDef);
        TreeUtil.addMixin(child, MIX_CREATED, typeRoot, "userId");

        verify(child, times(1)).getProperty(JcrConstants.JCR_MIXINTYPES);
        verify(child, times(1)).setProperty(JcrConstants.JCR_MIXINTYPES, List.of(MIX_LOCKABLE, MIX_VERSIONABLE, MIX_CREATED), Type.NAMES);
    }

    @Test
    public void testAutoCreateItemsNoAutoCreateDefs() throws Exception {
        Tree noAutoCreate = when(mockTree("/definitions/definition/notAutoCreate", true).getProperty(JCR_AUTOCREATED)).thenReturn(PropertyStates.createProperty(JCR_AUTOCREATED, false)).getMock();
        Tree defWithChildren = when(mockTree("/definitions/definition", true).getChildren()).thenReturn(ImmutableList.of(noAutoCreate)).getMock();
        Tree definitions = when(mockTree("/definitions", true).getChildren()).thenReturn(ImmutableList.of(defWithChildren)).getMock();

        when(ntDef.getChild(REP_NAMED_PROPERTY_DEFINITIONS)).thenReturn(definitions);
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(definitions);

        TreeUtil.autoCreateItems(child, ntDef, typeRoot, "userId");

        verify(child, never()).setProperty(any(PropertyState.class));
        verify(child, never()).addChild(anyString());
    }

    @Test
    public void testAutoCreateItemsExcludedProperties() throws Exception {
        List<Tree> list = ImmutableList.of(
                mockTree("/some/path/"+ REP_PRIMARY_TYPE, true),
                mockTree("/some/path/" + REP_MIXIN_TYPES, true));
        Tree defWithExcludedProperties = when(mock(Tree.class).getChildren()).thenReturn(list).getMock();
        when(ntDef.getChild(REP_NAMED_PROPERTY_DEFINITIONS)).thenReturn(defWithExcludedProperties);

        Tree defWithoutChildren = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of()).getMock();
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(defWithoutChildren);

        TreeUtil.autoCreateItems(child, ntDef, typeRoot, "userId");

        verify(child, never()).setProperty(any(PropertyState.class));
        verify(child, never()).addChild(anyString());
    }

    @Test
    public void testAutoCreateItemsExistingUuid() throws Exception {
        Tree definition = when(mock(Tree.class).getProperty(JCR_AUTOCREATED)).thenReturn(PropertyStates.createProperty(JCR_AUTOCREATED, true)).getMock();
        Tree definitions = mockTree("/some/path/"+ REP_UUID, true);
        when(definitions.getChildren()).thenReturn(ImmutableList.of(definition));
        Tree defWithExcludedProperties = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of(definitions)).getMock();
        when(ntDef.getChild(REP_NAMED_PROPERTY_DEFINITIONS)).thenReturn(defWithExcludedProperties);

        Tree defWithoutChildren = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of()).getMock();
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(defWithoutChildren);

        when(child.hasProperty(JCR_UUID)).thenReturn(true);
        TreeUtil.autoCreateItems(child, ntDef, typeRoot, "userId");

        verify(child, times(1)).hasProperty(JCR_UUID);
        verify(child, never()).setProperty(any(PropertyState.class));
        verify(child, never()).addChild(anyString());
    }

    @Test
    public void testAutoCreateItemsMissingUuid() throws Exception {
        Tree definition = when(mock(Tree.class).getProperty(JCR_AUTOCREATED)).thenReturn(PropertyStates.createProperty(JCR_AUTOCREATED, true)).getMock();
        Tree definitions = mockTree("/some/path/"+ REP_UUID, true);
        when(definitions.getChildren()).thenReturn(ImmutableList.of(definition));
        Tree defWithExcludedProperties = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of(definitions)).getMock();
        when(ntDef.getChild(REP_NAMED_PROPERTY_DEFINITIONS)).thenReturn(defWithExcludedProperties);

        Tree defWithoutChildren = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of()).getMock();
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(defWithoutChildren);

        when(child.hasProperty(JCR_UUID)).thenReturn(false);
        TreeUtil.autoCreateItems(child, ntDef, typeRoot, "userId");

        verify(child, times(1)).hasProperty(JCR_UUID);
        verify(child, times(1)).setProperty(any(PropertyState.class));
        verify(child, never()).addChild(anyString());
    }

    @Test(expected = RepositoryException.class)
    public void testAutoCreateItemsMissingDefaultValue() throws Exception {
        Tree definition = when(mock(Tree.class).getProperty(JCR_AUTOCREATED)).thenReturn(PropertyStates.createProperty(JCR_AUTOCREATED, true)).getMock();
        Tree definitions = mockTree("/some/path/unknownProperty", true);
        when(definitions.getChildren()).thenReturn(ImmutableList.of(definition));
        Tree defWithExcludedProperties = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of(definitions)).getMock();
        when(ntDef.getChild(REP_NAMED_PROPERTY_DEFINITIONS)).thenReturn(defWithExcludedProperties);

        Tree defWithoutChildren = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of()).getMock();
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(defWithoutChildren);

        when(child.hasProperty("unknownProperty")).thenReturn(false);
        TreeUtil.autoCreateItems(child, ntDef, typeRoot, "userId");
    }

    @Test
    public void testAutoCreateItemsExistingChild() throws Exception {
        Tree definition = when(mock(Tree.class).getProperty(JCR_AUTOCREATED)).thenReturn(PropertyStates.createProperty(JCR_AUTOCREATED, true)).getMock();
        Tree definitions = mockTree("/some/path/autoChild", true);
        when(definitions.getChildren()).thenReturn(ImmutableList.of(definition));

        Tree defWithAutoChild = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of(definitions)).getMock();
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(defWithAutoChild);

        Tree defWithoutChildren = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of()).getMock();
        when(ntDef.getChild(REP_NAMED_PROPERTY_DEFINITIONS)).thenReturn(defWithoutChildren);

        when(child.hasChild("autoChild")).thenReturn(true);
        TreeUtil.autoCreateItems(child, ntDef, typeRoot, "userId");

        verify(child, times(1)).hasChild("autoChild");
        verify(child, never()).setProperty(any(PropertyState.class));
        verify(child, never()).addChild(anyString());
    }

    @Test
    public void testAutoCreateItemsNonExistingChild() throws Exception {
        Tree definition = when(mock(Tree.class).getProperty(JCR_AUTOCREATED)).thenReturn(PropertyStates.createProperty(JCR_AUTOCREATED, true)).getMock();
        when(definition.getProperty(JCR_DEFAULTPRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JCR_DEFAULTPRIMARYTYPE, NT_OAK_UNSTRUCTURED, Type.NAME));

        Tree definitions = mockTree("/some/path/autoChild", true);
        when(definitions.getChildren()).thenReturn(ImmutableList.of(definition));

        Tree defWithAutoChild = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of(definitions)).getMock();
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(defWithAutoChild);

        Tree defWithoutChildren = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of()).getMock();
        when(ntDef.getChild(REP_NAMED_PROPERTY_DEFINITIONS)).thenReturn(defWithoutChildren);

        Tree newChild = mock(Tree.class);
        when(newChild.hasChild("autoChild")).thenReturn(true);

        when(child.hasChild("autoChild")).thenReturn(false);
        when(child.addChild("autoChild")).thenReturn(newChild);

        TreeUtil.autoCreateItems(child, ntDef, typeRoot, "userId");

        verify(child, times(1)).hasChild("autoChild");
        verify(child, never()).setProperty(any(PropertyState.class));
        verify(child, times(1)).addChild("autoChild");
        verify(newChild, times(1)).setProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, NAME);
    }

    @Test
    public void testAutoCreatePropertyBuiltIn() {
        assertNull(TreeUtil.autoCreateProperty("anyName", propDef, "userId"));

        String uuid = TreeUtil.autoCreateProperty(JCR_UUID, propDef, null).getValue(Type.STRING);
        assertTrue(UUIDUtils.isValidUUID(uuid));

        assertEquals("userId", TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_CREATEDBY, propDef, "userId").getValue(Type.STRING));
        assertTrue(TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_CREATEDBY, propDef, null).getValue(Type.STRING).isEmpty());

        assertEquals("userId", TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY, propDef, "userId").getValue(Type.STRING));
        assertTrue(TreeUtil.autoCreateProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY, propDef, null).getValue(Type.STRING).isEmpty());

        assertEquals(Type.DATE, TreeUtil.autoCreateProperty(JcrConstants.JCR_CREATED, propDef, null).getType());
        assertEquals(Type.DATE, TreeUtil.autoCreateProperty(JcrConstants.JCR_LASTMODIFIED, propDef, null).getType());
    }

    @Test
    public void testAutoCreatePropertyFromDefaultValues() {
        PropertyState defaultSingleValue = PropertyStates.createProperty(JCR_DEFAULTVALUES, ImmutableList.of(34L), Type.LONGS);
        when(propDef.getProperty(JCR_DEFAULTVALUES)).thenReturn(defaultSingleValue);
        when(propDef.getProperty(JCR_MULTIPLE)).thenReturn(PropertyStates.createProperty(JCR_MULTIPLE, false));

        PropertyState autocreated = TreeUtil.autoCreateProperty("anyName", propDef, "userId");
        assertNotNull(autocreated);
        assertEquals("anyName", autocreated.getName());
        assertEquals(Type.LONG, autocreated.getType());
        assertEquals(34, autocreated.getValue(Type.LONG).longValue());
    }

    @Test
    public void testAutoCreatePropertyFromEmptyDefaultValues() {
        PropertyState defaultSingleValue = PropertyStates.createProperty(JCR_DEFAULTVALUES, ImmutableList.of(), Type.DATES);
        when(propDef.getProperty(JCR_DEFAULTVALUES)).thenReturn(defaultSingleValue);
        when(propDef.getProperty(JCR_MULTIPLE)).thenReturn(PropertyStates.createProperty(JCR_MULTIPLE, false));

        PropertyState autocreated = TreeUtil.autoCreateProperty("anyName", propDef, "userId");
        assertNull(autocreated);
    }

    @Test
    public void testAutoCreatePropertyFromMvDefaultValues() {
        PropertyState defaultMvValue = PropertyStates.createProperty(JCR_DEFAULTVALUES, ImmutableList.of(true, false, true), Type.BOOLEANS);
        when(propDef.getProperty(JCR_DEFAULTVALUES)).thenReturn(defaultMvValue);
        when(propDef.getProperty(JCR_MULTIPLE)).thenReturn(PropertyStates.createProperty(JCR_MULTIPLE, true));

        PropertyState autocreated = TreeUtil.autoCreateProperty("anyName", propDef, "userId");
        assertNotNull(autocreated);
        assertEquals("anyName", autocreated.getName());
        assertEquals(defaultMvValue.getType(), autocreated.getType());
        assertEquals(defaultMvValue.getValue(defaultMvValue.getType()), autocreated.getValue(defaultMvValue.getType()));
    }

    @Test
    public void testGetDefaultChildTypeFromNamed() {
        Tree def = mock(Tree.class);
        Tree definitions = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of(def)).getMock();
        PropertyState ps = PropertyStates.createProperty(JCR_DEFAULTPRIMARYTYPE, NT_RESOURCE, Type.NAME);
        when(def.getProperty(JCR_DEFAULTPRIMARYTYPE)).thenReturn(ps);
        PropertyState sns = PropertyStates.createProperty(JCR_SAMENAMESIBLINGS, false, Type.BOOLEAN);
        when(def.getProperty(JCR_SAMENAMESIBLINGS)).thenReturn(sns);

        Tree named = when(mock(Tree.class).getChild("newChild")).thenReturn(definitions).getMock();
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(named);

        Tree emptyDefinitions = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of(def)).getMock();
        when(ntDef.getChild(REP_RESIDUAL_CHILD_NODE_DEFINITIONS)).thenReturn(emptyDefinitions);

        assertEquals(NT_RESOURCE, TreeUtil.getDefaultChildType(typeRoot, child, "newChild"));
        assertNull(TreeUtil.getDefaultChildType(typeRoot, child, "newChild[4]"));
    }

    @Test
    public void testGetDefaultChildTypeFromNamedWithSns() {
        Tree def = mock(Tree.class);
        Tree definitions = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of(def)).getMock();
        PropertyState ps = PropertyStates.createProperty(JCR_DEFAULTPRIMARYTYPE, NT_RESOURCE, Type.NAME);
        when(def.getProperty(JCR_DEFAULTPRIMARYTYPE)).thenReturn(ps);
        PropertyState sns = PropertyStates.createProperty(JCR_SAMENAMESIBLINGS, true, Type.BOOLEAN);
        when(def.getProperty(JCR_SAMENAMESIBLINGS)).thenReturn(sns);

        Tree named = when(mock(Tree.class).getChild("newChild")).thenReturn(definitions).getMock();
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(named);

        assertEquals(NT_RESOURCE, TreeUtil.getDefaultChildType(typeRoot, child, "newChild"));
        assertEquals(NT_RESOURCE, TreeUtil.getDefaultChildType(typeRoot, child, "newChild[4]"));
    }

    @Test
    public void testGetDefaultChildTypeFromResidual() {
        Tree emptyDefinitions = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of()).getMock();
        Tree named = when(mock(Tree.class).getChild("newChild")).thenReturn(emptyDefinitions).getMock();
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(named);

        Tree def = mock(Tree.class);
        PropertyState ps = PropertyStates.createProperty(JCR_DEFAULTPRIMARYTYPE, NT_RESOURCE, Type.NAME);
        when(def.getProperty(JCR_DEFAULTPRIMARYTYPE)).thenReturn(ps);
        PropertyState sns = PropertyStates.createProperty(JCR_SAMENAMESIBLINGS, false, Type.BOOLEAN);
        when(def.getProperty(JCR_SAMENAMESIBLINGS)).thenReturn(sns);
        Tree definitions = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of(def)).getMock();
        when(ntDef.getChild(REP_RESIDUAL_CHILD_NODE_DEFINITIONS)).thenReturn(definitions);

        assertEquals(NT_RESOURCE, TreeUtil.getDefaultChildType(typeRoot, child, "newChild"));
        assertNull(TreeUtil.getDefaultChildType(typeRoot, child, "newChild[4]"));
    }

    @Test
    public void testGetDefaultChildTypeFromResidualSns() {
        Tree emptyDefinitions = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of()).getMock();
        Tree named = when(mock(Tree.class).getChild("newChild")).thenReturn(emptyDefinitions).getMock();
        when(ntDef.getChild(REP_NAMED_CHILD_NODE_DEFINITIONS)).thenReturn(named);

        Tree def = mock(Tree.class);
        PropertyState ps = PropertyStates.createProperty(JCR_DEFAULTPRIMARYTYPE, NT_RESOURCE, Type.NAME);
        when(def.getProperty(JCR_DEFAULTPRIMARYTYPE)).thenReturn(ps);
        PropertyState sns = PropertyStates.createProperty(JCR_SAMENAMESIBLINGS, true, Type.BOOLEAN);
        when(def.getProperty(JCR_SAMENAMESIBLINGS)).thenReturn(sns);
        Tree definitions = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of(def)).getMock();
        when(ntDef.getChild(REP_RESIDUAL_CHILD_NODE_DEFINITIONS)).thenReturn(definitions);

        assertEquals(NT_RESOURCE, TreeUtil.getDefaultChildType(typeRoot, child, "newChild"));
        assertEquals(NT_RESOURCE, TreeUtil.getDefaultChildType(typeRoot, child, "newChild[4]"));
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
    public void testGetEffectiveTypeNonExistingPrimaryDef() {
        assertEquals(ImmutableList.of(), TreeUtil.getEffectiveType(mockTree("/anotherTree", rootTree, false, "rep:NonExistingType"), typeRoot));
    }

    @Test
    public void testGetEffectiveTypeNonExistingMixinDef() {
        Tree tree = mockTree("/anotherTree", rootTree, false);
        when(tree.getProperty(JCR_MIXINTYPES)).thenReturn(PropertyStates.createProperty(JCR_MIXINTYPES, ImmutableList.of("rep:NonExistingType"), Type.NAMES));

        assertEquals(ImmutableList.of(), TreeUtil.getEffectiveType(tree, typeRoot));
    }

    @Test
    public void testFindDefaultPrimaryTypeUnknownDefinition() {
        assertNull(TreeUtil.findDefaultPrimaryType(typeRoot, false));
        assertNull(TreeUtil.findDefaultPrimaryType(typeRoot, true));
    }

    @Test
    public void testFindDefaultPrimaryType() {
        Tree def = mock(Tree.class);
        Tree definitions = when(mock(Tree.class).getChildren()).thenReturn(ImmutableList.of(def)).getMock();
        assertNull(TreeUtil.findDefaultPrimaryType(definitions, false));
        assertNull(TreeUtil.findDefaultPrimaryType(definitions, true));

        PropertyState ps = PropertyStates.createProperty(JCR_DEFAULTPRIMARYTYPE, NT_RESOURCE, Type.NAME);
        when(def.getProperty(JCR_DEFAULTPRIMARYTYPE)).thenReturn(ps);
        assertEquals(NT_RESOURCE, TreeUtil.findDefaultPrimaryType(definitions, false));
        assertNull(TreeUtil.findDefaultPrimaryType(definitions, true));

        PropertyState sns = PropertyStates.createProperty(JCR_SAMENAMESIBLINGS, false, Type.BOOLEAN);
        when(def.getProperty(JCR_SAMENAMESIBLINGS)).thenReturn(sns);
        assertEquals(NT_RESOURCE, TreeUtil.findDefaultPrimaryType(definitions, false));
        assertNull(TreeUtil.findDefaultPrimaryType(definitions, true));

        sns = PropertyStates.createProperty(JCR_SAMENAMESIBLINGS, true, Type.BOOLEAN);
        when(def.getProperty(JCR_SAMENAMESIBLINGS)).thenReturn(sns);
        assertEquals(NT_RESOURCE, TreeUtil.findDefaultPrimaryType(definitions, false));
        assertEquals(NT_RESOURCE, TreeUtil.findDefaultPrimaryType(definitions, true));
    }

    @Test
    public void testIsNodeType() {
        assertTrue(TreeUtil.isNodeType(child, NT_OAK_UNSTRUCTURED, typeRoot));
        assertFalse(TreeUtil.isNodeType(child, NT_UNSTRUCTURED, typeRoot));
    }

    @Test
    public void testIsNodeTypeMissingTypeProperties() {
        Tree tree = mockTree(PathUtils.ROOT_PATH, true);
        assertFalse(TreeUtil.isNodeType(tree, NT_REP_ROOT, typeRoot));
    }

    @Test
    public void testIsNodeTypeContainedInSupertypes() {
        PropertyState supertypes = PropertyStates.createProperty(REP_SUPERTYPES, ImmutableList.of(NT_BASE), Type.NAMES);
        when(ntDef.getProperty(REP_SUPERTYPES)).thenReturn(supertypes);

        assertTrue(TreeUtil.isNodeType(child, NT_BASE, typeRoot));
        assertFalse(TreeUtil.isNodeType(child, NT_HIERARCHYNODE, typeRoot));
    }

    @Test
    public void testIsNodeTypeMixin() {
        assertTrue(TreeUtil.isNodeType(child, JcrConstants.MIX_LOCKABLE, typeRoot));
        assertFalse(TreeUtil.isNodeType(z, JcrConstants.MIX_LOCKABLE, typeRoot));
    }

    @Test
    public void testIsNodeTypeMixinContainedInSupertypes() {
        PropertyState supertypes = PropertyStates.createProperty(REP_SUPERTYPES, ImmutableList.of(MIX_REFERENCEABLE), Type.NAMES);
        when(ntDef.getProperty(REP_SUPERTYPES)).thenReturn(supertypes);

        PropertyState mixinNames = PropertyStates.createProperty(JcrConstants.JCR_MIXINTYPES, List.of(JcrConstants.MIX_VERSIONABLE), Type.NAMES);
        Tree tree = when(mockTree(CHILD_PATH, z, true).getProperty(JCR_MIXINTYPES)).thenReturn(mixinNames).getMock();
        assertTrue(TreeUtil.isNodeType(tree, MIX_REFERENCEABLE, typeRoot));
        assertFalse(TreeUtil.isNodeType(child, MIX_LASTMODIFIED, typeRoot));
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
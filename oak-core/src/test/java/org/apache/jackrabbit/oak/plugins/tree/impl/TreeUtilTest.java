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
package org.apache.jackrabbit.oak.plugins.tree.impl;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.LazyValue;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.junit.Test;
import org.mockito.Mockito;

import javax.jcr.GuestCredentials;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class TreeUtilTest extends AbstractSecurityTest {

    @Test
    public void testJcrCreatedBy() throws Exception {
        Tree tree = TreeUtil.addChild(root.getTree("/"), "test", JcrConstants.NT_FOLDER, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "userId");
        PropertyState ps = tree.getProperty(NodeTypeConstants.JCR_CREATEDBY);
        assertNotNull(ps);
        assertEquals("userId", ps.getValue(Type.STRING));
    }

    @Test
    public void testJcrCreatedByNullUserId() throws Exception {
        Tree tree = TreeUtil.addChild(root.getTree("/"), "test", JcrConstants.NT_FOLDER, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), null);
        PropertyState ps = tree.getProperty(NodeTypeConstants.JCR_CREATEDBY);
        assertNotNull(ps);
        assertEquals("", ps.getValue(Type.STRING));
    }

    @Test
    public void testJcrLastModifiedBy() throws Exception {
        Tree ntRoot = root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
        Tree tree = TreeUtil.addChild(root.getTree("/"), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED, ntRoot, "userId");
        TreeUtil.addMixin(tree, NodeTypeConstants.MIX_LASTMODIFIED, ntRoot, "userId");
        PropertyState ps = tree.getProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY);
        assertNotNull(ps);
        assertEquals("userId", ps.getValue(Type.STRING));
    }

    @Test
    public void testJcrLastModifiedByNullUserId() throws Exception {
        Tree ntRoot = root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
        Tree tree = TreeUtil.addChild(root.getTree("/"), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED, ntRoot, null);
        TreeUtil.addMixin(tree, NodeTypeConstants.MIX_LASTMODIFIED, ntRoot, null);
        PropertyState ps = tree.getProperty(NodeTypeConstants.JCR_LASTMODIFIEDBY);
        assertNotNull(ps);
        assertEquals("", ps.getValue(Type.STRING));
    }

    @Test
    public void testGetPrimaryTypeNameAccessible() throws Exception {
        Tree tree = root.getTree("/");
        String expected = TreeUtil.getPrimaryTypeName(tree);
        assertEquals(expected, TreeUtil.getPrimaryTypeName(tree, new LazyValue<Tree>() {
            @Override
            protected Tree createValue() {
                return tree;
            }
        }));
    }

    @Test
    public void testGetPrimaryTypeNameNotAccessible() throws Exception {
        Tree tree = root.getTree("/");
        String expected = TreeUtil.getPrimaryTypeName(tree);
        try (ContentSession cs = login(new GuestCredentials())) {
            Root r = cs.getLatestRoot();
            assertNull(TreeUtil.getPrimaryTypeName(r.getTree("/")));
            assertEquals(expected, TreeUtil.getPrimaryTypeName(r.getTree("/"), new LazyValue<Tree>() {
                @Override
                protected Tree createValue() {
                    return tree;
                }
            }));
        }
    }

    @Test
    public void testGetPrimaryTypeNameNotAccessibleNew() throws Exception {
        Tree testTree = TreeUtil.addChild(root.getTree("/"), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        Tree t = Mockito.mock(Tree.class);
        when(t.hasProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(false);
        when(t.getStatus()).thenReturn(Tree.Status.NEW);

        assertNull(TreeUtil.getPrimaryTypeName(t, new LazyValue<Tree>() {
            @Override
            protected Tree createValue() {
                return testTree;
            }
        }));
    }

    @Test
    public void testGetMixinTypeNamesMissingAccessible() {
        assertTrue(Iterables.isEmpty(TreeUtil.getMixinTypeNames(root.getTree("/"))));
        assertTrue(Iterables.isEmpty(TreeUtil.getMixinTypeNames(root.getTree("/"), new LazyValue<Tree>() {
            @Override
            protected Tree createValue() {
                return root.getTree("/");
            }
        })));
    }

    @Test
    public void testGetMixinTypeNamesMissingNotAccessible() throws Exception {
        Tree tree = root.getTree("/");
        assertTrue(Iterables.isEmpty(TreeUtil.getMixinTypeNames(tree)));

        try (ContentSession cs = login(new GuestCredentials())) {
            Root guestRoot = cs.getLatestRoot();
            assertTrue(Iterables.isEmpty(TreeUtil.getMixinTypeNames(guestRoot.getTree("/"))));
            assertTrue(Iterables.isEmpty(TreeUtil.getMixinTypeNames(guestRoot.getTree("/"), new LazyValue<Tree>() {
                @Override
                protected Tree createValue() {
                    return tree;
                }
            })));
        }
    }

    @Test
    public void testGetMixinTypeNamesPresentAccessible() throws Exception {
        Tree testTree = TreeUtil.addChild(root.getTree("/"), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        TreeUtil.addMixin(testTree, "mix:title", root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "uid");

        String path = testTree.getPath();
        Iterable<String> expected = TreeUtil.getMixinTypeNames(root.getTree(path));
        assertTrue(Iterables.contains(expected, "mix:title"));

        assertTrue(Iterables.elementsEqual(expected, TreeUtil.getMixinTypeNames(testTree, new LazyValue<Tree>() {
            @Override
            protected Tree createValue() {
                return testTree;
            }
        })));
    }

    @Test
    public void testGetMixinTypeNamesPresentNotAccessible() throws Exception {
        Tree testTree = TreeUtil.addChild(root.getTree("/"), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        TreeUtil.addMixin(testTree, "mix:title", root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "uid");
        root.commit();

        String path = testTree.getPath();
        Iterable<String> expected = TreeUtil.getMixinTypeNames(root.getTree(path));
        assertTrue(Iterables.contains(expected, "mix:title"));

        try (ContentSession cs = login(new GuestCredentials())) {
            Root guestRoot = cs.getLatestRoot();
            assertTrue(Iterables.isEmpty(TreeUtil.getMixinTypeNames(guestRoot.getTree(path))));
            assertTrue(Iterables.elementsEqual(expected, TreeUtil.getMixinTypeNames(guestRoot.getTree(path), new LazyValue<Tree>() {
                @Override
                protected Tree createValue() {
                    return testTree;
                }
            })));
        }
    }

    @Test
    public void testGetMixinTypeNamesPresentNotAccessibleNew() throws Exception {
        Tree testTree = TreeUtil.addChild(root.getTree("/"), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        TreeUtil.addMixin(testTree, "mix:title", root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "uid");
        root.commit();

        Tree t = Mockito.mock(Tree.class);
        when(t.hasProperty(JcrConstants.JCR_MIXINTYPES)).thenReturn(false);
        when(t.getStatus()).thenReturn(Tree.Status.NEW);

        assertTrue(Iterables.isEmpty(TreeUtil.getMixinTypeNames(t, new LazyValue<Tree>() {
            @Override
            protected Tree createValue() {
                return testTree;
            }
        })));
    }
}
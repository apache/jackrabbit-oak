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

import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.junit.Before;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class AbstractTreeTest {

    static final String CHILD_PATH = "/z/child";
    static final String STRING_VALUE = "value";
    static final long LONG_VALUE = 1;

    Tree rootTree;
    Tree nonExisting;
    Tree z;
    Tree child;
    Root root;

    @Before
    public void before() throws Exception {

        rootTree = mockTree("/", null, true, NodeTypeConstants.NT_REP_ROOT);
        when(rootTree.hasProperty("p")).thenReturn(true);
        when(rootTree.getProperty("p")).thenReturn(PropertyStates.createProperty("p", LONG_VALUE));

        nonExisting = mockTree("/nonExisting", rootTree, false, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        Tree x = mockTree("/x", rootTree, true);
        z = mockTree("/z", rootTree, true, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        child = mockTree("/z/child", z, true, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        when(child.hasProperty("p")).thenReturn(true);
        when(child.getProperty("p")).thenReturn(PropertyStates.createProperty("p", STRING_VALUE));
        when(child.hasProperty("pp")).thenReturn(true);
        PropertyState pp = PropertyStates.createProperty("pp", Lists.newArrayList("v1", "v2"), Type.STRINGS);
        when(child.getProperty("pp")).thenReturn(pp);
        when(child.hasProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(true);
        when(child.hasProperty(JcrConstants.JCR_MIXINTYPES)).thenReturn(true);
        PropertyState mixinNames = PropertyStates.createProperty(JcrConstants.JCR_MIXINTYPES, Lists.newArrayList(JcrConstants.MIX_LOCKABLE, JcrConstants.MIX_VERSIONABLE), Type.NAMES);
        when(child.getProperty(JcrConstants.JCR_MIXINTYPES)).thenReturn(mixinNames);

        when(z.getChild("child")).thenReturn(child);
        when(rootTree.getChild("z")).thenReturn(z);
        when(rootTree.getChild("x")).thenReturn(x);
        when(rootTree.getChild("nonExisting")).thenReturn(nonExisting);

        root = Mockito.mock(Root.class);
        when(root.getTree("/")).thenReturn(rootTree);
    }

    public Tree mockTree(String path, boolean exists) {
        Tree parent = PathUtils.denotesRoot(path) ? null : mockTree(PathUtils.getAncestorPath(path, 1), true);
        return mockTree(path, parent, exists);
    }

    public Tree mockTree(String path, Tree parent, boolean exists) {
        return mockTree(path, parent, exists, new Class[0]);
    }

    public Tree mockTree(String path, Tree parent, boolean exists, String primaryType) {
        Tree t = mockTree(path, parent, exists, new Class[0]);
        when(t.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, primaryType, Type.NAME));
        return t;
    }


    public Tree mockTree(String path, Tree parent, boolean exists, Class<?>... interfaces) {
        Tree t;
        if (interfaces.length > 0) {
            t = Mockito.mock(Tree.class, withSettings().extraInterfaces(interfaces));
        } else {
            t = Mockito.mock(Tree.class);
        }
        when(t.getPath()).thenReturn(path);
        when(t.getName()).thenReturn(PathUtils.getName(path));

        if (PathUtils.denotesRoot(path)) {
            when(t.getParent()).thenThrow(IllegalStateException.class);
            when(t.isRoot()).thenReturn(true);
        } else {
            when(t.getParent()).thenReturn(parent);
            when(t.isRoot()).thenReturn(false);
        }

        when(t.exists()).thenReturn(exists);
        when(t.hasProperty("nonExisting")).thenReturn(false);
        when(t.hasChild("nonExisting")).thenReturn(false);
        when(t.getChild("nonExisting")).thenReturn(nonExisting);
        when(t.remove()).thenThrow(new UnsupportedOperationException());
        return t;
    }
}
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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.when;

public class TreeTypeProviderTest {

    private TreeTypeProvider typeProvider;

    private List<TypeTest> tests;

    @Before
    public void before() throws Exception {
        typeProvider = new TreeTypeProvider(new TreeContext(){

            @Override
            public boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property) {
                return false;
            }

            @Override
            public boolean definesContextRoot(@Nonnull Tree tree) {
                return false;
            }

            @Override
            public boolean definesTree(@Nonnull Tree tree) {
                return false;
            }

            @Override
            public boolean definesLocation(@Nonnull TreeLocation location) {
                return false;
            }

            @Override
            public boolean definesInternal(@Nonnull Tree tree) {
                return false;
            }
        });

        tests = new ArrayList<TypeTest>();
        tests.add(new TypeTest("/", TreeType.DEFAULT));
        tests.add(new TypeTest("/content", TreeType.DEFAULT));
        tests.add(new TypeTest('/' + JcrConstants.JCR_SYSTEM, TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH, TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:system/rep:namedChildNodeDefinitions/jcr:versionStorage", TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:system/rep:namedChildNodeDefinitions/jcr:activities", TreeType.DEFAULT));
        tests.add(new TypeTest(NodeTypeConstants.NODE_TYPES_PATH + "/rep:system/rep:namedChildNodeDefinitions/jcr:configurations", TreeType.DEFAULT));

        tests.add(new TypeTest("/:hidden", TreeType.HIDDEN));
        tests.add(new TypeTest("/:hidden/child", TreeType.HIDDEN, TreeType.HIDDEN));

        tests.add(new TypeTest("/oak:index/nodetype/:index", TreeType.HIDDEN));
        tests.add(new TypeTest("/oak:index/nodetype/:index/child", TreeType.HIDDEN, TreeType.HIDDEN));

        for (String versionPath : VersionConstants.SYSTEM_PATHS) {
            tests.add(new TypeTest(versionPath, TreeType.VERSION));
            tests.add(new TypeTest(versionPath + "/a/b/child", TreeType.VERSION, TreeType.VERSION));
        }
    }

    private static Tree mockTree(@Nonnull String path) {
        Tree t = Mockito.mock(Tree.class);
        when(t.getPath()).thenReturn(path);
        when(t.getName()).thenReturn(PathUtils.getName(path));
        if (PathUtils.denotesRoot(path)) {
            when(t.getParent()).thenThrow(IllegalStateException.class);
            when(t.isRoot()).thenReturn(true);
        } else {
            Tree parent = mockTree(PathUtils.getAncestorPath(path, 1));
            when(t.getParent()).thenReturn(parent);
            when(t.isRoot()).thenReturn(false);
        }
        return t;
    }

    @Test
    public void testGetType() {
        for (TypeTest test : tests) {
            assertEquals(test.path, test.type, typeProvider.getType(mockTree(test.path)));
        }
    }

    @Test
    public void testGetTypeWithParentType() {
        for (TypeTest test : tests) {
            assertEquals(test.path, test.type, typeProvider.getType(mockTree(test.path), test.parentType));
        }
    }

    @Test
    public void testGetTypeWithDefaultParentType() {
        for (TypeTest test : tests) {
            TreeType typeIfParentDefault = typeProvider.getType(mockTree(test.path), TreeType.DEFAULT);

            if (TreeType.DEFAULT == test.parentType) {
                assertEquals(test.path, test.type, typeIfParentDefault);
            } else {
                assertNotEquals(test.path, test.type, typeIfParentDefault);
            }
        }
    }

    @Test
    public void testGetTypeForRootTree() {
        Tree t = mockTree(PathUtils.ROOT_PATH);
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t));

        // the type of the root tree is always 'DEFAULT' irrespective of the passed parent type.
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.DEFAULT));
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.HIDDEN));
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.VERSION));
    }

    private static final class TypeTest {

        private final String path;
        private final TreeType type;
        private final TreeType parentType;

        private TypeTest(@Nonnull String path, TreeType type) {
            this(path, type, TreeType.DEFAULT);
        }
        
        private TypeTest(@Nonnull String path, TreeType type, TreeType parentType) {
            this.path = path;
            this.type = type;
            this.parentType = parentType;
        }
    }
}
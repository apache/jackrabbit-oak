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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TreeTypeProviderTest extends AbstractTreeTest {

    private TreeTypeProvider typeProvider;

    private List<TypeTest> tests;

    @Before
    public void before() throws Exception {
        super.before();
        typeProvider = new TreeTypeProvider(new TreeContext(){

            @Override
            public boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property) {
                return false;
            }

            @Override
            public boolean definesContextRoot(@Nonnull Tree tree) {
                return tree.getName().equals("ctxRoot");
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
                return tree.getName().equals("internal");
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

    @Test
    public void testGetType() {
        for (TypeTest test : tests) {
            assertEquals(test.path, test.type, typeProvider.getType(mockTree(test.path, true)));
        }
    }

    @Test
    public void testGetTypeWithParentType() {
        for (TypeTest test : tests) {
            assertEquals(test.path, test.type, typeProvider.getType(mockTree(test.path, true), test.parentType));
        }
    }

    @Test
    public void testGetTypeWithDefaultParentType() {
        for (TypeTest test : tests) {
            TreeType typeIfParentDefault = typeProvider.getType(mockTree(test.path, true), TreeType.DEFAULT);

            if (TreeType.DEFAULT == test.parentType) {
                assertEquals(test.path, test.type, typeIfParentDefault);
            } else {
                assertNotEquals(test.path, test.type, typeIfParentDefault);
            }
        }
    }

    @Test
    public void testGetTypeForRootTree() {
        Tree t = mockTree(PathUtils.ROOT_PATH, true);
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t));

        // the type of the root tree is always 'DEFAULT' irrespective of the passed parent type.
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.DEFAULT));
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.HIDDEN));
        assertEquals(TreeType.DEFAULT, typeProvider.getType(t, TreeType.VERSION));
    }

    @Test
    public void testInternal() {
        Tree internal = mockTree("/internal", rootTree, true);
        Tree internalChild = mockTree("/internal/child", internal, true);

        assertEquals(TreeType.INTERNAL, typeProvider.getType(internal));
        assertEquals(TreeType.INTERNAL, typeProvider.getType(internalChild));
        assertEquals(TreeType.INTERNAL, typeProvider.getType(child, TreeType.INTERNAL));
    }

    @Test
    public void testAc() {
        Tree ctxRoot = mockTree("/ctxRoot", rootTree, true);
        Tree ctxRootChild = mockTree("/ctxRoot/child", ctxRoot, true);

        assertEquals(TreeType.ACCESS_CONTROL, typeProvider.getType(ctxRoot));
        assertEquals(TreeType.ACCESS_CONTROL, typeProvider.getType(ctxRootChild));
        assertEquals(TreeType.ACCESS_CONTROL, typeProvider.getType(child, TreeType.ACCESS_CONTROL));
    }

    @Test
    public void testTypeAware() {
        Tree typeAware = mockTree("/typeAware", rootTree, true, TreeTypeAware.class);
        Tree awareChild = mockTree("/typeAware/child", typeAware, true, TreeTypeAware.class);

        assertTrue(typeAware instanceof TreeTypeAware);
        assertTrue(awareChild instanceof TreeTypeAware);

        assertEquals(TreeType.DEFAULT, typeProvider.getType(typeAware));
        assertEquals(TreeType.DEFAULT, typeProvider.getType(awareChild));

        assertEquals(TreeType.VERSION, typeProvider.getType(typeAware, TreeType.VERSION));
        assertEquals(TreeType.VERSION, typeProvider.getType(awareChild, TreeType.VERSION));
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
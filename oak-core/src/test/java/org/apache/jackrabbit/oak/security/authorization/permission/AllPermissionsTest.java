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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test {@code AllPermissions}.
 */
public class AllPermissionsTest extends AbstractSecurityTest {

    private final CompiledPermissions all = AllPermissions.getInstance();

    private List<String> paths = new ArrayList<String>();

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        paths.add("/");
        paths.add(VersionConstants.VERSION_STORE_PATH);
        paths.add(NodeTypeConstants.NODE_TYPES_PATH);
        paths.add(getTestUser().getPath());
    }

    @Test
    public void testGetRepositoryPermission() {
        assertSame(RepositoryPermission.ALL, all.getRepositoryPermission());
    }

    @Test
    public void testGetTreePermission() {
        for (String path : paths) {
            Tree tree = getRootProvider().createReadOnlyRoot(root).getTree(path);
            assertTrue(tree.exists());

            assertSame(TreePermission.ALL, all.getTreePermission(tree, TreePermission.EMPTY));
            for (Tree child : tree.getChildren()) {
                assertSame(TreePermission.ALL, all.getTreePermission(child, TreePermission.EMPTY));
            }
        }
    }

    @Test
    public void testIsGranted() {
        for (String path : paths) {
            Tree tree = getRootProvider().createReadOnlyRoot(root).getTree(path);
            assertTrue(tree.exists());

            assertTrue(all.isGranted(tree, null, Permissions.ALL));
            for (PropertyState prop : tree.getProperties()) {
                assertTrue(all.isGranted(tree, prop, Permissions.ALL));
            }
            for (Tree child : tree.getChildren()) {
                assertTrue(all.isGranted(child, null, Permissions.ALL));
            }
        }
    }

    @Test
    public void testSame() {
        assertSame(all, AllPermissions.getInstance());
    }
}
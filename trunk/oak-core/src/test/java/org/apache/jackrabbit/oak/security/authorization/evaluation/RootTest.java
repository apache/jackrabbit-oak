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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Testing {@link Root} with access control restrictions in place.
 */
public class RootTest extends AbstractOakCoreTest {

    @Test
    public void testGetTree() throws Exception {
        setupPermission("/", testPrincipal, true, PrivilegeConstants.JCR_READ);
        setupPermission("/a/bb", testPrincipal, false, PrivilegeConstants.JCR_READ);

        Root testRoot = getTestRoot();

        List<String> accessible = ImmutableList.of("/", "/a", "/a/b", "/a/b/c");
        for (String path : accessible) {
            assertTrue(testRoot.getTree(path).exists());
        }

        assertFalse(testRoot.getTree("/a/bb").exists());
    }

    @Test
    public void testGetTree2() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_READ);
        setupPermission("/a/b", testPrincipal, false, PrivilegeConstants.JCR_READ);
        setupPermission("/a/b/c", testPrincipal, true, PrivilegeConstants.JCR_READ);

        Root testRoot = getTestRoot();

        List<String> notAccessible = ImmutableList.of("/", "/a/b");
        for (String path : notAccessible) {
            assertFalse(path, testRoot.getTree(path).exists());
        }

        List<String> accessible = ImmutableList.of("/a", "/a/bb", "/a/b/c");
        for (String path : accessible) {
            assertTrue(path, testRoot.getTree(path).exists());
        }
    }


    @Test
    public void testGetNodeLocation() throws Exception {
        setupPermission("/", testPrincipal, true, PrivilegeConstants.REP_READ_NODES);
        setupPermission("/a/bb", testPrincipal, false, PrivilegeConstants.REP_READ_NODES);

        Root testRoot = getTestRoot();

        List<String> accessible = ImmutableList.of("/", "/a", "/a/b", "/a/b/c");
        for (String path : accessible) {
            Tree tree = testRoot.getTree(path);
            assertNotNull(tree);
            assertTrue(tree.exists());
        }

        Tree tree = testRoot.getTree("/a/bb");
        assertNotNull(tree);
        assertFalse(tree.exists());
    }

    @Test
    public void testGetNodeLocation2() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.REP_READ_NODES);
        setupPermission("/a/b", testPrincipal, false, PrivilegeConstants.REP_READ_NODES);
        setupPermission("/a/b/c", testPrincipal, true, PrivilegeConstants.REP_READ_NODES);

        Root testRoot = getTestRoot();

        List<String> notAccessible = ImmutableList.of("/", "/a/b");
        for (String path : notAccessible) {
            Tree tree = testRoot.getTree(path);
            assertNotNull(path, tree);
            assertFalse(tree.exists());
        }

        List<String> accessible = ImmutableList.of("/a", "/a/bb", "/a/b/c");
        for (String path : accessible) {
            Tree tree = testRoot.getTree(path);
            assertNotNull(path, tree);
            assertTrue(tree.exists());
        }
    }

    @Test
    public void testGetNodeLocation3() throws Exception {
        // only property reading is allowed
        setupPermission("/", testPrincipal, true, PrivilegeConstants.REP_READ_PROPERTIES);

        Root testRoot = getTestRoot();

        List<String> notAccessible = ImmutableList.of("/", "/a", "/a/b", "/a/bb", "/a/b/c");
        for (String path : notAccessible) {
            Tree tree = testRoot.getTree(path);
            assertNotNull(path, tree);
            assertFalse(tree.exists());
        }
    }

    @Test
    public void testGetPropertyLocation() throws Exception {
        setupPermission("/", testPrincipal, true, PrivilegeConstants.JCR_READ);

        Root testRoot = getTestRoot();

        List<String> accessible = ImmutableList.of("/", "/a", "/a/b", "/a/bb", "/a/b/c");
        for (String path : accessible) {
            String propertyPath = PathUtils.concat(path, JcrConstants.JCR_PRIMARYTYPE);
            PropertyState property = testRoot.getTree(path).getProperty(JcrConstants.JCR_PRIMARYTYPE);
            assertNotNull(propertyPath, property);
        }

        List<String> propPaths = ImmutableList.of("/a/aProp", "/a/b/bProp", "/a/bb/bbProp", "/a/b/c/cProp");
        for (String path : propPaths) {
            PropertyState property = testRoot.getTree(PathUtils.getParentPath(path)).getProperty(PathUtils.getName(path));
            assertNotNull(path, property);
        }
    }

    @Test
    public void testGetPropertyLocation2() throws Exception {
        setupPermission("/", testPrincipal, true, PrivilegeConstants.REP_READ_PROPERTIES);

        Root testRoot = getTestRoot();

        List<String> accessible = ImmutableList.of("/", "/a", "/a/b", "/a/bb", "/a/b/c");
        for (String path : accessible) {
            String propertyPath = PathUtils.concat(path, JcrConstants.JCR_PRIMARYTYPE);
            PropertyState property = testRoot.getTree(path).getProperty(JcrConstants.JCR_PRIMARYTYPE);
            assertNotNull(propertyPath, property);
        }

        List<String> propPaths = ImmutableList.of("/a/aProp", "/a/b/bProp", "/a/bb/bbProp", "/a/b/c/cProp");
        for (String path : propPaths) {
            PropertyState property = testRoot.getTree(PathUtils.getParentPath(path)).getProperty(PathUtils.getName(path));
            assertNotNull(path, property);
        }
    }

    @Test
    public void testGetPropertyLocation3() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.REP_READ_PROPERTIES);
        setupPermission("/a/b", testPrincipal, false, PrivilegeConstants.REP_READ_PROPERTIES);
        setupPermission("/a/b/c", testPrincipal, true, PrivilegeConstants.REP_READ_PROPERTIES);

        Root testRoot = getTestRoot();

        List<String> accessible = ImmutableList.of("/a/aProp", "/a/bb/bbProp", "/a/b/c/cProp");
        for (String path : accessible) {
            PropertyState property = testRoot.getTree(PathUtils.getParentPath(path)).getProperty(PathUtils.getName(path));
            assertNotNull(path, property);
        }

        List<String> notAccessible = ImmutableList.of("/jcr:primaryType", "/a/b/bProp");
        for (String path : notAccessible) {
            PropertyState property = testRoot.getTree(PathUtils.getParentPath(path)).getProperty(PathUtils.getName(path));
            assertNull(path, property);
        }
    }
}
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
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Testing {@link Root} with access control restrictions in place.
 */
public class RootTest extends AbstractOakCoreTest {

    // TODO: include acl setup with restrictions
    // TODO: test location for access control content (with and without JCR_READ_ACCESSCONTROL privilege)

    @Test
    public void testGetTree() throws Exception {
        setupPermission("/", testPrincipal, true, PrivilegeConstants.JCR_READ);
        setupPermission("/a/bb", testPrincipal, false, PrivilegeConstants.JCR_READ);

        Root testRoot = getTestRoot();

        List<String> accessible = ImmutableList.of("/", "/a", "/a/b", "/a/b/c");
        for (String path : accessible) {
            assertNotNull(testRoot.getTree(path));
        }

        assertNull(testRoot.getTree("/a/bb"));
    }

    @Ignore("OAK-766") // FIXME
    @Test
    public void testGetTree2() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_READ);
        setupPermission("/a/b", testPrincipal, false, PrivilegeConstants.JCR_READ);
        setupPermission("/a/b/c", testPrincipal, true, PrivilegeConstants.JCR_READ);

        Root testRoot = getTestRoot();

        List<String> notAccessible = ImmutableList.of("/", "/a/b");
        for (String path : notAccessible) {
            assertNull(path, testRoot.getTree(path));
        }

        List<String> accessible = ImmutableList.of("/a", "/a/bb", "/a/b/c");
        for (String path : accessible) {
            assertNotNull(path, testRoot.getTree(path));
        }
    }


    @Test
    public void testGetNodeLocation() throws Exception {
        setupPermission("/", testPrincipal, true, PrivilegeConstants.REP_READ_NODES);
        setupPermission("/a/bb", testPrincipal, false, PrivilegeConstants.REP_READ_NODES);

        Root testRoot = getTestRoot();

        List<String> accessible = ImmutableList.of("/", "/a", "/a/b", "/a/b/c");
        for (String path : accessible) {
            TreeLocation location = testRoot.getLocation(path);
            assertNotNull(location);
            assertNotNull(location.getTree());
        }

        TreeLocation location = testRoot.getLocation("/a/bb");
        assertNotNull(location);
        assertNull(location.getTree());
    }

    @Ignore("OAK-766") // FIXME
    @Test
    public void testGetNodeLocation2() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.REP_READ_NODES);
        setupPermission("/a/b", testPrincipal, false, PrivilegeConstants.REP_READ_NODES);
        setupPermission("/a/b/c", testPrincipal, true, PrivilegeConstants.REP_READ_NODES);

        Root testRoot = getTestRoot();

        List<String> notAccessible = ImmutableList.of("/", "/a/b");
        for (String path : notAccessible) {
            TreeLocation location = testRoot.getLocation(path);
            assertNotNull(path, location);
            assertNull(location.getTree());
        }

        List<String> accessible = ImmutableList.of("/a", "/a/bb", "/a/b/c");
        for (String path : accessible) {
            TreeLocation location = testRoot.getLocation(path);
            assertNotNull(path, location);
            assertNotNull(path, location.getTree());
        }
    }

    @Test
    public void testGetNodeLocation3() throws Exception {
        // only property reading is allowed
        setupPermission("/", testPrincipal, true, PrivilegeConstants.REP_READ_PROPERTIES);

        Root testRoot = getTestRoot();

        List<String> notAccessible = ImmutableList.of("/", "/a", "/a/b", "/a/bb", "/a/b/c");
        for (String path : notAccessible) {
            TreeLocation location = testRoot.getLocation(path);
            assertNotNull(path, location);
            assertNull(location.getTree());
        }
    }

    @Test
    public void testGetPropertyLocation() throws Exception {
        setupPermission("/", testPrincipal, true, PrivilegeConstants.JCR_READ);

        Root testRoot = getTestRoot();

        List<String> accessible = ImmutableList.of("/", "/a", "/a/b", "/a/bb", "/a/b/c");
        for (String path : accessible) {
            String propertyPath = PathUtils.concat(path, JcrConstants.JCR_PRIMARYTYPE);
            TreeLocation location = testRoot.getLocation(propertyPath);
            assertNotNull(propertyPath, location);
            assertNotNull(propertyPath, location.getProperty());
        }

        List<String> propPaths = ImmutableList.of("/a/aProp", "/a/b/bProp", "/a/bb/bbProp", "/a/b/c/cProp");
        for (String path : propPaths) {
            TreeLocation location = testRoot.getLocation(path);
            assertNotNull(path, location);
            assertNotNull(path, location.getProperty());
        }
    }

    @Ignore("OAK-766") // FIXME
    @Test
    public void testGetPropertyLocation2() throws Exception {
        setupPermission("/", testPrincipal, true, PrivilegeConstants.REP_READ_PROPERTIES);

        Root testRoot = getTestRoot();

        List<String> accessible = ImmutableList.of("/", "/a", "/a/b", "/a/bb", "/a/b/c");
        for (String path : accessible) {
            String propertyPath = PathUtils.concat(path, JcrConstants.JCR_PRIMARYTYPE);
            TreeLocation location = testRoot.getLocation(propertyPath);
            assertNotNull(propertyPath, location);
            assertNotNull(propertyPath, location.getProperty());
        }

        List<String> propPaths = ImmutableList.of("/a/aProp", "/a/b/bProp", "/a/bb/bbProp", "/a/b/c/cProp");
        for (String path : propPaths) {
            TreeLocation location = testRoot.getLocation(path);
            assertNotNull(path, location);
            assertNotNull(path, location.getProperty());
        }
    }

    @Ignore("OAK-766") // FIXME
    @Test
    public void testGetPropertyLocation3() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.REP_READ_PROPERTIES);
        setupPermission("/a/b", testPrincipal, false, PrivilegeConstants.REP_READ_PROPERTIES);
        setupPermission("/a/b/c", testPrincipal, true, PrivilegeConstants.REP_READ_PROPERTIES);

        Root testRoot = getTestRoot();

        List<String> accessible = ImmutableList.of("/a/aProp", "/a/bb/bbProp", "/a/b/c/cProp");
        for (String path : accessible) {
            TreeLocation location = testRoot.getLocation(path);
            assertNotNull(path, location);
            assertNotNull(path, location.getProperty());
        }

        List<String> notAccessible = ImmutableList.of("/jcr:primaryType", "/a/b/bProp");
        for (String path : notAccessible) {
            TreeLocation location = testRoot.getLocation(path);
            assertNotNull(path, location);
            assertNull(path, location.getProperty());
        }
    }
}
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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadablePathsPermissionTest extends AbstractPrincipalBasedTest {

    private Iterator<String> readablePaths;
    private Iterator<String> readableChildPaths;

    private PrincipalBasedPermissionProvider permissionProvider;

    @Before
    public void before() throws Exception {
        super.before();

        Set<String> paths = getConfig(AuthorizationConfiguration.class).getParameters().getConfigValue(PermissionConstants.PARAM_READ_PATHS, PermissionConstants.DEFAULT_READ_PATHS);
        assertFalse(paths.isEmpty());

        readablePaths = Iterators.cycle(paths);
        Set<String> childPaths = Sets.newHashSet();
        for (String path : paths) {
            Iterables.addAll(childPaths, Iterables.transform(root.getTree(path).getChildren(), Tree::getPath));
        }
        readableChildPaths = Iterators.cycle(childPaths);

        permissionProvider = new PrincipalBasedPermissionProvider(root, root.getContentSession().getWorkspaceName(), Collections.singleton(getTestSystemUser().getPath()), getPrincipalBasedAuthorizationConfiguration());
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME,
                ConfigurationParameters.of(PermissionConstants.PARAM_READ_PATHS, new String[]{
                        NamespaceConstants.NAMESPACES_PATH,
                        PrivilegeConstants.PRIVILEGES_PATH
                }));
    }

    @NotNull
    private Tree getTree(@NotNull String path) {
        return root.getTree(path);
    }

    @Test
    public void testHasPrivileges() {
        assertTrue(permissionProvider.hasPrivileges(getTree(readablePaths.next()), JCR_READ));
        assertTrue(permissionProvider.hasPrivileges(getTree(readablePaths.next()), REP_READ_PROPERTIES));
        assertTrue(permissionProvider.hasPrivileges(getTree(readableChildPaths.next()), REP_READ_NODES));
        assertTrue(permissionProvider.hasPrivileges(getTree(readableChildPaths.next()), REP_READ_NODES, REP_READ_PROPERTIES));
    }

    @Test
    public void testNotHasPrivileges() {
        assertFalse(permissionProvider.hasPrivileges(getTree(readablePaths.next()), JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        assertFalse(permissionProvider.hasPrivileges(getTree(readablePaths.next()), PrivilegeConstants.JCR_WRITE));
        assertFalse(permissionProvider.hasPrivileges(getTree(readableChildPaths.next()), PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL, REP_READ_PROPERTIES));
        assertFalse(permissionProvider.hasPrivileges(getTree(readableChildPaths.next()), REP_READ_NODES, PrivilegeConstants.JCR_REMOVE_NODE));

        assertFalse(permissionProvider.hasPrivileges(getTree(ROOT_PATH), JCR_READ));
        String systemPath = PathUtils.concat(ROOT_PATH, JCR_SYSTEM);
        assertFalse(permissionProvider.hasPrivileges(getTree(systemPath), REP_READ_NODES));
    }

    @Test
    public void testGetPrivileges() {
        Set<String> expected = Collections.singleton(JCR_READ);

        assertEquals(expected, permissionProvider.getPrivileges(getTree(readablePaths.next())));
        assertEquals(expected, permissionProvider.getPrivileges(getTree(readableChildPaths.next())));

        assertTrue(permissionProvider.getPrivileges(getTree(ROOT_PATH)).isEmpty());
        String systemPath = PathUtils.concat(ROOT_PATH, JCR_SYSTEM);
        assertTrue(permissionProvider.getPrivileges(getTree(systemPath)).isEmpty());
    }

    @Test
    public void testIsGrantedPath() {
        assertTrue(permissionProvider.isGranted(readablePaths.next(), Permissions.getString(Permissions.READ)));
        assertTrue(permissionProvider.isGranted(readablePaths.next(), Permissions.getString(Permissions.READ_NODE|Permissions.READ_PROPERTY)));
        assertTrue(permissionProvider.isGranted(readableChildPaths.next(), Permissions.getString(Permissions.READ_NODE)));
        assertTrue(permissionProvider.isGranted(PathUtils.concat(readableChildPaths.next(), JCR_PRIMARYTYPE), Permissions.getString(Permissions.READ_PROPERTY)));
        assertTrue(permissionProvider.isGranted(PathUtils.concat(readableChildPaths.next(), "nonExisting"), Permissions.getString(Permissions.READ)));
    }

    @Test
    public void testNotIsGrantedPath() {
        assertFalse(permissionProvider.isGranted(readablePaths.next(), Permissions.getString(Permissions.READ|Permissions.VERSION_MANAGEMENT)));
        assertFalse(permissionProvider.isGranted(readablePaths.next(), Permissions.getString(Permissions.READ_ACCESS_CONTROL|Permissions.READ_NODE)));
        assertFalse(permissionProvider.isGranted(readableChildPaths.next(), Permissions.getString(Permissions.READ_PROPERTY|Permissions.ALL)));

        assertFalse(permissionProvider.isGranted(ROOT_PATH, Permissions.getString(Permissions.READ)));
        assertFalse(permissionProvider.isGranted(PathUtils.concat(ROOT_PATH, JCR_SYSTEM), Permissions.getString(Permissions.READ_NODE)));
        assertFalse(permissionProvider.isGranted("/nonExistingContent", Permissions.getString(Permissions.READ_PROPERTY)));
    }

    @Test
    public void testIsGrantedTree() {
        assertTrue(permissionProvider.isGranted(getTree(readablePaths.next()), null, Permissions.READ));
        assertTrue(permissionProvider.isGranted(getTree(readablePaths.next()), null, Permissions.READ_NODE));
        Tree t = getTree(readablePaths.next());
        assertTrue(permissionProvider.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));

        assertTrue(permissionProvider.isGranted(getTree(readableChildPaths.next()), null, Permissions.READ));
        assertTrue(permissionProvider.isGranted(getTree(readableChildPaths.next()), null, Permissions.READ_NODE));
        t = getTree(readableChildPaths.next());
        assertTrue(permissionProvider.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
    }

    @Test
    public void testNotIsGrantedTree() {
        assertFalse(permissionProvider.isGranted(getTree(readablePaths.next()), null, Permissions.READ_ACCESS_CONTROL|Permissions.READ));
        assertFalse(permissionProvider.isGranted(getTree(readablePaths.next()), null, Permissions.ADD_NODE));
        Tree t = getTree(readableChildPaths.next());
        assertFalse(permissionProvider.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.MODIFY_PROPERTY|Permissions.READ_PROPERTY));

        t = getTree(PathUtils.ROOT_PATH);
        assertFalse(permissionProvider.isGranted(t, null, Permissions.READ));
        assertFalse(permissionProvider.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
        assertFalse(permissionProvider.isGranted(t.getChildren().iterator().next(), null, Permissions.READ_NODE));
    }

    @Test
    public void testIsGrantedLocation() {
        assertTrue(permissionProvider.isGranted(TreeLocation.create(root, readablePaths.next()), Permissions.READ));
        assertTrue(permissionProvider.isGranted(TreeLocation.create(root, readableChildPaths.next()), Permissions.READ_NODE));
        assertTrue(permissionProvider.isGranted(TreeLocation.create(root, readableChildPaths.next()).getChild(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
    }

    @Test
    public void testNotIsGrantedLocation() {
        assertFalse(permissionProvider.isGranted(TreeLocation.create(root, readablePaths.next()), Permissions.READ|Permissions.WRITE));
        assertFalse(permissionProvider.isGranted(TreeLocation.create(root, readableChildPaths.next()), Permissions.ALL));
        assertFalse(permissionProvider.isGranted(TreeLocation.create(root, readableChildPaths.next()).getChild(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY|Permissions.MODIFY_PROPERTY));

        TreeLocation location = TreeLocation.create(root);
        assertFalse(permissionProvider.isGranted(location, Permissions.READ));
        assertFalse(permissionProvider.isGranted(location.getChild(JCR_SYSTEM), Permissions.READ_NODE));
        assertFalse(permissionProvider.isGranted(location.getChild(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
    }

    @Test
    public void testTreePermission() {
        Tree t = getTree(ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(t, TreePermission.EMPTY);
        assertFalse(tp.isGranted(Permissions.READ));
        assertFalse(tp.canRead());
        assertFalse(tp.canRead(t.getProperty(JCR_PRIMARYTYPE)));

        tp = permissionProvider.getTreePermission(t.getChild(JCR_SYSTEM), tp);
        assertFalse(tp.isGranted(Permissions.READ_NODE));
        assertFalse(tp.canReadProperties());
        assertFalse(tp.canReadAll());

        // readable paths
        t = root.getTree(readablePaths.next());
        tp = permissionProvider.getTreePermission(t, tp);
        assertTrue(tp.isGranted(Permissions.READ));
        assertFalse(tp.isGranted(Permissions.READ_ACCESS_CONTROL));

        assertTrue(tp.canRead());
        assertTrue(tp.canRead(t.getProperty(JCR_PRIMARYTYPE)));
        assertFalse(tp.canReadProperties());

        t = t.getChildren().iterator().next();
        tp = permissionProvider.getTreePermission(t, tp);
        assertTrue(tp.isGranted(Permissions.READ_NODE));
        assertTrue(tp.isGranted(Permissions.READ_PROPERTY, t.getProperty(JCR_PRIMARYTYPE)));
        assertFalse(tp.isGranted(Permissions.READ_PROPERTY|Permissions.MODIFY_PROPERTY, t.getProperty(JCR_PRIMARYTYPE)));
        assertTrue(tp.canRead());
        assertFalse(tp.canReadAll());
    }

    @Test
    public void testRepositoryPermission() {
        assertFalse(permissionProvider.getRepositoryPermission().isGranted(Permissions.READ));
    }
}
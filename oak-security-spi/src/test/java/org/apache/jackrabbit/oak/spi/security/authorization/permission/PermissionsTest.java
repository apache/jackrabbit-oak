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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.Session;

import org.apache.jackrabbit.guava.common.base.Splitter;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class PermissionsTest {

    private static final Map<Long, Set<Long>> TEST = Map.of(
            Permissions.ADD_NODE|Permissions.ADD_PROPERTY,
            Set.of(Permissions.ADD_NODE, Permissions.ADD_PROPERTY),
            Permissions.LOCK_MANAGEMENT|Permissions.MODIFY_CHILD_NODE_COLLECTION,
            Set.of(Permissions.LOCK_MANAGEMENT, Permissions.MODIFY_CHILD_NODE_COLLECTION),
            Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL,
            Set.of(Permissions.READ_ACCESS_CONTROL,Permissions.MODIFY_ACCESS_CONTROL),
            Permissions.NAMESPACE_MANAGEMENT|Permissions.WORKSPACE_MANAGEMENT|Permissions.NODE_TYPE_DEFINITION_MANAGEMENT|Permissions.PRIVILEGE_MANAGEMENT,
            Set.of(Permissions.NAMESPACE_MANAGEMENT,Permissions.WORKSPACE_MANAGEMENT,Permissions.NODE_TYPE_DEFINITION_MANAGEMENT,Permissions.PRIVILEGE_MANAGEMENT)
    );

    private Tree existingTree;

    @Before
    public void before() {
        existingTree = Mockito.mock(Tree.class);
        when(existingTree.exists()).thenReturn(true);
        when(existingTree.getName()).thenReturn(PathUtils.ROOT_NAME);
        when(existingTree.getPath()).thenReturn(PathUtils.ROOT_PATH);
        when(existingTree.hasProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(true);
        when(existingTree.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, "rep:root"));
    }

    private static TreeLocation createNonExistingTreeLocation(@NotNull String path) {
        String name = Text.getName(path);
        Tree nonExistingTree = Mockito.mock(Tree.class);
        when(nonExistingTree.exists()).thenReturn(false);
        when(nonExistingTree.getName()).thenReturn(name);
        when(nonExistingTree.getPath()).thenReturn(path);
        when(nonExistingTree.getChild(name)).thenReturn(nonExistingTree);
        return TreeLocation.create(nonExistingTree);
    }

    @Test
    public void testGetNamesSinglePermission() {
        for (long permission : Permissions.aggregates(Permissions.ALL)) {
            Set<String> names = Permissions.getNames(permission);
            assertEquals(1, names.size());
            assertEquals(Permissions.PERMISSION_NAMES.get(permission), names.iterator().next());
        }
    }

    @Test
    public void testGetNamesAllPermission() {
        Set<String> names = Permissions.getNames(Permissions.ALL);
        assertEquals(1, names.size());
        assertEquals(Permissions.PERMISSION_NAMES.get(Permissions.ALL), names.iterator().next());
    }

    @Test
    public void testGetNamesNoPermission() {
        Set<String> names = Permissions.getNames(Permissions.NO_PERMISSION);
        assertTrue(names.isEmpty());
    }

    @Test
    public void testGetNamesMultiple() {
        Map<Long, Set<Long>> test = Map.of(
                Permissions.ADD_NODE|Permissions.ADD_PROPERTY,
                Set.of(Permissions.ADD_NODE, Permissions.ADD_PROPERTY),
                Permissions.LOCK_MANAGEMENT|Permissions.MODIFY_CHILD_NODE_COLLECTION,
                Set.of(Permissions.LOCK_MANAGEMENT, Permissions.MODIFY_CHILD_NODE_COLLECTION),
                Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL,
                Set.of(Permissions.READ_ACCESS_CONTROL,Permissions.MODIFY_ACCESS_CONTROL),
                Permissions.NAMESPACE_MANAGEMENT|Permissions.WORKSPACE_MANAGEMENT|Permissions.NODE_TYPE_DEFINITION_MANAGEMENT|Permissions.PRIVILEGE_MANAGEMENT,
                Set.of(Permissions.NAMESPACE_MANAGEMENT,Permissions.WORKSPACE_MANAGEMENT,Permissions.NODE_TYPE_DEFINITION_MANAGEMENT,Permissions.PRIVILEGE_MANAGEMENT)
        );

        test.forEach((key, value) -> {
            Set<String> expected = new HashSet<>();
            for (long p : value) {
                expected.add(Permissions.PERMISSION_NAMES.get(p));
            }
            assertEquals(expected, Permissions.getNames(key));
        });
    }

    @Test
    public void testGetNamesAggregates() {
        Map<Long, Set<Long>> test = Map.of(
                Permissions.READ|Permissions.READ_ACCESS_CONTROL,
                Set.of(Permissions.READ, Permissions.READ_NODE, Permissions.READ_PROPERTY, Permissions.READ_ACCESS_CONTROL),
                Permissions.REMOVE|Permissions.SET_PROPERTY,
                Set.of(Permissions.REMOVE_NODE, Permissions.ADD_PROPERTY, Permissions.MODIFY_PROPERTY, Permissions.REMOVE_PROPERTY, Permissions.SET_PROPERTY, Permissions.REMOVE),
                Permissions.WRITE|Permissions.SET_PROPERTY,
                Set.of(Permissions.WRITE),
                Permissions.WRITE|Permissions.VERSION_MANAGEMENT,
                Set.of(Permissions.WRITE, Permissions.VERSION_MANAGEMENT, Permissions.REMOVE_NODE, Permissions.ADD_PROPERTY, Permissions.MODIFY_PROPERTY, Permissions.ADD_NODE, Permissions.REMOVE_PROPERTY, Permissions.SET_PROPERTY, Permissions.REMOVE)
        );

        test.forEach((key, value) -> {
            Set<String> expected = new HashSet<>();
            for (long p : value) {
                expected.add(Permissions.PERMISSION_NAMES.get(p));
            }
            assertEquals(expected, Permissions.getNames(key));
        });
    }

    @Test
    public void testGetStringSinglePermission() {
        for (long permission : Permissions.aggregates(Permissions.ALL)) {
            String str = Permissions.getString(permission);
            assertEquals(Permissions.PERMISSION_NAMES.get(permission), str);
        }
    }

    @Test
    public void testGetStringAllPermission() {
        String str = Permissions.getString(Permissions.ALL);
        assertEquals(Permissions.PERMISSION_NAMES.get(Permissions.ALL), str);
    }

    @Test
    public void testGetStringNoPermission() {
        String str = Permissions.getString(Permissions.NO_PERMISSION);
        assertTrue(str.isEmpty());
    }

    @Test
    public void testGetStringMultiple() {
        TEST.forEach((key, value) -> {
            Set<String> expected = new HashSet<>();
            for (long p : value) {
                expected.add(Permissions.PERMISSION_NAMES.get(p));
            }
            assertEquals(expected, CollectionUtils.toSet(Splitter.on(',').split(Permissions.getString(key))));
        });
    }

    @Test
    public void testGetStringAggregates() {
        Map<Long, Set<Long>> test = Map.of(
                Permissions.READ|Permissions.READ_ACCESS_CONTROL,
                Set.of(Permissions.READ, Permissions.READ_NODE, Permissions.READ_PROPERTY, Permissions.READ_ACCESS_CONTROL),
                Permissions.REMOVE|Permissions.SET_PROPERTY,
                Set.of(Permissions.REMOVE_NODE, Permissions.ADD_PROPERTY, Permissions.MODIFY_PROPERTY, Permissions.REMOVE_PROPERTY, Permissions.SET_PROPERTY, Permissions.REMOVE),
                Permissions.WRITE|Permissions.SET_PROPERTY,
                Set.of(Permissions.WRITE),
                Permissions.WRITE|Permissions.VERSION_MANAGEMENT,
                Set.of(Permissions.WRITE, Permissions.VERSION_MANAGEMENT, Permissions.REMOVE_NODE, Permissions.ADD_PROPERTY, Permissions.MODIFY_PROPERTY, Permissions.ADD_NODE, Permissions.REMOVE_PROPERTY, Permissions.SET_PROPERTY, Permissions.REMOVE)
        );

        test.forEach((key, value) -> {
            Set<String> expected = new HashSet<>();
            for (long p : value) {
                expected.add(Permissions.PERMISSION_NAMES.get(p));
            }
            assertEquals(expected, CollectionUtils.toSet(Splitter.on(',').split(Permissions.getString(key))));
        });
    }

    @Test
    public void testIsAggregate() {
        List<Long> aggregates = ImmutableList.of(Permissions.ALL, Permissions.WRITE, Permissions.READ, Permissions.SET_PROPERTY, Permissions.REMOVE);
        for (long permission : Permissions.PERMISSION_NAMES.keySet()) {
            if (aggregates.contains(permission)) {
                assertTrue(Permissions.getString(permission), Permissions.isAggregate(permission));
            } else {
                assertFalse(Permissions.getString(permission), Permissions.isAggregate(permission));
            }
        }
    }

    @Test
    public void testIsAggregateNoPermission() {
        assertFalse(Permissions.isAggregate(Permissions.NO_PERMISSION));
    }

    @Test
    public void testAggregates() {
        Map<Long, Set<Long>> aggregation = Map.of(
                Permissions.READ, Set.of(Permissions.READ_NODE, Permissions.READ_PROPERTY),
                Permissions.SET_PROPERTY, Set.of(Permissions.ADD_PROPERTY, Permissions.MODIFY_PROPERTY, Permissions.REMOVE_PROPERTY),
                Permissions.WRITE, Set.of(Permissions.ADD_NODE, Permissions.REMOVE_NODE, Permissions.ADD_PROPERTY, Permissions.REMOVE_PROPERTY,Permissions.MODIFY_PROPERTY)
        );
        aggregation.forEach((key, value) -> assertEquals(value, ImmutableSet.copyOf(Permissions.aggregates(key))));
    }

    @Test
    public void testAggregatesNoPermission() {
        assertFalse(Permissions.aggregates(Permissions.NO_PERMISSION).iterator().hasNext());
    }

    @Test
    public void testAggregatesAllPermission() {
        Iterable<Long> aggregates = Permissions.aggregates(Permissions.ALL);

        assertFalse(Iterables.contains(aggregates, Permissions.ALL));

        Set<Long> expected = new HashSet<>(Permissions.PERMISSION_NAMES.keySet());
        expected.removeAll(ImmutableList.of(Permissions.ALL, Permissions.WRITE, Permissions.READ, Permissions.SET_PROPERTY, Permissions.REMOVE));

        assertEquals(expected, CollectionUtils.toSet(aggregates));
    }

    @Test
    public void testIsRepositoryPermission() {
        Set<Long> repoPermissions = Set.of(Permissions.NAMESPACE_MANAGEMENT, Permissions.NODE_TYPE_DEFINITION_MANAGEMENT, Permissions.PRIVILEGE_MANAGEMENT, Permissions.WORKSPACE_MANAGEMENT);
        for (long permission : Permissions.aggregates(Permissions.ALL)) {
            assertEquals(repoPermissions.contains(permission), Permissions.isRepositoryPermission(permission));
        }
    }

    @Test
    public void testRespectParentPermissions() {
        List<Long> permissions = ImmutableList.of(
                Permissions.ALL,
                Permissions.ADD_NODE,
                Permissions.ADD_NODE|Permissions.ADD_PROPERTY,
                Permissions.ADD_NODE|Permissions.REMOVE_NODE,
                Permissions.ADD_NODE|Permissions.READ,
                Permissions.REMOVE_NODE,
                Permissions.REMOVE_NODE|Permissions.LOCK_MANAGEMENT,
                Permissions.WRITE,
                Permissions.REMOVE
        );
        for (long p : permissions) {
            assertTrue(Permissions.getString(p), Permissions.respectParentPermissions(p));
        }
    }

    @Test
    public void testNotRespectParentPermissions() {
        List<Long> permissions = ImmutableList.of(
                Permissions.READ,
                Permissions.ADD_PROPERTY,
                Permissions.REMOVE_PROPERTY,
                Permissions.ADD_PROPERTY|Permissions.REMOVE_PROPERTY,
                Permissions.MODIFY_CHILD_NODE_COLLECTION|Permissions.MODIFY_PROPERTY,
                Permissions.NODE_TYPE_MANAGEMENT|Permissions.VERSION_MANAGEMENT,
                Permissions.SET_PROPERTY,
                Permissions.WORKSPACE_MANAGEMENT|Permissions.NAMESPACE_MANAGEMENT
        );
        for (long p : permissions) {
            assertFalse(Permissions.getString(p), Permissions.respectParentPermissions(p));
        }
    }

    @Test
    public void testDiff() {
        assertEquals(Permissions.NO_PERMISSION, Permissions.diff(Permissions.ADD_NODE, Permissions.ADD_NODE));
        assertEquals(Permissions.READ_PROPERTY, Permissions.diff(Permissions.READ, Permissions.READ_NODE));
        assertEquals(Permissions.WRITE, Permissions.diff(Permissions.WRITE, Permissions.MODIFY_ACCESS_CONTROL));
        assertEquals(Permissions.WRITE, Permissions.diff(Permissions.WRITE, Permissions.NO_PERMISSION));
        assertEquals(Permissions.NO_PERMISSION, Permissions.diff(Permissions.WRITE, Permissions.WRITE));
        assertEquals(Permissions.SET_PROPERTY | Permissions.REMOVE_NODE | Permissions.LOCK_MANAGEMENT, Permissions.diff(Permissions.WRITE | Permissions.LOCK_MANAGEMENT, Permissions.ADD_NODE));
        assertEquals(Permissions.LOCK_MANAGEMENT, Permissions.diff(Permissions.LOCK_MANAGEMENT | Permissions.ADD_PROPERTY, Permissions.ADD_PROPERTY));
    }

    @Test
    public void testDiffFromAllPermissions() {
        assertEquals(Permissions.ALL, Permissions.diff(Permissions.ALL, Permissions.NO_PERMISSION));
        assertEquals(Permissions.NO_PERMISSION, Permissions.diff(Permissions.ALL, Permissions.ALL));

        long expected = (Permissions.READ_ACCESS_CONTROL
                | Permissions.MODIFY_ACCESS_CONTROL
                | Permissions.NODE_TYPE_MANAGEMENT
                | Permissions.VERSION_MANAGEMENT
                | Permissions.LOCK_MANAGEMENT
                | Permissions.LIFECYCLE_MANAGEMENT
                | Permissions.RETENTION_MANAGEMENT
                | Permissions.MODIFY_CHILD_NODE_COLLECTION
                | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT
                | Permissions.NAMESPACE_MANAGEMENT
                | Permissions.WORKSPACE_MANAGEMENT
                | Permissions.PRIVILEGE_MANAGEMENT
                | Permissions.USER_MANAGEMENT
                | Permissions.INDEX_DEFINITION_MANAGEMENT
        );
        assertEquals(expected, Permissions.diff(Permissions.ALL, Permissions.READ|Permissions.WRITE));
    }

    @Test
    public void testDiffFromNoPermissions() {
        assertEquals(Permissions.NO_PERMISSION, Permissions.diff(Permissions.NO_PERMISSION, Permissions.ADD_NODE));
        assertEquals(Permissions.NO_PERMISSION, Permissions.diff(Permissions.NO_PERMISSION, Permissions.ALL));
        assertEquals(Permissions.NO_PERMISSION, Permissions.diff(Permissions.NO_PERMISSION, Permissions.NO_PERMISSION));
    }

    @Test
    public void testGetPermissionsFromActions() {
        TreeLocation tl = TreeLocation.create(existingTree);
        Map<String, Long> map = Map.of(
                Session.ACTION_READ, Permissions.READ_NODE,
                Session.ACTION_READ + "," + Session.ACTION_REMOVE, Permissions.READ_NODE|Permissions.REMOVE_NODE
        );

        map.forEach((key, value) -> assertEquals(value.longValue(), Permissions.getPermissions(key, tl, false)));
    }

    @Test
    public void testGetPermissionsFromPermissionNameActions() {
        TreeLocation tl = TreeLocation.create(existingTree);
        long permissions = Permissions.NODE_TYPE_MANAGEMENT|Permissions.LOCK_MANAGEMENT|Permissions.VERSION_MANAGEMENT;
        Set<String> names = Permissions.getNames(permissions);
        String jcrActions = Text.implode(names.toArray(new String[0]), ",");
        assertEquals(permissions, Permissions.getPermissions(jcrActions, tl, false));
    }

    @Test
    public void testGetPermissionsFromInvalidActions() {
        TreeLocation tl = TreeLocation.create(existingTree);
        List<String> l = ImmutableList.of(
                Session.ACTION_READ + ",invalid", "invalid", "invalid," + Session.ACTION_REMOVE
        );

        for (String invalid : l) {
            try {
                Permissions.getPermissions(invalid, tl, false);
                fail();
            } catch (IllegalArgumentException e) {
                // success
            }
        }
    }

    @Test
    public void testGetPermissionsFromJackrabbitActions() {
        TreeLocation tl = TreeLocation.create(existingTree);
        Map<String, Long> map = new HashMap<>();
        map.put(Session.ACTION_ADD_NODE, Permissions.ADD_NODE);
        map.put(JackrabbitSession.ACTION_ADD_PROPERTY, Permissions.ADD_PROPERTY);
        map.put(JackrabbitSession.ACTION_MODIFY_PROPERTY, Permissions.MODIFY_PROPERTY);
        map.put(JackrabbitSession.ACTION_REMOVE_PROPERTY, Permissions.REMOVE_PROPERTY);
        map.put(JackrabbitSession.ACTION_REMOVE_NODE, Permissions.REMOVE_NODE);
        map.put(JackrabbitSession.ACTION_NODE_TYPE_MANAGEMENT, Permissions.NODE_TYPE_MANAGEMENT);
        map.put(JackrabbitSession.ACTION_LOCKING, Permissions.LOCK_MANAGEMENT);
        map.put(JackrabbitSession.ACTION_VERSIONING, Permissions.VERSION_MANAGEMENT);
        map.put(JackrabbitSession.ACTION_READ_ACCESS_CONTROL, Permissions.READ_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_USER_MANAGEMENT, Permissions.USER_MANAGEMENT);

        map.forEach((key, value) -> assertEquals(value.longValue(), Permissions.getPermissions(key, tl, false)));
    }

    @Test
    public void testGetPermissionsOnAccessControlledNode() {
        TreeLocation tl = createNonExistingTreeLocation(PathUtils.ROOT_PATH + AccessControlConstants.REP_POLICY);
        Map<String, Long> map = new HashMap<>();

        // read -> mapped to read-access-control
        map.put(Session.ACTION_READ, Permissions.READ_ACCESS_CONTROL);

        // all regular write -> mapped to modify-access-control (compatible and in
        // accordance to the previous behavior, where specifying an explicit
        // modify_access_control action was not possible.
        map.put(Session.ACTION_ADD_NODE, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(Session.ACTION_REMOVE, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(Session.ACTION_SET_PROPERTY, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_ADD_PROPERTY, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_MODIFY_PROPERTY, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_REMOVE_PROPERTY, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_REMOVE_NODE, Permissions.MODIFY_ACCESS_CONTROL);

        // all other actions are mapped to the corresponding permission without
        // testing for item being ac-content
        map.put(JackrabbitSession.ACTION_READ_ACCESS_CONTROL, Permissions.READ_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL, Permissions.MODIFY_ACCESS_CONTROL);
        map.put(JackrabbitSession.ACTION_LOCKING, Permissions.LOCK_MANAGEMENT);
        map.put(JackrabbitSession.ACTION_VERSIONING, Permissions.VERSION_MANAGEMENT);
        map.put(JackrabbitSession.ACTION_USER_MANAGEMENT, Permissions.USER_MANAGEMENT);

        map.forEach((key, value) -> assertEquals(key, value.longValue(), Permissions.getPermissions(key, tl, true)));
    }

    @Test
    public void testActionRead() {
        TreeLocation treeLocation = TreeLocation.create(existingTree);
        assertNull(treeLocation.getProperty());
        assertEquals(Permissions.READ_NODE, Permissions.getPermissions(Session.ACTION_READ, treeLocation, false));
        assertEquals(Permissions.READ_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_READ, treeLocation, true));

        TreeLocation nonExistingTree = createNonExistingTreeLocation("/nonExisting");
        assertNull(nonExistingTree.getProperty());
        assertEquals(Permissions.READ, Permissions.getPermissions(Session.ACTION_READ, nonExistingTree, false));
        assertEquals(Permissions.READ_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_READ, nonExistingTree, true));

        TreeLocation nonExistingProp = createNonExistingTreeLocation("/nonExisting").getChild("nonExisting");
        assertNull(nonExistingProp.getProperty());
        assertEquals(Permissions.READ, Permissions.getPermissions(Session.ACTION_READ, nonExistingProp, false));
        assertEquals(Permissions.READ_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_READ, nonExistingProp, true));

        TreeLocation existingProp = treeLocation.getChild(JcrConstants.JCR_PRIMARYTYPE);
        assertNotNull(existingProp.getProperty());
        assertEquals(Permissions.READ_PROPERTY, Permissions.getPermissions(Session.ACTION_READ, existingProp, false));
        assertEquals(Permissions.READ_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_READ, existingProp, true));
    }

    @Test
    public void testActionSetProperty() {
        TreeLocation treeLocation = TreeLocation.create(existingTree);
        assertNull(treeLocation.getProperty());
        assertEquals(Permissions.ADD_PROPERTY, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, treeLocation, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, treeLocation, true));

        TreeLocation nonExistingTree = createNonExistingTreeLocation("/nonExisting");
        assertNull(nonExistingTree.getProperty());
        assertEquals(Permissions.ADD_PROPERTY, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, nonExistingTree, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, nonExistingTree, true));

        TreeLocation nonExistingProp = createNonExistingTreeLocation("/nonExisting").getChild("nonExisting");
        assertNull(nonExistingProp.getProperty());
        assertEquals(Permissions.ADD_PROPERTY, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, nonExistingProp, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, nonExistingProp, true));

        TreeLocation existingProp = treeLocation.getChild(JcrConstants.JCR_PRIMARYTYPE);
        assertNotNull(existingProp.getProperty());
        assertEquals(Permissions.MODIFY_PROPERTY, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, existingProp, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, existingProp, true));
    }

    @Test
    public void testActionRemove() {
        TreeLocation treeLocation = TreeLocation.create(existingTree);
        assertNull(treeLocation.getProperty());
        assertEquals(Permissions.REMOVE_NODE, Permissions.getPermissions(Session.ACTION_REMOVE, treeLocation, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_REMOVE, treeLocation, true));

        TreeLocation nonExistingTree = createNonExistingTreeLocation("/nonExisting");
        assertNull(nonExistingTree.getProperty());
        assertEquals(Permissions.REMOVE, Permissions.getPermissions(Session.ACTION_REMOVE, nonExistingTree, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_REMOVE, nonExistingTree, true));

        TreeLocation nonExistingProp = createNonExistingTreeLocation("/nonExisting").getChild("nonExisting");
        assertNull(nonExistingProp.getProperty());
        assertEquals(Permissions.REMOVE, Permissions.getPermissions(Session.ACTION_REMOVE, nonExistingProp, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_REMOVE, nonExistingProp, true));

        TreeLocation existingProp = treeLocation.getChild(JcrConstants.JCR_PRIMARYTYPE);
        assertNotNull(existingProp.getProperty());
        assertEquals(Permissions.REMOVE_PROPERTY, Permissions.getPermissions(Session.ACTION_REMOVE, existingProp, false));
        assertEquals(Permissions.MODIFY_ACCESS_CONTROL, Permissions.getPermissions(Session.ACTION_SET_PROPERTY, existingProp, true));
    }

    @Test
    public void testGetPermissionsNullString() {
        assertEquals(Permissions.NO_PERMISSION, Permissions.getPermissions(null));
    }

    @Test
    public void testGetPermissionsEmptyString() {
        assertEquals(Permissions.NO_PERMISSION, Permissions.getPermissions(""));
    }

    @Test
    public void testGetPermissionsUnknownName() {
        assertEquals(Permissions.NO_PERMISSION, Permissions.getPermissions("unknown"));
        assertEquals(Permissions.NO_PERMISSION, Permissions.getPermissions("unknown,permission,strings"));
    }

    @Test
    public void testGetPermissionsSingleName() {
        Permissions.PERMISSION_NAMES.forEach((key, value) -> assertEquals(key.longValue(), Permissions.getPermissions(value)));
    }

    @Test
    public void testGetPermissionsMultipleNames() {
        TEST.forEach((key, value) -> {
            Set<String> names = new HashSet<>();
            for (long p : value) {
                names.add(Permissions.PERMISSION_NAMES.get(p));
            }
            String s = String.join(",", names);

            assertEquals(key.longValue(), Permissions.getPermissions(s));
        });
    }

    @Test
    public void testGetPermissionsMultipleNamesWithMissing() {
        String str = Permissions.PERMISSION_NAMES.get(Permissions.READ) + ", ,," + Permissions.PERMISSION_NAMES.get(Permissions.READ);
        assertEquals(Permissions.READ, Permissions.getPermissions(str));
    }

    @Test
    public void testGetPermissionsForReservedPaths() {
        Map<String, Long> mapping = Map.of(
                NamespaceConstants.NAMESPACES_PATH, Permissions.NAMESPACE_MANAGEMENT,
                NodeTypeConstants.NODE_TYPES_PATH, Permissions.NODE_TYPE_DEFINITION_MANAGEMENT,
                PrivilegeConstants.PRIVILEGES_PATH, Permissions.PRIVILEGE_MANAGEMENT
        );

        mapping.forEach((key, value) -> {
            for (long defaultPermission : Permissions.PERMISSION_NAMES.keySet()) {
                assertEquals(value.longValue(), Permissions.getPermission(key, defaultPermission));
            }
        });
    }

    @Test
    public void testGetPermissionsForVersionPaths() {
        for (String path : VersionConstants.SYSTEM_PATHS) {
            for (long defaultPermission : Permissions.PERMISSION_NAMES.keySet()) {
                assertEquals(Permissions.VERSION_MANAGEMENT, Permissions.getPermission(path, defaultPermission));
            }
        }
    }

    @Test
    public void testGetPermissionsForRegularPaths() {
        for (String path : ImmutableList.of("/", "/a/b/c", "/myfile/jcr:content")) {
            for (long defaultPermission : Permissions.PERMISSION_NAMES.keySet()) {
                assertEquals(defaultPermission, Permissions.getPermission(path, defaultPermission));
            }
        }
    }
}

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
import javax.annotation.Nonnull;
import javax.jcr.Session;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.util.Text;
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

    private static final Map<Long, Set<Long>> TEST = ImmutableMap.<Long, Set<Long>>of(
            Permissions.ADD_NODE|Permissions.ADD_PROPERTY,
            ImmutableSet.of(Permissions.ADD_NODE, Permissions.ADD_PROPERTY),
            Permissions.LOCK_MANAGEMENT|Permissions.MODIFY_CHILD_NODE_COLLECTION,
            ImmutableSet.of(Permissions.LOCK_MANAGEMENT, Permissions.MODIFY_CHILD_NODE_COLLECTION),
            Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL,
            ImmutableSet.of(Permissions.READ_ACCESS_CONTROL,Permissions.MODIFY_ACCESS_CONTROL),
            Permissions.NAMESPACE_MANAGEMENT|Permissions.WORKSPACE_MANAGEMENT|Permissions.NODE_TYPE_DEFINITION_MANAGEMENT|Permissions.PRIVILEGE_MANAGEMENT,
            ImmutableSet.of(Permissions.NAMESPACE_MANAGEMENT,Permissions.WORKSPACE_MANAGEMENT,Permissions.NODE_TYPE_DEFINITION_MANAGEMENT,Permissions.PRIVILEGE_MANAGEMENT)
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

    private static TreeLocation createNonExistingTreeLocation(@Nonnull String path) {
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
        Map<Long, Set<Long>> test = ImmutableMap.<Long, Set<Long>>of(
                Permissions.ADD_NODE|Permissions.ADD_PROPERTY,
                ImmutableSet.of(Permissions.ADD_NODE, Permissions.ADD_PROPERTY),
                Permissions.LOCK_MANAGEMENT|Permissions.MODIFY_CHILD_NODE_COLLECTION,
                ImmutableSet.of(Permissions.LOCK_MANAGEMENT, Permissions.MODIFY_CHILD_NODE_COLLECTION),
                Permissions.READ_ACCESS_CONTROL|Permissions.MODIFY_ACCESS_CONTROL,
                ImmutableSet.of(Permissions.READ_ACCESS_CONTROL,Permissions.MODIFY_ACCESS_CONTROL),
                Permissions.NAMESPACE_MANAGEMENT|Permissions.WORKSPACE_MANAGEMENT|Permissions.NODE_TYPE_DEFINITION_MANAGEMENT|Permissions.PRIVILEGE_MANAGEMENT,
                ImmutableSet.of(Permissions.NAMESPACE_MANAGEMENT,Permissions.WORKSPACE_MANAGEMENT,Permissions.NODE_TYPE_DEFINITION_MANAGEMENT,Permissions.PRIVILEGE_MANAGEMENT)
        );

        for (long permissions : test.keySet()) {
            Set<String> expected = new HashSet<>();
            for (long p : test.get(permissions)) {
                expected.add(Permissions.PERMISSION_NAMES.get(p));
            }
            assertEquals(expected, Permissions.getNames(permissions));
        }
    }

    @Test
    public void testGetNamesAggregates() {
        Map<Long, Set<Long>> test = ImmutableMap.<Long, Set<Long>>of(
                Permissions.READ|Permissions.READ_ACCESS_CONTROL,
                ImmutableSet.of(Permissions.READ, Permissions.READ_NODE, Permissions.READ_PROPERTY, Permissions.READ_ACCESS_CONTROL),
                Permissions.REMOVE|Permissions.SET_PROPERTY,
                ImmutableSet.of(Permissions.REMOVE_NODE, Permissions.ADD_PROPERTY, Permissions.MODIFY_PROPERTY, Permissions.REMOVE_PROPERTY, Permissions.SET_PROPERTY, Permissions.REMOVE),
                Permissions.WRITE|Permissions.SET_PROPERTY,
                ImmutableSet.of(Permissions.WRITE),
                Permissions.WRITE|Permissions.VERSION_MANAGEMENT,
                ImmutableSet.of(Permissions.WRITE, Permissions.VERSION_MANAGEMENT, Permissions.REMOVE_NODE, Permissions.ADD_PROPERTY, Permissions.MODIFY_PROPERTY, Permissions.ADD_NODE, Permissions.REMOVE_PROPERTY, Permissions.SET_PROPERTY, Permissions.REMOVE)
        );

        for (long permissions : test.keySet()) {
            Set<String> expected = new HashSet<>();
            for (long p : test.get(permissions)) {
                expected.add(Permissions.PERMISSION_NAMES.get(p));
            }
            assertEquals(expected, Permissions.getNames(permissions));
        }
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
        for (long permissions : TEST.keySet()) {
            Set<String> expected = new HashSet<>();
            for (long p : TEST.get(permissions)) {
                expected.add(Permissions.PERMISSION_NAMES.get(p));
            }
            assertEquals(expected, Sets.newHashSet(Splitter.on(',').split(Permissions.getString(permissions))));
        }
    }

    @Test
    public void testGetStringAggregates() {
        Map<Long, Set<Long>> test = ImmutableMap.<Long, Set<Long>>of(
                Permissions.READ|Permissions.READ_ACCESS_CONTROL,
                ImmutableSet.of(Permissions.READ, Permissions.READ_NODE, Permissions.READ_PROPERTY, Permissions.READ_ACCESS_CONTROL),
                Permissions.REMOVE|Permissions.SET_PROPERTY,
                ImmutableSet.of(Permissions.REMOVE_NODE, Permissions.ADD_PROPERTY, Permissions.MODIFY_PROPERTY, Permissions.REMOVE_PROPERTY, Permissions.SET_PROPERTY, Permissions.REMOVE),
                Permissions.WRITE|Permissions.SET_PROPERTY,
                ImmutableSet.of(Permissions.WRITE),
                Permissions.WRITE|Permissions.VERSION_MANAGEMENT,
                ImmutableSet.of(Permissions.WRITE, Permissions.VERSION_MANAGEMENT, Permissions.REMOVE_NODE, Permissions.ADD_PROPERTY, Permissions.MODIFY_PROPERTY, Permissions.ADD_NODE, Permissions.REMOVE_PROPERTY, Permissions.SET_PROPERTY, Permissions.REMOVE)
        );

        for (long permissions : test.keySet()) {
            Set<String> expected = new HashSet<>();
            for (long p : test.get(permissions)) {
                expected.add(Permissions.PERMISSION_NAMES.get(p));
            }
            assertEquals(expected, Sets.newHashSet(Splitter.on(',').split(Permissions.getString(permissions))));
        }
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
        Map<Long, Set<Long>> aggregation = ImmutableMap.<Long, Set<Long>>of(
                Permissions.READ, ImmutableSet.of(Permissions.READ_NODE, Permissions.READ_PROPERTY),
                Permissions.SET_PROPERTY, ImmutableSet.of(Permissions.ADD_PROPERTY, Permissions.MODIFY_PROPERTY, Permissions.REMOVE_PROPERTY),
                Permissions.WRITE, ImmutableSet.of(Permissions.ADD_NODE, Permissions.REMOVE_NODE, Permissions.ADD_PROPERTY, Permissions.REMOVE_PROPERTY,Permissions.MODIFY_PROPERTY)
        );
        for (long permission : aggregation.keySet()) {
            assertEquals(aggregation.get(permission), ImmutableSet.copyOf(Permissions.aggregates(permission)));
        }
    }

    @Test
    public void testAggregatesNoPermission() {
        assertFalse(Permissions.aggregates(Permissions.NO_PERMISSION).iterator().hasNext());
    }

    @Test
    public void testAggregatesAllPermission() {
        Iterable<Long> aggregates = Permissions.aggregates(Permissions.ALL);

        assertFalse(Iterables.contains(aggregates, Permissions.ALL));

        Set<Long> expected = Sets.newHashSet(Permissions.PERMISSION_NAMES.keySet());
        expected.removeAll(ImmutableList.of(Permissions.ALL, Permissions.WRITE, Permissions.READ, Permissions.SET_PROPERTY, Permissions.REMOVE));

        assertEquals(expected, Sets.newHashSet(aggregates));
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
        Map<String, Long> map = ImmutableMap.of(
                Session.ACTION_READ, Permissions.READ_NODE,
                Session.ACTION_READ + "," + Session.ACTION_REMOVE, Permissions.READ_NODE|Permissions.REMOVE_NODE
        );

        for (Map.Entry<String, Long> entry : map.entrySet()) {
            assertEquals(entry.getValue().longValue(), Permissions.getPermissions(entry.getKey(), tl, false));
        }
    }

    @Test
    public void testGetPermissionsFromPermissionNameActions() {
        TreeLocation tl = TreeLocation.create(existingTree);
        long permissions = Permissions.NODE_TYPE_MANAGEMENT|Permissions.LOCK_MANAGEMENT|Permissions.VERSION_MANAGEMENT;
        Set<String> names = Permissions.getNames(permissions);
        String jcrActions = Text.implode(names.toArray(new String[names.size()]), ",");
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
        Map<String, Long> map = new HashMap<String, Long>();
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

        for (Map.Entry<String, Long> entry : map.entrySet()) {
            assertEquals(entry.getValue().longValue(), Permissions.getPermissions(entry.getKey(), tl, false));
        }
    }

    @Test
    public void testGetPermissionsOnAccessControlledNode() {
        TreeLocation tl = createNonExistingTreeLocation(PathUtils.ROOT_PATH + AccessControlConstants.REP_POLICY);
        Map<String, Long> map = new HashMap<String, Long>();

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

        for (Map.Entry<String, Long> entry : map.entrySet()) {
            assertEquals(entry.getKey(), entry.getValue().longValue(), Permissions.getPermissions(entry.getKey(), tl, true));
        }
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
        for (Map.Entry<Long, String> entry : Permissions.PERMISSION_NAMES.entrySet()) {
            assertEquals(entry.getKey().longValue(), Permissions.getPermissions(entry.getValue()));
        }
    }

    @Test
    public void testGetPermissionsMultipleNames() {
        for (Map.Entry<Long, Set<Long>> entry : TEST.entrySet()) {
            Set<String> names = new HashSet<>();
            for (long p : entry.getValue()) {
                names.add(Permissions.PERMISSION_NAMES.get(p));
            }
            String s = Joiner.on(',').join(names);

            assertEquals(entry.getKey().longValue(), Permissions.getPermissions(s));
        }
    }

    @Test
    public void testGetPermissionsForReservedPaths() {
        Map<String, Long> mapping = ImmutableMap.of(
                NamespaceConstants.NAMESPACES_PATH, Permissions.NAMESPACE_MANAGEMENT,
                NodeTypeConstants.NODE_TYPES_PATH, Permissions.NODE_TYPE_DEFINITION_MANAGEMENT,
                PrivilegeConstants.PRIVILEGES_PATH, Permissions.PRIVILEGE_MANAGEMENT
        );

        for (String path : mapping.keySet()) {
            for (long defaultPermission : Permissions.PERMISSION_NAMES.keySet()) {
                assertEquals(mapping.get(path).longValue(), Permissions.getPermission(path, defaultPermission));
            }
        }
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
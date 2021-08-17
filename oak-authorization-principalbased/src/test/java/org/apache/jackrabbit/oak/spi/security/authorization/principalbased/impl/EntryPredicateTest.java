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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.MockUtility.mockTree;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class EntryPredicateTest {

    private static final String TREE_PATH = "/parent/path";
    private static final String PROP_PATH = "/parent/path/prop";
    private final String PARENT_PATH = PathUtils.getParentPath(TREE_PATH);

    private final PermissionEntry pe = mock(PermissionEntry.class);
    private Tree tree;
    private final PropertyState propertyState = PropertyStates.createProperty("prop", "value");

    @Before
    public void before() {
        tree = mockTree(TREE_PATH, false);
    }
    
    @After
    public void after() {
        reset(pe, tree);
    }

    @Test
    public void testCreateNullPath() {
        Predicate<PermissionEntry> predicate = EntryPredicate.create();
        predicate.apply(pe);

        verify(pe, times(1)).matches();
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreatePath() {
        Predicate<PermissionEntry> predicate = EntryPredicate.create("/path");
        predicate.apply(pe);

        verify(pe, times(1)).matches("/path");
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreatePathNotIsProperty() {
        Predicate<PermissionEntry> predicate = EntryPredicate.create("/path", false);
        predicate.apply(pe);

        verify(pe, times(1)).matches("/path", false);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreatePathIsProperty() {
        Predicate<PermissionEntry> predicate = EntryPredicate.create("/path", true);
        predicate.apply(pe);

        verify(pe, times(1)).matches("/path", true);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateNonExistingTree() {
        doReturn(false).when(tree).exists();
        Predicate<PermissionEntry> predicate = EntryPredicate.create(tree, null);
        predicate.apply(pe);

        verify(pe, times(1)).matches(tree.getPath(), false);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateNonExistingTreeProperty() {
        Predicate<PermissionEntry> predicate = EntryPredicate.create(tree, propertyState);
        predicate.apply(pe);

        verify(pe, times(1)).matches(PROP_PATH, true);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateExistingTree() {
        when(tree.exists()).thenReturn(true);

        Predicate<PermissionEntry> predicate = EntryPredicate.create(tree, null);
        predicate.apply(pe);

        verify(pe, times(1)).matches(tree, null);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateExistingTreeProperty() {
        when(tree.exists()).thenReturn(true);

        Predicate<PermissionEntry> predicate = EntryPredicate.create(tree, propertyState);
        predicate.apply(pe);

        verify(pe, times(1)).matches(tree, propertyState);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateParentPathReadPermission() {
        assertSame(Predicates.alwaysFalse(), EntryPredicate.createParent(tree.getPath(), null, Permissions.READ));
        assertSame(Predicates.alwaysFalse(), EntryPredicate.createParent(tree.getPath(), mock(Tree.class), Permissions.READ));
    }

    @Test
    public void testCreateParentPathEmpty() {
        assertSame(Predicates.alwaysFalse(), EntryPredicate.createParent("", null, Permissions.ALL));
        assertSame(Predicates.alwaysFalse(), EntryPredicate.createParent("", tree, Permissions.ALL));
    }

    @Test
    public void testCreateParentPathRoot() {
        assertSame(Predicates.alwaysFalse(), EntryPredicate.createParent(PathUtils.ROOT_PATH, tree, Permissions.ALL));
    }

    @Test
    public void testCreateParentPath() {
        when(pe.appliesTo(PARENT_PATH)).thenReturn(true);

        Predicate<PermissionEntry> predicate = EntryPredicate.createParent(TREE_PATH, null, Permissions.ALL);
        predicate.apply(pe);
        
        verify(pe, times(1)).appliesTo(PARENT_PATH);
        verify(pe, times(1)).matches(PARENT_PATH, false);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateParentPathTree() {
        when(pe.appliesTo(PARENT_PATH)).thenReturn(true);

        Tree parentTree = mockTree(PARENT_PATH, true);
        Predicate<PermissionEntry> predicate = EntryPredicate.createParent(TREE_PATH, parentTree, Permissions.ALL);
        predicate.apply(pe);

        verify(pe, times(1)).appliesTo(PARENT_PATH);
        verify(pe, times(1)).matches(parentTree, null);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateParentPathTreeNotExisting() {
        when(pe.appliesTo(PARENT_PATH)).thenReturn(true);

        Predicate<PermissionEntry> predicate = EntryPredicate.createParent(TREE_PATH, mockTree(PARENT_PATH, false), Permissions.ALL);
        predicate.apply(pe);

        verify(pe, times(1)).appliesTo(PARENT_PATH);
        verify(pe, times(1)).matches(PARENT_PATH, false);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateParentPathMismatch() {
        Predicate<PermissionEntry> predicate = EntryPredicate.createParent(TREE_PATH, null, Permissions.ALL);
        predicate.apply(pe);

        String parentPath = PathUtils.getParentPath(TREE_PATH);
        verify(pe, times(1)).appliesTo(parentPath);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateParentPathTreeMismatch() {
        Tree parentTree = mockTree(PARENT_PATH, true);

        Predicate<PermissionEntry> predicate = EntryPredicate.createParent(TREE_PATH, parentTree, Permissions.ALL);
        predicate.apply(pe);

        String parentPath = PathUtils.getParentPath(TREE_PATH);
        verify(pe, times(1)).appliesTo(parentPath);
        verifyNoMoreInteractions(pe); }

    @Test
    public void testCreateParentTreeReadPermission() {
        when(tree.exists()).thenReturn(true);

        assertSame(Predicates.alwaysFalse(), EntryPredicate.createParent(tree, Permissions.READ));
    }

    @Test
    public void testCreateParentTreeNotExisting() {
        when(pe.appliesTo(PARENT_PATH)).thenReturn(true);

        Predicate<PermissionEntry> predicate = EntryPredicate.createParent(tree, Permissions.ADD_NODE|Permissions.READ_NODE);
        predicate.apply(pe);
        
        verify(pe, times(1)).appliesTo(PARENT_PATH);
        verify(pe, times(1)).matches(PARENT_PATH, false);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateParentTreeNotExistingMismatch() {
        Predicate<PermissionEntry> predicate = EntryPredicate.createParent(tree, Permissions.ADD_NODE|Permissions.READ_NODE);
        predicate.apply(pe);

        verify(pe, times(1)).appliesTo(PARENT_PATH);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateParentTreeRoot() {
        Tree rootTree = mockTree(PathUtils.ROOT_PATH, true);
        assertSame(Predicates.alwaysFalse(), EntryPredicate.createParent(rootTree, Permissions.REMOVE|Permissions.MODIFY_ACCESS_CONTROL));
    }

    @Test
    public void testCreateParentTree() {
        when(pe.appliesTo(PARENT_PATH)).thenReturn(true);

        Tree parentTree = mockTree(PARENT_PATH, true);

        when(tree.exists()).thenReturn(true);
        when(tree.getParent()).thenReturn(parentTree);

        Predicate<PermissionEntry> predicate = EntryPredicate.createParent(tree, Permissions.REMOVE_NODE|Permissions.READ_PROPERTY|Permissions.LOCK_MANAGEMENT);
        predicate.apply(pe);

        verify(pe, times(1)).appliesTo(PARENT_PATH);
        verify(pe, times(1)).matches(parentTree, null);
        verifyNoMoreInteractions(pe);
    }

    @Test
    public void testCreateParentTreeMismatch() {
        Tree parentTree = mockTree(PARENT_PATH, true);

        when(tree.exists()).thenReturn(true);
        when(tree.getParent()).thenReturn(parentTree);

        Predicate<PermissionEntry> predicate = EntryPredicate.createParent(tree, Permissions.REMOVE_NODE|Permissions.READ_PROPERTY|Permissions.LOCK_MANAGEMENT);
        predicate.apply(pe);

        verify(pe, times(1)).appliesTo(PARENT_PATH);
        verifyNoMoreInteractions(pe);
    }
}